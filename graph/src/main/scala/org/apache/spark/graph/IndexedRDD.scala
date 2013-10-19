/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.spark.graph

import java.nio.ByteBuffer


import java.util.{HashMap => JHashMap, BitSet => JBitSet, HashSet => JHashSet}

import scala.collection.JavaConversions._
import scala.collection.mutable.ArrayBuffer
import scala.collection.mutable.BitSet


import org.apache.spark._
import org.apache.spark.rdd._
import org.apache.spark.SparkContext._
import org.apache.spark.Partitioner._

import org.apache.spark.storage.StorageLevel






/**
 * The BlockIndex is the internal map structure used inside the index 
 * of the IndexedRDD.
 */
class BlockIndex[@specialized K: ClassManifest] extends JHashMap[K,Int]


/**
 * The RDDIndex is an opaque type used to represent the organization 
 * of values in an RDD
 */
class RDDIndex[@specialized K: ClassManifest](private[spark] val rdd: RDD[BlockIndex[K]]) {
  def persist(newLevel: StorageLevel): RDDIndex[K] = {
    rdd.persist(newLevel)
    return this
  }

  def partitioner: Partitioner = rdd.partitioner.get
}



/**
 * An IndexedRDD[K,V] extends the RDD[(K,V)] by pre-indexing the keys and 
 * organizing the values to enable faster join operations.
 *
 * In addition to providing the basic RDD[(K,V)] functionality the IndexedRDD
 * exposes an index member which can be used to "key" other IndexedRDDs
 * 
 */
class IndexedRDD[K: ClassManifest, V: ClassManifest](
    @transient val index:  RDDIndex[K],
    @transient val valuesRDD: RDD[ (IndexedSeq[V], BitSet) ])
  extends RDD[(K, V)](index.rdd.context, 
    List(new OneToOneDependency(index.rdd), new OneToOneDependency(valuesRDD)) ) {


  /**
   * Construct a new IndexedRDD that is indexed by only the keys in the RDD
   */
   def reindex(): IndexedRDD[K,V] = IndexedRDD(this)


  /**
   * An internal representation which joins the block indices with the values
   */
  protected[spark] val tuples = 
    new ZippedRDD(index.rdd.context, index.rdd, valuesRDD)


  /**
   * The partitioner is defined by the index
   */
  override val partitioner = index.rdd.partitioner
  

  /**
   * The actual partitions are defined by the tuples.
   */
  override def getPartitions: Array[Partition] = tuples.getPartitions 
  

  /**
   * The preferred locations are computed based on the preferred locations of the tuples.
   */
  override def getPreferredLocations(s: Partition): Seq[String] = 
    tuples.getPreferredLocations(s)


  /**
   * Caching an IndexedRDD causes the index and values to be cached separately. 
   */
  override def persist(newLevel: StorageLevel): RDD[(K,V)] = {
    index.persist(newLevel)
    valuesRDD.persist(newLevel)
    return this
  }


  /**
   * Pass each value in the key-value pair RDD through a map function without changing the keys;
   * this also retains the original RDD's partitioning.
   */
  def mapValues[U: ClassManifest](f: V => U): IndexedRDD[K, U] = {
    val cleanF = index.rdd.context.clean(f)
    val newValuesRDD: RDD[ (IndexedSeq[U], BitSet) ] = 
    valuesRDD.mapPartitions(iter => iter.map{ 
      case (values, bs) => 
        val newValues = new Array[U](values.size)
        for ( ind <- bs ) {
          newValues(ind) = f(values(ind))
        }
        (newValues.toIndexedSeq, bs)
      }, preservesPartitioning = true)
    new IndexedRDD[K,U](index, newValuesRDD)
  }


  /**
   * Pass each value in the key-value pair RDD through a map function without changing the keys;
   * this also retains the original RDD's partitioning.
   */
  def mapValuesWithKeys[U: ClassManifest](f: (K, V) => U): IndexedRDD[K, U] = {
    val cleanF = index.rdd.context.clean(f)
    val newValues: RDD[ (IndexedSeq[U], BitSet) ] = 
      index.rdd.zipPartitions(valuesRDD){ 
      (keysIter, valuesIter) => 
      val index = keysIter.next()
      assert(keysIter.hasNext() == false)
      val (oldValues, bs) = valuesIter.next()
      assert(valuesIter.hasNext() == false)
       // Allocate the array to store the results into
      val newValues: Array[U] = new Array[U](oldValues.size)
      // Populate the new Values
      for( (k,i) <- index ) {
        if (bs(i)) { newValues(i) = f(k, oldValues(i)) }      
      }
      Array((newValues.toIndexedSeq, bs)).iterator
    }
    new IndexedRDD[K,U](index, newValues)
  }


  def zipJoin[W: ClassManifest](other: IndexedRDD[K,W]): IndexedRDD[K,(V,W)] = {
    if(index != other.index) {
      throw new SparkException("A zipJoin can only be applied to RDDs with the same index!")
    }
    val newValuesRDD: RDD[ (IndexedSeq[(V,W)], BitSet) ] = valuesRDD.zipPartitions(other.valuesRDD){
      (thisIter, otherIter) => 
      val (thisValues, thisBS) = thisIter.next()
      assert(!thisIter.hasNext)
      val (otherValues, otherBS) = otherIter.next()
      assert(!otherIter.hasNext)
      val newBS = thisBS & otherBS
      val newValues = thisValues.view.zip(otherValues)
      Iterator((newValues.toIndexedSeq, newBS))
    }
    new IndexedRDD(index, newValuesRDD)
  }


  def leftZipJoin[W: ClassManifest](other: IndexedRDD[K,W]): IndexedRDD[K,(V,Option[W])] = {
    if(index != other.index) {
      throw new SparkException("A zipJoin can only be applied to RDDs with the same index!")
    }
    val newValuesRDD: RDD[ (IndexedSeq[(V,Option[W])], BitSet) ] = valuesRDD.zipPartitions(other.valuesRDD){
      (thisIter, otherIter) => 
      val (thisValues, thisBS) = thisIter.next()
      assert(!thisIter.hasNext)
      val (otherValues, otherBS) = otherIter.next()
      assert(!otherIter.hasNext)
      val otherOption = otherValues.view.zipWithIndex
        .map{ (x: (W, Int)) => if(otherBS(x._2)) Option(x._1) else None }
      val newValues = thisValues.view.zip(otherOption)
      Iterator((newValues.toIndexedSeq, thisBS))
    }
    new IndexedRDD(index, newValuesRDD)
  }



  def leftJoin[W: ClassManifest](
    other: RDD[(K,W)], merge: (W,W) => W = (a:W, b:W) => a):
    IndexedRDD[K, (V, Option[W]) ] = {
    val cleanMerge = index.rdd.context.clean(merge)

    other match {
      case other: IndexedRDD[_, _] if index == other.index => {
        leftZipJoin(other)
      }    
      case _ => {
        // Get the partitioner from the index
        val partitioner = index.rdd.partitioner match {
          case Some(p) => p
          case None => throw new SparkException("An index must have a partitioner.")
        }
        // Shuffle the other RDD using the partitioner for this index
        val otherShuffled = 
          if (other.partitioner == Some(partitioner)) other 
          else other.partitionBy(partitioner)
        val newValues: RDD[ (IndexedSeq[(V,Option[W])], BitSet) ] = 
          index.rdd.zipPartitions(valuesRDD, other) {
          (thisIndexIter, thisIter, tuplesIter) =>
          val index = thisIndexIter.next()
          assert(!thisIndexIter.hasNext)
          val (thisValues, thisBS) = thisIter.next()
          assert(!thisIter.hasNext)
          val newW = new Array[W](thisValues.size)
          // track which values are matched with values in other
          val wBS = new BitSet(thisValues.size)
          for( (k, w) <- tuplesIter if index.contains(k) ) {
            val ind = index.get(k)
            if(thisBS(ind)) {
              if(wBS(ind)) {
                newW(ind) = cleanMerge(newW(ind), w) 
              } else {
                newW(ind) = w
                wBS(ind) = true
              }
            }
          }

          val otherOption = newW.view.zipWithIndex
            .map{ (x: (W, Int)) => if(wBS(x._2)) Option(x._1) else None }
          val newValues = thisValues.view.zip(otherOption)

          Iterator((newValues.toIndexedSeq, thisBS))
        } // end of newValues
        new IndexedRDD(index, newValues) 
      }
    }
  }


/**
   * For each key k in `this` or `other`, return a resulting RDD that contains a tuple with the
   * list of values for that key in `this` as well as `other`.
   */
  def cogroup[W: ClassManifest](other: RDD[(K, W)], partitioner: Partitioner): 
  IndexedRDD[K, (Seq[V], Seq[W])] = {
    //RDD[(K, (Seq[V], Seq[W]))] = {
    other match {
      case other: IndexedRDD[_, _] if index == other.index => {
        // if both RDDs share exactly the same index and therefore the same super set of keys
        // then we simply merge the value RDDs. 
        // However it is possible that both RDDs are missing a value for a given key in 
        // which case the returned RDD should have a null value
        val newValues: RDD[(IndexedSeq[(Seq[V], Seq[W])], BitSet)] = 
          valuesRDD.zipPartitions(other.valuesRDD){
          (thisIter, otherIter) => 
            val (thisValues, thisBS) = thisIter.next()
            assert(!thisIter.hasNext)
            val (otherValues, otherBS) = otherIter.next()
            assert(!otherIter.hasNext)

            val newValues = new Array[(Seq[V], Seq[W])](thisValues.size)
            val newBS = thisBS | otherBS

            for( ind <- newBS ) {
              val a = if (thisBS(ind)) Seq(thisValues(ind)) else Seq.empty[V]
              val b = if (otherBS(ind)) Seq(otherValues(ind)) else Seq.empty[W]
              newValues(ind) = (a, b)
            }
            Iterator((newValues.toIndexedSeq, newBS))
        }
        new IndexedRDD(index, newValues) 
      }
      case other: IndexedRDD[_, _] 
        if index.rdd.partitioner == other.index.rdd.partitioner => {
        // If both RDDs are indexed using different indices but with the same partitioners
        // then we we need to first merge the indicies and then use the merged index to
        // merge the values.
        val newIndex = 
          index.rdd.zipPartitions(other.index.rdd)(
            (thisIter, otherIter) => {
            val thisIndex = thisIter.next()
            assert(!thisIter.hasNext)
            val otherIndex = otherIter.next()
            assert(!otherIter.hasNext)
            val newIndex = new BlockIndex[K]()
            // @todo Merge only the keys that correspond to non-null values
            // Merge the keys
            newIndex.putAll(thisIndex)
            newIndex.putAll(otherIndex)
            // We need to rekey the index
            var ctr = 0
            for (e <- newIndex.entrySet) {
              e.setValue(ctr)
              ctr += 1
            }
            List(newIndex).iterator
          }).cache()
        // Use the new index along with the this and the other indices to merge the values
        val newValues: RDD[(IndexedSeq[(Seq[V], Seq[W])], BitSet)] = 
          newIndex.zipPartitions(tuples, other.tuples)(
            (newIndexIter, thisTuplesIter, otherTuplesIter) => {
              // Get the new index for this partition
              val newIndex = newIndexIter.next()
              assert(!newIndexIter.hasNext)
              // Get the corresponding indicies and values for this and the other IndexedRDD
              val (thisIndex, (thisValues, thisBS)) = thisTuplesIter.next()
              assert(!thisTuplesIter.hasNext)
              val (otherIndex, (otherValues, otherBS)) = otherTuplesIter.next()
              assert(!otherTuplesIter.hasNext)
              // Preallocate the new Values array
              val newValues = new Array[(Seq[V], Seq[W])](newIndex.size)
              val newBS = new BitSet(newIndex.size)

              // Lookup the sequences in both submaps
              for ((k,ind) <- newIndex) {
                // Get the left key
                val a = if (thisIndex.contains(k)) {
                  val ind = thisIndex.get(k)
                  if(thisBS(ind)) Seq(thisValues(ind)) else Seq.empty[V]
                } else Seq.empty[V]
                // Get the right key
                val b = if (otherIndex.contains(k)) {
                  val ind = otherIndex.get(k)
                  if (otherBS(ind)) Seq(otherValues(ind)) else Seq.empty[W]
                } else Seq.empty[W]
                // If at least one key was present then we generate a tuple.
                if (!a.isEmpty || !b.isEmpty) {
                  newValues(ind) = (a, b)
                  newBS(ind) = true                  
                }
              }
              Iterator((newValues.toIndexedSeq, newBS))
            })
        new IndexedRDD(new RDDIndex(newIndex), newValues)
      }
      case _ => {
        // Get the partitioner from the index
        val partitioner = index.rdd.partitioner match {
          case Some(p) => p
          case None => throw new SparkException("An index must have a partitioner.")
        }
        // Shuffle the other RDD using the partitioner for this index
        val otherShuffled = 
          if (other.partitioner == Some(partitioner)) {
            other
          } else {
            new ShuffledRDD[K, W, (K,W)](other, partitioner)
          }
        // Join the other RDD with this RDD building a new valueset and new index on the fly
        val groups = tuples.zipPartitions(otherShuffled)(
          (thisTuplesIter, otherTuplesIter) => {
            // Get the corresponding indicies and values for this IndexedRDD
            val (thisIndex, (thisValues, thisBS)) = thisTuplesIter.next()
            assert(!thisTuplesIter.hasNext())
            // Construct a new index
            val newIndex = thisIndex.clone().asInstanceOf[BlockIndex[K]]
            // Construct a new array Buffer to store the values
            val newValues = ArrayBuffer.fill[ (Seq[V], Seq[W]) ](thisValues.size)(null)
            val newBS = new BitSet(thisValues.size)
            // populate the newValues with the values in this IndexedRDD
            for ((k,i) <- thisIndex) {
              if (thisBS(i)) {
                newValues(i) = (Seq(thisValues(i)), ArrayBuffer.empty[W]) 
                newBS(i) = true
              }
            }
            // Now iterate through the other tuples updating the map
            for ((k,w) <- otherTuplesIter){
              if (newIndex.contains(k)) {
                val ind = newIndex.get(k)
                if(newBS(ind)) {
                  newValues(ind)._2.asInstanceOf[ArrayBuffer[W]].append(w)
                } else {
                  // If the other key was in the index but not in the values 
                  // of this indexed RDD then create a new values entry for it 
                  newBS(ind) = true
                  newValues(ind) = (Seq.empty[V], ArrayBuffer(w))
                }              
              } else {
                // update the index
                val ind = newIndex.size
                newIndex.put(k, ind)
                newBS(ind) = true
                // Update the values
                newValues.append( (Seq.empty[V], ArrayBuffer(w) ) ) 
              }
            }
            // // Finalize the new values array
            // val newValuesArray: Seq[Seq[(Seq[V],Seq[W])]] = 
            //   newValues.view.map{ 
            //     case null => null
            //     case (s, ab) => Seq((s, ab.toSeq)) 
            //     }.toSeq 
            Iterator( (newIndex, (newValues.toIndexedSeq, newBS)) )
          }).cache()

        // Extract the index and values from the above RDD  
        val newIndex = groups.mapPartitions(_.map{ case (kMap,vAr) => kMap }, true)
        val newValues: RDD[(IndexedSeq[(Seq[V], Seq[W])], BitSet)] = 
          groups.mapPartitions(_.map{ case (kMap,vAr) => vAr }, true)
          
        new IndexedRDD[K, (Seq[V], Seq[W])](new RDDIndex(newIndex), newValues)
      }
    }
  }


  // 
  // def zipJoinToRDD[W: ClassManifest](other: IndexedRDD[K,W]): RDD[(K,(V,W))] = {
  //   if(index != other.index) {
  //     throw new SparkException("ZipJoinRDD can only be applied to RDDs with the same index!")
  //   }
  //   index.rdd.zipPartitions(valuesRDD, other.valuesRDD){
  //     (thisIndexIter, thisIter, otherIter) => 
  //     val index = thisIndexIter.next()
  //     assert(!thisIndexIter.hasNext)
  //     val (thisValues, thisBS) = thisIter.next()
  //     assert(!thisIter.hasNext)
  //     val (otherValues, otherBS) = otherIter.next()
  //     assert(!otherIter.hasNext)
  //     val newBS = thisBS & otherBS
  //     index.iterator.filter{ case (k,i) => newBS(i) }.map{ 
  //       case (k,i) => (k, (thisValues(i), otherValues(i)))
  //     }
  //   }
  // }


/*  This is probably useful but we are not using it
  def zipJoinWithKeys[W: ClassManifest, Z: ClassManifest](
    other: RDD[(K,W)])(
    f: (K, V, W) => Z, 
    merge: (Z,Z) => Z = (a:Z, b:Z) => a):
    IndexedRDD[K,Z] = {
    val cleanF = index.rdd.context.clean(f)
    val cleanMerge = index.rdd.context.clean(merge)
    other match {
      case other: IndexedRDD[_, _] if index == other.index => {
        val newValues = index.rdd.zipPartitions(valuesRDD, other.valuesRDD){
          (thisIndexIter, thisIter, otherIter) => 
          val index = thisIndexIter.next()
          assert(!thisIndexIter.hasNext)
          val (thisValues, thisBS) = thisIter.next()
          assert(!thisIter.hasNext)
          val (otherValues, otherBS) = otherIter.next()
          assert(!otherIter.hasNext)
          val newValues = new Array[Z](thisValues.size)
          val newBS = thisBS & otherBS
          for( (k,i) <- index ) {
            if (newBS(i)) { 
              newValues(i) = cleanF(k, thisValues(i), otherValues(i))
            }       
          }
          List((newValues, newBS)).iterator
        }
        new IndexedRDD(index, newValues) 
      }
    
      case _ => {
        // Get the partitioner from the index
        val partitioner = index.rdd.partitioner match {
          case Some(p) => p
          case None => throw new SparkException("An index must have a partitioner.")
        }
        // Shuffle the other RDD using the partitioner for this index
        val otherShuffled = 
          if (other.partitioner == Some(partitioner)) other 
          else other.partitionBy(partitioner)

        val newValues = index.rdd.zipPartitions(valuesRDD, other) {
          (thisIndexIter, thisIter, tuplesIter) =>
          val index = thisIndexIter.next()
          assert(!thisIndexIter.hasNext)
          val (thisValues, thisBS) = thisIter.next()
          assert(!thisIter.hasNext)

          val newValues = new Array[Z](thisValues.size)
          // track which values are matched with values in other
          val tempBS = new BitSet(thisValues.size)

          for( (k, w) <- tuplesIter if index.contains(k) ) {
            val ind = index.get(k)
            if(thisBS(ind)) {
              val result = cleanF(k, thisValues(ind), w)
              if(tempBS(ind)) {
                newValues(ind) = cleanMerge(newValues(ind), result) 
              } else {
                newValues(ind) = result
                tempBS(ind) = true
              }
            }
          } 
          List((newValues, tempBS)).iterator
        } // end of newValues
        new IndexedRDD(index, newValues) 
      }
    }
  }
*/

/*
  def zipJoinLeftWithKeys[W: ClassManifest, Z: ClassManifest](
    other: RDD[(K,W)])(
    f: (K, V, Option[W]) => Z, 
    merge: (Z,Z) => Z = (a:Z, b:Z) => a):
    IndexedRDD[K,Z] = {
    val cleanF = index.rdd.context.clean(f)
    val cleanMerge = index.rdd.context.clean(merge)
    other match {
      case other: IndexedRDD[_, _] if index == other.index => {
        val newValues = index.rdd.zipPartitions(valuesRDD, other.valuesRDD){
          (thisIndexIter, thisIter, otherIter) => 
          val index = thisIndexIter.next()
          assert(!thisIndexIter.hasNext)
          val (thisValues, thisBS) = thisIter.next()
          assert(!thisIter.hasNext)
          val (otherValues, otherBS) = otherIter.next()
          assert(!otherIter.hasNext)
          val newValues = new Array[Z](thisValues.size)
          for( (k,i) <- index ) {
            if (thisBS(i)) { 
              val otherVal = if(otherBS(i)) Some(otherValues(i)) else None
              newValues(i) = cleanF(k, thisValues(i), otherVal)
            }       
          }
          List((newValues, thisBS)).iterator
        }
        new IndexedRDD(index, newValues) 
      }
    
      case _ => {
        // Get the partitioner from the index
        val partitioner = index.rdd.partitioner match {
          case Some(p) => p
          case None => throw new SparkException("An index must have a partitioner.")
        }
        // Shuffle the other RDD using the partitioner for this index
        val otherShuffled = 
          if (other.partitioner == Some(partitioner)) other 
          else other.partitionBy(partitioner)
        val newValues = index.rdd.zipPartitions(valuesRDD, other) {
          (thisIndexIter, thisIter, tuplesIter) =>
          val index = thisIndexIter.next()
          assert(!thisIndexIter.hasNext)
          val (thisValues, thisBS) = thisIter.next()
          assert(!thisIter.hasNext)

          val newValues = new Array[Z](thisValues.size)
          // track which values are matched with values in other
          val tempBS = new BitSet(thisValues.size)

          for( (k, w) <- tuplesIter if index.contains(k) ) {
            val ind = index.get(k)
            if(thisBS(ind)) {
              val result = cleanF(k, thisValues(ind), Option(w))
              if(tempBS(ind)) {
                newValues(ind) = cleanMerge(newValues(ind), result) 
              } else {
                newValues(ind) = result
                tempBS(ind) = true
              }
            }
          } 

          // Process remaining keys in lef join
          for( (k,ind) <- index if thisBS(ind) && !tempBS(ind)) {
            newValues(ind) = cleanF(k, thisValues(ind), None)
          }
          List((newValues, thisBS)).iterator
        } // end of newValues
        new IndexedRDD(index, newValues) 
      }
    }
  }

*/



  override def filter(f: Tuple2[K,V] => Boolean): RDD[(K,V)] = {
    val cleanF = index.rdd.context.clean(f)
    val newValues = index.rdd.zipPartitions(valuesRDD){ 
      (keysIter, valuesIter) => 
      val index = keysIter.next()
      assert(keysIter.hasNext() == false)
      val (oldValues, bs) = valuesIter.next()
      assert(valuesIter.hasNext() == false)
      // Allocate the array to store the results into
      val newBS = new BitSet(oldValues.size)
      // Populate the new Values
      for( (k,i) <- index ) {
        newBS(i) = bs(i) && cleanF( (k, oldValues(i)) ) 
      }
      Array((oldValues, newBS)).iterator
    }
    new IndexedRDD[K,V](index, newValues)
  }


  /**
   * Provide the RDD[(K,V)] equivalent output. 
   */
  override def compute(part: Partition, context: TaskContext): Iterator[(K, V)] = {
    tuples.compute(part, context).flatMap { case (indexMap, (values, bs) ) => 
      // Walk the index to construct the key, value pairs
      indexMap.iterator 
        // Extract rows with key value pairs and indicators
        .map{ case (k, ind) => (bs(ind), k, ind)  }
        // Remove tuples that aren't actually present in the array
        .filter( _._1 )
        // Extract the pair (removing the indicator from the tuple)
        .map( x => (x._2, values(x._3) ) )
    }
  }

} // End of IndexedRDD




object IndexedRDD {


  def apply[K: ClassManifest, V: ClassManifest](rdd: RDD[(K,V)]): IndexedRDD[K,V] = 
    apply(rdd, (a:V, b:V) => a )

  def apply[K: ClassManifest, V: ClassManifest](
    rdd: RDD[(K,V)], reduceFunc: (V, V) => V): IndexedRDD[K,V] = {
    // Preaggregate and shuffle if necessary
    // Preaggregation.
    val aggregator = new Aggregator[K, V, V](v => v, reduceFunc, reduceFunc)
    val partitioner = new HashPartitioner(rdd.partitions.size)
    val preAgg = rdd.mapPartitions(aggregator.combineValuesByKey).partitionBy(partitioner)

    val groups = preAgg.mapPartitions( iter => {
      val indexMap = new BlockIndex[K]()
      val values = new ArrayBuffer[V]
      val bs = new BitSet
      for ((k,v) <- iter) {
        if(!indexMap.contains(k)) {
          val ind = indexMap.size
          indexMap.put(k, ind)
          values.append(v)
          bs(ind) = true
        } else {
          val ind = indexMap.get(k)
          values(ind) = reduceFunc(values(ind), v)
        }
      }
      Iterator( (indexMap, (values.toIndexedSeq, bs)) )
      }, true).cache
    // extract the index and the values
    val index = groups.mapPartitions(_.map{ case (kMap, vAr) => kMap }, true)
    val values: RDD[(IndexedSeq[V], BitSet)] = 
      groups.mapPartitions(_.map{ case (kMap,vAr) => vAr }, true)
    new IndexedRDD[K,V](new RDDIndex(index), values)
  }



  def apply[K: ClassManifest, V: ClassManifest](
    rdd: RDD[(K,V)], index: RDDIndex[K]): IndexedRDD[K,V] = 
    apply(rdd, index, (a:V,b:V) => a)


  def apply[K: ClassManifest, V: ClassManifest](
    rdd: RDD[(K,V)], index: RDDIndex[K], 
    reduceFunc: (V, V) => V): IndexedRDD[K,V] = 
    apply(rdd,index, (v:V) => v, reduceFunc, reduceFunc)
  // {
  //   // Get the index Partitioner
  //   val partitioner = index.rdd.partitioner match {
  //     case Some(p) => p
  //     case None => throw new SparkException("An index must have a partitioner.")
  //   }
  //   // Preaggregate and shuffle if necessary
  //   val partitioned = 
  //     if (rdd.partitioner != Some(partitioner)) {
  //       // Preaggregation.
  //       val aggregator = new Aggregator[K, V, V](v => v, reduceFunc, reduceFunc)
  //       rdd.mapPartitions(aggregator.combineValuesByKey).partitionBy(partitioner)
  //     } else {
  //       rdd
  //     }

  //   // Use the index to build the new values table
  //   val values = index.rdd.zipPartitions(partitioned)( (indexIter, tblIter) => {
  //     // There is only one map
  //     val index = indexIter.next()
  //     assert(!indexIter.hasNext())
  //     val values = new Array[V](index.size)
  //     val bs = new BitSet(index.size)
  //     for ((k,v) <- tblIter) {
  //       if (!index.contains(k)) {
  //         throw new SparkException("Error: Trying to bind an external index " +
  //           "to an RDD which contains keys that are not in the index.")
  //       }
  //       val ind = index(k)
  //       if (bs(ind)) { 
  //         values(ind) = reduceFunc(values(ind), v) 
  //       } else {
  //         values(ind) = v
  //         bs(ind) = true
  //       }
  //     }
  //     List((values, bs)).iterator
  //   })
  //   new IndexedRDD[K,V](index, values)
  // } // end of apply


  def apply[K: ClassManifest, V: ClassManifest, C: ClassManifest](
    rdd: RDD[(K,V)], 
    index: RDDIndex[K],
    createCombiner: V => C,
    mergeValue: (C, V) => C,
    mergeCombiners: (C, C) => C): IndexedRDD[K,C] = {
    // Get the index Partitioner
    val partitioner = index.rdd.partitioner match {
      case Some(p) => p
      case None => throw new SparkException("An index must have a partitioner.")
    }
    // Preaggregate and shuffle if necessary
    val partitioned = 
      if (rdd.partitioner != Some(partitioner)) {
        // Preaggregation.
        val aggregator = new Aggregator[K, V, C](createCombiner, mergeValue, 
          mergeCombiners)
        rdd.mapPartitions(aggregator.combineValuesByKey).partitionBy(partitioner)
      } else {
        rdd.mapValues(x => createCombiner(x))
      }

    // Use the index to build the new values table
    val values: RDD[ (IndexedSeq[C], BitSet) ] = index.rdd.zipPartitions(partitioned)( (indexIter, tblIter) => {
      // There is only one map
      val index = indexIter.next()
      assert(!indexIter.hasNext())
      val values = new Array[C](index.size)
      val bs = new BitSet(index.size)
      for ((k,c) <- tblIter) {
        if (!index.contains(k)) {
          throw new SparkException("Error: Trying to bind an external index " +
            "to an RDD which contains keys that are not in the index.")
        }
        val ind = index(k)
        if (bs(ind)) { 
          values(ind) = mergeCombiners(values(ind), c) 
        } else {
          values(ind) = c
          bs(ind) = true
        }
      }
      Iterator((values, bs))
    })
    new IndexedRDD(index, values)
  } // end of apply


  /**
   * Construct and index of the unique values in a given RDD.
   */
  def makeIndex[K: ClassManifest](keys: RDD[K], 
    partitioner: Option[Partitioner] = None): RDDIndex[K] = {
    // @todo: I don't need the boolean its only there to be the second type since I want to shuffle a single RDD
    // Ugly hack :-(.  In order to partition the keys they must have values. 
    val tbl = keys.mapPartitions(_.map(k => (k, false)), true)
    // Shuffle the table (if necessary)
    val shuffledTbl = partitioner match {
      case None =>  {
        if (tbl.partitioner.isEmpty) {
          // @todo: I don't need the boolean its only there to be the second type of the shuffle. 
          new ShuffledRDD[K, Boolean, (K, Boolean)](tbl, Partitioner.defaultPartitioner(tbl))
        } else { tbl }
      }
      case Some(partitioner) => 
        tbl.partitionBy(partitioner)
    }

    val index = shuffledTbl.mapPartitions( iter => {
      val indexMap = new BlockIndex[K]()
      for ( (k,_) <- iter ){
        if(!indexMap.contains(k)){
          val ind = indexMap.size
          indexMap.put(k, ind)   
        }
      }
      Iterator(indexMap)
      }, true).cache
    new RDDIndex(index)
  }

} // end of object IndexedRDD





