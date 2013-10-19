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
 * The `VertexSetIndex` maintains the per-partition mapping from vertex id
 * to the corresponding location in the per-partition values array. 
 * This class is meant to be an opaque type. 
 * 
 */
class VertexSetIndex(private[spark] val rdd: RDD[VertexIdToIndexMap]) {
  /**
   * The persist function behaves like the standard RDD persist
   */
  def persist(newLevel: StorageLevel): VertexSetIndex = {
    rdd.persist(newLevel)
    return this
  }

  /**
   * Returns the partitioner object of the underlying RDD.  This is used
   * by the VertexSetRDD to partition the values RDD.
   */
  def partitioner: Partitioner = rdd.partitioner.get
} // end of VertexSetIndex



/**
 * An VertexSetRDD[V] extends the RDD[(Vid,V)] by ensuring that there is only 
 * one entry for each vertex and by pre-indexing the entries for fast, efficient
 * joins.
 *
 * In addition to providing the basic RDD[(Vid,V)] functionality the VertexSetRDD
 * exposes an index member which can be used to "key" other VertexSetRDDs
 * 
 * @tparam V the vertex attribute associated with each vertex in the set.
 *
 * @param index the index which contains the vertex id information and is used
 * to organize the values in the RDD.
 * @param valuesRDD the values RDD contains the actual vertex attributes organized
 * as an array within each partition.  
 *
 * To construct a `VertexSetRDD` use the singleton object:
 *
 * @example Construct a `VertexSetRDD` from a plain RDD
 * {{{
 * // Construct an intial vertex set
 * val someData: RDD[(Vid, SomeType)] = loadData(someFile)
 * val vset = VertexSetRDD(someData)
 * // If there were redundant values in someData we would use a reduceFunc
 * val vset2 = VertexSetRDD(someData, reduceFunc)
 * // Finally we can use the VertexSetRDD to index another dataset
 * val otherData: RDD[(Vid, OtherType)] = loadData(otherFile)
 * val vset3 = VertexSetRDD(otherData, vset.index)
 * // Now we can construct very fast joins between the two sets
 * val vset4: VertexSetRDD[(SomeType, OtherType)] = vset.leftJoin(vset3)
 * }}}
 *
 */
class VertexSetRDD[@specialized V: ClassManifest](
    @transient val index:  VertexSetIndex,
    @transient val valuesRDD: RDD[ (IndexedSeq[V], BitSet) ])
  extends RDD[(Vid, V)](index.rdd.context, 
    List(new OneToOneDependency(index.rdd), new OneToOneDependency(valuesRDD)) ) {


  /**
   * Construct a new VertexSetRDD that is indexed by only the keys in the RDD.
   * The resulting VertexSet will be based on a different index and can
   * no longer be quickly joined with this RDD.
   */
   def reindex(): VertexSetRDD[V] = VertexSetRDD(this)


  /**
   * An internal representation which joins the block indices with the values
   * This is used by the compute function to emulate RDD[(Vid, V)]
   */
  protected[spark] val tuples = 
    new ZippedRDD(index.rdd.context, index.rdd, valuesRDD)


  /**
   * The partitioner is defined by the index.  
   */
  override val partitioner = index.rdd.partitioner
  

  /**
   * The actual partitions are defined by the tuples.
   */
  override def getPartitions: Array[Partition] = tuples.getPartitions 
  

  /**
   * The preferred locations are computed based on the preferred 
   * locations of the tuples. 
   */
  override def getPreferredLocations(s: Partition): Seq[String] = 
    tuples.getPreferredLocations(s)


  /**
   * Caching an VertexSetRDD causes the index and values to be cached separately. 
   */
  override def persist(newLevel: StorageLevel): VertexSetRDD[V] = {
    index.persist(newLevel)
    valuesRDD.persist(newLevel)
    return this
  }


  /** Persist this RDD with the default storage level (`MEMORY_ONLY`). */
  override def persist(): VertexSetRDD[V] = persist(StorageLevel.MEMORY_ONLY)


  /** Persist this RDD with the default storage level (`MEMORY_ONLY`). */
  override def cache(): VertexSetRDD[V] = persist()


  /**
   * Provide the RDD[(K,V)] equivalent output. 
   */
  override def compute(part: Partition, context: TaskContext): Iterator[(Vid, V)] = {
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
  } // end of compute


  /**
   * @todo finish documenting
   */
  override def filter(f: Tuple2[Vid,V] => Boolean): VertexSetRDD[V] = {
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
    new VertexSetRDD[V](index, newValues)
  } // end of filter


  /**
   * Pass each vertex attribute through a map function and retain 
   * the original RDD's partitioning and index.
   * 
   * @tparam U the type returned by the map function
   *
   * @param f the function applied to each value in the RDD
   * @return a new VertexSet with values obtaind by applying `f` to each of the
   * entries in the original VertexSet.  The resulting VertexSetRDD retains the
   * same index.
   */
  def mapValues[U: ClassManifest](f: V => U): VertexSetRDD[U] = {
    val cleanF = index.rdd.context.clean(f)
    val newValuesRDD: RDD[ (IndexedSeq[U], BitSet) ] = 
      valuesRDD.mapPartitions(iter => iter.map{ 
        case (values, bs) => 
          /** 
           * @todo Consider using a view rather than creating a new array. 
           * This is already being done for join operations.  It could reduce
           * memory overhead but require additional recomputation.  
           */
          val newValues = new Array[U](values.size)
          for ( ind <- bs ) {
            newValues(ind) = f(values(ind))
          }
          (newValues.toIndexedSeq, bs)
          }, preservesPartitioning = true)   
    new VertexSetRDD[U](index, newValuesRDD)
  } // end of mapValues


  /**
   * Pass each vertex attribute along with the vertex id through a 
   * map function and retain the original RDD's partitioning and index.
   * 
   * @tparam U the type returned by the map function
   *
   * @param f the function applied to each vertex id and vertex 
   * attribute in the RDD
   * @return a new VertexSet with values obtaind by applying `f` to each of the
   * entries in the original VertexSet.  The resulting VertexSetRDD retains the
   * same index.
   */
  def mapValuesWithKeys[U: ClassManifest](f: (Vid, V) => U): VertexSetRDD[U] = {
    val cleanF = index.rdd.context.clean(f)
    val newValues: RDD[ (IndexedSeq[U], BitSet) ] = 
      index.rdd.zipPartitions(valuesRDD){ 
        (keysIter, valuesIter) => 
        val index = keysIter.next()
        assert(keysIter.hasNext() == false)
        val (oldValues, bs) = valuesIter.next()
        assert(valuesIter.hasNext() == false)
        /** 
         * @todo Consider using a view rather than creating a new array. 
         * This is already being done for join operations.  It could reduce
         * memory overhead but require additional recomputation.  
         */
        // Allocate the array to store the results into
        val newValues: Array[U] = new Array[U](oldValues.size)
        // Populate the new Values
        for( (k,i) <- index ) {
          if (bs(i)) { newValues(i) = f(k, oldValues(i)) }      
        }
        Array((newValues.toIndexedSeq, bs)).iterator
      }
    new VertexSetRDD[U](index, newValues)
  } // end of mapValuesWithKeys


  /**
   * Inner join this VertexSet with another VertexSet which has the same Index.  
   * This function will fail if both VertexSets do not share the same index.  
   * The resulting vertex set will only contain vertices that are in both this
   * and the other vertex set.   
   *
   * @tparam W the attribute type of the other VertexSet
   * 
   * @param other the other VertexSet with which to join.  
   * @return a VertexSetRDD containing only the vertices in both this and the
   * other VertexSet and with tuple attributes.
   *
   */
  def zipJoin[W: ClassManifest](other: VertexSetRDD[W]): VertexSetRDD[(V,W)] = {
    if(index != other.index) {
      throw new SparkException("A zipJoin can only be applied to RDDs with the same index!")
    }
    val newValuesRDD: RDD[ (IndexedSeq[(V,W)], BitSet) ] = 
      valuesRDD.zipPartitions(other.valuesRDD){
        (thisIter, otherIter) => 
        val (thisValues, thisBS) = thisIter.next()
        assert(!thisIter.hasNext)
        val (otherValues, otherBS) = otherIter.next()
        assert(!otherIter.hasNext)
        val newBS = thisBS & otherBS
        val newValues = thisValues.view.zip(otherValues)
        Iterator((newValues.toIndexedSeq, newBS))
      }
    new VertexSetRDD(index, newValuesRDD)
  }


  /**
   * Left join this VertexSet with another VertexSet which has the same Index.  
   * This function will fail if both VertexSets do not share the same index.  
   * The resulting vertex set contains an entry for each vertex in this set.
   * If the other VertexSet is missing any vertex in this VertexSet then a
   * `None` attribute is generated  
   *
   * @tparam W the attribute type of the other VertexSet
   * 
   * @param other the other VertexSet with which to join.  
   * @return a VertexSetRDD containing all the vertices in this VertexSet
   * with `None` attributes used for Vertices missing in the other VertexSet.
   *
   */
  def leftZipJoin[W: ClassManifest](other: VertexSetRDD[W]): VertexSetRDD[(V,Option[W])] = {
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
    new VertexSetRDD(index, newValuesRDD)
  } // end of leftZipJoin


  /**
   * Left join this VertexSet with an RDD containing vertex attribute pairs.  
   * If the other RDD is backed by a VertexSet with the same index than the 
   * efficient leftZipJoin implementation is used.
   * The resulting vertex set contains an entry for each vertex in this set.
   * If the other VertexSet is missing any vertex in this VertexSet then a
   * `None` attribute is generated  
   *
   * @tparam W the attribute type of the other VertexSet
   * 
   * @param other the other VertexSet with which to join.
   * @param merge the function used combine duplicate vertex attributes   
   * @return a VertexSetRDD containing all the vertices in this VertexSet
   * with `None` attributes used for Vertices missing in the other VertexSet.
   *
   */
  def leftJoin[W: ClassManifest](
    other: RDD[(Vid,W)], merge: (W,W) => W = (a:W, b:W) => a):
    VertexSetRDD[(V, Option[W]) ] = {
    val cleanMerge = index.rdd.context.clean(merge)
    // Test if the other vertex is a VertexSetRDD to choose the optimal
    // join strategy
    other match {
      // If the other set is a VertexSetRDD and shares the same index as
      // this vertex set then we use the much more efficient leftZipJoin
      case other: VertexSetRDD[_] if index == other.index => {
        leftZipJoin(other)
      }    
      case _ => {
        // Otherwise we treat the other RDD as a collectiong of 
        // vertex-attribute pairs.  
        // If necessary shuffle the other RDD using the partitioner 
        // for this VertexSet
        val otherShuffled = 
          if (other.partitioner == partitioner) other 
          else other.partitionBy(partitioner.get)
        // Compute the new values RDD
        val newValues: RDD[ (IndexedSeq[(V,Option[W])], BitSet) ] = 
          index.rdd.zipPartitions(valuesRDD, otherShuffled) {
          (thisIndexIter, thisIter, tuplesIter) =>
          // Get the Index and values for this RDD
          val index = thisIndexIter.next()
          assert(!thisIndexIter.hasNext)
          val (thisValues, thisBS) = thisIter.next()
          assert(!thisIter.hasNext)
          // Create a new array to store the values in the resulting VertexSet
          val newW = new Array[W](thisValues.size)
          // track which values are matched with values in other
          val wBS = new BitSet(thisValues.size)
          // Loop over all the tuples that have vertices in this VertexSet.  
          for( (k, w) <- tuplesIter if index.contains(k) ) {
            val ind = index.get(k)
            // Not all the vertex ids in the index are in this VertexSet. 
            // If there is a vertex in this set then record the other value
            if(thisBS(ind)) {
              if(wBS(ind)) {
                newW(ind) = cleanMerge(newW(ind), w) 
              } else {
                newW(ind) = w
                wBS(ind) = true
              }
            }
          } // end of for loop over tuples
          // Some vertices in this vertex set may not have a corresponding
          // tuple in the join and so a None value should be returned. 
          val otherOption = newW.view.zipWithIndex
            .map{ (x: (W, Int)) => if(wBS(x._2)) Option(x._1) else None }
          // the final values is the zip of the values in this RDD along with
          // the values in the other
          val newValues = thisValues.view.zip(otherOption)
          Iterator((newValues.toIndexedSeq, thisBS))
        } // end of newValues
        new VertexSetRDD(index, newValues) 
      }
    }
  } // end of leftJoin


  /**
   * For each key k in `this` or `other`, return a resulting RDD that contains a 
   * tuple with the list of values for that key in `this` as well as `other`.
   */
  def cogroup[W: ClassManifest](other: RDD[(Vid, W)], partitioner: Partitioner): 
  VertexSetRDD[(Seq[V], Seq[W])] = {
    //RDD[(K, (Seq[V], Seq[W]))] = {
    other match {
      case other: VertexSetRDD[_] if index == other.index => {
        // if both RDDs share exactly the same index and therefore the same 
        // super set of keys then we simply merge the value RDDs. 
        // However it is possible that both RDDs are missing a value for a given key in 
        // which case the returned RDD should have a null value
        val newValues: RDD[(IndexedSeq[(Seq[V], Seq[W])], BitSet)] = 
          valuesRDD.zipPartitions(other.valuesRDD){
          (thisIter, otherIter) => 
            val (thisValues, thisBS) = thisIter.next()
            assert(!thisIter.hasNext)
            val (otherValues, otherBS) = otherIter.next()
            assert(!otherIter.hasNext)
            /** 
             * @todo consider implementing this with a view as in leftJoin to 
             * reduce array allocations
             */
            val newValues = new Array[(Seq[V], Seq[W])](thisValues.size)
            val newBS = thisBS | otherBS
            for( ind <- newBS ) {
              val a = if (thisBS(ind)) Seq(thisValues(ind)) else Seq.empty[V]
              val b = if (otherBS(ind)) Seq(otherValues(ind)) else Seq.empty[W]
              newValues(ind) = (a, b)
            }
            Iterator((newValues.toIndexedSeq, newBS))
        }
        new VertexSetRDD(index, newValues) 
      }
      case other: VertexSetRDD[_] 
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
            val newIndex = new VertexIdToIndexMap()
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
              // Get the corresponding indicies and values for this and the other VertexSetRDD
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
        new VertexSetRDD(new VertexSetIndex(newIndex), newValues)
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
            other.partitionBy(partitioner)
          }
        // Join the other RDD with this RDD building a new valueset and new index on the fly
        val groups = tuples.zipPartitions(otherShuffled)(
          (thisTuplesIter, otherTuplesIter) => {
            // Get the corresponding indicies and values for this VertexSetRDD
            val (thisIndex, (thisValues, thisBS)) = thisTuplesIter.next()
            assert(!thisTuplesIter.hasNext())
            // Construct a new index
            val newIndex = thisIndex.clone().asInstanceOf[VertexIdToIndexMap]
            // Construct a new array Buffer to store the values
            val newValues = ArrayBuffer.fill[ (Seq[V], Seq[W]) ](thisValues.size)(null)
            val newBS = new BitSet(thisValues.size)
            // populate the newValues with the values in this VertexSetRDD
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
            Iterator( (newIndex, (newValues.toIndexedSeq, newBS)) )
          }).cache()

        // Extract the index and values from the above RDD  
        val newIndex = groups.mapPartitions(_.map{ case (kMap,vAr) => kMap }, true)
        val newValues: RDD[(IndexedSeq[(Seq[V], Seq[W])], BitSet)] = 
          groups.mapPartitions(_.map{ case (kMap,vAr) => vAr }, true)
          
        new VertexSetRDD[(Seq[V], Seq[W])](new VertexSetIndex(newIndex), newValues)
      }
    }
  } // end of cogroup
} // End of VertexSetRDD



/**
 * The VertexSetRDD singleton is used to construct VertexSets
 */
object VertexSetRDD {

  /**
   * Construct a vertex set from an RDD of vertex-attribute pairs.  
   * Duplicate entries are removed arbitrarily.  
   *
   * @tparam V the vertex attribute type
   *
   * @param rdd the collection of vertex-attribute pairs
   */
  def apply[V: ClassManifest](rdd: RDD[(Vid,V)]): VertexSetRDD[V] = 
    apply(rdd, (a:V, b:V) => a )

  /**
   * Construct a vertex set from an RDD of vertex-attribute pairs where
   * duplicate entries are merged using the reduceFunc
   *
   * @tparam V the vertex attribute type
   * 
   * @param rdd the collection of vertex-attribute pairs
   * @param reduceFunc the function used to merge attributes of duplicate
   * vertices.
   */
  def apply[V: ClassManifest](
    rdd: RDD[(Vid,V)], reduceFunc: (V, V) => V): VertexSetRDD[V] = {
    // Preaggregate and shuffle if necessary
    // Preaggregation.
    val aggregator = new Aggregator[Vid, V, V](v => v, reduceFunc, reduceFunc)
    val partitioner = new HashPartitioner(rdd.partitions.size)
    val preAgg = rdd.mapPartitions(aggregator.combineValuesByKey).partitionBy(partitioner)

    val groups = preAgg.mapPartitions( iter => {
      val indexMap = new VertexIdToIndexMap()
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
    new VertexSetRDD[V](new VertexSetIndex(index), values)
  } // end of apply


  /**
   * @todo finish documenting
   */
  def apply[V: ClassManifest](
    rdd: RDD[(Vid,V)], index: VertexSetIndex): VertexSetRDD[V] = 
    apply(rdd, index, (a:V,b:V) => a)


  /**
   * @todo finish documenting
   */
  def apply[V: ClassManifest](
    rdd: RDD[(Vid,V)], index: VertexSetIndex,
    reduceFunc: (V, V) => V): VertexSetRDD[V] = 
    apply(rdd,index, (v:V) => v, reduceFunc, reduceFunc)
  

  /**
   * @todo finish documenting
   */
  def apply[V: ClassManifest, C: ClassManifest](
    rdd: RDD[(Vid,V)], 
    index: VertexSetIndex,
    createCombiner: V => C,
    mergeValue: (C, V) => C,
    mergeCombiners: (C, C) => C): VertexSetRDD[C] = {
    // Get the index Partitioner
    val partitioner = index.rdd.partitioner match {
      case Some(p) => p
      case None => throw new SparkException("An index must have a partitioner.")
    }
    // Preaggregate and shuffle if necessary
    val partitioned = 
      if (rdd.partitioner != Some(partitioner)) {
        // Preaggregation.
        val aggregator = new Aggregator[Vid, V, C](createCombiner, mergeValue, 
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
    new VertexSetRDD(index, values)
  } // end of apply


  /**
   * Construct and index of the unique values in a given RDD.
   *
   * @todo finish documenting
   */
  def makeIndex(keys: RDD[Vid], 
    partitioner: Option[Partitioner] = None): VertexSetIndex = {
    // @todo: I don't need the boolean its only there to be the second type since I want to shuffle a single RDD
    // Ugly hack :-(.  In order to partition the keys they must have values. 
    val tbl = keys.mapPartitions(_.map(k => (k, false)), true)
    // Shuffle the table (if necessary)
    val shuffledTbl = partitioner match {
      case None =>  {
        if (tbl.partitioner.isEmpty) {
          // @todo: I don't need the boolean its only there to be the second type of the shuffle. 
          new ShuffledRDD[Vid, Boolean, (Vid, Boolean)](tbl, Partitioner.defaultPartitioner(tbl))
        } else { tbl }
      }
      case Some(partitioner) => 
        tbl.partitionBy(partitioner)
    }

    val index = shuffledTbl.mapPartitions( iter => {
      val indexMap = new VertexIdToIndexMap()
      for ( (k,_) <- iter ){
        if(!indexMap.contains(k)){
          val ind = indexMap.size
          indexMap.put(k, ind)   
        }
      }
      Iterator(indexMap)
      }, true).cache
    new VertexSetIndex(index)
  }

} // end of object VertexSetRDD





