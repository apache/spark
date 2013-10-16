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

package org.apache.spark.rdd

import java.util.{HashMap => JHashMap, BitSet => JBitSet, HashSet => JHashSet}

import scala.collection.JavaConversions._
import scala.collection.mutable.ArrayBuffer
import scala.collection.mutable.BitSet


import org.apache.spark._



class IndexedRDDFunctions[K: ClassManifest, V: ClassManifest](self: IndexedRDD[K,V])
  extends PairRDDFunctions[K,V](self) {

  /**
   * Construct a new IndexedRDD that is indexed by only the keys in the RDD
   */
   def reindex(): IndexedRDD[K,V] = IndexedRDD(self)


  /**
   * Pass each value in the key-value pair RDD through a map function without changing the keys;
   * this also retains the original RDD's partitioning.
   */
  override def mapValues[U: ClassManifest](f: V => U): RDD[(K, U)] = {
    val cleanF = self.index.rdd.context.clean(f)
    val newValuesRDD = self.valuesRDD.mapPartitions(iter => iter.map{ 
      case (values, bs) => 
        val newValues = new Array[U](values.size)
        for ( ind <- bs ) {
          newValues(ind) = f(values(ind))
        }
        (newValues, bs)
      }, preservesPartitioning = true)
    new IndexedRDD[K,U](self.index, newValuesRDD)
  }


  /**
   * Pass each value in the key-value pair RDD through a map function without changing the keys;
   * this also retains the original RDD's partitioning.
   */
  override def mapValuesWithKeys[U: ClassManifest](f: (K, V) => U): RDD[(K, U)] = {
    val cleanF = self.index.rdd.context.clean(f)
    val newValues = self.index.rdd.zipPartitions(self.valuesRDD){ 
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
      Array((newValues, bs)).iterator
    }
    new IndexedRDD[K,U](self.index, newValues)
  }



  // /**
  //  * Pass each value in the key-value pair RDD through a flatMap function without changing the
  //  * keys; this also retains the original RDD's partitioning.
  //  */
  // override def flatMapValues[U: ClassManifest](f: V => TraversableOnce[U]): RDD[(K,U)] = {
  //   val cleanF = self.index.rdd.context.clean(f)
  //   val newValues = self.valuesRDD.mapPartitions(_.map(values => values.map{
  //       case null => null 
  //       case row => row.flatMap(x => f(x))
  //     }), true)
  //   new IndexedRDD[K,U](self.index, newValues)
  // }


  /**
   * Generic function to combine the elements for each key using a custom set of aggregation
   * functions. Turns an RDD[(K, V)] into a result of type RDD[(K, C)], for a "combined type" C
   * Note that V and C can be different -- for example, one might group an RDD of type
   * (Int, Int) into an RDD of type (Int, Seq[Int]). Users provide three functions:
   *
   * - `createCombiner`, which turns a V into a C (e.g., creates a one-element list)
   * - `mergeValue`, to merge a V into a C (e.g., adds it to the end of a list)
   * - `mergeCombiners`, to combine two C's into a single one.
   */
  override def combineByKey[C: ClassManifest](createCombiner: V => C,
      mergeValue: (C, V) => C,
      mergeCombiners: (C, C) => C,
      partitioner: Partitioner,
      mapSideCombine: Boolean = true,
      serializerClass: String = null): RDD[(K, C)] = {
    mapValues(createCombiner)
  }

 

  // /**
  //  * Group the values for each key in the RDD into a single sequence. Hash-partitions the
  //  * resulting RDD with the existing partitioner/parallelism level.
  //  */
  // override def groupByKey(partitioner: Partitioner): RDD[(K, Seq[V])] = {
  //   val newValues = self.valuesRDD.mapPartitions(_.map{ar => ar.map{s => Seq(s)} }, true)
  //   new IndexedRDD[K, Seq[V]](self.index, newValues)
  // }


  /**
   * For each key k in `this` or `other`, return a resulting RDD that contains a tuple with the
   * list of values for that key in `this` as well as `other`.
   */
  override def cogroup[W: ClassManifest](other: RDD[(K, W)], partitioner: Partitioner): 
  IndexedRDD[K, (Seq[V], Seq[W])] = {
    //RDD[(K, (Seq[V], Seq[W]))] = {
    other match {
      case other: IndexedRDD[_, _] if self.index == other.index => {
        // if both RDDs share exactly the same index and therefore the same super set of keys
        // then we simply merge the value RDDs. 
        // However it is possible that both RDDs are missing a value for a given key in 
        // which case the returned RDD should have a null value
        val newValues = 
          self.valuesRDD.zipPartitions(other.valuesRDD){
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
            List((newValues, newBS)).iterator
        }
        new IndexedRDD(self.index, newValues) 
      }
      case other: IndexedRDD[_, _] 
        if self.index.rdd.partitioner == other.index.rdd.partitioner => {
        // If both RDDs are indexed using different indices but with the same partitioners
        // then we we need to first merge the indicies and then use the merged index to
        // merge the values.
        val newIndex = 
          self.index.rdd.zipPartitions(other.index.rdd)(
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
        val newValues = 
          newIndex.zipPartitions(self.tuples, other.tuples)(
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
              List((newValues, newBS)).iterator
            })
        new IndexedRDD(new RDDIndex(newIndex), newValues)
      }
      case _ => {
        // Get the partitioner from the index
        val partitioner = self.index.rdd.partitioner match {
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
        val groups = 
          self.tuples.zipPartitions(otherShuffled)(
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
            List( (newIndex, (newValues.toArray, newBS)) ).iterator
          }).cache()

        // Extract the index and values from the above RDD  
        val newIndex = groups.mapPartitions(_.map{ case (kMap,vAr) => kMap }, true)
        val newValues = groups.mapPartitions(_.map{ case (kMap,vAr) => vAr }, true)
          
        new IndexedRDD[K, (Seq[V], Seq[W])](new RDDIndex(newIndex), newValues)
      }
    }
  }
  

}

//(self: IndexedRDD[K, V]) extends PairRDDFunctions(self) { }


