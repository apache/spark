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

package spark.rdd

import java.nio.ByteBuffer


import java.util.{HashMap => JHashMap, BitSet => JBitSet, HashSet => JHashSet}

import scala.collection.JavaConversions._
import scala.collection.mutable.ArrayBuffer


import spark._
import spark.rdd._
import spark.SparkContext._
import spark.Partitioner._




// import java.io.{ObjectOutputStream, IOException}

/**
 * An IndexedRDD is an RDD[(K,V)] where each K is unique.  
 * 
 * The IndexedRDD contains an index datastructure that can 
 * be used to accelerate join and aggregation operations. 
 */
class IndexedRDD[K: ClassManifest, V: ClassManifest](
    val index:  RDD[ JHashMap[K, Int] ],
    val valuesRDD: RDD[ Seq[Seq[V]] ])
  extends RDD[(K, V)](index.context, 
    List(new OneToOneDependency(index), new OneToOneDependency(valuesRDD)) ) {
  //with PairRDDFunctions[K,V] {



  val tuples = new ZippedRDD[JHashMap[K, Int], Seq[Seq[V]]](index.context, index, valuesRDD)


  override val partitioner = index.partitioner
  override def getPartitions: Array[Partition] = tuples.getPartitions 
  override def getPreferredLocations(s: Partition): Seq[String] = tuples.getPreferredLocations(s)

  override def cache: IndexedRDD[K,V] = {
    index.cache
    valuesRDD.cache
    return this
  }


  /**
   * Construct a new IndexedRDD that is indexed by only the keys in the RDD
   */
   def reindex(): IndexedRDD[K,V] = IndexedRDD(this)


  /**
   * Pass each value in the key-value pair RDD through a map function without changing the keys;
   * this also retains the original RDD's partitioning.
   */
  def mapValues[U: ClassManifest](f: V => U): IndexedRDD[K, U] = {
    val cleanF = index.context.clean(f)
    val newValues = valuesRDD.mapPartitions(_.map{ values =>
      values.map{_.map(x => f(x))}
      }, true)
    new IndexedRDD[K,U](index, newValues)
  }


  /**
   * Pass each value in the key-value pair RDD through a flatMap function without changing the
   * keys; this also retains the original RDD's partitioning.
   */
  def flatMapValues[U: ClassManifest](f: V => TraversableOnce[U]): IndexedRDD[K,U] = {
    val cleanF = index.context.clean(f)
    val newValues = valuesRDD.mapPartitions(_.map{ values =>
      values.map{_.flatMap(x => f(x))}
      }, true)
    new IndexedRDD[K,U](index, newValues)
  }


  /**
   * Generic function to combine the elements for each key using a custom set of aggregation
   * functions. Turns an RDD[(K, V)] into a result of type RDD[(K, C)], for a "combined type" C
   * Note that V and C can be different -- for example, one might group an RDD of type
   * (Int, Int) into an RDD of type (Int, Seq[Int]). Users provide three functions:
   *
   * - `createCombiner`, which turns a V into a C (e.g., creates a one-element list)
   * - `mergeValue`, to merge a V into a C (e.g., adds it to the end of a list)
   * - `mergeCombiners`, to combine two C's into a single one.
   *
   * In addition, users can control the partitioning of the output RDD, and whether to perform
   * map-side aggregation (if a mapper can produce multiple items with the same key).
   */
  def combineByKey[C: ClassManifest](createCombiner: V => C,
      mergeValue: (C, V) => C,
      mergeCombiners: (C, C) => C,
      partitioner: Partitioner,
      mapSideCombine: Boolean = true,
      serializerClass: String = null): IndexedRDD[K, C] = {
    val newValues = valuesRDD.mapPartitions(
      _.map{ groups: Seq[Seq[V]] => 
        groups.map{ group: Seq[V] => 
          if (group != null && !group.isEmpty) {
            val c: C = createCombiner(group.head)
            val sum: C = group.tail.foldLeft(c)(mergeValue)
            Seq(sum)
          } else {
            null
          }
        }
      }, true)
    new IndexedRDD[K,C](index, newValues)
  }


  /**
   * Simplified version of combineByKey that hash-partitions the output RDD.
   */
  def combineByKey[C: ClassManifest](createCombiner: V => C,
      mergeValue: (C, V) => C,
      mergeCombiners: (C, C) => C,
      numPartitions: Int): IndexedRDD[K, C] = {
    combineByKey(createCombiner, mergeValue, mergeCombiners, new HashPartitioner(numPartitions))
  }


  /**
   * Simplified version of combineByKey that hash-partitions the resulting RDD using the
   * existing partitioner/parallelism level.
   */
  def combineByKey[C: ClassManifest](createCombiner: V => C, mergeValue: (C, V) => C, 
    mergeCombiners: (C, C) => C)
      : IndexedRDD[K, C] = {
    combineByKey(createCombiner, mergeValue, mergeCombiners, defaultPartitioner(index))
  }


  /**
   * Merge the values for each key using an associative function and a neutral "zero value" which may
   * be added to the result an arbitrary number of times, and must not change the result (e.g., Nil for
   * list concatenation, 0 for addition, or 1 for multiplication.).
   */
  def foldByKey(zeroValue: V, partitioner: Partitioner)(func: (V, V) => V): IndexedRDD[K, V] = {
    // Serialize the zero value to a byte array so that we can get a new clone of it on each key
    val zeroBuffer = SparkEnv.get.closureSerializer.newInstance().serialize(zeroValue)
    val zeroArray = new Array[Byte](zeroBuffer.limit)
    zeroBuffer.get(zeroArray)

    // When deserializing, use a lazy val to create just one instance of the serializer per task
    lazy val cachedSerializer = SparkEnv.get.closureSerializer.newInstance()
    def createZero() = cachedSerializer.deserialize[V](ByteBuffer.wrap(zeroArray))
    combineByKey[V]((v: V) => func(createZero(), v), func, func, partitioner)
  }

  /**
   * Merge the values for each key using an associative function and a neutral "zero value" which may
   * be added to the result an arbitrary number of times, and must not change the result (e.g., Nil for
   * list concatenation, 0 for addition, or 1 for multiplication.).
   */
  def foldByKey(zeroValue: V, numPartitions: Int)(func: (V, V) => V): IndexedRDD[K, V] = {
    foldByKey(zeroValue, new HashPartitioner(numPartitions))(func)
  }


  /**
   * Merge the values for each key using an associative function and a neutral "zero value" which may
   * be added to the result an arbitrary number of times, and must not change the result (e.g., Nil for
   * list concatenation, 0 for addition, or 1 for multiplication.).
   */
  def foldByKey(zeroValue: V)(func: (V, V) => V): IndexedRDD[K, V] = {
    foldByKey(zeroValue, defaultPartitioner(index))(func)
  }


  /**
   * Merge the values for each key using an associative reduce function. This will also perform
   * the merging locally on each mapper before sending results to a reducer, similarly to a
   * "combiner" in MapReduce.
   */
  def reduceByKey(partitioner: Partitioner, func: (V, V) => V): IndexedRDD[K, V] = {
    combineByKey[V]((v: V) => v, func, func, partitioner)
  }

  /**
   * Merge the values for each key using an associative reduce function. This will also perform
   * the merging locally on each mapper before sending results to a reducer, similarly to a
   * "combiner" in MapReduce. Output will be hash-partitioned with numPartitions partitions.
   */
  def reduceByKey(func: (V, V) => V, numPartitions: Int): IndexedRDD[K, V] = {
    reduceByKey(new HashPartitioner(numPartitions), func)
  }

  /**
   * Merge the values for each key using an associative reduce function. This will also perform
   * the merging locally on each mapper before sending results to a reducer, similarly to a
   * "combiner" in MapReduce. Output will be hash-partitioned with the existing partitioner/
   * parallelism level.
   */
  def reduceByKey(func: (V, V) => V): IndexedRDD[K, V] = {
    reduceByKey(defaultPartitioner(index), func)
  }


  /**
   * Group the values for each key in the RDD into a single sequence. Allows controlling the
   * partitioning of the resulting key-value pair RDD by passing a Partitioner.
   */
  def groupByKey(partitioner: Partitioner): IndexedRDD[K, Seq[V]] = {
    val newValues = valuesRDD.mapPartitions(_.map{ar => ar.map{s => Seq(s)} }, true)
    new IndexedRDD[K, Seq[V]](index, newValues)
  }


  /**
   * Group the values for each key in the RDD into a single sequence. Hash-partitions the
   * resulting RDD with into `numPartitions` partitions.
   */
  def groupByKey(numPartitions: Int): IndexedRDD[K, Seq[V]] = {
    groupByKey(new HashPartitioner(numPartitions))
  }


  /**
   * Group the values for each key in the RDD into a single sequence. Hash-partitions the
   * resulting RDD with the existing partitioner/parallelism level.
   */
  def groupByKey(): IndexedRDD[K, Seq[V]] = {
    groupByKey(defaultPartitioner(index))
  }


  /**
   * For each key k in `this` or `other`, return a resulting RDD that contains a tuple with the
   * list of values for that key in `this` as well as `other`.
   */
  def cogroup[W: ClassManifest](other: RDD[(K, W)], partitioner: Partitioner): 
        IndexedRDD[K, (Seq[V], Seq[W])] = {
    //RDD[(K, (Seq[V], Seq[W]))] = {
    other match {
      case other: IndexedRDD[_, _] if other.index == index => {
        // if both RDDs share exactly the same index and therefore the same super set of keys
        // then we simply merge the value RDDs. 
        // However it is possible that both RDDs are missing a value for a given key in 
        // which case the returned RDD should have a null value
        val newValues = 
          valuesRDD.zipPartitions[ Seq[Seq[W]], Seq[Seq[(Seq[V], Seq[W])]] ](
          (thisIter, otherIter) => {
            val thisValues: Seq[Seq[V]] = thisIter.next()
            assert(!thisIter.hasNext())
            val otherValues: Seq[Seq[W]] = otherIter.next()
            assert(!otherIter.hasNext())   
            // Zip the values and if both arrays are null then the key is not present and 
            // so the resulting value must be null (not a tuple of empty sequences)
            val tmp: Seq[Seq[(Seq[V], Seq[W])]] = thisValues.view.zip(otherValues).map{               
              case (null, null) => null // The key is not present in either RDD
              case (a, null) => Seq((a, Seq.empty[W]))
              case (null, b) => Seq((Seq.empty[V], b))
              case (a, b) => Seq((a,b))
            }.toSeq
            List(tmp).iterator
          }, other.valuesRDD)
        new IndexedRDD[K, (Seq[V], Seq[W])](index, newValues) 
      }
      case other: IndexedRDD[_, _] if other.index.partitioner == index.partitioner => {
        // If both RDDs are indexed using different indices but with the same partitioners
        // then we we need to first merge the indicies and then use the merged index to
        // merge the values.
        val newIndex = 
          index.zipPartitions[JHashMap[K,Int], JHashMap[K,Int]]( (thisIter, otherIter) => {
            val thisIndex = thisIter.next()
            assert(!thisIter.hasNext())
            val otherIndex = otherIter.next()
            assert(!otherIter.hasNext())
            val newIndex = new JHashMap[K, Int]()
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
          }, other.index).cache()
        // Use the new index along with the this and the other indices to merge the values
        val newValues = 
          newIndex.zipPartitions[
            (JHashMap[K, Int], Seq[Seq[V]]), 
            (JHashMap[K, Int], Seq[Seq[W]]), 
            Seq[Seq[(Seq[V],Seq[W])]] ](
            (newIndexIter, thisTuplesIter, otherTuplesIter) => {
              // Get the new index for this partition
              val newIndex = newIndexIter.next()
              assert(!newIndexIter.hasNext())
              // Get the corresponding indicies and values for this and the other IndexedRDD
              val (thisIndex, thisValues) = thisTuplesIter.next()
              assert(!thisTuplesIter.hasNext())
              val (otherIndex, otherValues) = otherTuplesIter.next()
              assert(!otherTuplesIter.hasNext())
              // Preallocate the new Values array
              val newValues = new Array[Seq[(Seq[V],Seq[W])]](newIndex.size)
              // Lookup the sequences in both submaps
              for ((k,ind) <- newIndex) {
                val thisSeq = if (thisIndex.contains(k)) thisValues(thisIndex.get(k)) else null
                val otherSeq = if (otherIndex.contains(k)) otherValues(otherIndex.get(k)) else null
                // if either of the sequences is not null then the key was in one of the two tables
                // and so the value should appear in the returned table
                newValues(ind) = (thisSeq, otherSeq) match {
                  case (null, null) => null
                  case (a, null) => Seq( (a, Seq.empty[W]) )
                  case (null, b) => Seq( (Seq.empty[V], b) )
                  case (a, b) => Seq( (a,b) ) 
                }
              }
              List(newValues.toSeq).iterator
            }, tuples, other.tuples)
        new IndexedRDD(newIndex, newValues)
      }
      case _ => {
        // Get the partitioner from the index
        val partitioner = index.partitioner match {
          case Some(p) => p
          case None => throw new SparkException("An index must have a partitioner.")
        }
        // Shuffle the other RDD using the partitioner for this index
        val otherShuffled = 
          if (other.partitioner == Some(partitioner)) {
            other
          } else {
            new ShuffledRDD[K,W](other, partitioner)
          }
        // Join the other RDD with this RDD building a new valueset and new index on the fly
        val groups = tuples.zipPartitions[(K, W), (JHashMap[K, Int], Seq[Seq[(Seq[V],Seq[W])]]) ](
          (thisTuplesIter, otherTuplesIter) => {
            // Get the corresponding indicies and values for this IndexedRDD
            val (thisIndex, thisValues) = thisTuplesIter.next()
            assert(!thisTuplesIter.hasNext())
            // Construct a new index
            val newIndex = thisIndex.clone().asInstanceOf[JHashMap[K, Int]]
            // Construct a new array Buffer to store the values
            val newValues = ArrayBuffer.fill[(Seq[V], ArrayBuffer[W])](thisValues.size)(null)
            // populate the newValues with the values in this IndexedRDD
            for ((k,i) <- thisIndex) {
              if (thisValues(i) != null) {
                newValues(i) = (thisValues(i), ArrayBuffer.empty[W]) 
              }
            }
            // Now iterate through the other tuples updating the map
            for ((k,w) <- otherTuplesIter){
              if (!newIndex.contains(k)) {
                // update the index
                val ind = newIndex.size
                newIndex.put(k, ind)
                // Update the values
                newValues.append( (Seq.empty[V], ArrayBuffer(w) ) )               
              } else {
                val ind = newIndex.get(k)
                newValues(ind)._2.append(w)
              }
            }
            // Finalize the new values array
            val newValuesArray: Seq[Seq[(Seq[V],Seq[W])]] = 
              newValues.view.map{ case (s, ab) => Seq((s, ab.toSeq)) }.toSeq 
            List( (newIndex, newValuesArray) ).iterator
          }, otherShuffled).cache()

        // Extract the index and values from the above RDD  
        val newIndex = groups.mapPartitions(_.map{ case (kMap,vAr) => kMap }, true)
        val newValues = groups.mapPartitions(_.map{ case (kMap,vAr) => vAr }, true)
          
        new IndexedRDD[K, (Seq[V], Seq[W])](newIndex, newValues)
      }
    }
  }
  

  /**
   * For each key k in `this` or `other`, return a resulting RDD that contains a tuple with the
   * list of values for that key in `this` as well as `other`.
   */
  def cogroup[W: ClassManifest](other: RDD[(K, W)]): IndexedRDD[K, (Seq[V], Seq[W])] = {
    cogroup(other, defaultPartitioner(this, other))
  }

  // /**
  //  * For each key k in `this` or `other1` or `other2`, return a resulting RDD that contains a
  //  * tuple with the list of values for that key in `this`, `other1` and `other2`.
  //  */
  // def cogroup[W1, W2](other1: RDD[(K, W1)], other2: RDD[(K, W2)])
  //     : IndexedRDD[K, (Seq[V], Seq[W1], Seq[W2])] = {
  //   cogroup(other1, other2, defaultPartitioner(this, other1, other2))
  // }

  /**
   * For each key k in `this` or `other`, return a resulting RDD that contains a tuple with the
   * list of values for that key in `this` as well as `other`.
   */
  def cogroup[W: ClassManifest](other: RDD[(K, W)], numPartitions: Int): IndexedRDD[K, (Seq[V], Seq[W])] = {
    cogroup(other, new HashPartitioner(numPartitions))
  }

  // /**
  //  * For each key k in `this` or `other1` or `other2`, return a resulting RDD that contains a
  //  * tuple with the list of values for that key in `this`, `other1` and `other2`.
  //  */
  // def cogroup[W1, W2](other1: RDD[(K, W1)], other2: RDD[(K, W2)], numPartitions: Int)
  //     : RDD[(K, (Seq[V], Seq[W1], Seq[W2]))] = {
  //   cogroup(other1, other2, new HashPartitioner(numPartitions))
  // }

  /** Alias for cogroup. */
  def groupWith[W: ClassManifest](other: RDD[(K, W)]): IndexedRDD[K, (Seq[V], Seq[W])] = {
    cogroup(other, defaultPartitioner(this, other))
  }

  // /** Alias for cogroup. */
  // def groupWith[W1, W2](other1: RDD[(K, W1)], other2: RDD[(K, W2)])
  //     : RDD[(K, (Seq[V], Seq[W1], Seq[W2]))] = {
  //   cogroup(other1, other2, defaultPartitioner(self, other1, other2))
  // }

  /**
   * Return an RDD containing all pairs of elements with matching keys in `this` and `other`. Each
   * pair of elements will be returned as a (k, (v1, v2)) tuple, where (k, v1) is in `this` and
   * (k, v2) is in `other`. Uses the given Partitioner to partition the output RDD.
   */
  def join[W: ClassManifest](other: RDD[(K, W)], partitioner: Partitioner): IndexedRDD[K, (V, W)] = {
    cogroup(other, partitioner).flatMapValues {
      case (vs, ws) =>
        for (v <- vs.iterator; w <- ws.iterator) yield (v, w)
    }
  }

  /**
   * Perform a left outer join of `this` and `other`. For each element (k, v) in `this`, the
   * resulting RDD will either contain all pairs (k, (v, Some(w))) for w in `other`, or the
   * pair (k, (v, None)) if no elements in `other` have key k. Uses the given Partitioner to
   * partition the output RDD.
   */
  def leftOuterJoin[W: ClassManifest](other: RDD[(K, W)], partitioner: Partitioner): IndexedRDD[K, (V, Option[W])] = {
    cogroup(other, partitioner).flatMapValues {
      case (vs, ws) =>
        if (ws.isEmpty) {
          vs.iterator.map(v => (v, None))
        } else {
          for (v <- vs.iterator; w <- ws.iterator) yield (v, Some(w))
        }
    }
  }

  /**
   * Perform a right outer join of `this` and `other`. For each element (k, w) in `other`, the
   * resulting RDD will either contain all pairs (k, (Some(v), w)) for v in `this`, or the
   * pair (k, (None, w)) if no elements in `this` have key k. Uses the given Partitioner to
   * partition the output RDD.
   */
  def rightOuterJoin[W: ClassManifest](other: RDD[(K, W)], partitioner: Partitioner)
      : IndexedRDD[K, (Option[V], W)] = {
    cogroup(other, partitioner).flatMapValues {
      case (vs, ws) =>
        if (vs.isEmpty) {
          ws.iterator.map(w => (None, w))
        } else {
          for (v <- vs.iterator; w <- ws.iterator) yield (Some(v), w)
        }
    }
  }

  /**
   * Return an RDD containing all pairs of elements with matching keys in `this` and `other`. Each
   * pair of elements will be returned as a (k, (v1, v2)) tuple, where (k, v1) is in `this` and
   * (k, v2) is in `other`. Performs a hash join across the cluster.
   */
  def join[W: ClassManifest](other: RDD[(K, W)]): IndexedRDD[K, (V, W)] = {
    join(other, defaultPartitioner(this, other))
  }

  /**
   * Return an RDD containing all pairs of elements with matching keys in `this` and `other`. Each
   * pair of elements will be returned as a (k, (v1, v2)) tuple, where (k, v1) is in `this` and
   * (k, v2) is in `other`. Performs a hash join across the cluster.
   */
  def join[W: ClassManifest](other: RDD[(K, W)], numPartitions: Int): IndexedRDD[K, (V, W)] = {
    join(other, new HashPartitioner(numPartitions))
  }

  /**
   * Perform a left outer join of `this` and `other`. For each element (k, v) in `this`, the
   * resulting RDD will either contain all pairs (k, (v, Some(w))) for w in `other`, or the
   * pair (k, (v, None)) if no elements in `other` have key k. Hash-partitions the output
   * using the existing partitioner/parallelism level.
   */
  def leftOuterJoin[W: ClassManifest](other: RDD[(K, W)]): IndexedRDD[K, (V, Option[W])] = {
    leftOuterJoin(other, defaultPartitioner(this, other))
  }

  /**
   * Perform a left outer join of `this` and `other`. For each element (k, v) in `this`, the
   * resulting RDD will either contain all pairs (k, (v, Some(w))) for w in `other`, or the
   * pair (k, (v, None)) if no elements in `other` have key k. Hash-partitions the output
   * into `numPartitions` partitions.
   */
  def leftOuterJoin[W: ClassManifest](other: RDD[(K, W)], numPartitions: Int): IndexedRDD[K, (V, Option[W])] = {
    leftOuterJoin(other, new HashPartitioner(numPartitions))
  }

  /**
   * Perform a right outer join of `this` and `other`. For each element (k, w) in `other`, the
   * resulting RDD will either contain all pairs (k, (Some(v), w)) for v in `this`, or the
   * pair (k, (None, w)) if no elements in `this` have key k. Hash-partitions the resulting
   * RDD using the existing partitioner/parallelism level.
   */
  def rightOuterJoin[W: ClassManifest](other: RDD[(K, W)]): IndexedRDD[K, (Option[V], W)] = {
    rightOuterJoin(other, defaultPartitioner(this, other))
  }

  /**
   * Perform a right outer join of `this` and `other`. For each element (k, w) in `other`, the
   * resulting RDD will either contain all pairs (k, (Some(v), w)) for v in `this`, or the
   * pair (k, (None, w)) if no elements in `this` have key k. Hash-partitions the resulting
   * RDD into the given number of partitions.
   */
  def rightOuterJoin[W: ClassManifest](other: RDD[(K, W)], numPartitions: Int): IndexedRDD[K, (Option[V], W)] = {
    rightOuterJoin(other, new HashPartitioner(numPartitions))
  }




  /**
   * Provide the RDD[(K,V)] equivalent output. 
   */
  override def compute(part: Partition, context: TaskContext): Iterator[(K, V)] = {
    tuples.compute(part, context).flatMap { case (indexMap, values) => 
      // Walk the index to construct the key, value pairs
      indexMap.iterator
        // Extract rows with key value pairs and indicators
        .map{ case (k, ind) => (k, values(ind))  }
        // Remove tuples that aren't actually present in the array
        .filter{ case (_, valar) => valar != null && !valar.isEmpty()}
        // Extract the pair (removing the indicator from the tuple)
        .flatMap{ case (k, valar) =>  valar.map(v => (k,v))}
    }
  }

} // End of IndexedRDD




object IndexedRDD {
  def apply[K: ClassManifest, V: ClassManifest](
    tbl: RDD[(K,V)],
    existingIndex: RDD[JHashMap[K,Int]] = null ): IndexedRDD[K, V] = {

    if (existingIndex == null) {
      // Shuffle the table (if necessary)
      val shuffledTbl =
        if (tbl.partitioner.isEmpty) {
          new ShuffledRDD[K,V](tbl, Partitioner.defaultPartitioner(tbl))
        } else { tbl }

      val groups = shuffledTbl.mapPartitions( iter => {
        val indexMap = new JHashMap[K, Int]()
        val values = new ArrayBuffer[Seq[V]]()
        for ((k,v) <- iter){
          if(!indexMap.contains(k)) {
            val ind = indexMap.size
            indexMap.put(k, ind)
            values.append(ArrayBuffer.empty[V])
          }
          val ind = indexMap.get(k)
          values(ind).asInstanceOf[ArrayBuffer[V]].append(v)
        }
        List((indexMap, values.toSeq)).iterator
        }, true).cache
      // extract the index and the values
      val index = groups.mapPartitions(_.map{ case (kMap,vAr) => kMap }, true)
      val values = groups.mapPartitions(_.map{ case (kMap,vAr) => vAr }, true)
      new IndexedRDD[K,V](index, values)
    } else {
      val index = existingIndex
      val partitioner = index.partitioner match {
        case Some(p) => p
        case None => throw new SparkException("An index must have a partitioner.")
      }

      // Shuffle the table according to the index (if necessary)
      val shuffledTbl = 
        if (tbl.partitioner == Some(partitioner)) {
          tbl
        } else {
          new ShuffledRDD[K,V](tbl, partitioner)
        }

      // Use the index to build the new values table
      val values = index.zipPartitions[ (K, V), Seq[Seq[V]] ](
        (indexIter, tblIter) => {
          // There is only one map
          val index: JHashMap[K,Int] = indexIter.next()
          assert(!indexIter.hasNext())
          val values = new Array[Seq[V]](index.size)
          for ((k,v) <- tblIter) {
            if (!index.contains(k)) {
              throw new SparkException("Error: Try to bind an external index " +
                "to an RDD which contains keys that are not in the index.")
            }
            val ind = index(k)
            if (values(ind) == null) {
              values(ind) = ArrayBuffer.empty[V]
            }
            values(ind).asInstanceOf[ArrayBuffer[V]].append(v)
          }
          List(values.toSeq).iterator
          }, shuffledTbl)

      new IndexedRDD[K,V](index, values)
    }
  }

}





