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

import java.util.{HashMap => JHashMap, BitSet => JBitSet, HashSet => JHashSet}

import scala.collection.JavaConversions._
import scala.collection.mutable.ArrayBuffer


import spark.{Utils, OneToOneDependency, RDD, SparkContext, Partition, TaskContext}

import spark.PairRDDFunctions
import spark.SparkContext._


// import java.io.{ObjectOutputStream, IOException}


/**
 * An IndexedRDD is an RDD[(K,V)] where each K is unique.  
 * 
 * The IndexedRDD contains an index datastructure that can 
 * be used to accelerate join and aggregation operations. 
 */
class IndexedRDD[K: ClassManifest, V: ClassManifest](
    sc: SparkContext,
    val indexRDD:  RDD[ JHashMap[K, Int] ],
    val valuesRDD: RDD[ Array[Seq[V]] ])
  extends RDD[(K, V)](sc, 
    List(new OneToOneDependency(indexRDD), new OneToOneDependency(valuesRDD)) ) {



  val tuples = new ZippedRDD[ JHashMap[K, Int], Array[Seq[V]] ](sc, indexRDD, valuesRDD)


  override val partitioner = indexRDD.partitioner
  override def getPartitions: Array[Partition] = tuples.getPartitions 
  override def getPreferredLocations(s: Partition): Seq[String] = tuples.getPreferredLocations(s)





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
        .filter{ case (_, valar) => valar != null }
        // Extract the pair (removing the indicator from the tuple)
        .flatMap{ case (k, valar) =>  valar.map(v => (k,v))}
    }
  }
}

object IndexedRDD {
  def apply[K: ClassManifest, V: ClassManifest](
    tbl: RDD[(K,V)],
    existingIndex: RDD[JHashMap[K,Int]] = null ): IndexedRDD[K, V] = {

    if(existingIndex == null) {
      // build th index
      val groups = tbl.groupByKey().mapPartitions( iter => {
        val indexMap = new JHashMap[K, Int]()
        val values = new ArrayBuffer[Seq[V]]()
        for((k,ar) <- iter){
          val ind = values.size
          indexMap.put(k, ind)
          values.append(ar)
        }
        List((indexMap, values.toArray)).iterator
        }, true).cache
      // extract the index and the values
      val index = groups.mapPartitions(_.map{ case (kMap,vAr) => kMap }, true)
      val values = groups.mapPartitions(_.map{ case (kMap,vAr) => vAr }, true)
      new IndexedRDD[K,V](tbl.context, index, values)
    } else {
      val index = existingIndex
      // Shuffle the table according to the index (if necessary)
      val shuffledTbl = 
        if (tbl.partitioner == Some(index.partitioner)) {
          tbl
        } else {
          new ShuffledRDD[K,V](tbl, index.partitioner.get())
        }

      // Use the index to build the new values table
      val values = index.zipPartitions[ (K, Seq[V]), Array[Seq[V]] ](
        (indexIter, tblIter) => {
          // There is only one map
          val index: JHashMap[K,Int] = iter.next()
          assert(!iter.hasNext())
          val values = new Array[Seq[V]](index.size)
          for((k,a) <- tblIter) {
            assert(index.contains(k))
            values(index.get(k)) = a
          }
          values
          }, shuffleTbl)

      new IndexedRDD[K,V](index, values)
    }
  }

}





