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
    @transient val valuesRDD: RDD[ (Array[V], BitSet) ])
  extends RDD[(K, V)](index.rdd.context, 
    List(new OneToOneDependency(index.rdd), new OneToOneDependency(valuesRDD)) ) {


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
   * The IndexedRDD has its own optimized version of the pairRDDFunctions.  
   */
  override def pairRDDFunctions[K1, V1](
      implicit t: (K, V) <:< (K1,V1), k: ClassManifest[K1], v: ClassManifest[V1]): 
    PairRDDFunctions[K1, V1] = {
    new IndexedRDDFunctions[K1,V1](this.asInstanceOf[IndexedRDD[K1,V1]])
  }


  override def filter(f: Tuple2[K,V] => Boolean): RDD[(K,V)] = {
    val cleanF = index.rdd.context.clean(f)
    val newValues = index.rdd.zipPartitions(valuesRDD){ 
      (keysIter, valuesIter) => 
      val index = keysIter.next()
      assert(keysIter.hasNext() == false)
      val (oldValues, bs) = valuesIter.next()
      assert(valuesIter.hasNext() == false)
       // Allocate the array to store the results into
      val newValues: Array[V] = oldValues.clone().asInstanceOf[Array[V]]
      val newBS = new BitSet(oldValues.size)
      // Populate the new Values
      for( (k,i) <- index ) {
        if ( bs(i) && f( (k, oldValues(i)) ) ) { newBS(i) = true }
        else { newValues(i) = null.asInstanceOf[V] }
      }
      Array((newValues, newBS)).iterator
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
      val values = new ArrayBuffer[V]()
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
      List( (indexMap, (values.toArray, bs)) ).iterator
      }, true).cache
    // extract the index and the values
    val index = groups.mapPartitions(_.map{ case (kMap, vAr) => kMap }, true)
    val values = groups.mapPartitions(_.map{ case (kMap,vAr) => vAr }, true)
    new IndexedRDD[K,V](new RDDIndex(index), values)
  }



  def apply[K: ClassManifest, V: ClassManifest](
    rdd: RDD[(K,V)], index: RDDIndex[K]): IndexedRDD[K,V] = 
  apply(rdd, index, (a:V,b:V) => a)

  def apply[K: ClassManifest, V: ClassManifest](
    rdd: RDD[(K,V)], index: RDDIndex[K], 
    reduceFunc: (V, V) => V): IndexedRDD[K,V] = {
    // Get the index Partitioner
    val partitioner = index.rdd.partitioner match {
      case Some(p) => p
      case None => throw new SparkException("An index must have a partitioner.")
    }
    // Preaggregate and shuffle if necessary
    val partitioned = 
      if (rdd.partitioner != Some(partitioner)) {
        // Preaggregation.
        val aggregator = new Aggregator[K, V, V](v => v, reduceFunc, reduceFunc)
        rdd.mapPartitions(aggregator.combineValuesByKey).partitionBy(partitioner)
      } else {
        rdd
      }

    // Use the index to build the new values table
    val values = index.rdd.zipPartitions(partitioned)( (indexIter, tblIter) => {
      // There is only one map
      val index = indexIter.next()
      assert(!indexIter.hasNext())
      val values = new Array[V](index.size)
      val bs = new BitSet(index.size)
      for ((k,v) <- tblIter) {
        if (!index.contains(k)) {
          throw new SparkException("Error: Trying to bind an external index " +
            "to an RDD which contains keys that are not in the index.")
        }
        val ind = index(k)
        if (bs(ind)) { 
          values(ind) = reduceFunc(values(ind), v) 
        } else {
          values(ind) = v
          bs(ind) = true
        }
      }
      List((values, bs)).iterator
    })
    new IndexedRDD[K,V](index, values)
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
      List(indexMap).iterator
      }, true).cache
    new RDDIndex(index)
  }

} // end of object IndexedRDD





