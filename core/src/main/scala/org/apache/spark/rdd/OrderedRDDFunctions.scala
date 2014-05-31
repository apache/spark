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

import java.util.Comparator

import scala.collection.mutable.ArrayBuffer
import scala.reflect.ClassTag

import org.apache.spark.{Logging, RangePartitioner, SparkEnv}
import org.apache.spark.util.collection.{ExternalAppendOnlyMap, AppendOnlyMap}

/**
 * Extra functions available on RDDs of (key, value) pairs where the key is sortable through
 * an implicit conversion. Import `org.apache.spark.SparkContext._` at the top of your program to
 * use these functions. They will work with any key type `K` that has an implicit `Ordering[K]` in
 * scope.  Ordering objects already exist for all of the standard primitive types.  Users can also
 * define their own orderings for custom types, or to override the default ordering.  The implicit
 * ordering that is in the closest scope will be used.
 *
 * {{{
 *   import org.apache.spark.SparkContext._
 *
 *   val rdd: RDD[(String, Int)] = ...
 *   implicit val caseInsensitiveOrdering = new Ordering[String] {
 *     override def compare(a: String, b: String) = a.toLowerCase.compare(b.toLowerCase)
 *   }
 *
 *   // Sort by key, using the above case insensitive ordering.
 *   rdd.sortByKey()
 * }}}
 */

class OrderedRDDFunctions[K : Ordering : ClassTag,
                          V: ClassTag,
                          P <: Product2[K, V] : ClassTag](
  self: RDD[P])
extends Logging with Serializable {

  private val ordering = implicitly[Ordering[K]]

  private type SortCombiner = ArrayBuffer[V]
  /**
    * Sort the RDD by key, so that each partition contains a sorted range of the elements. Calling
    * `collect` or `save` on the resulting RDD will return or output an ordered list of records
    * (in the `save` case, they will be written to multiple `part-X` files in the filesystem, in
      * order of the keys).
    */
  def sortByKey(ascending: Boolean = true, numPartitions: Int = self.partitions.size): RDD[P] = {
    val externalSorting = SparkEnv.get.conf.getBoolean("spark.shuffle.spill", true)
    val part = new RangePartitioner(numPartitions, self, ascending)
    val shuffled = new ShuffledRDD[K, V, P](self, part)
    if (!externalSorting) {
      shuffled.mapPartitions(iter => {
          val buf = iter.toArray
          if (ascending) {
            buf.sortWith((x, y) => ordering.lt(x._1, y._1)).iterator
          } else {
            buf.sortWith((x, y) => ordering.gt(x._1, y._1)).iterator
          }
        }, preservesPartitioning = true)
    } else {
      shuffled.mapPartitions(iter => {
          val map = createExternalMap(ascending)
          while (iter.hasNext) { 
            val kv = iter.next()
            map.insert(kv._1, kv._2)
          }
          map.iterator
        }).flatMap(elem => {
          elem._2.iterator.map(x => (elem._1, x).asInstanceOf[P])
        })
    }
  }

  private def createExternalMap(ascending: Boolean): ExternalAppendOnlyMap[K, V, SortCombiner] = {
    val createCombiner: (V => SortCombiner) = value => {
      val newCombiner = new SortCombiner
      newCombiner += value
      newCombiner
    }
    val mergeValue: (SortCombiner, V) => SortCombiner = (combiner, value) => {
      combiner += value
      combiner
    }
    val mergeCombiners: (SortCombiner, SortCombiner) => SortCombiner = (combiner1, combiner2) => {
      combiner1 ++= combiner2
    }
    new SortedExternalAppendOnlyMap[K, V, SortCombiner](
      createCombiner, mergeValue, mergeCombiners, 
      new KeyComparator[K, SortCombiner](ascending, ordering))
  }

  private class KeyComparator[K, SortCombiner](ascending: Boolean, ord: Ordering[K]) 
  extends Comparator[(K, SortCombiner)] {
    def compare (kc1: (K, SortCombiner), kc2: (K, SortCombiner)): Int = {
      if (ascending) {
        ord.compare(kc1._1, kc2._1)
      } else {
        ord.compare(kc2._1, kc1._1)
      }
    }
  }

  private class SortedExternalAppendOnlyMap[K, V, C](
    createCombiner: V => C,
    mergeValue: (C, V) => C,
    mergeCombiners: (C, C) => C,
    comparator: Comparator[(K, C)] = null)
  extends ExternalAppendOnlyMap[K, V, C](
    createCombiner, mergeValue, mergeCombiners, customizedComparator = comparator) {

    override def iterator: Iterator[(K, C)] = {
      if (spilledMaps.isEmpty) {
        currentMap.destructiveSortedIterator(comparator)
      } else {
        new ExternalIterator()
      }
    }
  }
}
