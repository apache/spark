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

import scala.reflect.ClassTag

import org.apache.spark.{HashPartitioner, Logging, Partitioner, RangePartitioner}
import org.apache.spark.annotation.DeveloperApi

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
                          P <: Product2[K, V] : ClassTag] @DeveloperApi() (
    self: RDD[P])
  extends Logging with Serializable
{
  private val ordering = implicitly[Ordering[K]]

  /**
   * Sort the RDD by key, so that each partition contains a sorted range of the elements. Calling
   * `collect` or `save` on the resulting RDD will return or output an ordered list of records
   * (in the `save` case, they will be written to multiple `part-X` files in the filesystem, in
   * order of the keys).
   */
  // TODO: this currently doesn't work on P other than Tuple2!
  def sortByKey(ascending: Boolean = true, numPartitions: Int = self.partitions.size)
      : RDD[(K, V)] = {
    val part = new RangePartitioner(numPartitions, self, ascending)
    new ShuffledRDD[K, V, V](self, part)
      .setKeyOrdering(if (ascending) ordering else ordering.reverse)
  }

  /**
   * Repartition the RDD according to the given partitioner and, within each resulting partition,
   * sort records by their keys.
   *
   * This is more efficient than calling `repartition` and then sorting within each partition
   * because it can push the sorting down into the shuffle machinery.
   */
  def repartitionAndSortWithinPartitions(partitioner: Partitioner): RDD[(K, V)] = {
    new ShuffledRDD[K, V, V](self, partitioner).setKeyOrdering(ordering)
  }

  /**
   * Group the values for each key in the RDD into a single sequence, and, within each partition,
   * return the keys in sorted order. This replicates the semantics of the Hadoop MapReduce shuffle
   * and avoids pulling all the records in a group into memory at once.
   *
   * Allows controlling the partitioning of the resulting key-value pair RDD by passing a
   * Partitioner.
   *
   * Note: When iterating over the resulting records, the iterator for a group is rendered invalid
   * after moving on to the next group.
   */
  def groupByKeyAndSortWithinPartitions(): RDD[(K, Iterable[V])] = {
    groupByKeyAndSortWithinPartitions(Partitioner.defaultPartitioner(self))
  }

  /**
   * Group the values for each key in the RDD into a single sequence, and, within each partition,
   * return the keys in sorted order. This replicates the semantics of the Hadoop MapReduce shuffle
   * and avoids pulling all the records in a group into memory at once.
   *
   * Allows controlling the partitioning of the resulting key-value pair RDD by passing a
   * Partitioner.
   *
   * Note: When iterating over the resulting records, the iterator for a group is rendered invalid
   * after moving on to the next group.
   */
  def groupByKeyAndSortWithinPartitions(numPartitions: Int): RDD[(K, Iterable[V])] = {
    groupByKeyAndSortWithinPartitions(new HashPartitioner(numPartitions))
  }

  /**
   * Group the values for each key in the RDD into a single sequence, and, within each partition,
   * return the keys in sorted order. This replicates the semantics of the Hadoop MapReduce shuffle
   * and avoids pulling all the records in a group into memory at once.
   *
   * Allows controlling the partitioning of the resulting key-value pair RDD by passing a
   * Partitioner.
   *
   * Note: When iterating over the resulting records, the iterator for a group is rendered invalid
   * after moving on to the next group.
   */
  def groupByKeyAndSortWithinPartitions(partitioner: Partitioner): RDD[(K, Iterable[V])] = {
    val shuffled = repartitionAndSortWithinPartitions(partitioner)
    shuffled.mapPartitions(iter => {
      new Iterator[(K, Iterable[V])] with Serializable {
        var cur: (K, V) = if (iter.hasNext) iter.next() else null
        var curIter: Iterator[V] = null

        private def groupIterator(key: K) = new Iterator[V] with Serializable {
          def next(): V = {
            if (cur._1 == null) {
              throw new NoSuchElementException("Next on empty iterator.")
            }
            if (cur._1 != key) {
              throw new IllegalStateException(
                "Can't use group iterator after moving to next key.")
            }

            val ret = cur
            cur = if (iter.hasNext) iter.next() else null
            ret._2
          }

          def hasNext(): Boolean = cur != null && cur._1 == key
        }

        /** Move on to the next key. */
        private def drainCur() {
          if (curIter != null) {
            while (curIter.hasNext) {
              curIter.next()
            }
          }
        }

        def hasNext(): Boolean = {
          drainCur()
          cur == null
        }

        def next(): (K, Iterable[V]) = {
          drainCur()
          if (cur == null) {
            throw new NoSuchElementException("Next on empty iterator.")
          }
          curIter = groupIterator(cur._1)
          (cur._1, new Iterable[V] with Serializable {
            val iter = curIter
            def iterator = iter
          })
        }
      }
    })
  }

}
