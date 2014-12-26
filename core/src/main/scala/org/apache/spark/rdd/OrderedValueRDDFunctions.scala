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
import scala.collection.mutable.ArrayBuffer

import org.apache.spark.{Logging, Partitioner, Partition, TaskContext, HashPartitioner}
import org.apache.spark.annotation.DeveloperApi
import org.apache.spark.util.KeyValueOrdering

/**
 * Extra functions available on RDDs of (key, value) pairs where the value is sortable through
 * an implicit conversion. They will work with any value type `V` that has an implicit `Ordering[V]`
 * in scope. See OrderedRDD for more details on how to define and override the Ordering.
 */
class OrderedValueRDDFunctions[K : ClassTag,
                               V: Ordering : ClassTag,
                               P <: Product2[K, V] : ClassTag] @DeveloperApi() (self: RDD[P])
    extends Logging with Serializable {
  private val valueOrdering = implicitly[Ordering[V]]

  /**
   * Group the values for each key in the RDD into a single sorted sequence. Allows controlling the
   * partitioning of the resulting key-value pair RDD by passing a Partitioner.
   *
   * Note: This operation may be very expensive. If you are grouping in order to perform an
   * aggregation (such as a sum or average) over each key, using [[PairRDDFunctions.aggregateByKey]]
   * or [[PairRDDFunctions.reduceByKey]] will provide much better performance.
   */
  def groupByKeyAndSortValues(partitioner: Partitioner): RDD[(K, Iterable[V])] = {
    val keyPartitioner = new Partitioner{
      override def numPartitions: Int = partitioner.numPartitions
      override def getPartition(key: Any): Int = 
        partitioner.getPartition(key.asInstanceOf[Product2[Any, Any]]._1)
    }

    val shuffled = new ShuffledRDD[Product2[K, V], Unit, Unit](self.map{ kv => (kv, ())}, keyPartitioner)
      .setKeyOrdering(new KeyValueOrdering[K, V](None, Some(valueOrdering)))

    new RDD[(K, Iterable[V])](shuffled) {
      def compute(split: Partition, context: TaskContext): Iterator[(K, Iterable[V])] = 
        new Iterator[(K, Iterable[V])] {
          private val iter = shuffled.compute(split, context).map(_._1).buffered

          override def hasNext: Boolean = iter.hasNext

          override def next(): (K, Iterable[V]) = {
            val key = iter.head._1
            val buffer = new ArrayBuffer[V]
            while (iter.hasNext && iter.head._1 == key)
              buffer += iter.next()._2
            (key, buffer)
          }
        }

      protected def getPartitions: Array[Partition] = shuffled.getPartitions
    }
  }

  /**
   * Simplified version of groupByKeyAndSortValues that hash-partitions the output RDD.
   */
  def groupByKeyAndSortValues(numPartitions: Int): RDD[(K, Iterable[V])] = 
    groupByKeyAndSortValues(new HashPartitioner(numPartitions))
}
