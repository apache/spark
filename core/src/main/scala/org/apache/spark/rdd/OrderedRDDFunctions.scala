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

import org.apache.spark.{Logging, RangePartitioner}

/**
 * Extra functions available on RDDs of (key, value) pairs where the key is sortable through
 * an implicit conversion. Import `org.apache.spark.SparkContext._` at the top of your program to
 * use these functions. They will work with any key type that has a `scala.math.Ordered`
 * implementation.
 */
class OrderedRDDFunctions[K <% Ordered[K]: ClassTag,
                          V: ClassTag,
                          P <: Product2[K, V] : ClassTag](
    self: RDD[P])
  extends Logging with Serializable {

  /**
   * Sort the RDD by key, so that each partition contains a sorted range of the elements. Calling
   * `collect` or `save` on the resulting RDD will return or output an ordered list of records
   * (in the `save` case, they will be written to multiple `part-X` files in the filesystem, in
   * order of the keys).
   */
  def sortByKey(ascending: Boolean = true, numPartitions: Int = self.partitions.size): RDD[P] = {
    val part = new RangePartitioner(numPartitions, self, ascending)
    val shuffled = new ShuffledRDD[K, V, P](self, part)
    shuffled.mapPartitions(iter => {
      val buf = iter.toArray
      if (ascending) {
        buf.sortWith((x, y) => x._1 < y._1).iterator
      } else {
        buf.sortWith((x, y) => x._1 > y._1).iterator
      }
    }, preservesPartitioning = true)
  }
}
