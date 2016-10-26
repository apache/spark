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

object ZipPartitionsRDDUtils {

  /**
   * The zipPartitionsWithPreferredLocation helper function is similar to RDD.zipPartitions, but
   * it can be used to specify the zipPartitions task preferred locations to be consistent with
   * the fisrt zipped RDD. If the fisrt zipped RDD do not have preferred location,
   * it will fallback to the default `zipPartition` preferred location strategy.
   * This helper function can be used when one large RDD zipped with other small RDDs, we can set
   * the first zipped RDD (the `majorRdd` parameter) to be the large RDD to improve data locality.
   *
   * `preservesPartitioner` indicates whether the input function preserves the partitioner, which
   * should be `false` unless this is a pair RDD and the input function doesn't modify the keys.
   */
  def zipPartitionsWithPreferredLocation[A: ClassTag, B: ClassTag, V: ClassTag]
      (majorRdd: RDD[A], rdd2: RDD[B], preservesPartitioner: Boolean)
      (f: (Iterator[A], Iterator[B]) => Iterator[V]): RDD[V] = {
    val sc = majorRdd.sparkContext
    majorRdd.withScope {
      new ZippedPartitionsRDD2(sc, sc.clean(f), majorRdd, rdd2, preservesPartitioner,
        useFirstParentPreferredLocations = true)
    }
  }

  def zipPartitionsWithPreferredLocation[A: ClassTag, B: ClassTag, V: ClassTag]
      (majorRdd: RDD[A], rdd2: RDD[B])
      (f: (Iterator[A], Iterator[B]) => Iterator[V]): RDD[V] = {
    zipPartitionsWithPreferredLocation(majorRdd, rdd2, preservesPartitioner = false)(f)
  }

  def zipPartitionsWithPreferredLocation[A: ClassTag, B: ClassTag, C: ClassTag, V: ClassTag]
      (majorRdd: RDD[A], rdd2: RDD[B], rdd3: RDD[C], preservesPartitioner: Boolean)
      (f: (Iterator[A], Iterator[B], Iterator[C]) => Iterator[V]): RDD[V] = {
    val sc = majorRdd.sparkContext
    majorRdd.withScope {
      new ZippedPartitionsRDD3(sc, sc.clean(f), majorRdd, rdd2, rdd3,
        preservesPartitioner, useFirstParentPreferredLocations = true)
    }
  }

  def zipPartitionsWithPreferredLocation[A: ClassTag, B: ClassTag, C: ClassTag, V: ClassTag]
      (majorRdd: RDD[A], rdd2: RDD[B], rdd3: RDD[C])
      (f: (Iterator[A], Iterator[B], Iterator[C]) => Iterator[V]): RDD[V] = {
    zipPartitionsWithPreferredLocation(majorRdd, rdd2, rdd3, preservesPartitioner = false)(f)
  }

  def zipPartitionsWithPreferredLocation
      [A: ClassTag, B: ClassTag, C: ClassTag, D: ClassTag, V: ClassTag]
      (majorRdd: RDD[A], rdd2: RDD[B], rdd3: RDD[C], rdd4: RDD[D],
        preservesPartitioner: Boolean)
      (f: (Iterator[A], Iterator[B], Iterator[C], Iterator[D]) => Iterator[V]): RDD[V] = {
    val sc = majorRdd.sparkContext
    majorRdd.withScope {
      new ZippedPartitionsRDD4(sc, sc.clean(f), majorRdd, rdd2, rdd3, rdd4,
        preservesPartitioner, useFirstParentPreferredLocations = true)
    }
  }

  def zipPartitionsWithPreferredLocation
      [A: ClassTag, B: ClassTag, C: ClassTag, D: ClassTag, V: ClassTag]
      (majorRdd: RDD[A], rdd2: RDD[B], rdd3: RDD[C], rdd4: RDD[D])
      (f: (Iterator[A], Iterator[B], Iterator[C], Iterator[D]) => Iterator[V]): RDD[V] = {
    zipPartitionsWithPreferredLocation(
      majorRdd, rdd2, rdd3, rdd4, preservesPartitioner = false)(f)
  }

}
