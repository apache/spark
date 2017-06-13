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

/**
 * The `ZipPartitionsRDDUtils` contains a series of `zipPartitionsWithPreferredLocation` helper
 * functions which are similar to `RDD.zipPartitions`, but it can be used to specify the
 * zipPartitions task preferred locations to be consistent with the fisrt zipped RDD.
 *
 * If the fisrt zipped RDD do not have preferred location, it will fallback to the default
 * `zipPartition` preferred location strategy.
 */
object ZipPartitionsRDDUtils {

  /**
   * This helper function can be used when one large RDD zipped with another one small RDD, we
   * can set first zipped RDD (the `majorRdd` parameter) to be the large RDD to improve data
   * locality.
   *
   * `preservesPartitioning` indicates whether the input function preserves the partitioner, which
   * should be `false` unless this is a pair RDD and the input function doesn't modify the keys.
   */
  def zipPartitionsWithPreferredLocation[A: ClassTag, B: ClassTag, V: ClassTag]
      (majorRdd: RDD[A], rdd2: RDD[B], preservesPartitioning: Boolean)
      (f: (Iterator[A], Iterator[B]) => Iterator[V]): RDD[V] = {
    val sc = majorRdd.sparkContext
    majorRdd.withScope {
      new ZippedPartitionsRDD2(sc, sc.clean(f), majorRdd, rdd2, preservesPartitioning,
        useFirstParentPreferredLocations = true)
    }
  }

  /**
   * This helper function can be used when one large RDD zipped with another one small RDD, we
   * can set first zipped RDD (the `majorRdd` parameter) to be the large RDD to improve data
   * locality.
   */
  def zipPartitionsWithPreferredLocation[A: ClassTag, B: ClassTag, V: ClassTag]
      (majorRdd: RDD[A], rdd2: RDD[B])
      (f: (Iterator[A], Iterator[B]) => Iterator[V]): RDD[V] = {
    zipPartitionsWithPreferredLocation(majorRdd, rdd2, preservesPartitioning = false)(f)
  }

  /**
   * This helper function can be used when one large RDD zipped with another two small RDDs, we
   * can set first zipped RDD (the `majorRdd` parameter) to be the large RDD to improve data
   * locality.
   *
   * `preservesPartitioning` indicates whether the input function preserves the partitioner, which
   * should be `false` unless this is a pair RDD and the input function doesn't modify the keys.
   */
  def zipPartitionsWithPreferredLocation[A: ClassTag, B: ClassTag, C: ClassTag, V: ClassTag]
      (majorRdd: RDD[A], rdd2: RDD[B], rdd3: RDD[C], preservesPartitioning: Boolean)
      (f: (Iterator[A], Iterator[B], Iterator[C]) => Iterator[V]): RDD[V] = {
    val sc = majorRdd.sparkContext
    majorRdd.withScope {
      new ZippedPartitionsRDD3(sc, sc.clean(f), majorRdd, rdd2, rdd3,
        preservesPartitioning, useFirstParentPreferredLocations = true)
    }
  }

  /**
   * This helper function can be used when one large RDD zipped with another two small RDDs, we
   * can set first zipped RDD (the `majorRdd` parameter) to be the large RDD to improve data
   * locality.
   */
  def zipPartitionsWithPreferredLocation[A: ClassTag, B: ClassTag, C: ClassTag, V: ClassTag]
      (majorRdd: RDD[A], rdd2: RDD[B], rdd3: RDD[C])
      (f: (Iterator[A], Iterator[B], Iterator[C]) => Iterator[V]): RDD[V] = {
    zipPartitionsWithPreferredLocation(majorRdd, rdd2, rdd3, preservesPartitioning = false)(f)
  }

  /**
   * This helper function can be used when one large RDD zipped with another three small RDDs, we
   * can set first zipped RDD (the `majorRdd` parameter) to be the large RDD to improve data
   * locality.
   *
   * `preservesPartitioning` indicates whether the input function preserves the partitioner, which
   * should be `false` unless this is a pair RDD and the input function doesn't modify the keys.
   */
  def zipPartitionsWithPreferredLocation
      [A: ClassTag, B: ClassTag, C: ClassTag, D: ClassTag, V: ClassTag]
      (majorRdd: RDD[A], rdd2: RDD[B], rdd3: RDD[C], rdd4: RDD[D],
        preservesPartitioning: Boolean)
      (f: (Iterator[A], Iterator[B], Iterator[C], Iterator[D]) => Iterator[V]): RDD[V] = {
    val sc = majorRdd.sparkContext
    majorRdd.withScope {
      new ZippedPartitionsRDD4(sc, sc.clean(f), majorRdd, rdd2, rdd3, rdd4,
        preservesPartitioning, useFirstParentPreferredLocations = true)
    }
  }

  /**
   * This helper function can be used when one large RDD zipped with another three small RDDs, we
   * can set first zipped RDD (the `majorRdd` parameter) to be the large RDD to improve data
   * locality.
   */
  def zipPartitionsWithPreferredLocation
      [A: ClassTag, B: ClassTag, C: ClassTag, D: ClassTag, V: ClassTag]
      (majorRdd: RDD[A], rdd2: RDD[B], rdd3: RDD[C], rdd4: RDD[D])
      (f: (Iterator[A], Iterator[B], Iterator[C], Iterator[D]) => Iterator[V]): RDD[V] = {
    zipPartitionsWithPreferredLocation(
      majorRdd, rdd2, rdd3, rdd4, preservesPartitioning = false)(f)
  }

}
