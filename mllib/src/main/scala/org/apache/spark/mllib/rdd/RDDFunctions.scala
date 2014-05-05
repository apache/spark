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

package org.apache.spark.mllib.rdd

import scala.language.implicitConversions
import scala.reflect.ClassTag

import org.apache.spark.rdd.{PartitionPruningRDD, RDD}

/**
 * Machine learning specific RDD functions.
 */
private[mllib]
class RDDFunctions[T: ClassTag](self: RDD[T]) {

  /**
   * Returns a RDD from grouping items of its parent RDD in fixed size blocks by passing a sliding
   * window over them. The ordering is first based on the partition index and then the ordering of
   * items within each partition. This is similar to sliding in Scala collections, except that it
   * becomes an empty RDD if the window size is greater than the total number of items. It needs to
   * trigger a Spark job if the parent RDD has more than one partitions and the window size is
   * greater than 1.
   */
  def sliding(windowSize: Int): RDD[Seq[T]] = {
    require(windowSize > 0, s"Sliding window size must be positive, but got $windowSize.")
    if (windowSize == 1) {
      self.map(Seq(_))
    } else {
      new SlidingRDD[T](self, windowSize)
    }
  }

  /**
   * Computes the all-reduced RDD of the parent RDD, which has the same number of partitions and
   * locality information as its parent RDD. Each partition contains only one record, which is the
   * same as calling `RDD#reduce` on its parent RDD.
   *
   * @param f reducer
   * @return all-reduced RDD
   */
  def allReduce(f: (T, T) => T): RDD[T] = {
    val numPartitions = self.partitions.size
    require(numPartitions > 0, "Parent RDD does not have any partitions.")
    val nextPowerOfTwo = {
      var i = 0
      while ((numPartitions >> i) > 0) {
        i += 1
      }
      1 << i
    }
    var butterfly = self.mapPartitions( (iter) =>
      Iterator(iter.reduce(f)),
      preservesPartitioning = true
    ).cache()

    if (nextPowerOfTwo > numPartitions) {
      val padding = self.context.parallelize(Seq.empty[T], nextPowerOfTwo - numPartitions)
      butterfly = butterfly.union(padding)
    }

    var offset = nextPowerOfTwo >> 1
    while (offset > 0) {
      butterfly = new ButterflyReducedRDD[T](butterfly, f, offset).cache()
      offset >>= 1
    }

    if (nextPowerOfTwo > numPartitions) {
      PartitionPruningRDD.create(butterfly, (i) => i < numPartitions)
    } else {
      butterfly
    }
  }

  /**
   * Reduce the elements of this RDD using the binary tree algorithm.
   */
  def binaryTreeReduce(f: (T, T) => T): T = {
    var reduced = self.mapPartitions( (iter) =>
      if (iter.isEmpty) {
        Iterator.empty
      } else {
        Iterator(iter.reduce(f))
      },
      preservesPartitioning = true
    )
    while (reduced.partitions.size > 3) {
      reduced = new BinaryTreeReducedRDD(reduced, f)
    }
    reduced.reduce(f)
  }
}

private[mllib]
object RDDFunctions {

  /** Implicit conversion from an RDD to RDDFunctions. */
  implicit def fromRDD[T: ClassTag](rdd: RDD[T]) = new RDDFunctions[T](rdd)
}
