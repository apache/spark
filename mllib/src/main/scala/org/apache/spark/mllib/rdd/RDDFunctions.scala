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

import org.apache.spark.annotation.DeveloperApi
import org.apache.spark.rdd.RDD

/**
 * Machine learning specific RDD functions.
 */
@DeveloperApi
class RDDFunctions[T: ClassTag](self: RDD[T]) extends Serializable {

  /**
   * Returns a RDD from grouping items of its parent RDD in fixed size blocks by passing a sliding
   * window over them. The ordering is first based on the partition index and then the ordering of
   * items within each partition. This is similar to sliding in Scala collections, except that it
   * becomes an empty RDD if the window size is greater than the total number of items. It needs to
   * trigger a Spark job if the parent RDD has more than one partitions and the window size is
   * greater than 1.
   */
  def sliding(windowSize: Int, step: Int): RDD[Array[T]] = {
    require(windowSize > 0, s"Sliding window size must be positive, but got $windowSize.")
    if (windowSize == 1 && step == 1) {
      self.map(Array(_))
    } else {
      new SlidingRDD[T](self, windowSize, step)
    }
  }

  /**
   * [[sliding(Int, Int)*]] with step = 1.
   */
  def sliding(windowSize: Int): RDD[Array[T]] = sliding(windowSize, 1)

  /**
   * Reduces the elements of this RDD in a multi-level tree pattern.
   *
   * @param depth suggested depth of the tree (default: 2)
   * @see [[org.apache.spark.rdd.RDD#treeReduce]]
   * @deprecated Use [[org.apache.spark.rdd.RDD#treeReduce]] instead.
   */
  @deprecated("Use RDD.treeReduce instead.", "1.3.0")
  def treeReduce(f: (T, T) => T, depth: Int = 2): T = self.treeReduce(f, depth)

  /**
   * Aggregates the elements of this RDD in a multi-level tree pattern.
   *
   * @param depth suggested depth of the tree (default: 2)
   * @see [[org.apache.spark.rdd.RDD#treeAggregate]]
   * @deprecated Use [[org.apache.spark.rdd.RDD#treeAggregate]] instead.
   */
  @deprecated("Use RDD.treeAggregate instead.", "1.3.0")
  def treeAggregate[U: ClassTag](zeroValue: U)(
      seqOp: (U, T) => U,
      combOp: (U, U) => U,
      depth: Int = 2): U = {
    self.treeAggregate(zeroValue)(seqOp, combOp, depth)
  }
}

@DeveloperApi
object RDDFunctions {

  /** Implicit conversion from an RDD to RDDFunctions. */
  implicit def fromRDD[T: ClassTag](rdd: RDD[T]): RDDFunctions[T] = new RDDFunctions[T](rdd)
}
