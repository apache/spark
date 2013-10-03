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

import scala.collection.mutable.ArrayBuffer

import org.apache.spark.FutureJob

/**
 * A set of asynchronous RDD actions available through an implicit conversion.
 * Import `org.apache.spark.SparkContext._` at the top of your program to use these functions.
 */
class AsyncRDDActions[T: ClassManifest](self: RDD[T]) {

  private val allPartitions = Range(0, self.partitions.size)

  /**
   * Return a future for retrieving all elements of this RDD.
   */
  def collectAsync(): FutureJob[Seq[T]] = {
    val results = new ArrayBuffer[T]
    self.context.submitJob[T, Array[T], Seq[T]](self, _.toArray, allPartitions,
      (index, data) => results ++= data, () => results)
  }

  /**
   * Applies a function f to all elements of this RDD.
   */
  def foreachAsync(f: T => Unit): FutureJob[Unit] = {
    val cleanF = self.context.clean(f)
    self.context.submitJob[T, Unit, Unit](self, _.foreach(cleanF), allPartitions,
      (index, data) => Unit, () => Unit)
  }

  /**
   * Applies a function f to each partition of this RDD.
   */
  def foreachPartitionAsync(f: Iterator[T] => Unit): FutureJob[Unit] = {
    val cleanF = self.context.clean(f)
    self.context.submitJob[T, Unit, Unit](self, cleanF, allPartitions, (index, data) => Unit,
      () => Unit)
  }
}
