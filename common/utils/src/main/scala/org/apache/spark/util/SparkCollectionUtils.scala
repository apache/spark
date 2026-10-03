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
package org.apache.spark.util

import java.util.{Arrays, HashSet}

import scala.collection.immutable
import scala.reflect.ClassTag

private[spark] trait SparkCollectionUtils {
  /**
   * Creates a [[java.util.HashSet]] pre-sized to hold the given number of elements without
   * triggering a resize.
   *
   * A `HashSet` is backed by a `HashMap` whose bucket table is reallocated and every element
   * rehashed once the entry count exceeds the table capacity multiplied by the default load
   * factor of `0.75`. When the final element count is known in advance, allocating the table
   * at the required size up front avoids those intermediate reallocations and rehashes that a
   * set grown from the default capacity would incur. The initial capacity is derived as
   * `expectedSize / 0.75 + 1` so that all `expectedSize` elements are accommodated below the
   * resize threshold.
   *
   * @param expectedSize the number of elements the returned set is expected to hold; must be
   *                     non-negative
   * @tparam T the element type of the returned set
   * @return an empty `HashSet` with capacity sufficient to hold `expectedSize` elements
   *         without resizing
   */
  def newHashSetWithExpectedSize[T](expectedSize: Int): HashSet[T] = {
    new HashSet[T]((expectedSize / 0.75f + 1.0f).toInt)
  }

  /**
   * Same function as `keys.zipWithIndex.toMap`, but has perf gain.
   */
  def toMapWithIndex[K](keys: Iterable[K]): Map[K, Int] = {
    val builder = immutable.Map.newBuilder[K, Int]
    val keyIter = keys.iterator
    var idx = 0
    while (keyIter.hasNext) {
      builder += (keyIter.next(), idx).asInstanceOf[(K, Int)]
      idx = idx + 1
    }
    builder.result()
  }

  def isEmpty[K, V](map: java.util.Map[K, V]): Boolean = {
    map == null || map.isEmpty()
  }

  def isNotEmpty[K, V](map: java.util.Map[K, V]): Boolean = !isEmpty(map)

  def createArray[K: ClassTag](size: Int, defaultValue: K): Array[K] = {
    val arr = Array.ofDim[K](size)
    val classTag = implicitly[ClassTag[K]]
    classTag.runtimeClass match {
      case c if c == classOf[Boolean] =>
        Arrays.fill(arr.asInstanceOf[Array[Boolean]], defaultValue.asInstanceOf[Boolean])
      case c if c == classOf[Byte] =>
        Arrays.fill(arr.asInstanceOf[Array[Byte]], defaultValue.asInstanceOf[Byte])
      case c if c == classOf[Short] =>
        Arrays.fill(arr.asInstanceOf[Array[Short]], defaultValue.asInstanceOf[Short])
      case c if c == classOf[Char] =>
        Arrays.fill(arr.asInstanceOf[Array[Char]], defaultValue.asInstanceOf[Char])
      case c if c == classOf[Int] =>
        Arrays.fill(arr.asInstanceOf[Array[Int]], defaultValue.asInstanceOf[Int])
      case c if c == classOf[Long] =>
        Arrays.fill(arr.asInstanceOf[Array[Long]], defaultValue.asInstanceOf[Long])
      case c if c == classOf[Float] =>
        Arrays.fill(arr.asInstanceOf[Array[Float]], defaultValue.asInstanceOf[Float])
      case c if c == classOf[Double] =>
        Arrays.fill(arr.asInstanceOf[Array[Double]], defaultValue.asInstanceOf[Double])
      case _ =>
        Arrays.fill(arr.asInstanceOf[Array[AnyRef]], defaultValue.asInstanceOf[AnyRef])
    }
    arr
  }
}

private[spark] object SparkCollectionUtils extends SparkCollectionUtils
