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

import java.util.Arrays

import scala.collection.immutable
import scala.reflect.ClassTag

private[spark] trait SparkCollectionUtils {
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
