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

import scala.collection.immutable

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
}

private[spark] object SparkCollectionUtils extends SparkCollectionUtils
