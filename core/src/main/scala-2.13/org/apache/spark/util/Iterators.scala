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

object Iterators {

  /**
   * Counts the number of elements of an iterator.
   * This method is slower than `iterator.size` when using Scala 2.13,
   * but it can avoid overflowing problem.
   */
  def size(iterator: Iterator[_]): Long = {
    // SPARK-39928: For Scala 2.13, add check of `iterator.knownSize` refer to
    // `IterableOnceOps#size` to reduce the performance gap with `iterator.size`.
    if (iterator.knownSize > 0) iterator.knownSize.toLong
    else {
      var count = 0L
      while (iterator.hasNext) {
        count += 1L
        iterator.next()
      }
      count
    }
  }
}
