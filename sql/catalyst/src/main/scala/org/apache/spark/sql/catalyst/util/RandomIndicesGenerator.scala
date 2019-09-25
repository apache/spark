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

package org.apache.spark.sql.catalyst.util

import org.apache.commons.math3.random.MersenneTwister

/**
 * This class is used to generate a random indices of given length.
 *
 * This implementation uses the "inside-out" version of Fisher-Yates algorithm.
 * Reference:
 *   https://en.wikipedia.org/wiki/Fisher%E2%80%93Yates_shuffle#The_%22inside-out%22_algorithm
 */
case class RandomIndicesGenerator(randomSeed: Long) {
  private val random = new MersenneTwister(randomSeed)

  def getNextIndices(length: Int): Array[Int] = {
    val indices = new Array[Int](length)
    var i = 0
    while (i < length) {
      val j = random.nextInt(i + 1)
      if (j != i) {
        indices(i) = indices(j)
      }
      indices(j) = i
      i += 1
    }
    indices
  }
}
