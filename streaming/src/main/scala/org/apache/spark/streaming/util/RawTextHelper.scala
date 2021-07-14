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

package org.apache.spark.streaming.util

import org.apache.spark.SparkContext
import org.apache.spark.util.collection.OpenHashMap

private[streaming]
object RawTextHelper {

  /**
   * Splits lines and counts the words.
   */
  def splitAndCountPartitions(iter: Iterator[String]): Iterator[(String, Long)] = {
    val map = new OpenHashMap[String, Long]
    var i = 0
    var j = 0
    while (iter.hasNext) {
      val s = iter.next()
      i = 0
      while (i < s.length) {
        j = i
        while (j < s.length && s.charAt(j) != ' ') {
          j += 1
        }
        if (j > i) {
          val w = s.substring(i, j)
          map.changeValue(w, 1L, _ + 1L)
        }
        i = j
        while (i < s.length && s.charAt(i) == ' ') {
          i += 1
        }
      }
      map.toIterator.map {
        case (k, v) => (k, v)
      }
    }
    map.toIterator.map{case (k, v) => (k, v)}
  }

  /**
   * Gets the top k words in terms of word counts. Assumes that each word exists only once
   * in the `data` iterator (that is, the counts have been reduced).
   */
  def topK(data: Iterator[(String, Long)], k: Int): Iterator[(String, Long)] = {
    val taken = new Array[(String, Long)](k)

    var i = 0
    var len = 0
    var value: (String, Long) = null
    var swap: (String, Long) = null
    var count = 0

    while(data.hasNext) {
      value = data.next()
      if (value != null) {
        count += 1
        if (len == 0) {
          taken(0) = value
          len = 1
        } else if (len < k || value._2 > taken(len - 1)._2) {
          if (len < k) {
            len += 1
          }
          taken(len - 1) = value
          i = len - 1
          while(i > 0 && taken(i - 1)._2 < taken(i)._2) {
            swap = taken(i)
            taken(i) = taken(i-1)
            taken(i - 1) = swap
            i -= 1
          }
        }
      }
    }
    taken.toIterator
  }

  /**
   * Warms up the SparkContext in master and executor by running tasks to force JIT kick in
   * before real workload starts.
   */
  def warmUp(sc: SparkContext): Unit = {
    for (i <- 0 to 1) {
      sc.parallelize(1 to 200000, 1000)
        .map(_ % 1331).map(_.toString)
        .mapPartitions(splitAndCountPartitions).reduceByKey(_ + _, 10)
        .count()
    }
  }

  def add(v1: Long, v2: Long): Long = {
    v1 + v2
  }

  def subtract(v1: Long, v2: Long): Long = {
    v1 - v2
  }

  def max(v1: Long, v2: Long): Long = math.max(v1, v2)
}
