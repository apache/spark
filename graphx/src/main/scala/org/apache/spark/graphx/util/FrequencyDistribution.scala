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

package org.apache.spark.graphx.util

import scala.collection.mutable.ArrayBuffer

import org.apache.spark.rdd.RDD

private[graphx] trait FrequencyDistribution extends Serializable {

  /** Compute the distribution of entries in RDD. */
  def compute(numbers: RDD[Int]): Array[((Int, Int), Long)] = {
    // First, compute min/max from RDD
    val (max, min) = numbers.mapPartitions { iter =>
      var maxv = if (iter.hasNext) iter.next else 0
      var minv = maxv
      while (iter.hasNext) {
        iter.next match {
          case v if v > maxv => maxv = v
          case v if v < minv => minv = v
          case _ =>
        }
      }
      Iterator((maxv, minv))
    }.reduce { (a, b) =>
      val maxv = if (a._1 > b._1) a._1 else b._1
      val minv = if (a._2 < b._2) a._2 else b._2
      (maxv, minv)
    }

    // Compute a range [min, max] into buckets
    val ranges = divideRanges(max, min).ensuring{ r =>
      r.forall(r => r._1 <= r._2)
    }

    // Count # of degrees in each bucket
    val frequencies = numbers.mapPartitions{ iter =>
      val counters = new Array[Long](ranges.length)
      while (iter.hasNext) {
        val target = binaryRangeSearch(ranges, iter.next())
        if (target != -1) counters(target) += 1
      }
      Iterator(counters)
    }.reduce((a, b) => a.zip(b).map(pair => pair._1 + pair._2))
    ranges.zip(frequencies)
  }

  def divideRanges(max: Int, min: Int): Array[(Int, Int)]

  private def binaryRangeSearch(ranges: Array[(Int, Int)], num: Int): Int = {
    var (left, right) = (0, ranges.length - 1)
    while (left <= right) {
      val middle = (left + right) >>> 1
      if (num > ranges(middle)._2) left = middle + 1
      else if (num < ranges(middle)._1) right = middle - 1
      else return middle
    }
    -1
  }
}

private[graphx] object FrequencyDistribution {

  /**
   * Divide a range [min, max] into 'bucketNum' buckets
   * with equi-width.
   */
  def split(bucketNum: Int): FrequencyDistribution = {
    assert(bucketNum > 0)
    new FrequencyDistribution {
      override def divideRanges(max: Int, min: Int): Array[(Int, Int)] = {
        val span = max - min + 1
        val initSize = span / bucketNum
        val sizes = Array.fill(bucketNum)(initSize)
        val remainder = span - bucketNum * initSize
        for (i <- 0 until remainder) {
          sizes(i) += 1
        }
        assert(sizes.reduce(_ + _) == span)
        val ranges = ArrayBuffer.empty[(Int, Int)]
        var start = min
        sizes.filter(_ > 0).foreach { size =>
          val end = start + size - 1
          ranges += Tuple2(start, end)
          start = end + 1
        }
        assert(start == max + 1)
        ranges.toArray
      }
    }
  }

  /** Divide a range [min, max] into 'width'-size buckets. */
  def equiwidth(width: Int): FrequencyDistribution = {
    assert(width > 0)
    new FrequencyDistribution {
      override def divideRanges(max: Int, min: Int): Array[(Int, Int)] = {
        var start = min
        val ranges = ArrayBuffer.empty[(Int, Int)]
        while (start <= max) {
          val end = start + width - 1
          ranges += Tuple2(start, end)
          start = end + 1
        }
        ranges.toArray
      }
    }
  }
}
