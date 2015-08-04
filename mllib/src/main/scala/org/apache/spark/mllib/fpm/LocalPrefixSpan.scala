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

package org.apache.spark.mllib.fpm

import scala.collection.mutable

import org.apache.spark.Logging

/**
 * Calculate all patterns of a projected database in local.
 */
class LocalPrefixSpan(
    val minCount: Long,
    val maxPatternLength: Int) extends Logging with Serializable {
  import PrefixSpan.Suffix
  import LocalPrefixSpan.ReversedPrefix

  def run(suffixes: Array[Suffix]): Iterator[(Array[Int], Long)] = {
    genFreqPatterns(ReversedPrefix.empty, suffixes).map { case (prefix, count) =>
      (prefix.toArray, count)
    }
  }

  private def genFreqPatterns(
      prefix: ReversedPrefix,
      suffixes: Array[Suffix]): Iterator[(ReversedPrefix, Long)] = {
    if (maxPatternLength == prefix.length || suffixes.length < minCount) {
        return Iterator.empty
      }
    // find frequent items
    val counts = mutable.Map.empty[Int, Long].withDefaultValue(0)
    suffixes.foreach { suffix =>
      suffix.genSplitStats.foreach { case (x, _) =>
        counts(x) = counts(x) + 1L
      }
    }
    val freqItems = counts.toSeq.filter { case (_, count) =>
      count >= minCount
    }.sorted
    // project and recursively call genFreqPatterns
    freqItems.toIterator.flatMap { case (item, count) =>
      val newPrefix = prefix :+ item
      Iterator.single((newPrefix, count)) ++ {
        val projected = suffixes.map(_.project(item)).filter(_.nonEmpty)
        genFreqPatterns(newPrefix, projected)
      }
    }
  }
}

private object LocalPrefixSpan {

  class ReversedPrefix(val items: List[Int], val length: Int) extends Serializable {
    /**
     * Expands the prefix by one item.
     */
    def :+(item: Int): ReversedPrefix = {
      require(item != 0)
      if (item < 0) {
        new ReversedPrefix(-item :: items, length + 1)
      } else {
        new ReversedPrefix(item :: 0 :: items, length + 1)
      }
    }

    def toArray: Array[Int] = (0 :: items).toArray.reverse
  }

  object ReversedPrefix {
    val empty: ReversedPrefix = new ReversedPrefix(List.empty, 0)
  }
}
