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

import org.apache.spark.Logging
import org.apache.spark.annotation.Experimental

/**
 *
 * :: Experimental ::
 *
 * Calculate all patterns of a projected database in local.
 */
@Experimental
private[fpm] object LocalPrefixSpan extends Logging with Serializable {

  /**
   * Calculate all patterns of a projected database.
   * @param minCount minimum count
   * @param maxPatternLength maximum pattern length
   * @param prefix prefix
   * @param database the projected dabase
   * @return a set of sequential pattern pairs,
   *         the key of pair is sequential pattern (a list of items),
   *         the value of pair is the pattern's count.
   */
  def run(
      minCount: Long,
      maxPatternLength: Int,
      prefix: List[Int],
      database: Iterable[Array[Int]]): Iterator[(Array[Int], Long)] = {

    if (database.isEmpty) return Iterator.empty

    val frequentItemAndCounts = getFreqItemAndCounts(minCount, database)
    val frequentItems = frequentItemAndCounts.map(_._1)
    val frequentPatternAndCounts = frequentItemAndCounts
      .map { case (item, count) => ((item :: prefix).reverse.toArray, count) }

    val filteredProjectedDatabase = database.map(x => x.filter(frequentItems.contains(_)))

    if (prefix.length + 1 < maxPatternLength) {
      frequentPatternAndCounts ++ frequentItems.flatMap { item =>
        val nextProjected = project(filteredProjectedDatabase, item)
        run(minCount, maxPatternLength, item :: prefix, nextProjected)
      }
    } else {
      frequentPatternAndCounts
    }
  }

  /**
   * Calculate suffix sequence immediately after the first occurrence of an item.
   * @param item item to get suffix after
   * @param sequence sequence to extract suffix from
   * @return suffix sequence
   */
  def getSuffix(item: Int, sequence: Array[Int]): Array[Int] = {
    val index = sequence.indexOf(item)
    if (index == -1) {
      Array()
    } else {
      sequence.drop(index + 1)
    }
  }

  def project(database: Iterable[Array[Int]], prefix: Int): Iterable[Array[Int]] = {
    database
      .map(candidateSeq => getSuffix(prefix, candidateSeq))
      .filter(_.nonEmpty)
  }

  /**
   * Generates frequent items by filtering the input data using minimal count level.
   * @param minCount the minimum count for an item to be frequent
   * @param database database of sequences
   * @return item and count pairs
   */
  private def getFreqItemAndCounts(
      minCount: Long,
      database: Iterable[Array[Int]]): Iterator[(Int, Long)] = {
    database.flatMap(_.distinct)
      .foldRight(Map[Int, Long]().withDefaultValue(0L)) { case (item, ctr) =>
        ctr + (item -> (ctr(item) + 1))
      }
      .filter(_._2 >= minCount)
      .iterator
  }
}
