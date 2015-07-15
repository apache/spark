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
   * @param projectedDatabase the projected dabase
   * @return a set of sequential pattern pairs,
   *         the key of pair is sequential pattern (a list of items),
   *         the value of pair is the pattern's count.
   */
  def run(
      minCount: Long,
      maxPatternLength: Int,
      prefix: Array[Int],
      projectedDatabase: Array[Array[Int]]): Array[(Array[Int], Long)] = {
    val frequentPrefixAndCounts = getFreqItemAndCounts(minCount, projectedDatabase)
    val frequentPatternAndCounts = frequentPrefixAndCounts
      .map(x => (prefix ++ Array(x._1), x._2))
    val prefixProjectedDatabases = getPatternAndProjectedDatabase(
      prefix, frequentPrefixAndCounts.map(_._1), projectedDatabase)

    val continueProcess = prefixProjectedDatabases.nonEmpty && prefix.length + 1 < maxPatternLength
    if (continueProcess) {
      val nextPatterns = prefixProjectedDatabases
        .map(x => run(minCount, maxPatternLength, x._1, x._2))
        .reduce(_ ++ _)
      frequentPatternAndCounts ++ nextPatterns
    } else {
      frequentPatternAndCounts
    }
  }

  /**
   * calculate suffix sequence following a prefix in a sequence
   * @param prefix prefix
   * @param sequence sequence
   * @return suffix sequence
   */
  def getSuffix(prefix: Int, sequence: Array[Int]): Array[Int] = {
    val index = sequence.indexOf(prefix)
    if (index == -1) {
      Array()
    } else {
      sequence.drop(index + 1)
    }
  }

  /**
   * Generates frequent items by filtering the input data using minimal count level.
   * @param minCount the absolute minimum count
   * @param sequences sequences data
   * @return array of item and count pair
   */
  private def getFreqItemAndCounts(
      minCount: Long,
      sequences: Array[Array[Int]]): Array[(Int, Long)] = {
    sequences.flatMap(_.distinct)
      .groupBy(x => x)
      .mapValues(_.length.toLong)
      .filter(_._2 >= minCount)
      .toArray
  }

  /**
   * Get the frequent prefixes' projected database.
   * @param prePrefix the frequent prefixes' prefix
   * @param frequentPrefixes frequent prefixes
   * @param sequences sequences data
   * @return prefixes and projected database
   */
  private def getPatternAndProjectedDatabase(
      prePrefix: Array[Int],
      frequentPrefixes: Array[Int],
      sequences: Array[Array[Int]]): Array[(Array[Int], Array[Array[Int]])] = {
    val filteredProjectedDatabase = sequences
      .map(x => x.filter(frequentPrefixes.contains(_)))
    frequentPrefixes.map { x =>
      val sub = filteredProjectedDatabase.map(y => getSuffix(x, y)).filter(_.nonEmpty)
      (prePrefix ++ Array(x), sub)
    }.filter(x => x._2.nonEmpty)
  }
}
