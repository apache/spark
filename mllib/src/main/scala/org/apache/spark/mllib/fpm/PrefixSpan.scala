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
import org.apache.spark.rdd.RDD
import org.apache.spark.storage.StorageLevel

/**
 *
 * :: Experimental ::
 *
 * A parallel PrefixSpan algorithm to mine sequential pattern.
 * The PrefixSpan algorithm is described in
 * [[http://doi.org/10.1109/ICDE.2001.914830]].
 *
 * @param minSupport the minimal support level of the sequential pattern, any pattern appears
 *                   more than  (minSupport * size-of-the-dataset) times will be output
 * @param maxPatternLength the maximal length of the sequential pattern, any pattern appears
 *                   less than maxPatternLength will be output
 *
 * @see [[https://en.wikipedia.org/wiki/Sequential_Pattern_Mining Sequential Pattern Mining
 *       (Wikipedia)]]
 */
@Experimental
class PrefixSpan private (
    private var minSupport: Double,
    private var maxPatternLength: Int) extends Logging with Serializable {

  private val minPatternsBeforeShuffle: Int = 20

  /**
   * Constructs a default instance with default parameters
   * {minSupport: `0.1`, maxPatternLength: `10`}.
   */
  def this() = this(0.1, 10)

  /**
   * Sets the minimal support level (default: `0.1`).
   */
  def setMinSupport(minSupport: Double): this.type = {
    require(minSupport >= 0 && minSupport <= 1,
      "The minimum support value must be between 0 and 1, including 0 and 1.")
    this.minSupport = minSupport
    this
  }

  /**
   * Sets maximal pattern length (default: `10`).
   */
  def setMaxPatternLength(maxPatternLength: Int): this.type = {
    require(maxPatternLength >= 1,
      "The maximum pattern length value must be greater than 0.")
    this.maxPatternLength = maxPatternLength
    this
  }

  /**
   * Find the complete set of sequential patterns in the input sequences.
   * @param sequences input data set, contains a set of sequences,
   *                  a sequence is an ordered list of elements.
   * @return a set of sequential pattern pairs,
   *         the key of pair is pattern (a list of elements),
   *         the value of pair is the pattern's count.
   */
  def run(sequences: RDD[Array[Int]]): RDD[(Array[Int], Long)] = {
    if (sequences.getStorageLevel == StorageLevel.NONE) {
      logWarning("Input data is not cached.")
    }
    val minCount = getMinCount(sequences)
    val lengthOnePatternsAndCounts =
      getFreqItemAndCounts(minCount, sequences).collect()
    val prefixAndProjectedDatabase = getPrefixAndProjectedDatabase(
      lengthOnePatternsAndCounts.map(_._1), sequences)

    var patternsCount = lengthOnePatternsAndCounts.length
    var allPatternAndCounts = sequences.sparkContext.parallelize(
      lengthOnePatternsAndCounts.map(x => (Array(x._1), x._2)))
    var currentProjectedDatabase = prefixAndProjectedDatabase
    while (patternsCount <= minPatternsBeforeShuffle &&
      currentProjectedDatabase.count() != 0) {
      val (nextPatternAndCounts, nextProjectedDatabase) =
        getPatternCountsAndProjectedDatabase(minCount, currentProjectedDatabase)
      patternsCount = nextPatternAndCounts.count().toInt
      currentProjectedDatabase = nextProjectedDatabase
      allPatternAndCounts = allPatternAndCounts ++ nextPatternAndCounts
    }
    if (patternsCount > 0) {
      val groupedProjectedDatabase = currentProjectedDatabase
        .map(x => (x._1.toSeq, x._2))
        .groupByKey()
        .map(x => (x._1.toArray, x._2.toArray))
      val nextPatternAndCounts = getPatternsInLocal(minCount, groupedProjectedDatabase)
      allPatternAndCounts = allPatternAndCounts ++ nextPatternAndCounts
    }
    allPatternAndCounts
  }

  /**
   * Get the pattern and counts, and projected database
   * @param minCount minimum count
   * @param prefixAndProjectedDatabase prefix and projected database,
   * @return pattern and counts, and projected database
   *         (Array[pattern, count], RDD[prefix, projected database ])
   */
  private def getPatternCountsAndProjectedDatabase(
      minCount: Long,
      prefixAndProjectedDatabase: RDD[(Array[Int], Array[Int])]):
  (RDD[(Array[Int], Long)], RDD[(Array[Int], Array[Int])]) = {
    val prefixAndFreqentItemAndCounts = prefixAndProjectedDatabase.flatMap{ x =>
      x._2.distinct.map(y => ((x._1.toSeq, y), 1L))
    }.reduceByKey(_ + _)
      .filter(_._2 >= minCount)
    val patternAndCounts = prefixAndFreqentItemAndCounts
      .map(x => (x._1._1.toArray ++ Array(x._1._2), x._2))
    val prefixlength = prefixAndProjectedDatabase.take(1)(0)._1.length
    if (prefixlength + 1 >= maxPatternLength) {
      (patternAndCounts, prefixAndProjectedDatabase.filter(x => false))
    } else {
      val frequentItemsMap = prefixAndFreqentItemAndCounts
        .keys.map(x => (x._1, x._2))
        .groupByKey()
        .mapValues(_.toSet)
        .collect
        .toMap
      val nextPrefixAndProjectedDatabase = prefixAndProjectedDatabase
        .filter(x => frequentItemsMap.contains(x._1))
        .flatMap { x =>
        val frequentItemSet = frequentItemsMap(x._1)
        val filteredSequence = x._2.filter(frequentItemSet.contains(_))
        val subProjectedDabase = frequentItemSet.map{ y =>
          (y, LocalPrefixSpan.getSuffix(y, filteredSequence))
        }.filter(_._2.nonEmpty)
        subProjectedDabase.map(y => (x._1 ++ Array(y._1), y._2))
      }
      (patternAndCounts, nextPrefixAndProjectedDatabase)
    }
  }

  /**
   * Get the minimum count (sequences count * minSupport).
   * @param sequences input data set, contains a set of sequences,
   * @return minimum count,
   */
  private def getMinCount(sequences: RDD[Array[Int]]): Long = {
    if (minSupport == 0) 0L else math.ceil(sequences.count() * minSupport).toLong
  }

  /**
   * Generates frequent items by filtering the input data using minimal count level.
   * @param minCount the absolute minimum count
   * @param sequences original sequences data
   * @return array of item and count pair
   */
  private def getFreqItemAndCounts(
      minCount: Long,
      sequences: RDD[Array[Int]]): RDD[(Int, Long)] = {
    sequences.flatMap(_.distinct.map((_, 1L)))
      .reduceByKey(_ + _)
      .filter(_._2 >= minCount)
  }

  /**
   * Get the frequent prefixes' projected database.
   * @param frequentPrefixes frequent prefixes
   * @param sequences sequences data
   * @return prefixes and projected database
   */
  private def getPrefixAndProjectedDatabase(
      frequentPrefixes: Array[Int],
      sequences: RDD[Array[Int]]): RDD[(Array[Int], Array[Int])] = {
    val filteredSequences = sequences.map { p =>
      p.filter (frequentPrefixes.contains(_) )
    }
    filteredSequences.flatMap { x =>
      frequentPrefixes.map { y =>
        val sub = LocalPrefixSpan.getSuffix(y, x)
        (Array(y), sub)
      }.filter(_._2.nonEmpty)
    }
  }

  /**
   * calculate the patterns in local.
   * @param minCount the absolute minimum count
   * @param data patterns and projected sequences data data
   * @return patterns
   */
  private def getPatternsInLocal(
      minCount: Long,
      data: RDD[(Array[Int], Array[Array[Int]])]): RDD[(Array[Int], Long)] = {
    data.flatMap { case (prefix, projDB) =>
      LocalPrefixSpan.run(minCount, maxPatternLength, prefix.toList, projDB)
        .map { case (pattern: List[Int], count: Long) => (pattern.toArray.reverse, count) }
    }
  }
}
