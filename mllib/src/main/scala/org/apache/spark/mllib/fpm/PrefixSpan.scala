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

  /**
   * Constructs a default instance with default parameters
   * {minSupport: `0.1`, maxPatternLength: `10`}.
   */
  def this() = this(0.1, 10)

  /**
   * Sets the minimal support level (default: `0.1`).
   */
  def setMinSupport(minSupport: Double): this.type = {
    require(minSupport >= 0 && minSupport <= 1)
    this.minSupport = minSupport
    this
  }

  /**
   * Sets maximal pattern length (default: `10`).
   */
  def setMaxPatternLength(maxPatternLength: Int): this.type = {
    require(maxPatternLength >= 1)
    this.maxPatternLength = maxPatternLength
    this
  }

  /**
   * Find the complete set of sequential patterns in the input sequences.
   * @param sequences input data set, contains a set of sequences,
   *                  a sequence is an ordered list of elements.
   * @return a set of sequential pattern pairs,
   *         the key of pair is pattern (a list of elements),
   *         the value of pair is the pattern's support value.
   */
  def run(sequences: RDD[Array[Int]]): RDD[(Array[Int], Long)] = {
    if (sequences.getStorageLevel == StorageLevel.NONE) {
      logWarning("Input data is not cached.")
    }
    val minCount = getAbsoluteMinSupport(sequences)
    val (lengthOnePatternsAndCounts, prefixAndCandidates) =
      findLengthOnePatterns(minCount, sequences)
    val repartitionedRdd = makePrefixProjectedDatabases(prefixAndCandidates)
    val nextPatterns = getPatternsInLocal(minCount, repartitionedRdd)
    val allPatterns = lengthOnePatternsAndCounts.map(x => (Array(x._1), x._2)) ++ nextPatterns
    allPatterns
  }

  /**
   * Get the absolute minimum support value (sequences count * minSupport).
   * @param sequences input data set, contains a set of sequences,
   * @return absolute minimum support value,
   */
  private def getAbsoluteMinSupport(sequences: RDD[Array[Int]]): Long = {
    if (minSupport == 0) 0L else (sequences.count() * minSupport).toLong
  }

  /**
   * Generates frequent items by filtering the input data using minimal support level.
   * @param minCount the absolute minimum support
   * @param sequences original sequences data
   * @return array of frequent pattern ordered by their frequencies
   */
  private def getFreqItemAndCounts(
      minCount: Long,
      sequences: RDD[Array[Int]]): RDD[(Int, Long)] = {
    sequences.flatMap(_.distinct.map((_, 1L)))
      .reduceByKey(_ + _)
      .filter(_._2 >= minCount)
  }

  /**
   * Generates frequent items by filtering the input data using minimal support level.
   * @param minCount the absolute minimum support
   * @param sequences sequences data
   * @return array of frequent pattern ordered by their frequencies
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
   * @param frequentPrefixes frequent prefixes
   * @param sequences sequences data
   * @return prefixes and projected database
   */
  private def getPatternAndProjectedDatabase(
      frequentPrefixes: Array[Int],
      sequences: RDD[Array[Int]]): RDD[(Array[Int], Array[Int])] = {
    val filteredSequences = sequences.map { p =>
      p.filter (frequentPrefixes.contains(_) )
    }
    filteredSequences.flatMap { x =>
      frequentPrefixes.map { y =>
        val sub = getSuffix(y, x)
        (Array(y), sub)
      }
    }.filter(x => x._2.nonEmpty)
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

  /**
   * Find the patterns that it's length is one
   * @param minCount the absolute minimum support
   * @param sequences original sequences data
   * @return length-one patterns and projection table
   */
  private def findLengthOnePatterns(
      minCount: Long,
      sequences: RDD[Array[Int]]): (RDD[(Int, Long)], RDD[(Array[Int], Array[Int])]) = {
    val frequentLengthOnePatternAndCounts = getFreqItemAndCounts(minCount, sequences)
    val prefixAndProjectedDatabase = getPatternAndProjectedDatabase(
      frequentLengthOnePatternAndCounts.keys.collect(), sequences)
    (frequentLengthOnePatternAndCounts, prefixAndProjectedDatabase)
  }

  /**
   * Constructs prefix-projected databases from (prefix, suffix) pairs.
   * @param data patterns and projected sequences data before re-partition
   * @return patterns and projected sequences data after re-partition
   */
  private def makePrefixProjectedDatabases(
      data: RDD[(Array[Int], Array[Int])]): RDD[(Array[Int], Array[Array[Int]])] = {
    data.map(x => (x._1.toSeq, x._2))
      .groupByKey()
      .map(x => (x._1.toArray, x._2.toArray))
  }

  /**
   * calculate the patterns in local.
   * @param minCount the absolute minimum support
   * @param data patterns and projected sequences data data
   * @return patterns
   */
  private def getPatternsInLocal(
      minCount: Long,
      data: RDD[(Array[Int], Array[Array[Int]])]): RDD[(Array[Int], Long)] = {
    data.flatMap { x =>
      getPatternsWithPrefix(minCount, x._1, x._2)
    }
  }

  /**
   * calculate the patterns with one prefix in local.
   * @param minCount the absolute minimum support
   * @param prefix prefix
   * @param projectedDatabase patterns and projected sequences data
   * @return patterns
   */
  private def getPatternsWithPrefix(
      minCount: Long,
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
        .map(x => getPatternsWithPrefix(minCount, x._1, x._2))
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
  private def getSuffix(prefix: Int, sequence: Array[Int]): Array[Int] = {
    val index = sequence.indexOf(prefix)
    if (index == -1) {
      Array()
    } else {
      sequence.drop(index + 1)
    }
  }
}
