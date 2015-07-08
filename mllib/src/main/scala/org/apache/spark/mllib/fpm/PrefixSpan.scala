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

import org.apache.spark.annotation.Experimental
import org.apache.spark.rdd.RDD

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
class PrefixSpan(
    private var minSupport: Double,
    private var maxPatternLength: Int) extends java.io.Serializable {

    private var absMinSupport: Int = 0

  /**
   * Constructs a default instance with default parameters
   * {minSupport: `0.1`, maxPatternLength: 10}.
   */
  def this() = this(0.1, 10)

  /**
   * Sets the minimal support level (default: `0.1`).
   */
  def setMinSupport(minSupport: Double): this.type = {
    this.minSupport = minSupport
    this
  }

  /**
   * Sets maximal pattern length.
   */
  def setMaxPatternLength(maxPatternLength: Int): this.type = {
    this.maxPatternLength = maxPatternLength
    this
  }

  /**
   * Calculate sequential patterns:
   * a) find and collect length-one patterns
   * b) for each length-one patterns and each sequence,
   *    emit (pattern (prefix), suffix sequence) as key-value pairs
   * c) group by key and then map value iterator to array
   * d) local PrefixSpan on each prefix
   * @return sequential patterns
   */
  def run(sequences: RDD[Array[Int]]): RDD[(Seq[Int], Int)] = {
    absMinSupport = getAbsoluteMinSupport(sequences)
    val (lengthOnePatternsAndCounts, prefixAndCandidates) =
      findLengthOnePatterns(sequences)
    val repartitionedRdd = makePrefixProjectedDatabases(prefixAndCandidates)
    val nextPatterns = getPatternsInLocal(repartitionedRdd)
    val allPatterns = lengthOnePatternsAndCounts.map(x => (Seq(x._1), x._2)) ++ nextPatterns
    allPatterns
  }

  private def getAbsoluteMinSupport(sequences: RDD[Array[Int]]): Int =  {
    val result = if (minSupport <= 0) {
      0
    }else {
      val count = sequences.count()
      val support = if (minSupport <= 1) minSupport else 1
      (support * count).toInt
    }
    result
  }

  /**
   * Find the patterns that it's length is one
   * @param sequences original sequences data
   * @return length-one patterns and projection table
   */
  private def findLengthOnePatterns(
      sequences: RDD[Array[Int]]): (RDD[(Int, Int)], RDD[(Seq[Int], Array[Int])]) = {
    val LengthOnePatternAndCounts = sequences
      .flatMap(_.distinct.map((_, 1)))
      .reduceByKey(_ + _)
    val infrequentLengthOnePatterns: Array[Int] = LengthOnePatternAndCounts
      .filter(_._2 < absMinSupport)
      .map(_._1)
      .collect()
    val frequentLengthOnePatterns = LengthOnePatternAndCounts
      .filter(_._2 >= absMinSupport)
    val frequentLengthOnePatternsArray = frequentLengthOnePatterns
      .map(_._1)
      .collect()
    val filteredSequences =
      if (infrequentLengthOnePatterns.isEmpty) {
        sequences
      } else {
        sequences.map { p =>
          p.filter { x => !infrequentLengthOnePatterns.contains(x) }
        }
      }
    val prefixAndCandidates = filteredSequences.flatMap { x =>
      frequentLengthOnePatternsArray.map { y =>
        val sub = getSuffix(y, x)
        (Seq(y), sub)
      }
    }.filter(x => x._2.nonEmpty)
    (frequentLengthOnePatterns, prefixAndCandidates)
  }

  /**
   * Re-partition the RDD data, to get better balance and performance.
   * @param data patterns and projected sequences data before re-partition
   * @return patterns and projected sequences data after re-partition
   */
  private def makePrefixProjectedDatabases(
      data: RDD[(Seq[Int], Array[Int])]): RDD[(Seq[Int], Array[Array[Int]])] = {
    val dataMerged = data
      .groupByKey()
      .mapValues(_.toArray)
    dataMerged
  }

  /**
   * calculate the patterns in local.
   * @param data patterns and projected sequences data data
   * @return patterns
   */
  private def getPatternsInLocal(
      data: RDD[(Seq[Int], Array[Array[Int]])]): RDD[(Seq[Int], Int)] = {
    val result = data.flatMap { x =>
      getPatternsWithPrefix(x._1, x._2)
    }
    result
  }

  /**
   * calculate the patterns with one prefix in local.
   * @param prefix prefix
   * @param projectedDatabase patterns and projected sequences data
   * @return patterns
   */
  private def getPatternsWithPrefix(
      prefix: Seq[Int],
      projectedDatabase: Array[Array[Int]]): Array[(Seq[Int], Int)] = {
    val prefixAndCounts = projectedDatabase
      .flatMap(_.distinct)
      .groupBy(x => x)
      .mapValues(_.length)
    val frequentPrefixExtensions = prefixAndCounts.filter(x => x._2 >= absMinSupport)
    val frequentPrefixesAndCounts = frequentPrefixExtensions
      .map(x => (prefix ++ Seq(x._1), x._2))
      .toArray
    val cleanedSearchSpace = projectedDatabase
      .map(x => x.filter(y => frequentPrefixExtensions.contains(y)))
    val prefixProjectedDatabases = frequentPrefixExtensions.map { x =>
      val sub = cleanedSearchSpace.map(y => getSuffix(x._1, y)).filter(_.nonEmpty)
      (prefix ++ Seq(x._1), sub)
    }.filter(x => x._2.nonEmpty)
      .toArray
    val continueProcess = prefixProjectedDatabases.nonEmpty && prefix.length + 1 < maxPatternLength
    if (continueProcess) {
      val nextPatterns = prefixProjectedDatabases
        .map(x => getPatternsWithPrefix(x._1, x._2))
        .reduce(_ ++ _)
      frequentPrefixesAndCounts ++ nextPatterns
    } else {
      frequentPrefixesAndCounts
    }
  }

  /**
   * calculate suffix sequence following a prefix in a sequence
   * @param prefix prefix
   * @param sequence original sequence
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
