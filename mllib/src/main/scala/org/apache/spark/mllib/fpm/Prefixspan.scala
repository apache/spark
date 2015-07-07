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

import org.apache.spark.rdd.RDD

/**
 *
 * A parallel PrefixSpan algorithm to mine sequential pattern.
 * The PrefixSpan algorithm is described in
 * [[http://web.engr.illinois.edu/~hanj/pdf/span01.pdf]].
 *
 * @param sequences original sequences data
 * @param minSupport the minimal support level of the sequential pattern, any pattern appears
 *                   more than minSupport times will be output
 * @param maxPatternLength the maximal length of the sequential pattern, any pattern appears
 *                   less than maxPatternLength will be output
 *
 * @see [[https://en.wikipedia.org/wiki/Sequential_Pattern_Mining Sequential Pattern Mining
 *       (Wikipedia)]]
 */
class Prefixspan(
    val sequences: RDD[Array[Int]],
    val minSupport: Int = 2,
    val maxPatternLength: Int = 50) extends java.io.Serializable {

  /**
   * Calculate sequential patterns:
   * a) find and collect length-one patterns
   * b) for each length-one patterns and each sequence,
   *    emit (pattern (prefix), suffix sequence) as key-value pairs
   * c) group by key and then map value iterator to array
   * d) local PrefixSpan on each prefix
   * @return sequential patterns
   */
  def run(): RDD[(Seq[Int], Int)] = {
    val (patternsOneLength, prefixAndCandidates) = findPatternsLengthOne()
    val repartitionedRdd = repartitionSequences(prefixAndCandidates)
    val nextPatterns = getPatternsInLocal(repartitionedRdd)
    val allPatterns = patternsOneLength.map(x => (Seq(x._1), x._2)) ++ nextPatterns
    allPatterns
  }

  /**
   * Find the patterns that it's length is one
   * @return length-one patterns and projection table
   */
  private def findPatternsLengthOne(): (RDD[(Int, Int)], RDD[(Seq[Int], Array[Int])]) = {
    val patternsOneLength = sequences
      .map(_.distinct)
      .flatMap(p => p)
      .map((_, 1))
      .reduceByKey(_ + _)

    val removedElements: Array[Int] = patternsOneLength
      .filter(_._2 < minSupport)
      .map(_._1)
      .collect()

    val savedElements = patternsOneLength.filter(_._2 >= minSupport)

    val savedElementsArray = savedElements
      .map(_._1)
      .collect()

    val filteredSequences =
      if (removedElements.isEmpty) {
        sequences
      } else {
        sequences.map { p =>
          p.filter { x => !removedElements.contains(x) }
        }
      }

    val prefixAndCandidates = filteredSequences.flatMap { x =>
      savedElementsArray.map { y =>
        val sub = getSuffix(y, x)
        (Seq(y), sub)
      }
    }

    (savedElements, prefixAndCandidates)
  }

  /**
   * Re-partition the RDD data, to get better balance and performance.
   * @param data patterns and projected sequences data before re-partition
   * @return patterns and projected sequences data after re-partition
   */
  private def repartitionSequences(
      data: RDD[(Seq[Int], Array[Int])]): RDD[(Seq[Int], Array[Array[Int]])] = {
    val dataRemovedEmptyLine = data.filter(x => x._2.nonEmpty)
    val dataMerged = dataRemovedEmptyLine
      .groupByKey()
      .map(x => (x._1, x._2.toArray))
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
   * @param data patterns and projected sequences data
   * @return patterns
   */
  private def getPatternsWithPrefix(
      prefix: Seq[Int],
      data: Array[Array[Int]]): Array[(Seq[Int], Int)] = {
    val elements = data
      .map(x => x.distinct)
      .flatMap(x => x)
      .groupBy(x => x)
      .map(x => (x._1, x._2.length))

    val selectedSingleElements = elements.filter(x => x._2 >= minSupport)

    val selectedElements = selectedSingleElements
      .map(x => (prefix ++ Seq(x._1), x._2))
      .toArray

    val cleanedSearchSpace = data
      .map(x => x.filter(y => selectedSingleElements.contains(y)))

    val newSearchSpace = selectedSingleElements.map { x =>
      val sub = cleanedSearchSpace.map(y => getSuffix(x._1, y)).filter(_.nonEmpty)
      (prefix ++ Seq(x._1), sub)
    }.filter(x => x._2.nonEmpty)
      .toArray

    val continueProcess = newSearchSpace.nonEmpty && prefix.length + 1 < maxPatternLength

    if (continueProcess) {
      val nextPatterns = newSearchSpace
        .map(x => getPatternsWithPrefix(x._1, x._2))
        .reduce(_ ++ _)
      selectedElements ++ nextPatterns
    } else {
      selectedElements
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
      sequence.takeRight(sequence.length - index - 1)
    }
  }
}