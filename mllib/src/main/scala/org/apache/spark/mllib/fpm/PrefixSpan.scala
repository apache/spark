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

import scala.collection.mutable.ArrayBuffer

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

  private val maxProjectedDBSizeBeforeLocalProcessing: Long = 10000

  /**
   * Constructs a default instance with default parameters
   * {minSupport: `0.1`, maxPatternLength: `10`}.
   */
  def this() = this(0.1, 10)

  /**
   * Get the minimal support (i.e. the frequency of occurrence before a pattern is considered
   * frequent).
   */
  def getMinSupport(): Double = this.minSupport

  /**
   * Sets the minimal support level (default: `0.1`).
   */
  def setMinSupport(minSupport: Double): this.type = {
    require(minSupport >= 0 && minSupport <= 1,
      "The minimum support value must be in [0, 1].")
    this.minSupport = minSupport
    this
  }

  /**
   * Gets the maximal pattern length (i.e. the length of the longest sequential pattern to consider.
   */
  def getMaxPatternLength(): Double = this.maxPatternLength

  /**
   * Sets maximal pattern length (default: `10`).
   */
  def setMaxPatternLength(maxPatternLength: Int): this.type = {
    // TODO: support unbounded pattern length when maxPatternLength = 0
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

    // Convert min support to a min number of transactions for this dataset
    val minCount = if (minSupport == 0) 0L else math.ceil(sequences.count() * minSupport).toLong

    val itemCounts = sequences
      .flatMap(seq => seq.distinct.map(item => (item, 1L)))
      .reduceByKey(_ + _)
      .filter(_._2 >= minCount)
    var allPatternAndCounts = itemCounts.map(x => (List(x._1), x._2))

    val prefixSuffixPairs = {
      val frequentItems = itemCounts.map(_._1).collect()
      val candidates = sequences.map { p =>
        p.filter (frequentItems.contains(_) )
      }
      candidates.flatMap { x =>
        frequentItems.map { y =>
          val sub = LocalPrefixSpan.getSuffix(y, x)
          (List(y), sub)
        }.filter(_._2.nonEmpty)
      }
    }
    var (smallPrefixSuffixPairs, largePrefixSuffixPairs) = splitPrefixSuffixPairs(prefixSuffixPairs)

    while (largePrefixSuffixPairs.count() != 0) {
      val (nextPatternAndCounts, nextPrefixSuffixPairs) =
        getPatternCountsAndPrefixSuffixPairs(minCount, largePrefixSuffixPairs)
      largePrefixSuffixPairs.unpersist()
      val (smallerPairsPart, largerPairsPart) = splitPrefixSuffixPairs(nextPrefixSuffixPairs)
      largePrefixSuffixPairs = largerPairsPart
      largePrefixSuffixPairs.persist(StorageLevel.MEMORY_AND_DISK)
      smallPrefixSuffixPairs ++= smallerPairsPart
      allPatternAndCounts ++= nextPatternAndCounts
    }

    if (smallPrefixSuffixPairs.count() > 0) {
      val projectedDatabase = smallPrefixSuffixPairs
        // TODO aggregateByKey
        .groupByKey()
        .mapValues(_.toArray)
      val nextPatternAndCounts = getPatternsInLocal(minCount, projectedDatabase)
      allPatternAndCounts ++= nextPatternAndCounts
    }
    allPatternAndCounts.map { case (pattern, count) => (pattern.toArray, count) }
  }


  /**
   * Split prefix suffix pairs to two parts:
   * Prefixes with projected databases smaller than maxSuffixesBeforeLocalProcessing and
   * Prefixes with projected databases larger than maxSuffixesBeforeLocalProcessing
   * @param prefixSuffixPairs prefix (length n) and suffix pairs,
   * @return small size prefix suffix pairs and big size prefix suffix pairs
   *         (RDD[prefix, suffix], RDD[prefix, suffix ])
   */
  private def splitPrefixSuffixPairs(
      prefixSuffixPairs: RDD[(List[Int], Array[Int])]):
  (RDD[(List[Int], Array[Int])], RDD[(List[Int], Array[Int])]) = {
    val prefixToSuffixSize = prefixSuffixPairs
      .aggregateByKey(0)(
        seqOp = { case (count, suffix) => count + suffix.length },
        combOp = { _ + _ })
    val smallPrefixes = prefixToSuffixSize
      .filter(_._2 <= maxProjectedDBSizeBeforeLocalProcessing)
      .map(_._1)
      .collect()
      .toSet
    val small = prefixSuffixPairs.filter { case (prefix, _) => smallPrefixes.contains(prefix) }
    val large = prefixSuffixPairs.filter { case (prefix, _) => !smallPrefixes.contains(prefix) }
    (small, large)
  }

  /**
   * Get the pattern and counts, and prefix suffix pairs
   * @param minCount minimum count
   * @param prefixSuffixPairs prefix (length n) and suffix pairs,
   * @return pattern (length n+1) and counts, and prefix (length n+1) and suffix pairs
   *         (RDD[pattern, count], RDD[prefix, suffix ])
   */
  private def getPatternCountsAndPrefixSuffixPairs(
      minCount: Long,
      prefixSuffixPairs: RDD[(List[Int], Array[Int])]):
  (RDD[(List[Int], Long)], RDD[(List[Int], Array[Int])]) = {
    val prefixAndFrequentItemAndCounts = prefixSuffixPairs
      .flatMap { case (prefix, suffix) => suffix.distinct.map(y => ((prefix, y), 1L)) }
      .reduceByKey(_ + _)
      .filter(_._2 >= minCount)
    val patternAndCounts = prefixAndFrequentItemAndCounts
      .map { case ((prefix, item), count) => (item :: prefix, count) }
    val prefixToFrequentNextItemsMap = prefixAndFrequentItemAndCounts
      .keys
      .groupByKey()
      .mapValues(_.toSet)
      .collect()
      .toMap
    val nextPrefixSuffixPairs = prefixSuffixPairs
      .filter(x => prefixToFrequentNextItemsMap.contains(x._1))
      .flatMap { case (prefix, suffix) =>
      val frequentNextItems = prefixToFrequentNextItemsMap(prefix)
      val filteredSuffix = suffix.filter(frequentNextItems.contains(_))
      frequentNextItems.flatMap { item =>
        val suffix = LocalPrefixSpan.getSuffix(item, filteredSuffix)
        if (suffix.isEmpty) None
        else Some(item :: prefix, suffix)
      }
    }
    (patternAndCounts, nextPrefixSuffixPairs)
  }

  /**
   * calculate the patterns in local.
   * @param minCount the absolute minimum count
   * @param data prefixes and projected sequences data data
   * @return patterns
   */
  private def getPatternsInLocal(
      minCount: Long,
      data: RDD[(List[Int], Array[Array[Int]])]): RDD[(List[Int], Long)] = {
    data.flatMap {
      case (prefix, projDB) =>
        LocalPrefixSpan.run(minCount, maxPatternLength, prefix.toList.reverse, projDB)
          .map { case (pattern: List[Int], count: Long) =>
          (pattern.reverse, count)
        }
    }
  }
}
