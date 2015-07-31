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
  import PrefixSpan._

  /**
   * The maximum number of items allowed in a projected database before local processing. If a
   * projected database exceeds this size, another iteration of distributed PrefixSpan is run.
   */
  // TODO: make configurable with a better default value, 10000 may be too small
  private val maxLocalProjDBSize: Long = 10000

  /**
   * Constructs a default instance with default parameters
   * {minSupport: `0.1`, maxPatternLength: `10`}.
   */
  def this() = this(0.1, 10)

  /**
   * Get the minimal support (i.e. the frequency of occurrence before a pattern is considered
   * frequent).
   */
  def getMinSupport: Double = this.minSupport

  /**
   * Sets the minimal support level (default: `0.1`).
   */
  def setMinSupport(minSupport: Double): this.type = {
    require(minSupport >= 0 && minSupport <= 1, "The minimum support value must be in [0, 1].")
    this.minSupport = minSupport
    this
  }

  /**
   * Gets the maximal pattern length (i.e. the length of the longest sequential pattern to consider.
   */
  def getMaxPatternLength: Double = this.maxPatternLength

  /**
   * Sets maximal pattern length (default: `10`).
   */
  def setMaxPatternLength(maxPatternLength: Int): this.type = {
    // TODO: support unbounded pattern length when maxPatternLength = 0
    require(maxPatternLength >= 1, "The maximum pattern length value must be greater than 0.")
    this.maxPatternLength = maxPatternLength
    this
  }

  /**
   * Find the complete set of sequential patterns in the input sequences.
   * @param data ordered sequences of itemsets. Items are represented by non-negative integers.
   *                  Each itemset has one or more items and is delimited by [[DELIMITER]].
   * @return a set of sequential pattern pairs,
   *         the key of pair is pattern (a list of elements),
   *         the value of pair is the pattern's count.
   */
  // TODO: generalize to arbitrary item-types and use mapping to Ints for internal algorithm
  def run(data: RDD[Array[Int]]): RDD[(Array[Int], Long)] = {
    val sc = data.sparkContext

    if (data.getStorageLevel == StorageLevel.NONE) {
      logWarning("Input data is not cached.")
    }
    val sequences = data.map(_.toList)

    // Convert min support to a min number of transactions for this dataset
    val minCount = if (minSupport == 0) 0L else math.ceil(sequences.count() * minSupport).toLong

    // (Frequent items -> number of occurrences, all items here satisfy the `minSupport` threshold
    val freqItemCounts = sequences
      .flatMap(seq => splitAtDelimiter(seq).distinct.map(item => (item, 1L)))
      .reduceByKey(_ + _)
      .filter { case (item, count) => (count >= minCount) }
      .collect()
      .toList

    // Pairs of (length 1 prefix, suffix consisting of frequent items)
    val itemSuffixPairs = {
      val freqItems = freqItemCounts.map(_._1).toSet
      sequences.flatMap { seq =>
        val filteredSeq = insertDelimiters(
          splitAtDelimiter(seq).filter(item => freqItems.contains(item)))
        freqItems.flatMap { item =>
          val candidateSuffix = LocalPrefixSpan.getSuffix(item, filteredSeq)
          candidateSuffix match {
            case suffix if !suffix.isEmpty => Some((DELIMITER :: item, suffix))
            case _ => None
          }
        }
      }
    }
    // Accumulator for the computed results to be returned, initialized to the frequent items (i.e.
    // frequent length-one prefixes)
    var resultsAccumulator = freqItemCounts.map(x => (DELIMITER :: x._1, x._2))

    // Remaining work to be locally and distributively processed respectfully
    var (pairsForLocal, pairsForDistributed) = partitionByProjDBSize(itemSuffixPairs)

    // Continue processing until no pairs for distributed processing remain (i.e. all prefixes have
    // projected database sizes <= `maxLocalProjDBSize`) or `maxPatternLength` is reached
    var patternLength = 1
    while (pairsForDistributed.count() != 0 && patternLength < maxPatternLength) {
      val (nextPatternAndCounts, nextPrefixSuffixPairs) =
        extendPrefixes(minCount, pairsForDistributed)
      pairsForDistributed.unpersist()
      val (smallerPairsPart, largerPairsPart) = partitionByProjDBSize(nextPrefixSuffixPairs)
      pairsForDistributed = largerPairsPart
      pairsForDistributed.persist(StorageLevel.MEMORY_AND_DISK)
      pairsForLocal ++= smallerPairsPart
      resultsAccumulator ++= nextPatternAndCounts.collect()
      patternLength += 1 // pattern length grows one per iteration
    }

    // Process the small projected databases locally
    val remainingResults = getPatternsInLocal(
      minCount, sc.parallelize(pairsForLocal, 1).groupByKey())

    (sc.parallelize(resultsAccumulator, 1) ++ remainingResults)
      .map { case (pattern, count) => (pattern.reverse.toArray, count) }
  }


  /**
   * Partitions the prefix-suffix pairs by projected database size.
   * @param prefixSuffixPairs prefix (length n) and suffix pairs,
   * @return prefix-suffix pairs partitioned by whether their projected database size is <= or
   *         greater than [[maxLocalProjDBSize]]
   */
  private def partitionByProjDBSize(prefixSuffixPairs: RDD[(List[Int], List[Int])])
    : (List[(List[Int], List[Int])], RDD[(List[Int], List[Int])]) = {
    val prefixToSuffixSize = prefixSuffixPairs
      .aggregateByKey(0)(
        seqOp = { case (count, suffix) => count + suffix.length },
        combOp = { _ + _ })
    val smallPrefixes = prefixToSuffixSize
      .filter(_._2 <= maxLocalProjDBSize)
      .keys
      .collect()
      .toSet
    val small = prefixSuffixPairs.filter { case (prefix, _) => smallPrefixes.contains(prefix) }
    val large = prefixSuffixPairs.filter { case (prefix, _) => !smallPrefixes.contains(prefix) }
    (small.collect().toList, large)
  }

  /**
   * Extends all prefixes by one itemset from their suffix and computes the resulting frequent
   * prefixes and remaining work.
   * @param minCount minimum count
   * @param prefixSuffixPairs prefix (length N) and suffix pairs,
   * @return (frequent length N+1 extended prefix, count) pairs and (frequent length N+1 extended
   *         prefix, corresponding suffix) pairs.
   */
  private def extendPrefixes(
      minCount: Long,
      prefixSuffixPairs: RDD[(List[Int], List[Int])])
    : (RDD[(List[Int], Long)], RDD[(List[Int], List[Int])]) = {

    // (length N prefix, itemset from suffix) pairs and their corresponding number of occurrences
    // Every (prefix :+ suffix) is guaranteed to have support exceeding `minSupport`
    val prefixItemPairAndCounts = prefixSuffixPairs
      .flatMap { case (prefix, suffix) =>
        splitAtDelimiter(suffix).distinct.map(y => ((prefix, y), 1L))
      }
      .reduceByKey(_ + _)
      .filter { case (item, count) => (count >= minCount) }

    // Map from prefix to set of possible next items from suffix
    val prefixToNextItems = prefixItemPairAndCounts
      .keys
      .groupByKey()
      .mapValues(_.toSet)
      .collect()
      .toMap

    // Frequent patterns with length N+1 and their corresponding counts
    val extendedPrefixAndCounts = prefixItemPairAndCounts
      .map { case ((prefix, item), count) => (DELIMITER :: item ::: prefix, count) }

    // Remaining work, all prefixes will have length N+1
    val extendedPrefixAndSuffix = prefixSuffixPairs
      .filter(x => prefixToNextItems.contains(x._1))
      .flatMap { case (prefix, suffix) =>
        val frequentNextItems = prefixToNextItems(prefix)
        val filteredSuffix = insertDelimiters(
          splitAtDelimiter(suffix).filter(frequentNextItems.contains(_)))
        frequentNextItems.flatMap { item =>
          LocalPrefixSpan.getSuffix(item, filteredSuffix) match {
            case suffix if !suffix.isEmpty => Some(DELIMITER :: item ::: prefix, suffix)
            case _ => None
          }
        }
      }

    (extendedPrefixAndCounts, extendedPrefixAndSuffix)
  }

  /**
   * Calculate the patterns in local.
   * @param minCount the absolute minimum count
   * @param data prefixes and projected sequences data data
   * @return patterns
   */
  private def getPatternsInLocal(
      minCount: Long,
      data: RDD[(List[Int], Iterable[List[Int]])]): RDD[(List[Int], Long)] = {
    data.flatMap {
      case (prefix, projDB) =>
        LocalPrefixSpan.run(minCount, maxPatternLength, prefix, projDB)
          .map { case (pattern: List[Int], count: Long) =>
          (pattern, count)
        }
    }
  }

}

object PrefixSpan {
  private[fpm] val DELIMITER = -1

  private[fpm] def splitAtDelimiter(pattern: List[Int]): List[List[Int]] = {
    pattern.span(_ != DELIMITER) match {
      case (x, xs) if xs.length > 1 => x :: splitAtDelimiter(xs.tail)
      case (x, xs) => List(x)
    }
  }

  private[fpm] def insertDelimiters(sequence: List[List[Int]]): List[Int] = {
    // TODO: avoid allocating new arrays when appending
    sequence.zip(Seq.fill(sequence.size)(PrefixSpan.DELIMITER))
      .flatMap { case (a: List[Int], b: Int) =>
        a :+ b
      }
  }
}
