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

import java.{lang => jl, util => ju}
import java.util.concurrent.atomic.AtomicInteger

import scala.collection.mutable
import scala.collection.JavaConverters._
import scala.reflect.ClassTag

import org.apache.spark.Logging
import org.apache.spark.annotation.Experimental
import org.apache.spark.api.java.JavaRDD
import org.apache.spark.api.java.JavaSparkContext.fakeClassTag
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
  // TODO: make configurable with a better default value
  private val maxLocalProjDBSize: Long = 32000000L

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
   * Find the complete set of sequential patterns in the input sequences of itemsets.
   * @param data ordered sequences of itemsets.
   * @return a [[PrefixSpanModel]] that contains the frequent sequences
   */
  def run[Item: ClassTag](data: RDD[Array[Array[Item]]]): PrefixSpanModel[Item] = {
    if (data.getStorageLevel == StorageLevel.NONE) {
      logWarning("Input data is not cached.")
    }
    val minCount = math.ceil(minSupport * data.count()).toLong
    val freqItemAndCounts = data.flatMap { itemsets =>
        val uniqItems = mutable.Set.empty[Item]
        itemsets.foreach { _.foreach { item =>
          uniqItems += item
        }}
        uniqItems.toIterator.map((_, 1L))
      }.reduceByKey(_ + _)
      .filter { case (_, count) =>
        count >= minCount
      }.collect()
    val freqItems = freqItemAndCounts.sortBy(-_._2).map(_._1)
    val itemToInt = freqItems.zipWithIndex.toMap
    val dataInternalRepr = data.map { itemsets =>
      val allItems = mutable.ArrayBuilder.make[Int]
      allItems += 0
      itemsets.foreach { itemsets =>
        val items = mutable.ArrayBuilder.make[Int]
        itemsets.foreach { item =>
          if (itemToInt.contains(item)) {
            items += itemToInt(item) + 1
          }
        }
        val result = items.result()
        if (result.nonEmpty) {
          allItems ++= result.sorted
        }
        allItems += 0
      }
      allItems.result()
    }.persist(StorageLevel.MEMORY_AND_DISK)

    val results = genFreqPatterns(dataInternalRepr, minCount, maxPatternLength, maxLocalProjDBSize)

    def toPublicRepr(pattern: Array[Int]): Array[Array[Item]] = {
      val sequenceBuilder = mutable.ArrayBuilder.make[Array[Item]]
      val itemsetBuilder = mutable.ArrayBuilder.make[Item]
      val n = pattern.length
      var i = 1
      while (i < n) {
        val x = pattern(i)
        if (x == 0) {
          sequenceBuilder += itemsetBuilder.result()
          itemsetBuilder.clear()
        } else {
          itemsetBuilder += freqItems(x - 1)
        }
        i += 1
      }
      sequenceBuilder.result()
    }

    val freqSequences = results.map { case (seq: Array[Int], count: Long) =>
      new FreqSequence(toPublicRepr(seq), count)
    }
    new PrefixSpanModel(freqSequences)
  }

  /**
   * A Java-friendly version of [[run()]] that reads sequences from a [[JavaRDD]] and returns
   * frequent sequences in a [[PrefixSpanModel]].
   * @param data ordered sequences of itemsets stored as Java Iterable of Iterables
   * @tparam Item item type
   * @tparam Itemset itemset type, which is an Iterable of Items
   * @tparam Sequence sequence type, which is an Iterable of Itemsets
   * @return a [[PrefixSpanModel]] that contains the frequent sequences
   */
  def run[Item, Itemset <: jl.Iterable[Item], Sequence <: jl.Iterable[Itemset]](
      data: JavaRDD[Sequence]): PrefixSpanModel[Item] = {
    implicit val tag = fakeClassTag[Item]
    run(data.rdd.map(_.asScala.map(_.asScala.toArray).toArray))
  }

}

object PrefixSpan extends Logging {

  /**
   * Find the complete set of sequential patterns in the input sequences. This method utilizes
   * the internal representation of itemsets as Array[Int] where each itemset is represented by
   * a contiguous sequence of non-negative integers and delimiters represented by [[DELIMITER]].
   * @param data ordered sequences of itemsets. Items are represented by non-negative integers.
   *             Each itemset has one or more items and is delimited by [[DELIMITER]].
   * @return a set of sequential pattern pairs,
   *         the key of pair is pattern (a list of elements),
   *         the value of pair is the pattern's count.
   */
  def genFreqPatterns(
      data: RDD[Array[Int]],
      minCount: Long,
      maxPatternLength: Int,
      maxLocalProjDBSize: Long): RDD[(Array[Int], Long)] = {
    val sc = data.sparkContext

    if (data.getStorageLevel == StorageLevel.NONE) {
      logWarning("Input data is not cached.")
    }

    val suffices = data.map(items => new Suffix(items, 0, mutable.ArrayBuffer.empty))

    val localFreqPatterns = mutable.ArrayBuffer.empty[(Array[Int], Long)]
    val emptyPrefix = Prefix.empty
    var largePrefixes = mutable.Map(emptyPrefix.id -> emptyPrefix)
    val smallPrefixes = mutable.Map.empty[Int, Prefix]
    while (largePrefixes.nonEmpty) {
      val largePrefixArray = largePrefixes.values.toArray
      val splits = suffices.flatMap { suffix =>
        largePrefixArray.flatMap { prefix =>
          suffix.project(prefix).genSplitStats.map { case (item, suffixSize) =>
            ((prefix.id, item), (1L, suffixSize))
          }
        }
      }.reduceByKey { case ((c0, s0), (c1, s1)) =>
        (c0 + c1, s0 + s1)
      }.filter { case (_, (c, _)) => c >= minCount }
        .collect()
      val newLargePrefixes = mutable.Map.empty[Int, Prefix]
      splits.foreach { case ((id, item), (count, projDBSize)) =>
        val newPrefix = largePrefixes(id) :+ item
        localFreqPatterns += ((newPrefix.items :+ 0, count))
        if (newPrefix.length < maxPatternLength) {
          if (projDBSize > maxLocalProjDBSize) {
            newLargePrefixes += newPrefix.id -> newPrefix
          } else {
            smallPrefixes += newPrefix.id -> newPrefix
          }
        }
      }
      largePrefixes = newLargePrefixes
    }

    val bcSmallPrefixes = sc.broadcast(smallPrefixes)
    val distributedFreqPattern = suffices.flatMap { suffix =>
      bcSmallPrefixes.value.values.map { prefix =>
        (prefix.id, suffix.project(prefix).compressed)
      }.filter(_._2.nonEmpty)
    }.groupByKey().flatMap { case (id, projSuffixes) =>
      val prefix = bcSmallPrefixes.value(id)
      val localPrefixSpan = new LocalPrefixSpan(minCount, maxPatternLength - prefix.length)
      localPrefixSpan.run(projSuffixes.toArray).map { case (pattern, count) =>
        (prefix.items ++ pattern, count)
      }
    }
    sc.parallelize(localFreqPatterns, 1) ++ distributedFreqPattern
  }

  class Prefix(val items: Array[Int], val length: Int) extends Serializable {
    val id: Int = Prefix.nextId

    def :+(item: Int): Prefix = {
      require(item != 0)
      if (item < 0) {
        new Prefix(items :+ -item, length + 1)
      } else {
        new Prefix(items ++ Array(0, item), length + 1)
      }
    }
  }

  object Prefix {
    private val count = new AtomicInteger(-1)
    private def nextId: Int = count.incrementAndGet()
    val empty: Prefix = new Prefix(Array.empty, 0)
  }

  /**
   * A projected sequence. For example, a projected sequence of `[1, 2, 0, 1, 3]` with respect to 1
   * is stored as items = `[1, 2, 0, 1, 3]` and partialStarts = `[1, 4]`, which means [2, 0, 1, 3]
   * and [3].
   *
   * @param items item array that contains this sequence
   * @param partialStarts start indices of possible projections, strictly increasing
   */
  class Suffix(val items: Array[Int], val start: Int, val partialStarts: mutable.ArrayBuffer[Int]) extends Serializable {

    /**
     * Start index of the first full itemset contained in this sequence.
     */
    private[this] def fullStart: Int = {
      var i = start
      while (items(i) != 0) {
        i += 1
      }
      i
    }

    /**
     * Generates possible splits, where each split contains an item and its corresponding suffix.
     * There are two types of splits:
     *   a) the item can be assembled to the last itemset of the prefix, where we flip the sign,
     *   b) the item can be appended to the prefix.
     * @return an iterator of splits
     */
    def genSplitStats: Iterator[(Int, Int)] = {
      val n1 = items.length - 1
      // For each unique item (subject to sign) in this sequence, we output exact one split.
      val stats = mutable.Map.empty[Int, Int]
      // a) items that can be assembled to the last itemset of the prefix
      partialStarts.foreach { start =>
        var i = start
        var x = -items(i)
        while (x != 0) {
          if (!stats.contains(x)) {
            stats(x) = n1 - i
          }
          i += 1
          x = -items(i)
        }
      }
      // b) items that can be appended to the prefix
      var i = fullStart
      while (i < n1) {
        val x = items(i)
        if (x != 0 && !stats.contains(x)) {
          stats(x) = n1 - i
        }
        i += 1
      }
      stats.toIterator
    }

    /** Tests whether this sequence is non-empty. */
    def nonEmpty: Boolean = items.length > start + 1

    def project(item: Int): Suffix = {
      require(item != 0)
      var matched = false
      var newStart = items.length - 1
      val newPartialStarts = mutable.ArrayBuffer.empty[Int]
      if (item < 0) {
        val target = -item
        partialStarts.foreach { start =>
          var i = start
          var x = items(i)
          while (x != target && x != 0) {
            i += 1
            x = items(i)
          }
          if (x == target) {
            i += 1
            if (!matched) {
              newStart = i
              matched = false
            }
            if (items(i) != 0) {
              newPartialStarts += i
            }
          }
        }
      } else {
        val target = item
        var i = fullStart
        while (i < items.length - 1) {
          val x = items(i)
          if (x == target) {
            if (!matched) {
              newStart = i
              matched = true
            }
            if (items(i + 1) != 0) {
              newPartialStarts += i + 1
            }
          }
          i += 1
        }
      }
      new Suffix(items, newStart, newPartialStarts)
    }

    /**
     * Returns the same sequence with compressed storage if possible.
     */
    def compressed: Suffix = {
      if (start > 0) {
        new Suffix(items.slice(start, items.length), 0, partialStarts.map(_ - start))
      } else {
        this
      }
    }

    override def toString: String = compressed.items.mkString(",")

    /**
     * Projects a sequence of items.
     */
    def project(prefix: Array[Int]): Suffix = {
      var partial = true
      var cur = this
      var i = 0
      val np = prefix.length
      while (i < np && cur.nonEmpty) {
        val x = prefix(i)
        if (x == 0) {
          partial = false
        } else {
          if (partial) {
            cur = cur.project(-x)
          } else {
            cur = cur.project(x)
            partial = true
          }
        }
        i += 1
      }
      cur
    }

    def project(prefix: Prefix): Suffix = project(prefix.items)
  }

  object Suffix {
    val empty: Suffix = new Suffix(Array(0), 0, mutable.ArrayBuffer.empty)
  }


  /**
   * Represents a frequence sequence.
   * @param sequence a sequence of itemsets stored as an Array of Arrays
   * @param freq frequency
   * @tparam Item item type
   */
  class FreqSequence[Item](val sequence: Array[Array[Item]], val freq: Long) extends Serializable {
    /**
     * Returns sequence as a Java List of lists for Java users.
     */
    def javaSequence: ju.List[ju.List[Item]] = sequence.map(_.toList.asJava).toList.asJava
  }
}

/**
 * Model fitted by [[PrefixSpan]]
 * @param freqSequences frequent sequences
 * @tparam Item item type
 */
class PrefixSpanModel[Item](val freqSequences: RDD[PrefixSpan.FreqSequence[Item]])
  extends Serializable
