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
private[fpm] object LocalPrefixSpan extends Logging with Serializable {

  /**
   * Calculate all patterns of a projected database.
   * @param minCount minimum count
   * @param maxPatternLength maximum pattern length
   * @param prefixes prefixes in reversed order
   * @param database the projected database
   * @return a set of sequential pattern pairs,
   *         the key of pair is sequential pattern (a list of items in reversed order),
   *         the value of pair is the pattern's count.
   */
  def run(
      minCount: Long,
      maxPatternLength: Int,
      prefixes: List[Int],
      database: Iterable[Array[Int]]): Iterator[(List[Int], Long)] = {
    if (prefixes.count(_ != -1) == maxPatternLength || database.isEmpty) return Iterator.empty
    val frequentPrefixAndCounts = getFreqPrefixAndCounts(minCount, prefixes, database)
    frequentPrefixAndCounts.iterator.flatMap { case (prefix, count) =>
      val newProjected = project(database, prefix)
      Iterator.single((prefix, count)) ++
        run(minCount, maxPatternLength, prefix, newProjected)
    }
  }

  /**
   * Calculate suffix sequence immediately after the first occurrence of a prefix.
   * @param prefix prefix to get suffix after
   * @param sequence sequence to extract suffix from
   * @return suffix sequence
   */
  def getSuffix(prefix: List[Int], sequence: Array[Int]): (Boolean, Array[Int]) = {
    val element = getLastElement(prefix)
    if (sequence.apply(0) != -3) {
      if (element.length == 1) {
        getSingleItemElementSuffix(element, sequence)
      } else {
        getMultiItemsElementSuffix(element, sequence)
      }
    } else {
      if (element.length == 1) {
        val firstElemPos = sequence.indexOf(-1)
        if (firstElemPos == -1) {
          (false, Array())
        } else {
          getSingleItemElementSuffix(element, sequence.drop(firstElemPos + 1))
        }
      } else {
        val newSequence = element.take(element.length - 1) ++ sequence.drop(1)
        getMultiItemsElementSuffix(element, newSequence)
      }
    }
  }

  private def getLastElement(prefix: List[Int]): Array[Int] = {
    val pos = prefix.indexOf(-1)
    if (pos == -1) {
      prefix.reverse.toArray
    } else {
      prefix.take(pos).reverse.toArray
    }
  }

  private def getSingleItemElementSuffix(
      element: Array[Int],
      sequence: Array[Int]): (Boolean, Array[Int]) = {
    val index = sequence.indexOf(element.apply(0))
    if (index == -1) {
      (false, Array())
    } else if (index == sequence.length - 1) {
      (true, Array())
    } else if (sequence.apply(index + 1) == -1) {
      (true, sequence.drop(index + 2))
    } else {
      (true, -3 +: sequence.drop(index + 1))
    }
  }

  private def getMultiItemsElementSuffix(
      element: Array[Int],
      sequence: Array[Int]): (Boolean, Array[Int]) = {
    var seqPos = 0
    var found = false
    while (seqPos < sequence.length && !found) {
      var elemPos = 0
      while (!found && elemPos < element.length &&
        seqPos < sequence.length && sequence.apply(seqPos) != -1 ) {
        if (element.apply(elemPos) == sequence.apply(seqPos)) {
          elemPos += 1
          seqPos += 1
        } else {
          seqPos += 1
        }
        found = elemPos == element.length
      }
      if (!found) seqPos += 1
    }
    if (found) {
      if (sequence.apply(seqPos) == -1) {
        (true, sequence.drop(seqPos + 1))
      } else {
        (true, -3 +: sequence.drop(seqPos))
      }
    } else {
      (false, Array())
    }
  }

  def project(database: Iterable[Array[Int]], prefix: List[Int]): Iterable[Array[Int]] = {
    database
      .map(getSuffix(prefix, _)._2)
      .filter(_.nonEmpty)
  }

  /**
   * Generates frequent prefix by filtering the input data using minimal count level.
   * @param minCount the minimum count for an item to be frequent
   * @param prefix the minimum count for an item to be frequent
   * @param database database of sequences
   * @return freq item to count map
   */
  private def getFreqPrefixAndCounts(
      minCount: Long,
      prefix: List[Int],
      database: Iterable[Array[Int]]): mutable.Map[List[Int], Long] = {
    // TODO: use PrimitiveKeyOpenHashMap

    // get frequent items
    val freqItems = database
      .flatMap(_.distinct.filter(x => x != -1 && x != -3))
      .groupBy(x => x)
      .mapValues(_.size)
      .filter(_._2 >= minCount)
      .map(_._1)
    if (freqItems.isEmpty) return mutable.Map[List[Int], Long]()

    // get prefixes and counts
    val singleItemCounts = mutable.Map[Int, Long]().withDefaultValue(0L)
    val multiItemsCounts = mutable.Map[Int, Long]().withDefaultValue(0L)
    val prefixLastElement = getLastElement(prefix)
    database.foreach { sequence =>
      if (sequence.apply(0) != -3) {
        freqItems.foreach { item =>
          if (getSingleItemElementSuffix(Array(item), sequence)._1) {
            singleItemCounts(item) += 1
          }
          if (prefixLastElement.nonEmpty &&
            getMultiItemsElementSuffix(prefixLastElement :+ item, sequence)._1) {
            multiItemsCounts(item) += 1
          }
        }
      } else {
        val firstElemPos = sequence.indexOf(-1)
        if (firstElemPos != -1) {
          val newSequence = sequence.drop(firstElemPos + 1)
          freqItems.foreach { item =>
            if (getSingleItemElementSuffix(Array(item), newSequence)._1) {
              singleItemCounts(item) += 1
            }
          }
        }
        val newSequence = prefixLastElement ++ sequence.drop(1)
        freqItems.foreach { item =>
          if (prefixLastElement.nonEmpty &&
            getMultiItemsElementSuffix(prefixLastElement :+ item, newSequence)._1) {
            multiItemsCounts(item) += 1
          }
        }
      }
    }

    if (prefix.nonEmpty) {
      singleItemCounts.filter(_._2 >= minCount).map(x => (x._1 :: (-1 :: prefix), x._2)) ++
        multiItemsCounts.filter(_._2 >= minCount).map(x => (x._1 :: prefix, x._2))
    } else {
      singleItemCounts.filter(_._2 >= minCount).map(x => (List(x._1), x._2))
    }
  }
}
