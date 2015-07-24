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
      database: Array[(Array[Int], Int)]): Iterator[(List[Int], Long)] = {
    if ((prefixes.nonEmpty && prefixes.filter(_ != -1).length == maxPatternLength) ||
      database.length < minCount) { return Iterator.empty }
    val frequentItemAndCounts = getFreqPrefixAndCounts(minCount, prefixes, database)
    frequentItemAndCounts.iterator.flatMap { case (prefix, count) =>
      val newProjected = project(database, prefix)
      Iterator.single((prefix, count)) ++
        run(minCount, maxPatternLength, prefix, newProjected)
    }
  }

  /**
   * Calculate suffix sequence immediately after the first occurrence of an item.
   * @param element the last element of prefix
   * @param sequenceAndFlag sequence to extract suffix from
   * @return suffix sequence
   */
  def getSuffix(
      element: Array[Int],
      sequenceAndFlag: (Array[Int], Int)): (Array[Int], Int) = {
    val (originalSequence, flag) = sequenceAndFlag
    val sequence =
      if (element.length > 1 && flag == 1) {
        element.take(element.length - 1) ++ originalSequence
      } else if (element.length == 1 && flag == 1) {
        val firstPosition =  originalSequence.indexOf(-1)
        if (firstPosition != -1) {
          originalSequence.drop(firstPosition + 1)
        } else {
          return (Array(), 0)
        }
      } else {
        originalSequence
      }
    var found = false
    var currentIndex = -1
    var nextIndex = 0
    while (nextIndex != -1 && !found) {
      nextIndex = sequence.indexOf(-1, currentIndex + 1)
      found = element.toSet.subsetOf(
        sequence.slice(currentIndex + 1, nextIndex).toSet)
      if (!found) currentIndex = nextIndex
    }
    if (found) {
      val itemPosition = sequence.indexOf(element.last, currentIndex)
      if (sequence.apply(itemPosition + 1) == -1) {
        (sequence.drop(itemPosition + 2), 0)
      } else {
        (sequence.drop(itemPosition + 1), 1)
      }
    } else {
      (Array(), 0)
    }
  }

  private def project(
      database: Array[(Array[Int], Int)],
      prefix: List[Int]): Array[(Array[Int], Int)] = {
    val lastElement = prefix.toArray.drop(prefix.lastIndexOf(-1) + 1)
    database
      .map(getSuffix(lastElement, _))
      .filter(_._1.nonEmpty)
  }

  /**
   * Generates frequent prefixes by filtering the input data using minimal count level.
   * @param minCount the minimum count for an item to be frequent
   * @param prefix prefix
   * @param suffixes suffixes
   * @return freq prefix to count map
   */
  private def getFreqPrefixAndCounts(
      minCount: Long,
      prefix: List[Int],
      suffixes: Array[(Array[Int], Int)]): mutable.Map[List[Int], Long] = {
    val counts = mutable.Map[List[Int], Long]().withDefaultValue(0L)
    val singleItemSet = suffixes.map { case (suffix, flag) =>
      if (flag == 0) suffix else suffix.drop(suffix.indexOf(-1) + 1)
    }.flatMap(_.filter(_ != -1).distinct)
      .groupBy(item => item).map(x => (x._1, x._2.length.toLong))
    singleItemSet.filter(_._2 >= minCount).foreach { case (item, count) =>
      if (prefix.nonEmpty) counts(prefix :+ -1 :+ item) = count else counts(List(item)) = count
    }
    if (prefix.nonEmpty) {
      val lastElement = prefix.drop(prefix.lastIndexOf(-1) + 1).toArray
      val multiItemSet = mutable.Map[Int, Long]().withDefaultValue(0L)
      suffixes.map { case (suffix, flag) =>
        if (flag == 0) suffix else lastElement ++ suffix
      }.foreach { suffix =>
        singleItemSet.keys.foreach { item =>
          if (!lastElement.contains(item)) {
            val element = lastElement :+ item
            if (isSubElement(suffix, element)) {
              multiItemSet(item) += 1L
            }
          }
        }
      }
      multiItemSet.filter(_._2 >= minCount).foreach { case (item, count) =>
        if (prefix.nonEmpty) {
          counts(prefix :+ item) = count
        } else {
          counts(List(item)) = count
        }
      }
   }
   counts
  }

  private def isSubElement(sequence: Array[Int], element: Array[Int]): Boolean = {
    var found = false
    var currentIndex = -1
    var nextIndex = 0
    while (nextIndex != -1 && !found) {
      nextIndex = sequence.indexOf(-1, currentIndex + 1)
      found = element.toSet.subsetOf(
        sequence.slice(currentIndex + 1, nextIndex).toSet)
      if (!found) currentIndex = nextIndex
    }
    found
  }
}
