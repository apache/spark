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
      prefixes: List[Array[Int]],
      database: Array[(Array[Array[Int]], Int)]): Iterator[(List[Array[Int]], Long)] = {
    if ((prefixes.nonEmpty && prefixes.map(_.length).sum == maxPatternLength) ||
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
      sequenceAndFlag: (Array[Array[Int]], Int)): (Array[Array[Int]], Int) = {
    val (originalSequence, flag) = sequenceAndFlag
    var index = 0
    if (element.length == 1 && flag == 1) index = 1
    val sequence =
      if (element.length > 1 && flag == 1) {
        (element.take(element.length - 1) ++ originalSequence.head) +: originalSequence.drop(1)
      } else {
        originalSequence
      }
    var found = false
    while (index < sequence.length && !found) {
      found = element.toSet.subsetOf(sequence.apply(index).toSet)
      index = index + 1
    }
    index = index - 1
    if (found) {
      val pos = sequence.apply(index).indexOf(element.apply(0))
      if (pos == sequence.apply(index).length - 1) {
        (sequence.drop(index + 1), 0)
      } else {
        (sequence.apply(index).drop(pos + 1) +: sequence.drop(index + 1), 1)
      }
    } else {
      (Array(), 0)
    }
  }

  private def project(
      database: Array[(Array[Array[Int]], Int)],
      prefix: List[Array[Int]]): Array[(Array[Array[Int]], Int)] = {
    database
      .map(getSuffix(prefix.last, _))
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
      prefix: List[Array[Int]],
      suffixes: Array[(Array[Array[Int]], Int)]): mutable.Map[List[Array[Int]], Long] = {
    val counts = mutable.Map[List[Array[Int]], Long]().withDefaultValue(0L)
    val singleItemSet = suffixes.map { case (suffix, flag) =>
      if (flag == 0) suffix else suffix.drop(1)
    }.flatMap { suffix =>
      suffix.flatMap(element => element).distinct
    }.groupBy(item => item).map(x => (x._1, x._2.length.toLong))
    singleItemSet.filter(_._2 >= minCount).foreach { case (item, count) =>
      counts(prefix :+ Array(item)) = count
    }
    if (prefix.nonEmpty) {
      val multiItemSet = mutable.Map[Int, Long]().withDefaultValue(0L)
      suffixes.map { case (suffix, flag) =>
        if (flag == 0) suffix else (prefix.last ++ suffix.head) +: suffix.drop(1)
      }.foreach { suffix =>
        singleItemSet.keys.foreach { item =>
          if (!prefix.last.contains(item)) {
            val element = prefix.last :+ item
            if (isSubElement(suffix, element)) {
              multiItemSet(item) += 1L
            }
          }
        }
      }
      multiItemSet.filter(_._2 >= minCount).foreach { case (item, count) =>
        if (prefix.nonEmpty) {
          counts(prefix.take(prefix.length - 1) :+ (prefix.last :+ item)) = count
        } else {
          counts(List(Array(item))) = count
        }
      }
   }
   counts
  }

  private def isSubElement(sequence: Array[Array[Int]], element: Array[Int]): Boolean = {
    var i = 0
    var found = false
    while (i < sequence.length && !found) {
      found = element.toSet.subsetOf(sequence.apply(i).toSet)
      i = i + 1
    }
    found
  }
}
