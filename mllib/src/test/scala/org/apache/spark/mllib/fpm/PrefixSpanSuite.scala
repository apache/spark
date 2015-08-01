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

import org.apache.spark.SparkFunSuite
import org.apache.spark.mllib.util.MLlibTestSparkContext

class PrefixSpanSuite extends SparkFunSuite with MLlibTestSparkContext {

  test("PrefixSpan using Integer type, singleton itemsets") {

    /*
      library("arulesSequences")
      prefixSpanSeqs = read_baskets("prefixSpanSeqs", info = c("sequenceID","eventID","SIZE"))
      freqItemSeq = cspade(
        prefixSpanSeqs,
        parameter = list(support =
          2 / length(unique(transactionInfo(prefixSpanSeqs)$sequenceID)), maxlen = 2 ))
      resSeq = as(freqItemSeq, "data.frame")
      resSeq
    */

    val sequences = Array(
      Array(1, -1, 3, -1, 4, -1, 5),
      Array(2, -1, 3, -1, 1),
      Array(2, -1, 4, -1, 1),
      Array(3, -1, 1, -1, 3, -1, 4, -1, 5),
      Array(3, -1, 4, -1, 4, -1, 3),
      Array(6, -1, 5, -1, 3))

    val rdd = sc.parallelize(sequences, 2).cache()

    val prefixspan = new PrefixSpan()
      .setMinSupport(0.33)
      .setMaxPatternLength(50)
    val result1 = prefixspan.run(rdd)
    val expectedValue1 = Array(
      (Array(1), 4L),
      (Array(1, -1, 3), 2L),
      (Array(1, -1, 3, -1, 4), 2L),
      (Array(1, -1, 3, -1, 4, -1, 5), 2L),
      (Array(1, -1, 3, -1, 5), 2L),
      (Array(1, -1, 4), 2L),
      (Array(1, -1, 4, -1, 5), 2L),
      (Array(1, -1, 5), 2L),
      (Array(2), 2L),
      (Array(2, -1, 1), 2L),
      (Array(3), 5L),
      (Array(3, -1, 1), 2L),
      (Array(3, -1, 3), 2L),
      (Array(3, -1, 4), 3L),
      (Array(3, -1, 4, -1, 5), 2L),
      (Array(3, -1, 5), 2L),
      (Array(4), 4L),
      (Array(4, -1, 5), 2L),
      (Array(5), 3L)
    )
    compareResults(expectedValue1, result1.collect())

    prefixspan.setMinSupport(0.5).setMaxPatternLength(50)
    val result2 = prefixspan.run(rdd)
    val expectedValue2 = Array(
      (Array(1), 4L),
      (Array(3), 5L),
      (Array(3, -1, 4), 3L),
      (Array(4), 4L),
      (Array(5), 3L)
    )
    compareResults(expectedValue2, result2.collect())

    prefixspan.setMinSupport(0.33).setMaxPatternLength(2)
    val result3 = prefixspan.run(rdd)
    val expectedValue3 = Array(
      (Array(1), 4L),
      (Array(1, -1, 3), 2L),
      (Array(1, -1, 4), 2L),
      (Array(1, -1, 5), 2L),
      (Array(2, -1, 1), 2L),
      (Array(2), 2L),
      (Array(3), 5L),
      (Array(3, -1, 1), 2L),
      (Array(3, -1, 3), 2L),
      (Array(3, -1, 4), 3L),
      (Array(3, -1, 5), 2L),
      (Array(4), 4L),
      (Array(4, -1, 5), 2L),
      (Array(5), 3L)
    )
    compareResults(expectedValue3, result3.collect())
  }

  test("PrefixSpan using Integer type, variable-size itemsets") {
    val sequences = Array(
      Array(1, -1, 1, 2, 3, -1, 1, 3, -1, 4, -1, 3, 6),
      Array(1, 4, -1, 3, -1, 2, 3, -1, 1, 5),
      Array(5, 6, -1, 1, 2, -1, 4, 6, -1, 3, -1, 2),
      Array(5, -1, 7, -1, 1, 6, -1, 3, -1, 2, -1, 3))
    val rdd = sc.parallelize(sequences, 2).cache()
    val prefixspan = new PrefixSpan().setMinSupport(0.5).setMaxPatternLength(5)
    val result = prefixspan.run(rdd)

    /*
      To verify results, create file "prefixSpanSeqs" with content
      (format = (transactionID, idxInTransaction, numItemsinItemset, itemset)):
        1 1 1 1
        1 2 3 1 2 3
        1 3 2 1 3
        1 4 1 4
        1 5 2 3 6
        2 1 2 1 4
        2 2 1 3
        2 3 2 2 3
        2 4 2 1 5
        3 1 2 5 6
        3 2 2 1 2
        3 3 2 4 6
        3 4 1 3
        3 5 1 2
        4 1 1 5
        4 2 1 7
        4 3 2 1 6
        4 4 1 3
        4 5 1 2
        4 6 1 3
      In R, run:
        library("arulesSequences")
        prefixSpanSeqs = read_baskets("prefixSpanSeqs", info = c("sequenceID","eventID","SIZE"))
        freqItemSeq = cspade(prefixSpanSeqs,
                             parameter = list(support = 0.5, maxlen = 5 ))
        resSeq = as(freqItemSeq, "data.frame")
        resSeq

                    sequence support
        1              <{1}>    1.00
        2              <{2}>    1.00
        3              <{3}>    1.00
        4              <{4}>    0.75
        5              <{5}>    0.75
        6              <{6}>    0.75
        7          <{1},{6}>    0.50
        8          <{2},{6}>    0.50
        9          <{5},{6}>    0.50
        10       <{1,2},{6}>    0.50
        11         <{1},{4}>    0.50
        12         <{2},{4}>    0.50
        13       <{1,2},{4}>    0.50
        14         <{1},{3}>    1.00
        15         <{2},{3}>    0.75
        16           <{2,3}>    0.50
        17         <{3},{3}>    0.75
        18         <{4},{3}>    0.75
        19         <{5},{3}>    0.50
        20         <{6},{3}>    0.50
        21     <{5},{6},{3}>    0.50
        22     <{6},{2},{3}>    0.50
        23     <{5},{2},{3}>    0.50
        24     <{5},{1},{3}>    0.50
        25     <{2},{4},{3}>    0.50
        26     <{1},{4},{3}>    0.50
        27   <{1,2},{4},{3}>    0.50
        28     <{1},{3},{3}>    0.75
        29       <{1,2},{3}>    0.50
        30     <{1},{2},{3}>    0.50
        31       <{1},{2,3}>    0.50
        32         <{1},{2}>    1.00
        33           <{1,2}>    0.50
        34         <{3},{2}>    0.75
        35         <{4},{2}>    0.50
        36         <{5},{2}>    0.50
        37         <{6},{2}>    0.50
        38     <{5},{6},{2}>    0.50
        39     <{6},{3},{2}>    0.50
        40     <{5},{3},{2}>    0.50
        41     <{5},{1},{2}>    0.50
        42     <{4},{3},{2}>    0.50
        43     <{1},{3},{2}>    0.75
        44 <{5},{6},{3},{2}>    0.50
        45 <{5},{1},{3},{2}>    0.50
        46         <{1},{1}>    0.50
        47         <{2},{1}>    0.50
        48         <{3},{1}>    0.50
        49         <{5},{1}>    0.50
        50       <{2,3},{1}>    0.50
        51     <{1},{3},{1}>    0.50
        52   <{1},{2,3},{1}>    0.50
        53     <{1},{2},{1}>    0.50
     */
    val expectedValue = Array(
      (Array(1), 4L),
      (Array(2), 4L),
      (Array(3), 4L),
      (Array(4), 3L),
      (Array(5), 3L),
      (Array(6), 3L),
      (Array(1, -1, 6), 2L),
      (Array(2, -1, 6), 2L),
      (Array(5, -1, 6), 2L),
      (Array(1, 2, -1, 6), 2L),
      (Array(1, -1, 4), 2L),
      (Array(2, -1, 4), 2L),
      (Array(1, 2, -1, 4), 2L),
      (Array(1, -1, 3), 4L),
      (Array(2, -1, 3), 3L),
      (Array(2, 3), 2L),
      (Array(3, -1, 3), 3L),
      (Array(4, -1, 3), 3L),
      (Array(5, -1, 3), 2L),
      (Array(6, -1, 3), 2L),
      (Array(5, -1, 6, -1, 3), 2L),
      (Array(6, -1, 2, -1, 3), 2L),
      (Array(5, -1, 2, -1, 3), 2L),
      (Array(5, -1, 1, -1, 3), 2L),
      (Array(2, -1, 4, -1, 3), 2L),
      (Array(1, -1, 4, -1, 3), 2L),
      (Array(1, 2, -1, 4, -1, 3), 2L),
      (Array(1, -1, 3, -1, 3), 3L),
      (Array(1, 2, -1, 3), 2L),
      (Array(1, -1, 2, -1, 3), 2L),
      (Array(1, -1, 2, 3), 2L),
      (Array(1, -1, 2), 4L),
      (Array(1, 2), 2L),
      (Array(3, -1, 2), 3L),
      (Array(4, -1, 2), 2L),
      (Array(5, -1, 2), 2L),
      (Array(6, -1, 2), 2L),
      (Array(5, -1, 6, -1, 2), 2L),
      (Array(6, -1, 3, -1, 2), 2L),
      (Array(5, -1, 3, -1, 2), 2L),
      (Array(5, -1, 1, -1, 2), 2L),
      (Array(4, -1, 3, -1, 2), 2L),
      (Array(1, -1, 3, -1, 2), 3L),
      (Array(5, -1, 6, -1, 3, -1, 2), 2L),
      (Array(5, -1, 1, -1, 3, -1, 2), 2L),
      (Array(1, -1, 1), 2L),
      (Array(2, -1, 1), 2L),
      (Array(3, -1, 1), 2L),
      (Array(5, -1, 1), 2L),
      (Array(2, 3, -1, 1), 2L),
      (Array(1, -1, 3, -1, 1), 2L),
      (Array(1, -1, 2, 3, -1, 1), 2L),
      (Array(1, -1, 2, -1, 1), 2L))

    compareResults(expectedValue, result.collect())
  }

  private def compareResults(
      expectedValue: Array[(Array[Int], Long)],
      actualValue: Array[(Array[Int], Long)]): Unit = {
    val expectedSet = expectedValue.map(x => (x._1.toSeq, x._2)).toSet
    val actualSet = actualValue.map(x => (x._1.toSeq, x._2)).toSet
    assert(expectedSet === actualSet)
  }

  private def insertDelimiter(sequence: Array[Int]): Array[Int] = {
    sequence.zip(Seq.fill(sequence.length)(PrefixSpan.DELIMITER)).map { case (a, b) =>
      List(a, b)
    }.flatten
  }

}
