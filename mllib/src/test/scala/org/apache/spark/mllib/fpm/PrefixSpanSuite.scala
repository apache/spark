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

import scala.language.existentials

import org.apache.spark.SparkFunSuite
import org.apache.spark.mllib.util.MLlibTestSparkContext
import org.apache.spark.util.Utils

class PrefixSpanSuite extends SparkFunSuite with MLlibTestSparkContext {

  test("PrefixSpan internal (integer seq, 0 delim) run, singleton itemsets") {

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
      Array(0, 1, 0, 3, 0, 4, 0, 5, 0),
      Array(0, 2, 0, 3, 0, 1, 0),
      Array(0, 2, 0, 4, 0, 1, 0),
      Array(0, 3, 0, 1, 0, 3, 0, 4, 0, 5, 0),
      Array(0, 3, 0, 4, 0, 4, 0, 3, 0),
      Array(0, 6, 0, 5, 0, 3, 0))

    val rdd = sc.parallelize(sequences, 2).cache()

    val result1 = PrefixSpan.genFreqPatterns(
      rdd, minCount = 2L, maxPatternLength = 50, maxLocalProjDBSize = 16L)
    val expectedValue1 = Array(
      (Array(0, 1, 0), 4L),
      (Array(0, 1, 0, 3, 0), 2L),
      (Array(0, 1, 0, 3, 0, 4, 0), 2L),
      (Array(0, 1, 0, 3, 0, 4, 0, 5, 0), 2L),
      (Array(0, 1, 0, 3, 0, 5, 0), 2L),
      (Array(0, 1, 0, 4, 0), 2L),
      (Array(0, 1, 0, 4, 0, 5, 0), 2L),
      (Array(0, 1, 0, 5, 0), 2L),
      (Array(0, 2, 0), 2L),
      (Array(0, 2, 0, 1, 0), 2L),
      (Array(0, 3, 0), 5L),
      (Array(0, 3, 0, 1, 0), 2L),
      (Array(0, 3, 0, 3, 0), 2L),
      (Array(0, 3, 0, 4, 0), 3L),
      (Array(0, 3, 0, 4, 0, 5, 0), 2L),
      (Array(0, 3, 0, 5, 0), 2L),
      (Array(0, 4, 0), 4L),
      (Array(0, 4, 0, 5, 0), 2L),
      (Array(0, 5, 0), 3L)
    )
    compareInternalResults(expectedValue1, result1.collect())

    val result2 = PrefixSpan.genFreqPatterns(
      rdd, minCount = 3, maxPatternLength = 50, maxLocalProjDBSize = 32L)
    val expectedValue2 = Array(
      (Array(0, 1, 0), 4L),
      (Array(0, 3, 0), 5L),
      (Array(0, 3, 0, 4, 0), 3L),
      (Array(0, 4, 0), 4L),
      (Array(0, 5, 0), 3L)
    )
    compareInternalResults(expectedValue2, result2.collect())

    val result3 = PrefixSpan.genFreqPatterns(
      rdd, minCount = 2, maxPatternLength = 2, maxLocalProjDBSize = 32L)
    val expectedValue3 = Array(
      (Array(0, 1, 0), 4L),
      (Array(0, 1, 0, 3, 0), 2L),
      (Array(0, 1, 0, 4, 0), 2L),
      (Array(0, 1, 0, 5, 0), 2L),
      (Array(0, 2, 0, 1, 0), 2L),
      (Array(0, 2, 0), 2L),
      (Array(0, 3, 0), 5L),
      (Array(0, 3, 0, 1, 0), 2L),
      (Array(0, 3, 0, 3, 0), 2L),
      (Array(0, 3, 0, 4, 0), 3L),
      (Array(0, 3, 0, 5, 0), 2L),
      (Array(0, 4, 0), 4L),
      (Array(0, 4, 0, 5, 0), 2L),
      (Array(0, 5, 0), 3L)
    )
    compareInternalResults(expectedValue3, result3.collect())
  }

  test("PrefixSpan internal (integer seq, -1 delim) run, variable-size itemsets") {
    val sequences = Array(
      Array(0, 1, 0, 1, 2, 3, 0, 1, 3, 0, 4, 0, 3, 6, 0),
      Array(0, 1, 4, 0, 3, 0, 2, 3, 0, 1, 5, 0),
      Array(0, 5, 6, 0, 1, 2, 0, 4, 6, 0, 3, 0, 2, 0),
      Array(0, 5, 0, 7, 0, 1, 6, 0, 3, 0, 2, 0, 3, 0))
    val rdd = sc.parallelize(sequences, 2).cache()
    val result = PrefixSpan.genFreqPatterns(
      rdd, minCount = 2, maxPatternLength = 5, maxLocalProjDBSize = 128L)

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
      (Array(0, 1, 0), 4L),
      (Array(0, 2, 0), 4L),
      (Array(0, 3, 0), 4L),
      (Array(0, 4, 0), 3L),
      (Array(0, 5, 0), 3L),
      (Array(0, 6, 0), 3L),
      (Array(0, 1, 0, 6, 0), 2L),
      (Array(0, 2, 0, 6, 0), 2L),
      (Array(0, 5, 0, 6, 0), 2L),
      (Array(0, 1, 2, 0, 6, 0), 2L),
      (Array(0, 1, 0, 4, 0), 2L),
      (Array(0, 2, 0, 4, 0), 2L),
      (Array(0, 1, 2, 0, 4, 0), 2L),
      (Array(0, 1, 0, 3, 0), 4L),
      (Array(0, 2, 0, 3, 0), 3L),
      (Array(0, 2, 3, 0), 2L),
      (Array(0, 3, 0, 3, 0), 3L),
      (Array(0, 4, 0, 3, 0), 3L),
      (Array(0, 5, 0, 3, 0), 2L),
      (Array(0, 6, 0, 3, 0), 2L),
      (Array(0, 5, 0, 6, 0, 3, 0), 2L),
      (Array(0, 6, 0, 2, 0, 3, 0), 2L),
      (Array(0, 5, 0, 2, 0, 3, 0), 2L),
      (Array(0, 5, 0, 1, 0, 3, 0), 2L),
      (Array(0, 2, 0, 4, 0, 3, 0), 2L),
      (Array(0, 1, 0, 4, 0, 3, 0), 2L),
      (Array(0, 1, 2, 0, 4, 0, 3, 0), 2L),
      (Array(0, 1, 0, 3, 0, 3, 0), 3L),
      (Array(0, 1, 2, 0, 3, 0), 2L),
      (Array(0, 1, 0, 2, 0, 3, 0), 2L),
      (Array(0, 1, 0, 2, 3, 0), 2L),
      (Array(0, 1, 0, 2, 0), 4L),
      (Array(0, 1, 2, 0), 2L),
      (Array(0, 3, 0, 2, 0), 3L),
      (Array(0, 4, 0, 2, 0), 2L),
      (Array(0, 5, 0, 2, 0), 2L),
      (Array(0, 6, 0, 2, 0), 2L),
      (Array(0, 5, 0, 6, 0, 2, 0), 2L),
      (Array(0, 6, 0, 3, 0, 2, 0), 2L),
      (Array(0, 5, 0, 3, 0, 2, 0), 2L),
      (Array(0, 5, 0, 1, 0, 2, 0), 2L),
      (Array(0, 4, 0, 3, 0, 2, 0), 2L),
      (Array(0, 1, 0, 3, 0, 2, 0), 3L),
      (Array(0, 5, 0, 6, 0, 3, 0, 2, 0), 2L),
      (Array(0, 5, 0, 1, 0, 3, 0, 2, 0), 2L),
      (Array(0, 1, 0, 1, 0), 2L),
      (Array(0, 2, 0, 1, 0), 2L),
      (Array(0, 3, 0, 1, 0), 2L),
      (Array(0, 5, 0, 1, 0), 2L),
      (Array(0, 2, 3, 0, 1, 0), 2L),
      (Array(0, 1, 0, 3, 0, 1, 0), 2L),
      (Array(0, 1, 0, 2, 3, 0, 1, 0), 2L),
      (Array(0, 1, 0, 2, 0, 1, 0), 2L))

    compareInternalResults(expectedValue, result.collect())
  }

  test("PrefixSpan projections with multiple partial starts") {
    val sequences = Seq(
      Array(Array(1, 2), Array(1, 2, 3)))
    val rdd = sc.parallelize(sequences, 2)
    val prefixSpan = new PrefixSpan()
      .setMinSupport(1.0)
      .setMaxPatternLength(2)
    val model = prefixSpan.run(rdd)
    val expected = Array(
      (Array(Array(1)), 1L),
      (Array(Array(1, 2)), 1L),
      (Array(Array(1), Array(1)), 1L),
      (Array(Array(1), Array(2)), 1L),
      (Array(Array(1), Array(3)), 1L),
      (Array(Array(1, 3)), 1L),
      (Array(Array(2)), 1L),
      (Array(Array(2, 3)), 1L),
      (Array(Array(2), Array(1)), 1L),
      (Array(Array(2), Array(2)), 1L),
      (Array(Array(2), Array(3)), 1L),
      (Array(Array(3)), 1L))
    compareResults(expected, model.freqSequences.collect())
  }

  test("PrefixSpan Integer type, variable-size itemsets") {
    val sequences = Seq(
      Array(Array(1, 2), Array(3)),
      Array(Array(1), Array(3, 2), Array(1, 2)),
      Array(Array(1, 2), Array(5)),
      Array(Array(6)))
    val rdd = sc.parallelize(sequences, 2).cache()

    val prefixSpan = new PrefixSpan()
      .setMinSupport(0.5)
      .setMaxPatternLength(5)

    /*
      To verify results, create file "prefixSpanSeqs2" with content
      (format = (transactionID, idxInTransaction, numItemsinItemset, itemset)):
        1 1 2 1 2
        1 2 1 3
        2 1 1 1
        2 2 2 3 2
        2 3 2 1 2
        3 1 2 1 2
        3 2 1 5
        4 1 1 6
      In R, run:
        library("arulesSequences")
        prefixSpanSeqs = read_baskets("prefixSpanSeqs", info = c("sequenceID","eventID","SIZE"))
        freqItemSeq = cspade(prefixSpanSeqs,
                             parameter = 0.5, maxlen = 5 ))
        resSeq = as(freqItemSeq, "data.frame")
        resSeq

           sequence support
        1     <{1}>    0.75
        2     <{2}>    0.75
        3     <{3}>    0.50
        4 <{1},{3}>    0.50
        5   <{1,2}>    0.75
     */

    val model = prefixSpan.run(rdd)
    val expected = Array(
      (Array(Array(1)), 3L),
      (Array(Array(2)), 3L),
      (Array(Array(3)), 2L),
      (Array(Array(1), Array(3)), 2L),
      (Array(Array(1, 2)), 3L)
    )
    compareResults(expected, model.freqSequences.collect())
  }

  test("PrefixSpan String type, variable-size itemsets") {
    // This is the same test as "PrefixSpan Int type, variable-size itemsets" except
    // mapped to Strings
    val intToString = (1 to 6).zip(Seq("a", "b", "c", "d", "e", "f")).toMap
    val sequences = Seq(
      Array(Array(1, 2), Array(3)),
      Array(Array(1), Array(3, 2), Array(1, 2)),
      Array(Array(1, 2), Array(5)),
      Array(Array(6))).map(seq => seq.map(itemSet => itemSet.map(intToString)))
    val rdd = sc.parallelize(sequences, 2).cache()

    val prefixSpan = new PrefixSpan()
      .setMinSupport(0.5)
      .setMaxPatternLength(5)

    val model = prefixSpan.run(rdd)
    val expected = Array(
      (Array(Array(1)), 3L),
      (Array(Array(2)), 3L),
      (Array(Array(3)), 2L),
      (Array(Array(1), Array(3)), 2L),
      (Array(Array(1, 2)), 3L)
    ).map { case (pattern, count) =>
      (pattern.map(itemSet => itemSet.map(intToString)), count)
    }
    compareResults(expected, model.freqSequences.collect())
  }

  test("PrefixSpan pre-processing's cleaning test") {

    // One item per itemSet
    val itemToInt1 = (4 to 5).zipWithIndex.toMap
    val sequences1 = Seq(
      Array(Array(4), Array(1), Array(2), Array(5), Array(2), Array(4), Array(5)),
      Array(Array(6), Array(7), Array(8)))
    val rdd1 = sc.parallelize(sequences1, 2).cache()

    val cleanedSequence1 = PrefixSpan.toDatabaseInternalRepr(rdd1, itemToInt1).collect()

    val expected1 = Array(Array(0, 4, 0, 5, 0, 4, 0, 5, 0))
      .map(_.map(x => if (x == 0) 0 else itemToInt1(x) + 1))

    compareInternalSequences(expected1, cleanedSequence1)

    // Multi-item sequence
    val itemToInt2 = (4 to 6).zipWithIndex.toMap
    val sequences2 = Seq(
      Array(Array(4, 5), Array(1, 6, 2), Array(2), Array(5), Array(2), Array(4), Array(5, 6, 7)),
      Array(Array(8, 9), Array(1, 2)))
    val rdd2 = sc.parallelize(sequences2, 2).cache()

    val cleanedSequence2 = PrefixSpan.toDatabaseInternalRepr(rdd2, itemToInt2).collect()

    val expected2 = Array(Array(0, 4, 5, 0, 6, 0, 5, 0, 4, 0, 5, 6, 0))
      .map(_.map(x => if (x == 0) 0 else itemToInt2(x) + 1))

    compareInternalSequences(expected2, cleanedSequence2)

    // Emptied sequence
    val itemToInt3 = (10 to 10).zipWithIndex.toMap
    val sequences3 = Seq(
      Array(Array(4, 5), Array(1, 6, 2), Array(2), Array(5), Array(2), Array(4), Array(5, 6, 7)),
      Array(Array(8, 9), Array(1, 2)))
    val rdd3 = sc.parallelize(sequences3, 2).cache()

    val cleanedSequence3 = PrefixSpan.toDatabaseInternalRepr(rdd3, itemToInt3).collect()
    val expected3 = Array[Array[Int]]()

    compareInternalSequences(expected3, cleanedSequence3)
  }

  test("model save/load") {
    val sequences = Seq(
      Array(Array(1, 2), Array(3)),
      Array(Array(1), Array(3, 2), Array(1, 2)),
      Array(Array(1, 2), Array(5)),
      Array(Array(6)))
    val rdd = sc.parallelize(sequences, 2).cache()

    val prefixSpan = new PrefixSpan()
      .setMinSupport(0.5)
      .setMaxPatternLength(5)
    val model = prefixSpan.run(rdd)

    val tempDir = Utils.createTempDir()
    val path = tempDir.toURI.toString
    try {
      model.save(sc, path)
      val newModel = PrefixSpanModel.load(sc, path)
      val originalSet = model.freqSequences.collect().map { x =>
        (x.sequence.map(_.toSet).toSeq, x.freq)
      }.toSet
      val newSet = newModel.freqSequences.collect().map { x =>
        (x.sequence.map(_.toSet).toSeq, x.freq)
      }.toSet
      assert(originalSet === newSet)
    } finally {
      Utils.deleteRecursively(tempDir)
    }
  }

  private def compareResults[Item](
      expectedValue: Array[(Array[Array[Item]], Long)],
      actualValue: Array[PrefixSpan.FreqSequence[Item]]): Unit = {
    val expectedSet = expectedValue.map { case (pattern: Array[Array[Item]], count: Long) =>
      (pattern.map(itemSet => itemSet.toSet).toSeq, count)
    }.toSet
    val actualSet = actualValue.map { x =>
      (x.sequence.map(_.toSet).toSeq, x.freq)
    }.toSet
    assert(expectedSet === actualSet)
  }

  private def compareInternalResults(
      expectedValue: Array[(Array[Int], Long)],
      actualValue: Array[(Array[Int], Long)]): Unit = {
    val expectedSet = expectedValue.map(x => (x._1.toSeq, x._2)).toSet
    val actualSet = actualValue.map(x => (x._1.toSeq, x._2)).toSet
    assert(expectedSet === actualSet)
  }

  private def compareInternalSequences(
      expectedValue: Array[Array[Int]],
      actualValue: Array[Array[Int]]): Unit = {
    val expectedSet = expectedValue.map(x => x.toSeq).toSet
    val actualSet = actualValue.map(x => x.toSeq).toSet
    assert(expectedSet === actualSet)
  }
}
