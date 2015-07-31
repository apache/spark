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

  test("PrefixSpan using Integer type") {

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
      Array(1, 3, 4, 5),
      Array(2, 3, 1),
      Array(2, 4, 1),
      Array(3, 1, 3, 4, 5),
      Array(3, 4, 4, 3),
      Array(6, 5, 3)).map(insertDelimiter)

    val rdd = sc.parallelize(sequences, 2).cache()

    val prefixspan = new PrefixSpan()
      .setMinSupport(0.33)
      .setMaxPatternLength(50)
    val result1 = prefixspan.run(rdd)
    val expectedValue1 = Array(
      (Array(1), 4L),
      (Array(1, 3), 2L),
      (Array(1, 3, 4), 2L),
      (Array(1, 3, 4, 5), 2L),
      (Array(1, 3, 5), 2L),
      (Array(1, 4), 2L),
      (Array(1, 4, 5), 2L),
      (Array(1, 5), 2L),
      (Array(2), 2L),
      (Array(2, 1), 2L),
      (Array(3), 5L),
      (Array(3, 1), 2L),
      (Array(3, 3), 2L),
      (Array(3, 4), 3L),
      (Array(3, 4, 5), 2L),
      (Array(3, 5), 2L),
      (Array(4), 4L),
      (Array(4, 5), 2L),
      (Array(5), 3L)
    ).map { case (seq, count) => (insertDelimiter(seq), count) }
    compareResults(expectedValue1, result1.collect())

    prefixspan.setMinSupport(0.5).setMaxPatternLength(50)
    val result2 = prefixspan.run(rdd)
    val expectedValue2 = Array(
      (Array(1), 4L),
      (Array(3), 5L),
      (Array(3, 4), 3L),
      (Array(4), 4L),
      (Array(5), 3L)
    ).map { case (seq, count) => (insertDelimiter(seq), count) }
    compareResults(expectedValue2, result2.collect())

    prefixspan.setMinSupport(0.33).setMaxPatternLength(2)
    val result3 = prefixspan.run(rdd)
    val expectedValue3 = Array(
      (Array(1), 4L),
      (Array(1, 3), 2L),
      (Array(1, 4), 2L),
      (Array(1, 5), 2L),
      (Array(2, 1), 2L),
      (Array(2), 2L),
      (Array(3), 5L),
      (Array(3, 1), 2L),
      (Array(3, 3), 2L),
      (Array(3, 4), 3L),
      (Array(3, 5), 2L),
      (Array(4), 4L),
      (Array(4, 5), 2L),
      (Array(5), 3L)
    ).map { case (seq, count) => (insertDelimiter(seq), count) }
    compareResults(expectedValue3, result3.collect())
  }

  test("PrefixSpan non-temporal sequences") {
    val sequences = Array(
      "a,abc,ac,d,cf",
      "ad,c,bc,ae",
      "ef,ab,df,c,b",
      "e,g,af,c,b,c")
    val coder = Array('a', 'b', 'c', 'd', 'e', 'f', 'g').zip(Array(1, 2, 3, 4, 5, 6, 7)).toMap
    val intSequences = sequences.map(_.split(",").flatMap(_.toArray.map(coder) :+ -1))
    val data = sc.parallelize(intSequences, 2).cache()

    val expectedValue4 = Array(
      "a:4",
      "b:4",
      "c:4",
      "d:3",
      "e:3",
      "f:3",
      "a,a:2",
      "a,b:4",
      "a,bc:2",
      "a,bc,a:2",
      "a,b,a:2",
      "a,b,c:2",
      "ab:2",
      "ab,c:2",
      "ab,d:2",
      "ab,d,c:2",
      "ab,f:2",
      "a,c:4",
      "a,c,a:2",
      "a,c,b:3",
      "a,c,c:3",
      "a,d:2",
      "a,d,c:2",
      "a,f:2",
      "b,a:2",
      "b,c:3",
      "bc:2",
      "bc,a:2",
      "b,d:2",
      "b,d,c:2",
      "b,f:2",
      "c,a:2",
      "c,b:3",
      "c,c:3",
      "d,b:2",
      "d,c:3",
      "d,c,b:2",
      "e,a:2",
      "e,a,b:2",
      "e,a,c:2",
      "e,a,c,b:2",
      "e,b:2",
      "e,b,c:2",
      "e,c:2",
      "e,c,b:2",
      "e,f:2",
      "e,f,b:2",
      "e,f,c:2",
      "e,f,c,b:2",
      "f,b:2",
      "f,b,c:2",
      "f,c:2",
      "f,c,b:2")
    val intExpectedValue = expectedValue4
      .map(_.split(":"))
      .map { x =>
        (x(0).split(",").flatMap(_.toArray.sorted.map(coder) :+ -1), x(1).toLong)
      }

    val prefixspan = new PrefixSpan()
      .setMinSupport(0.5)
      .setMaxPatternLength(5)

    val results = prefixspan.run(data)
    compareResults(intExpectedValue, results.collect())
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
