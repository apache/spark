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
package org.apache.spark.ml.fpm

import org.apache.spark.ml.util.MLTest
import org.apache.spark.sql.DataFrame

class PrefixSpanSuite extends MLTest {

  import testImplicits._

  override def beforeAll(): Unit = {
    super.beforeAll()
  }

  test("PrefixSpan projections with multiple partial starts") {
    val smallDataset = Seq(Seq(Seq(1, 2), Seq(1, 2, 3))).toDF("sequence")
    val result = new PrefixSpan()
      .setMinSupport(1.0)
      .setMaxPatternLength(2)
      .setMaxLocalProjDBSize(32000000)
      .findFrequentSequentialPatterns(smallDataset)
      .as[(Seq[Seq[Int]], Long)].collect()
    val expected = Array(
      (Seq(Seq(1)), 1L),
      (Seq(Seq(1, 2)), 1L),
      (Seq(Seq(1), Seq(1)), 1L),
      (Seq(Seq(1), Seq(2)), 1L),
      (Seq(Seq(1), Seq(3)), 1L),
      (Seq(Seq(1, 3)), 1L),
      (Seq(Seq(2)), 1L),
      (Seq(Seq(2, 3)), 1L),
      (Seq(Seq(2), Seq(1)), 1L),
      (Seq(Seq(2), Seq(2)), 1L),
      (Seq(Seq(2), Seq(3)), 1L),
      (Seq(Seq(3)), 1L))
    compareResults[Int](expected, result)
  }

  /*
  To verify expected results for `smallTestData`, create file "prefixSpanSeqs2" with content
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
  val smallTestData = Seq(
    Seq(Seq(1, 2), Seq(3)),
    Seq(Seq(1), Seq(3, 2), Seq(1, 2)),
    Seq(Seq(1, 2), Seq(5)),
    Seq(Seq(6)))

  val smallTestDataExpectedResult = Array(
    (Seq(Seq(1)), 3L),
    (Seq(Seq(2)), 3L),
    (Seq(Seq(3)), 2L),
    (Seq(Seq(1), Seq(3)), 2L),
    (Seq(Seq(1, 2)), 3L)
  )

  test("PrefixSpan Integer type, variable-size itemsets") {
    val df = smallTestData.toDF("sequence")
    val result = new PrefixSpan()
      .setMinSupport(0.5)
      .setMaxPatternLength(5)
      .setMaxLocalProjDBSize(32000000)
      .findFrequentSequentialPatterns(df)
      .as[(Seq[Seq[Int]], Long)].collect()

    compareResults[Int](smallTestDataExpectedResult, result)
  }

  test("PrefixSpan input row with nulls") {
    val df = (smallTestData :+ null).toDF("sequence")
    val result = new PrefixSpan()
      .setMinSupport(0.5)
      .setMaxPatternLength(5)
      .setMaxLocalProjDBSize(32000000)
      .findFrequentSequentialPatterns(df)
      .as[(Seq[Seq[Int]], Long)].collect()

    compareResults[Int](smallTestDataExpectedResult, result)
  }

  test("PrefixSpan String type, variable-size itemsets") {
    val intToString = (1 to 6).zip(Seq("a", "b", "c", "d", "e", "f")).toMap
    val df = smallTestData
      .map(seq => seq.map(itemSet => itemSet.map(intToString)))
      .toDF("sequence")
    val result = new PrefixSpan()
      .setMinSupport(0.5)
      .setMaxPatternLength(5)
      .setMaxLocalProjDBSize(32000000)
      .findFrequentSequentialPatterns(df)
      .as[(Seq[Seq[String]], Long)].collect()

    val expected = smallTestDataExpectedResult.map { case (seq, freq) =>
      (seq.map(itemSet => itemSet.map(intToString)), freq)
    }
    compareResults[String](expected, result)
  }

  private def compareResults[Item](
      expectedValue: Array[(Seq[Seq[Item]], Long)],
      actualValue: Array[(Seq[Seq[Item]], Long)]): Unit = {
    val expectedSet = expectedValue.map { x =>
      (x._1.map(_.toSet), x._2)
    }.toSet
    val actualSet = actualValue.map { x =>
      (x._1.map(_.toSet), x._2)
    }.toSet
    assert(expectedSet === actualSet)
  }
}

