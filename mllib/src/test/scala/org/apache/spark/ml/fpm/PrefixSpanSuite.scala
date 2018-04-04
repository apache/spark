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
    smallDataset = Seq(Seq(Seq(1, 2), Seq(1, 2, 3))).toDF("sequence")
  }

  @transient var smallDataset: DataFrame = _

  test("PrefixSpan projections with multiple partial starts") {
    val result = PrefixSpan.findFrequentSequentPatterns(smallDataset, "sequence",
      minSupport = 1.0, maxPatternLength = 2).as[(Seq[Seq[Int]], Long)].collect()
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
    val result = PrefixSpan.findFrequentSequentPatterns(df, "sequence",
      minSupport = 0.5, maxPatternLength = 5).as[(Seq[Seq[Int]], Long)].collect()

    compareResults[Int](smallTestDataExpectedResult, result)
  }

  test("PrefixSpan String type, variable-size itemsets") {
    val intToString = (1 to 6).zip(Seq("a", "b", "c", "d", "e", "f")).toMap
    val df = smallTestData
      .map(seq => seq.map(itemSet => itemSet.map(intToString)))
      .toDF("sequence")
    val result = PrefixSpan.findFrequentSequentPatterns(df, "sequence",
      minSupport = 0.5, maxPatternLength = 5).as[(Seq[Seq[String]], Long)].collect()

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

