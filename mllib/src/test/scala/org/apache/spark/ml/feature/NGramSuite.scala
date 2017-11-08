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

package org.apache.spark.ml.feature

import scala.beans.BeanInfo

import org.scalatest.Matchers

import org.apache.spark.SparkFunSuite
import org.apache.spark.ml.util.DefaultReadWriteTest
import org.apache.spark.mllib.util.MLlibTestSparkContext
import org.apache.spark.sql.{Dataset, Row}

@BeanInfo
case class NGramTestData(inputTokens: Array[String], wantedNGrams: Array[String])

class NGramSuite extends SparkFunSuite with MLlibTestSparkContext with DefaultReadWriteTest
    with Matchers {

  import org.apache.spark.ml.feature.NGramSuite._
  import testImplicits._

  test("default behavior yields bigram features") {
    val nGram = new NGram()
      .setInputCol("inputTokens")
      .setOutputCol("nGrams")
    val dataset = Seq(NGramTestData(
      Array("Test", "for", "ngram", "."),
      Array("Test for", "for ngram", "ngram .")
    )).toDF()
    testNGram(nGram, dataset)
  }

  test("NGramLength=4 yields length 4 n-grams") {
    val nGram = new NGram()
      .setInputCol("inputTokens")
      .setOutputCol("nGrams")
      .setN(4)
    val dataset = Seq(NGramTestData(
      Array("a", "b", "c", "d", "e"),
      Array("a b c d", "b c d e")
    )).toDF()
    testNGram(nGram, dataset)
  }

  test("empty input yields empty output") {
    val nGram = new NGram()
      .setInputCol("inputTokens")
      .setOutputCol("nGrams")
      .setN(4)
    val dataset = Seq(NGramTestData(Array(), Array())).toDF()
    testNGram(nGram, dataset)
  }

  test("input array < n yields empty output") {
    val nGram = new NGram()
      .setInputCol("inputTokens")
      .setOutputCol("nGrams")
      .setN(6)
    val dataset = Seq(NGramTestData(
      Array("a", "b", "c", "d", "e"),
      Array()
    )).toDF()
    testNGram(nGram, dataset)
  }

  test("NGramLength in [2, 4] yields length 2, 3, 4 n-grams") {
    val nGram = new NGram()
      .setInputCol("inputTokens")
      .setOutputCol("nGrams")
      .setN(2)
      .setMaxN(4)
    val dataset = Seq(NGramTestData(
      Array("a", "b", "c", "d", "e", "f", "g"),
      Array(
        "a b", "a b c", "a b c d",
        "b c", "b c d", "b c d e",
        "c d", "c d e", "c d e f",
        "d e", "d e f", "d e f g",
        "e f", "e f g",
        "f g"
      )
    )).toDF()
    testNGram(nGram, dataset)
  }

  test("read/write") {
    testDefaultReadWrite(new NGram()
      .setInputCol("myInputCol")
      .setOutputCol("myOutputCol")
      .setN(3)
    )

    testDefaultReadWrite(new NGram()
      .setInputCol("myInputCol")
      .setOutputCol("myOutputCol")
      .setN(3)
      .setMaxN(5)
    )
  }

  test("NGram.multiSliding should calculate empty results for out-of-bound inputs") {
    import NGram.multiSliding

    multiSliding(Seq.empty, -1, -1) shouldBe Seq.empty
    multiSliding(Seq.empty, -1, 0)  shouldBe Seq.empty
    multiSliding(Seq.empty, 0, -1)  shouldBe Seq.empty
    multiSliding(Seq.empty, 0, 0)   shouldBe Seq.empty
    multiSliding(Seq.empty, 2, 1)   shouldBe Seq.empty
    multiSliding(Seq.empty, 2, 2)   shouldBe Seq.empty

    multiSliding(1 to 10, -1, -1) shouldBe Seq.empty
    multiSliding(1 to 10, -1, 0)  shouldBe Seq.empty
    multiSliding(1 to 10, 0, -1)  shouldBe Seq.empty
    multiSliding(1 to 10, 0, 0)   shouldBe Seq.empty
    multiSliding(1 to 10, 2, 1)   shouldBe Seq.empty
  }

  test("NGram.multiSliding should calculate multiple sliding windows correctly") {
    import NGram.multiSliding

    multiSliding(1 to 5, min = 2, max = 4) shouldBe Seq(
      Seq(1, 2), Seq(1, 2, 3), Seq(1, 2, 3, 4),
      Seq(2, 3), Seq(2, 3, 4), Seq(2, 3, 4, 5),
      Seq(3, 4), Seq(3, 4, 5),
      Seq(4, 5)
    )

    multiSliding(1 to 10, min = 2, max = 5) shouldBe Seq(
      Seq(1, 2), Seq(1, 2, 3), Seq(1, 2, 3, 4), Seq(1, 2, 3, 4, 5),
      Seq(2, 3), Seq(2, 3, 4), Seq(2, 3, 4, 5), Seq(2, 3, 4, 5, 6),
      Seq(3, 4), Seq(3, 4, 5), Seq(3, 4, 5, 6), Seq(3, 4, 5, 6, 7),
      Seq(4, 5), Seq(4, 5, 6), Seq(4, 5, 6, 7), Seq(4, 5, 6, 7, 8),
      Seq(5, 6), Seq(5, 6, 7), Seq(5, 6, 7, 8), Seq(5, 6, 7, 8, 9),
      Seq(6, 7), Seq(6, 7, 8), Seq(6, 7, 8, 9), Seq(6, 7, 8, 9, 10),
      Seq(7, 8), Seq(7, 8, 9), Seq(7, 8, 9, 10),
      Seq(8, 9), Seq(8, 9, 10),
      Seq(9, 10)
    )
  }

}

object NGramSuite extends SparkFunSuite {

  def testNGram(t: NGram, dataset: Dataset[_]): Unit = {
    t.transform(dataset)
      .select("nGrams", "wantedNGrams")
      .collect()
      .foreach { case Row(actualNGrams, wantedNGrams) =>
        assert(actualNGrams === wantedNGrams)
      }
  }
}
