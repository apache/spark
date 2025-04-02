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

import org.apache.spark.ml.util.{DefaultReadWriteTest, MLTest}
import org.apache.spark.sql.{DataFrame, Row}

case class NGramTestData(inputTokens: Array[String], wantedNGrams: Array[String]) {
  def getInputTokens: Array[String] = inputTokens
  def getWantedNGrams: Array[String] = wantedNGrams
}

class NGramSuite extends MLTest with DefaultReadWriteTest {

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

  test("read/write") {
    val t = new NGram()
      .setInputCol("myInputCol")
      .setOutputCol("myOutputCol")
      .setN(3)
    testDefaultReadWrite(t)
  }

  def testNGram(t: NGram, dataFrame: DataFrame): Unit = {
    testTransformer[(Seq[String], Seq[String])](dataFrame, t, "nGrams", "wantedNGrams") {
      case Row(actualNGrams : scala.collection.Seq[_], wantedNGrams: scala.collection.Seq[_]) =>
        assert(actualNGrams === wantedNGrams)
    }
  }
}
