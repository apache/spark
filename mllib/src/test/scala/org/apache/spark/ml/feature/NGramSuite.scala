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

import org.apache.spark.SparkFunSuite
import org.apache.spark.mllib.util.MLlibTestSparkContext
import org.apache.spark.sql.{DataFrame, Row}

@BeanInfo
case class NGramTestData(inputTokens: Array[String], wantedNGrams: Array[String])

class NGramSuite extends SparkFunSuite with MLlibTestSparkContext {
  import org.apache.spark.ml.feature.NGramSuite._

  test("default behavior yields bigram features") {
    val nGram = new NGram()
      .setInputCol("inputTokens")
      .setOutputCol("nGrams")
    val dataset = sqlContext.createDataFrame(Seq(
      NGramTestData(
        Array("Test", "for", "ngram", "."),
        Array("Test for", "for ngram", "ngram .")
    )))
    testNGram(nGram, dataset)
  }

  test("NGramLength=4 yields length 4 n-grams") {
    val nGram = new NGram()
      .setInputCol("inputTokens")
      .setOutputCol("nGrams")
      .setN(4)
    val dataset = sqlContext.createDataFrame(Seq(
      NGramTestData(
        Array("a", "b", "c", "d", "e"),
        Array("a b c d", "b c d e")
      )))
    testNGram(nGram, dataset)
  }

  test("empty input yields empty output") {
    val nGram = new NGram()
      .setInputCol("inputTokens")
      .setOutputCol("nGrams")
      .setN(4)
    val dataset = sqlContext.createDataFrame(Seq(
      NGramTestData(
        Array(),
        Array()
      )))
    testNGram(nGram, dataset)
  }

  test("input array < n yields empty output") {
    val nGram = new NGram()
      .setInputCol("inputTokens")
      .setOutputCol("nGrams")
      .setN(6)
    val dataset = sqlContext.createDataFrame(Seq(
      NGramTestData(
        Array("a", "b", "c", "d", "e"),
        Array()
      )))
    testNGram(nGram, dataset)
  }
}

object NGramSuite extends SparkFunSuite {

  def testNGram(t: NGram, dataset: DataFrame): Unit = {
    t.transform(dataset)
      .select("nGrams", "wantedNGrams")
      .collect()
      .foreach { case Row(actualNGrams, wantedNGrams) =>
        assert(actualNGrams === wantedNGrams)
      }
  }
}
