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

class StopWordsRemoverSuite extends MLTest with DefaultReadWriteTest {

  import testImplicits._

  def testStopWordsRemover(t: StopWordsRemover, dataFrame: DataFrame): Unit = {
    testTransformer[(Array[String], Array[String])](dataFrame, t, "filtered", "expected") {
       case Row(tokens: Seq[_], wantedTokens: Seq[_]) =>
         assert(tokens === wantedTokens)
    }
  }

  test("StopWordsRemover default") {
    val remover = new StopWordsRemover()
      .setInputCol("raw")
      .setOutputCol("filtered")
    val dataSet = Seq(
      (Seq("test", "test"), Seq("test", "test")),
      (Seq("a", "b", "c", "d"), Seq("b", "c", "d")),
      (Seq("a", "the", "an"), Seq()),
      (Seq("A", "The", "AN"), Seq()),
      (Seq(null), Seq(null)),
      (Seq(), Seq())
    ).toDF("raw", "expected")

    testStopWordsRemover(remover, dataSet)
  }

  test("StopWordsRemover with particular stop words list") {
    val stopWords = Array("test", "a", "an", "the")
    val remover = new StopWordsRemover()
      .setInputCol("raw")
      .setOutputCol("filtered")
      .setStopWords(stopWords)
    val dataSet = Seq(
      (Seq("test", "test"), Seq()),
      (Seq("a", "b", "c", "d"), Seq("b", "c", "d")),
      (Seq("a", "the", "an"), Seq()),
      (Seq("A", "The", "AN"), Seq()),
      (Seq(null), Seq(null)),
      (Seq(), Seq())
    ).toDF("raw", "expected")

    testStopWordsRemover(remover, dataSet)
  }

  test("StopWordsRemover with localed input (case insensitive)") {
    val stopWords = Array("milk", "cookie")
    val remover = new StopWordsRemover()
      .setInputCol("raw")
      .setOutputCol("filtered")
      .setStopWords(stopWords)
      .setCaseSensitive(false)
      .setLocale("tr")  // Turkish alphabet: has no Q, W, X but has dotted and dotless 'I's.
    val dataSet = Seq(
      // scalastyle:off
      (Seq("mİlk", "and", "nuts"), Seq("and", "nuts")),
      // scalastyle:on
      (Seq("cookIe", "and", "nuts"), Seq("cookIe", "and", "nuts")),
      (Seq(null), Seq(null)),
      (Seq(), Seq())
    ).toDF("raw", "expected")

    testStopWordsRemover(remover, dataSet)
  }

  test("StopWordsRemover with localed input (case sensitive)") {
    val stopWords = Array("milk", "cookie")
    val remover = new StopWordsRemover()
      .setInputCol("raw")
      .setOutputCol("filtered")
      .setStopWords(stopWords)
      .setCaseSensitive(true)
      .setLocale("tr")  // Turkish alphabet: has no Q, W, X but has dotted and dotless 'I's.
    val dataSet = Seq(
      // scalastyle:off
      (Seq("mİlk", "and", "nuts"), Seq("mİlk", "and", "nuts")),
      // scalastyle:on
      (Seq("cookIe", "and", "nuts"), Seq("cookIe", "and", "nuts")),
      (Seq(null), Seq(null)),
      (Seq(), Seq())
    ).toDF("raw", "expected")

    testStopWordsRemover(remover, dataSet)
  }

  test("StopWordsRemover with invalid locale") {
    intercept[IllegalArgumentException] {
      val stopWords = Array("test", "a", "an", "the")
      new StopWordsRemover()
        .setInputCol("raw")
        .setOutputCol("filtered")
        .setStopWords(stopWords)
        .setLocale("rt")  // invalid locale
    }
  }

  test("StopWordsRemover case sensitive") {
    val remover = new StopWordsRemover()
      .setInputCol("raw")
      .setOutputCol("filtered")
      .setCaseSensitive(true)
    val dataSet = Seq(
      (Seq("A"), Seq("A")),
      (Seq("The", "the"), Seq("The"))
    ).toDF("raw", "expected")

    testStopWordsRemover(remover, dataSet)
  }

  test("default stop words of supported languages are not empty") {
    StopWordsRemover.supportedLanguages.foreach { lang =>
      assert(StopWordsRemover.loadDefaultStopWords(lang).nonEmpty,
        s"The default stop words of $lang cannot be empty.")
    }
  }

  test("StopWordsRemover with language selection") {
    val stopWords = StopWordsRemover.loadDefaultStopWords("turkish")
    val remover = new StopWordsRemover()
      .setInputCol("raw")
      .setOutputCol("filtered")
      .setStopWords(stopWords)
    val dataSet = Seq(
      (Seq("acaba", "ama", "biri"), Seq()),
      (Seq("hep", "her", "scala"), Seq("scala"))
    ).toDF("raw", "expected")

    testStopWordsRemover(remover, dataSet)
  }

  test("StopWordsRemover with ignored words") {
    val stopWords = StopWordsRemover.loadDefaultStopWords("english").toSet -- Set("a")
    val remover = new StopWordsRemover()
      .setInputCol("raw")
      .setOutputCol("filtered")
      .setStopWords(stopWords.toArray)
    val dataSet = Seq(
      (Seq("python", "scala", "a"), Seq("python", "scala", "a")),
      (Seq("Python", "Scala", "swift"), Seq("Python", "Scala", "swift"))
    ).toDF("raw", "expected")

    testStopWordsRemover(remover, dataSet)
  }

  test("StopWordsRemover with additional words") {
    val stopWords = StopWordsRemover.loadDefaultStopWords("english").toSet ++ Set("python", "scala")
    val remover = new StopWordsRemover()
      .setInputCol("raw")
      .setOutputCol("filtered")
      .setStopWords(stopWords.toArray)
    val dataSet = Seq(
      (Seq("python", "scala", "a"), Seq()),
      (Seq("Python", "Scala", "swift"), Seq("swift"))
    ).toDF("raw", "expected")

    testStopWordsRemover(remover, dataSet)
  }

  test("read/write") {
    val t = new StopWordsRemover()
      .setInputCol("myInputCol")
      .setOutputCol("myOutputCol")
      .setStopWords(Array("the", "a"))
      .setCaseSensitive(true)
    testDefaultReadWrite(t)
  }

  test("StopWordsRemover output column already exists") {
    val outputCol = "expected"
    val remover = new StopWordsRemover()
      .setInputCol("raw")
      .setOutputCol(outputCol)
    val dataSet = Seq((Seq("The", "the", "swift"), Seq("swift"))).toDF("raw", outputCol)

    testTransformerByInterceptingException[(Array[String], Array[String])](
      dataSet,
      remover,
      s"requirement failed: Column $outputCol already exists.",
      "expected")
  }
}
