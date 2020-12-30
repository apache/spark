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

import java.util.Locale

import org.apache.spark.ml.Pipeline
import org.apache.spark.ml.util.{DefaultReadWriteTest, MLTest}
import org.apache.spark.sql.{DataFrame, Row}

class StopWordsRemoverSuite extends MLTest with DefaultReadWriteTest {

  import testImplicits._

  def testStopWordsRemover(t: StopWordsRemover, dataFrame: DataFrame): Unit = {
    testTransformer[(Array[String], Array[String])](dataFrame, t, "filtered", "expected") {
       case Row(tokens: scala.collection.Seq[_], wantedTokens: scala.collection.Seq[_]) =>
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
    val t1 = new StopWordsRemover()
      .setInputCol("myInputCol")
      .setOutputCol("myOutputCol")
      .setStopWords(Array("the", "a"))
      .setCaseSensitive(true)
    testDefaultReadWrite(t1)

    val t2 = new StopWordsRemover()
      .setInputCols(Array("input1", "input2", "input3"))
      .setOutputCols(Array("result1", "result2", "result3"))
      .setStopWords(Array("the", "a"))
      .setCaseSensitive(true)
    testDefaultReadWrite(t2)
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
      s"requirement failed: Output Column $outputCol already exists.",
      "expected")
  }

  test("SPARK-28365: Fallback to en_US if default locale isn't in available locales") {
    val oldDefault = Locale.getDefault()
    try {
      val dummyLocale = Locale.forLanguageTag("test")
      Locale.setDefault(dummyLocale)

      val remover = new StopWordsRemover()
        .setInputCol("raw")
        .setOutputCol("filtered")
      assert(remover.getLocale == "en_US")
    } finally {
      Locale.setDefault(oldDefault)
    }
  }

  test("Multiple Columns: StopWordsRemover default") {
    val remover = new StopWordsRemover()
      .setInputCols(Array("raw1", "raw2"))
      .setOutputCols(Array("filtered1", "filtered2"))
    val df = Seq(
      (Seq("test", "test"), Seq("test1", "test2"), Seq("test", "test"), Seq("test1", "test2")),
      (Seq("a", "b", "c", "d"), Seq("a", "b"), Seq("b", "c", "d"), Seq("b")),
      (Seq("a", "the", "an"), Seq("the", "an"), Seq(), Seq()),
      (Seq("A", "The", "AN"), Seq("A", "The"), Seq(), Seq()),
      (Seq(null), Seq(null), Seq(null), Seq(null)),
      (Seq(), Seq(), Seq(), Seq())
    ).toDF("raw1", "raw2", "expected1", "expected2")

    remover.transform(df)
      .select("filtered1", "expected1", "filtered2", "expected2")
      .collect().foreach {
        case Row(r1: scala.collection.Seq[_], e1: scala.collection.Seq[_],
          r2: scala.collection.Seq[_], e2: scala.collection.Seq[_]) =>
          assert(r1 === e1,
            s"The result value is not correct after bucketing. Expected $e1 but found $r1")
          assert(r2 === e2,
            s"The result value is not correct after bucketing. Expected $e2 but found $r2")
    }
  }

  test("Multiple Columns: StopWordsRemover with particular stop words list") {
    val stopWords = Array("test", "a", "an", "the")
    val remover = new StopWordsRemover()
      .setInputCols(Array("raw1", "raw2"))
      .setOutputCols(Array("filtered1", "filtered2"))
      .setStopWords(stopWords)
    val df = Seq(
      (Seq("test", "test"), Seq("test1", "test2"), Seq(), Seq("test1", "test2")),
      (Seq("a", "b", "c", "d"), Seq("a", "b"), Seq("b", "c", "d"), Seq("b")),
      (Seq("a", "the", "an"), Seq("a", "the", "test1"), Seq(), Seq("test1")),
      (Seq("A", "The", "AN"), Seq("A", "The", "AN"), Seq(), Seq()),
      (Seq(null), Seq(null), Seq(null), Seq(null)),
      (Seq(), Seq(), Seq(), Seq())
    ).toDF("raw1", "raw2", "expected1", "expected2")

    remover.transform(df)
      .select("filtered1", "expected1", "filtered2", "expected2")
      .collect().foreach {
        case Row(r1: scala.collection.Seq[_], e1: scala.collection.Seq[_],
          r2: scala.collection.Seq[_], e2: scala.collection.Seq[_]) =>
          assert(r1 === e1,
            s"The result value is not correct after bucketing. Expected $e1 but found $r1")
          assert(r2 === e2,
            s"The result value is not correct after bucketing. Expected $e2 but found $r2")
    }
  }

  test("Compare single/multiple column(s) StopWordsRemover in pipeline") {
    val df = Seq(
      (Seq("test", "test"), Seq("test1", "test2")),
      (Seq("a", "b", "c", "d"), Seq("a", "b")),
      (Seq("a", "the", "an"), Seq("a", "the", "test1")),
      (Seq("A", "The", "AN"), Seq("A", "The", "AN")),
      (Seq(null), Seq(null)),
      (Seq(), Seq())
    ).toDF("input1", "input2")

    val multiColsRemover = new StopWordsRemover()
      .setInputCols(Array("input1", "input2"))
      .setOutputCols(Array("output1", "output2"))

    val plForMultiCols = new Pipeline()
      .setStages(Array(multiColsRemover))
      .fit(df)

    val removerForCol1 = new StopWordsRemover()
      .setInputCol("input1")
      .setOutputCol("output1")
    val removerForCol2 = new StopWordsRemover()
      .setInputCol("input2")
      .setOutputCol("output2")

    val plForSingleCol = new Pipeline()
      .setStages(Array(removerForCol1, removerForCol2))
      .fit(df)

    val resultForSingleCol = plForSingleCol.transform(df)
      .select("output1", "output2")
      .collect()
    val resultForMultiCols = plForMultiCols.transform(df)
      .select("output1", "output2")
      .collect()

    resultForSingleCol.zip(resultForMultiCols).foreach {
      case (rowForSingle, rowForMultiCols) =>
        assert(rowForSingle === rowForMultiCols)
    }
  }

  test("Multiple Columns: Mismatched sizes of inputCols/outputCols") {
    val remover = new StopWordsRemover()
      .setInputCols(Array("input1"))
      .setOutputCols(Array("result1", "result2"))
    val df = Seq(
      (Seq("A"), Seq("A")),
      (Seq("The", "the"), Seq("The"))
    ).toDF("input1", "input2")
    intercept[IllegalArgumentException] {
      remover.transform(df).count()
    }
  }

  test("Multiple Columns: Set both of inputCol/inputCols") {
    val remover = new StopWordsRemover()
      .setInputCols(Array("input1", "input2"))
      .setOutputCols(Array("result1", "result2"))
      .setInputCol("input1")
    val df = Seq(
      (Seq("A"), Seq("A")),
      (Seq("The", "the"), Seq("The"))
    ).toDF("input1", "input2")
    intercept[IllegalArgumentException] {
      remover.transform(df).count()
    }
  }
}
