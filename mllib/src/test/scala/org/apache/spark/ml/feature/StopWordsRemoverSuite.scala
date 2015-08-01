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

import org.apache.spark.SparkFunSuite
import org.apache.spark.mllib.util.MLlibTestSparkContext
import org.apache.spark.sql.{DataFrame, Row}

object StopWordsRemoverSuite extends SparkFunSuite {
  def testStopWordsRemover(t: StopWordsRemover, dataset: DataFrame): Unit = {
    t.transform(dataset)
      .select("filtered", "expected")
      .collect()
      .foreach { case Row(tokens, wantedTokens) =>
        assert(tokens === wantedTokens)
    }
  }
}

class StopWordsRemoverSuite extends SparkFunSuite with MLlibTestSparkContext {
  import StopWordsRemoverSuite._

  test("StopWordsRemover default") {
    val remover = new StopWordsRemover()
      .setInputCol("raw")
      .setOutputCol("filtered")
    val dataSet = sqlContext.createDataFrame(Seq(
      (Seq("test", "test"), Seq("test", "test")),
      (Seq("a", "b", "c", "d"), Seq("b", "c", "d")),
      (Seq("a", "the", "an"), Seq()),
      (Seq("A", "The", "AN"), Seq()),
      (Seq(null), Seq(null)),
      (Seq(), Seq())
    )).toDF("raw", "expected")

    testStopWordsRemover(remover, dataSet)
  }

  test("StopWordsRemover case sensitive") {
    val remover = new StopWordsRemover()
      .setInputCol("raw")
      .setOutputCol("filtered")
      .setCaseSensitive(true)
    val dataSet = sqlContext.createDataFrame(Seq(
      (Seq("A"), Seq("A")),
      (Seq("The", "the"), Seq("The"))
    )).toDF("raw", "expected")

    testStopWordsRemover(remover, dataSet)
  }

  test("StopWordsRemover with additional words") {
    val stopWords = StopWords.EnglishStopWords ++ Array("python", "scala")
    val remover = new StopWordsRemover()
      .setInputCol("raw")
      .setOutputCol("filtered")
      .setStopWords(stopWords)
    val dataSet = sqlContext.createDataFrame(Seq(
      (Seq("python", "scala", "a"), Seq()),
      (Seq("Python", "Scala", "swift"), Seq("swift"))
    )).toDF("raw", "expected")

    testStopWordsRemover(remover, dataSet)
  }
}
