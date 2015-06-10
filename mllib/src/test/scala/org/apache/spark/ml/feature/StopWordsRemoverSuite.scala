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

import scala.beans.BeanInfo

@BeanInfo
case class StopWordsTestData(raw: Array[String], wanted: Array[String])

object StopWordsRemoverSuite extends SparkFunSuite {
  def testStopWordsRemover(t: StopWordsRemover, dataset: DataFrame): Unit = {
    t.transform(dataset)
      .select("filtered", "wanted")
      .collect()
      .foreach { case Row(tokens, wantedTokens) =>
      assert(tokens === wantedTokens)
    }
  }
}

class StopWordsRemoverSuite extends SparkFunSuite with MLlibTestSparkContext {
  import org.apache.spark.ml.feature.StopWordsRemoverSuite._

  test("StopWordsRemover default") {
    val remover = new StopWordsRemover()
      .setInputCol("raw")
      .setOutputCol("filtered")
    val dataset = sqlContext.createDataFrame(Seq(
      StopWordsTestData(Array("test", "test"), Array("test", "test")),
      StopWordsTestData(Array("a", "b", "c", "d"), Array("b", "c", "d")),
      StopWordsTestData(Array("a", "the", "an"), Array()),
      StopWordsTestData(Array("A", "The", "AN"), Array()),
      StopWordsTestData(Array(null), Array(null)),
      StopWordsTestData(Array(), Array())
    ))
    testStopWordsRemover(remover, dataset)
  }

  test("StopWordsRemover case sensitive") {
    val remover = new StopWordsRemover()
      .setInputCol("raw")
      .setOutputCol("filtered")
      .setCaseSensitive(true)

    val dataset = sqlContext.createDataFrame(Seq(
      StopWordsTestData(Array("A"), Array("A")),
      StopWordsTestData(Array("The", "the"), Array("The"))
    ))
    testStopWordsRemover(remover, dataset)
  }

  test("StopWordsRemover with additional words") {
    val stopWords = StopWords.EnglishSet + "python" + "scala"
    val remover = new StopWordsRemover()
      .setInputCol("raw")
      .setOutputCol("filtered")
      .setStopWords(stopWords)

    val dataset = sqlContext.createDataFrame(Seq(
      StopWordsTestData(Array("python", "scala", "a"), Array()),
      StopWordsTestData(Array("Python", "Scala", "swift"), Array("swift"))
    ))
    testStopWordsRemover(remover, dataset)
  }
}
