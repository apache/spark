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

package org.apache.spark.sql

import org.apache.spark.sql.catalyst.plans.logical.Project
import org.apache.spark.sql.test.SharedSparkSession

class DataFrameZipSuite extends QueryTest with SharedSparkSession {
  import testImplicits._

  test("zip: select different columns from the same DataFrame") {
    val df = Seq((1, 2, 3), (4, 5, 6), (7, 8, 9)).toDF("a", "b", "c")
    val left = df.select("a")
    val right = df.select("b")

    checkAnswer(
      left.zip(right),
      Row(1, 2) :: Row(4, 5) :: Row(7, 8) :: Nil)
  }

  test("zip: select with expressions over the same DataFrame") {
    val df = Seq((1, 10), (2, 20), (3, 30)).toDF("a", "b")
    val left = df.select(($"a" + 1).as("a_plus_1"))
    val right = df.select(($"b" * 2).as("b_times_2"))

    checkAnswer(
      left.zip(right),
      Row(2, 20) :: Row(3, 40) :: Row(4, 60) :: Nil)
  }

  test("zip: one side selects all columns") {
    val df = Seq((1, 2), (3, 4)).toDF("a", "b")
    val right = df.select(($"a" + $"b").as("sum"))

    checkAnswer(
      df.zip(right),
      Row(1, 2, 3) :: Row(3, 4, 7) :: Nil)
  }

  test("zip: resolved plan is a Project") {
    val df = Seq((1, 2)).toDF("a", "b")
    val left = df.select("a")
    val right = df.select("b")
    val zipped = left.zip(right)

    assert(zipped.queryExecution.analyzed.isInstanceOf[Project])
  }

  test("zip: different base plans throws AnalysisException") {
    val df1 = Seq((1, 2)).toDF("a", "b")
    val df2 = Seq((3, 4, 5)).toDF("x", "y", "z")

    checkError(
      exception = intercept[AnalysisException] {
        df1.select("a").zip(df2.select("x")).queryExecution.assertAnalyzed()
      },
      condition = "ZIP_PLANS_NOT_MERGEABLE"
    )
  }

  test("zip: different base plans from spark.range throws AnalysisException") {
    val df1 = spark.range(10).toDF("id1")
    val df2 = spark.range(20).toDF("id2")

    checkError(
      exception = intercept[AnalysisException] {
        df1.zip(df2).queryExecution.assertAnalyzed()
      },
      condition = "ZIP_PLANS_NOT_MERGEABLE"
    )
  }

  test("zip: withColumn on both sides") {
    val df = Seq((1, 10), (2, 20), (3, 30)).toDF("a", "b")
    val left = df.withColumn("a_plus_1", $"a" + 1)
    val right = df.withColumn("b_times_2", $"b" * 2)
    val zipped = left.zip(right)

    assert(zipped.queryExecution.analyzed.isInstanceOf[Project])
    checkAnswer(
      zipped,
      Row(1, 10, 2, 1, 10, 20) ::
        Row(2, 20, 3, 2, 20, 40) ::
        Row(3, 30, 4, 3, 30, 60) :: Nil)
  }

  test("zip: chained withColumn (multiple Project layers on the same side)") {
    val df = Seq((1, 10), (2, 20)).toDF("a", "b")
    val left = df
      .withColumn("a_plus_1", $"a" + 1)
      .withColumn("a_plus_2", $"a" + 2)
    val right = df.withColumn("b_times_2", $"b" * 2)
    val zipped = left.zip(right)

    assert(zipped.queryExecution.analyzed.isInstanceOf[Project])
    checkAnswer(
      zipped,
      Row(1, 10, 2, 3, 1, 10, 20) ::
        Row(2, 20, 3, 4, 2, 20, 40) :: Nil)
  }

  test("zip: longer chain of selects on both sides") {
    val df = Seq((1, 2, 3), (4, 5, 6)).toDF("a", "b", "c")
    val left = df.select("a", "b", "c").select("a", "b").select("a")
    val right = df.select("c")
    val zipped = left.zip(right)

    assert(zipped.queryExecution.analyzed.isInstanceOf[Project])
    checkAnswer(zipped, Row(1, 3) :: Row(4, 6) :: Nil)
  }

  test("zip: parent and child with chain") {
    val df = Seq((1, 2), (3, 4)).toDF("a", "b")
    val child = df.select(($"a" + 1).as("a_plus_1")).select(($"a_plus_1" * 2).as("doubled"))
    val zipped = df.zip(child)

    assert(zipped.queryExecution.analyzed.isInstanceOf[Project])
    checkAnswer(zipped, Row(1, 2, 4) :: Row(3, 4, 8) :: Nil)
  }

  test("zip: withColumnRenamed on both sides") {
    val df = Seq((1, 2), (3, 4)).toDF("a", "b")
    val left = df.withColumnRenamed("a", "a1")
    val right = df.withColumnRenamed("b", "b1")

    checkAnswer(
      left.zip(right),
      Row(1, 2, 1, 2) :: Row(3, 4, 3, 4) :: Nil)
  }
}
