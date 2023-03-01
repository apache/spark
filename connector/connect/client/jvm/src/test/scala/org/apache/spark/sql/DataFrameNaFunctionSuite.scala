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

import org.apache.spark.sql.connect.client.util.{IntegrationTestUtils, RemoteSparkSession}
import org.apache.spark.sql.types._

class DataFrameNaFunctionSuite extends RemoteSparkSession {

  private def createDF(): DataFrame = {
    val testDataPath = java.nio.file.Paths
      .get(
        IntegrationTestUtils.sparkHome,
        "connector",
        "connect",
        "common",
        "src",
        "test",
        "resources",
        "query-tests",
        "test-data",
        "people.gz.parquet")
      .toAbsolutePath
    spark.read
      .format("parquet")
      .option("path", testDataPath.toString)
      .schema(
        StructType(
          StructField("name", StringType) ::
            StructField("age", IntegerType) ::
            StructField("height", DoubleType) :: Nil))
      .load()
  }

  test("drop") {
    val input = createDF()
    val rows = input.collect()

    val result1 = input.na.drop("name" :: Nil).select("name").collect()
    val expected1 = Array(Row("Bob"), Row("Alice"), Row("David"), Row("Nina"), Row("Amy"))
    assert(result1 === expected1)

    val result2 = input.na.drop("age" :: Nil).select("name").collect()
    val expected2 = Array(Row("Bob"), Row("David"), Row("Nina"))
    assert(result2 === expected2)

    val result3 = input.na.drop("age" :: "height" :: Nil).collect()
    val expected3 = Array(rows(0))
    assert(result3 === expected3)

    val result4 = input.na.drop().collect()
    assert(result4 === expected3)

    // dropna on an a dataframe with no column should return an empty data frame.
    val empty = input.filter("age > 100")
    assert(empty.na.drop().count() === 0L)

    // Make sure the columns are properly named.
    assert(input.na.drop().columns.toSeq === input.columns.toSeq)
  }

  test("drop with how") {
    val input = createDF()
    val rows = input.collect()

    val result1 = input.na.drop("all").select("name").collect()
    val expected1 = Array(Row("Bob"), Row("Alice"), Row("David"), Row("Nina"), Row("Amy"))
    assert(result1 === expected1)

    val result2 = input.na.drop("any").collect()
    val expected2 = Array(rows(0))
    assert(result2 === expected2)

    val result3 = input.na.drop("any", Seq("age", "height")).collect()
    assert(result3 === expected2)

    val result4 = input.na.drop("all", Seq("age", "height")).select("name").collect()
    val expected4 = Array(Row("Bob"), Row("Alice"), Row("David"), Row("Nina"))
    assert(result4 === expected4)
  }

  test("drop with threshold") {
    val input = createDF()
    val rows = input.collect()

    val result1 = input.na.drop(2, Seq("age", "height")).collect()
    val expected1 = Array(rows(0))
    assert(result1 === expected1)

    val result2 = input.na.drop(3, Seq("name", "age", "height")).collect()
    assert(result2 === expected1)

    // Make sure the columns are properly named.
    assert(input.na.drop(2, Seq("age", "height")).columns.toSeq === input.columns.toSeq)
  }

  test("fill") {
    val input = createDF()

    val boolInput = spark.sql("select name, spy from (values " +
      "('Bob', false), " +
      "('Alice', null), " +
      "('Mallory', true), " +
      "(null, null))" +
      "as t(name, spy)")

    val df1 = input.na.fill(50.6)

    val result1 = df1.collect()
    val expected1 = Array(
      Row("Bob", 16, 176.5),
      Row("Alice", 50, 164.3),
      Row("David", 60, 50.6),
      Row("Nina", 25, 50.6),
      Row("Amy", 50, 50.6),
      Row(null, 50, 50.6))
    assert(result1 === expected1)

    // Make sure the columns are properly named.
    assert(df1.columns.toSeq === input.columns.toSeq)

    // string
    val result2 = input.na.fill("unknown").select("name").collect()
    val expected2 = Array(
      Row("Bob"), Row("Alice"), Row("David"),
      Row("Nina"), Row("Amy"), Row("unknown"))
    assert(result2 === expected2)

    // boolean
    val result3 = boolInput.na.fill(true).select("spy").collect()
    val expected3 = Array(Row(false), Row(true), Row(true), Row(true))
    assert(result3 === expected3)

    // fill double with subset columns
    val result4 = input.na.fill(50.6, "age" :: Nil).select("name", "age").collect()
    val expected4 = Array(
      Row("Bob", 16),
      Row("Alice", 50),
      Row("David", 60),
      Row("Nina", 25),
      Row("Amy", 50),
      Row(null, 50))
    assert(result4 === expected4)

    // fill boolean with subset columns
    val result5 = boolInput.na.fill(true, "spy" :: Nil).select("name", "spy").collect()
    val expected5 = Array(
      Row("Bob", false),
      Row("Alice", true),
      Row("Mallory", true),
      Row(null, true))
    assert(result5 === expected5)

    // fill string with subset columns
    val df2 = spark.sql("select col1, col2 from (values " +
      "('col1', 'col2'), " +
      "(null, null))" +
      "as t(col1, col2)")
    val result6 = df2.filter("col1 is null").na.fill("test", Seq("col1")).collect()
    val expected6 = Array(Row("test", null))
    assert(result6 === expected6)
  }

  test("fill with map") {

  }

  test("fill with col(*)") {

  }

  test("drop with col(*)") {

  }

  test("replace") {
    val input = createDF()

    val out = input.na.replace(Seq("age", "height"), Map(
      16 -> 61,
      60 -> 6,
      164.3 -> 461.3 // Alice is really tall
    )).collect()
    assert(out(0) === Row("Bob", 61, 176.5))
    assert(out(1) === Row("Alice", null, 461.3))
    assert(out(2) === Row("David", 6, null))
    assert(out(3).get(2).asInstanceOf[Double].isNaN)
    assert(out(4) === Row("Amy", null, null))
    assert(out(5) === Row(null, null, null))
  }
}
