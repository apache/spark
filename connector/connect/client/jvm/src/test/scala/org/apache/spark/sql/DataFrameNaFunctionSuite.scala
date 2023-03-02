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

import scala.collection.JavaConverters._

import org.apache.spark.sql.connect.client.util.{IntegrationTestUtils, QueryTest}
import org.apache.spark.sql.types.{ByteType, DoubleType, FloatType, IntegerType, LongType, ShortType, StringType, StructField, StructType}

class DataFrameNaFunctionSuite extends QueryTest {

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
        "people_nan_null.gz.parquet")
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

  private def createNaNDF(): DataFrame = {
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
        "nan.gz.parquet")
      .toAbsolutePath
    spark.read
      .format("parquet")
      .option("path", testDataPath.toString)
      .schema(
        StructType(
          StructField("int", IntegerType) ::
            StructField("long", LongType) ::
            StructField("short", ShortType) ::
            StructField("byte", ByteType) ::
            StructField("float", FloatType) ::
            StructField("double", DoubleType) :: Nil))
      .load()
  }

  test("drop") {
    val input = createDF()
    val rows = input.collect()

    val result1 = input.na.drop("name" :: Nil).select("name")
    val expected1 = Array(Row("Bob"), Row("Alice"), Row("David"), Row("Nina"), Row("Amy"))
    checkAnswer(result1, expected1)

    val result2 = input.na.drop("age" :: Nil).select("name")
    val expected2 = Array(Row("Bob"), Row("David"), Row("Nina"))
    checkAnswer(result2, expected2)

    val result3 = input.na.drop("age" :: "height" :: Nil)
    val expected3 = Array(rows(0))
    checkAnswer(result3, expected3)

    val result4 = input.na.drop()
    checkAnswer(result4, expected3)

    // dropna on an a dataframe with no column should return an empty data frame.
    val empty = input.filter("age > 100")
    assert(empty.na.drop().count() === 0L)

    // Make sure the columns are properly named.
    assert(input.na.drop().columns.toSeq === input.columns.toSeq)
  }

  test("drop with how") {
    val input = createDF()
    val rows = input.collect()

    checkAnswer(
      input.na.drop("all").select("name"),
      Row("Bob") :: Row("Alice") :: Row("David") :: Row("Nina") :: Row("Amy") :: Nil)

    checkAnswer(input.na.drop("any"), rows(0) :: Nil)

    checkAnswer(input.na.drop("any", Seq("age", "height")), rows(0) :: Nil)

    checkAnswer(
      input.na.drop("all", Seq("age", "height")).select("name"),
      Row("Bob") :: Row("Alice") :: Row("David") :: Row("Nina") :: Nil)
  }

  test("drop with threshold") {
    val input = createDF()
    val rows = input.collect()

    checkAnswer(input.na.drop(2, Seq("age", "height")), rows(0) :: Nil)

    checkAnswer(input.na.drop(3, Seq("name", "age", "height")), rows(0))

    // Make sure the columns are properly named.
    assert(input.na.drop(2, Seq("age", "height")).columns.toSeq === input.columns.toSeq)
  }

  test("fill") {
    val input = createDF()

    val boolInput = spark.sql(
      "select name, spy from (values " +
        "('Bob', false), " +
        "('Alice', null), " +
        "('Mallory', true), " +
        "(null, null))" +
        "as t(name, spy)")

    val fillNumeric = input.na.fill(50.6)
    checkAnswer(
      fillNumeric,
      Row("Bob", 16, 176.5) ::
        Row("Alice", 50, 164.3) ::
        Row("David", 60, 50.6) ::
        Row("Nina", 25, 50.6) ::
        Row("Amy", 50, 50.6) ::
        Row(null, 50, 50.6) :: Nil)

    // Make sure the columns are properly named.
    assert(fillNumeric.columns.toSeq === input.columns.toSeq)

    // string
    checkAnswer(
      input.na.fill("unknown").select("name"),
      Row("Bob") :: Row("Alice") :: Row("David") ::
        Row("Nina") :: Row("Amy") :: Row("unknown") :: Nil)
    assert(input.na.fill("unknown").columns.toSeq === input.columns.toSeq)

    // boolean
    checkAnswer(
      boolInput.na.fill(true).select("spy"),
      Row(false) :: Row(true) :: Row(true) :: Row(true) :: Nil)
    assert(boolInput.na.fill(true).columns.toSeq === boolInput.columns.toSeq)

    // fill double with subset columns
    checkAnswer(
      input.na.fill(50.6, "age" :: Nil).select("name", "age"),
      Row("Bob", 16) ::
        Row("Alice", 50) ::
        Row("David", 60) ::
        Row("Nina", 25) ::
        Row("Amy", 50) ::
        Row(null, 50) :: Nil)

    // fill boolean with subset columns
    checkAnswer(
      boolInput.na.fill(true, "spy" :: Nil).select("name", "spy"),
      Row("Bob", false) ::
        Row("Alice", true) ::
        Row("Mallory", true) ::
        Row(null, true) :: Nil)

    // fill string with subset columns
    val df6 = spark.sql(
      "select col1, col2 from (values " +
        "('col1', 'col2'), " +
        "(null, null))" +
        "as t(col1, col2)")
    checkAnswer(df6.filter("col1 is null").na.fill("test", "col1" :: Nil), Row("test", null))

    val df7 = spark.sql(
      "select a, b from (values " +
        "(1, 2), " +
        "(-1, -2), " +
        "(9123146099426677101L, 9123146560113991650L))" +
        "as t(a, b)")
    checkAnswer(
      df7.na.fill(0),
      Row(1, 2) :: Row(-1, -2) :: Row(9123146099426677101L, 9123146560113991650L) :: Nil)
  }

  test("fill with map") {
    val df = spark.sql(
      "select stringFieldA, stringFieldB, integerField, " +
        "longField, floatField, doubleField, booleanField from (values " +
        "('1', '2', 1, 2L, 0.3f, 0.4d, true), " +
        "(null, null, null, null, null, null, null))" +
        "as t(stringFieldA, stringFieldB, integerField, " +
        "longField, floatField, doubleField, booleanField)")
    val input = df.filter("stringFieldA is null")

    val fillMap = Map(
      "stringFieldA" -> "test",
      "integerField" -> 1,
      "longField" -> 2L,
      "floatField" -> 3.3f,
      "doubleField" -> 4.4d,
      "booleanField" -> false)

    val expectedRow = Row("test", null, 1, 2L, 3.3f, 4.4d, false)
    checkAnswer(input.na.fill(fillMap), expectedRow)
    checkAnswer(input.na.fill(fillMap.asJava), expectedRow) // Test Java version

    // Ensure replacement values are cast to the column data type.
    checkAnswer(
      input.na.fill(
        Map("integerField" -> 1d, "longField" -> 2d, "floatField" -> 3d, "doubleField" -> 4d)),
      Row(null, null, 1, 2L, 3f, 4d, null))

    // Ensure column types do not change. Columns that have null values replaced
    // will no longer be flagged as nullable, so do not compare schemas directly.
    assert(
      input.na.fill(fillMap).schema.fields.map(_.dataType) ===
        input.schema.fields.map(_.dataType))
  }

  test("fill with col(*)") {
    val df = createDF()
    // If columns are specified with "*", they are ignored.
    checkAnswer(df.na.fill("new name", Seq("*")), df.collect())
  }

  test("drop with col(*)") {
    val df = createDF()
    val ex = intercept[RuntimeException] {
      df.na.drop("any", Seq("*")).collect()
    }
    assert(ex.getMessage.contains("UNRESOLVED_COLUMN.WITH_SUGGESTION"))
  }

  test("replace") {
    val input = createDF()

    val result1 = input.na
      .replace(
        Seq("age", "height"),
        Map(
          16 -> 61,
          60 -> 6,
          164.3 -> 461.3 // Alice is really tall
        ))
      .collect()
    assert(result1(0) === Row("Bob", 61, 176.5))
    assert(result1(1) === Row("Alice", null, 461.3))
    assert(result1(2) === Row("David", 6, null))
    assert(result1(3).get(2).asInstanceOf[Double].isNaN)
    assert(result1(4) === Row("Amy", null, null))
    assert(result1(5) === Row(null, null, null))

    // Replace only the age column
    val result2 = input.na
      .replace(
        "age",
        Map(
          16 -> 61,
          60 -> 6,
          164.3 -> 461.3 // Alice is really tall
        ))
      .collect()
    assert(result2(0) === Row("Bob", 61, 176.5))
    assert(result2(1) === Row("Alice", null, 164.3))
    assert(result2(2) === Row("David", 6, null))
    assert(result2(3).get(2).asInstanceOf[Double].isNaN)
    assert(result2(4) === Row("Amy", null, null))
    assert(result2(5) === Row(null, null, null))
  }

  test("replace with null") {
    val input = spark.sql(
      "select name, height, married from (values " +
        "('Bob', 176.5, true), " +
        "('Alice', 164.3, false), " +
        "('David', null, true))" +
        "as t(name, height, married)")

    // Replace String with String and null
    val result1 = input.na.replace("name", Map("Bob" -> "Bravo", "Alice" -> null))

    checkAnswer(
      result1,
      Row("Bravo", 176.5, true) ::
        Row(null, 164.3, false) ::
        Row("David", null, true) :: Nil)

    // Replace Double with null
    val result2 = input.na.replace("height", Map[Any, Any](164.3 -> null))
    checkAnswer(
      result2,
      Row("Bob", 176.5, true) ::
        Row("Alice", null, false) ::
        Row("David", null, true) :: Nil)

    // Replace Boolean with null
    checkAnswer(
      input.na.replace("*", Map[Any, Any](false -> null)),
      Row("Bob", 176.5, true) ::
        Row("Alice", 164.3, null) ::
        Row("David", null, true) :: Nil)

    // Replace String with null and then drop rows containing null
    checkAnswer(
      input.na.replace("name", Map("Bob" -> null)).na.drop("name" :: Nil).select("name"),
      Row("Alice") :: Row("David") :: Nil)
  }

  test("replace nan with float") {
    checkAnswer(
      createNaNDF().na.replace("*", Map(Float.NaN -> 10.0f)),
      Row(1, 1L, 1.toShort, 1.toByte, 1.0f, 1.0) ::
        Row(0, 0L, 0.toShort, 0.toByte, 10.0f, 10.0) :: Nil)
  }

  test("replace nan with double") {
    checkAnswer(
      createNaNDF().na.replace("*", Map(Double.NaN -> 10.0)),
      Row(1, 1L, 1.toShort, 1.toByte, 1.0f, 1.0) ::
        Row(0, 0L, 0.toShort, 0.toByte, 10.0f, 10.0) :: Nil)
  }

  test("replace float with nan") {
    checkAnswer(
      createNaNDF().na.replace("*", Map(1.0f -> Float.NaN)),
      Row(0, 0L, 0.toShort, 0.toByte, Float.NaN, Double.NaN) ::
        Row(0, 0L, 0.toShort, 0.toByte, Float.NaN, Double.NaN) :: Nil)
  }

  test("replace double with nan") {
    checkAnswer(
      createNaNDF().na.replace("*", Map(1.0 -> Double.NaN)),
      Row(0, 0L, 0.toShort, 0.toByte, Float.NaN, Double.NaN) ::
        Row(0, 0L, 0.toShort, 0.toByte, Float.NaN, Double.NaN) :: Nil)
  }

}
