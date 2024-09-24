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

import scala.jdk.CollectionConverters._

import org.apache.spark.sql.internal.SqlApiConf
import org.apache.spark.sql.test.{QueryTest, RemoteSparkSession}
import org.apache.spark.sql.types.{StringType, StructType}

class DataFrameNaFunctionSuite extends QueryTest with RemoteSparkSession {
  private def createDF(): DataFrame = {
    val sparkSession = spark
    import sparkSession.implicits._
    Seq[(String, java.lang.Integer, java.lang.Double)](
      ("Bob", 16, 176.5),
      ("Alice", null, 164.3),
      ("David", 60, null),
      ("Nina", 25, Double.NaN),
      ("Amy", null, null),
      (null, null, null)).toDF("name", "age", "height")
  }

  def createNaNDF(): DataFrame = {
    val sparkSession = spark
    import sparkSession.implicits._
    Seq[(
        java.lang.Integer,
        java.lang.Long,
        java.lang.Short,
        java.lang.Byte,
        java.lang.Float,
        java.lang.Double)](
      (1, 1L, 1.toShort, 1.toByte, 1.0f, 1.0),
      (0, 0L, 0.toShort, 0.toByte, Float.NaN, Double.NaN)).toDF(
      "int",
      "long",
      "short",
      "byte",
      "float",
      "double")
  }

  def createDFWithNestedColumns: DataFrame = {
    val schema = new StructType()
      .add(
        "c1",
        new StructType()
          .add("c1-1", StringType)
          .add("c1-2", StringType))
    val data = Seq(Row(Row(null, "a2")), Row(Row("b1", "b2")), Row(null))
    spark.createDataFrame(data.asJava, schema)
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
    val sparkSession = spark
    import sparkSession.implicits._

    val input = createDF()

    val boolInput = Seq[(String, java.lang.Boolean)](
      ("Bob", false),
      ("Alice", null),
      ("Mallory", true),
      (null, null)).toDF("name", "spy")

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
    checkAnswer(
      Seq[(String, String)]((null, null)).toDF("col1", "col2").na.fill("test", "col1" :: Nil),
      Row("test", null))

    checkAnswer(
      Seq[(Long, Long)]((1, 2), (-1, -2), (9123146099426677101L, 9123146560113991650L))
        .toDF("a", "b")
        .na
        .fill(0),
      Row(1, 2) :: Row(-1, -2) :: Row(9123146099426677101L, 9123146560113991650L) :: Nil)

    checkAnswer(
      Seq[(java.lang.Long, java.lang.Double)](
        (null, 3.14),
        (9123146099426677101L, null),
        (9123146560113991650L, 1.6),
        (null, null)).toDF("a", "b").na.fill(0.2),
      Row(0, 3.14) :: Row(9123146099426677101L, 0.2) :: Row(9123146560113991650L, 1.6)
        :: Row(0, 0.2) :: Nil)

    checkAnswer(
      Seq[(java.lang.Long, java.lang.Float)](
        (null, 3.14f),
        (9123146099426677101L, null),
        (9123146560113991650L, 1.6f),
        (null, null)).toDF("a", "b").na.fill(0.2),
      Row(0, 3.14f) :: Row(9123146099426677101L, 0.2f) :: Row(9123146560113991650L, 1.6f)
        :: Row(0, 0.2f) :: Nil)

    checkAnswer(
      Seq[(java.lang.Long, java.lang.Double)]((null, 1.23), (3L, null), (4L, 3.45))
        .toDF("a", "b")
        .na
        .fill(2.34),
      Row(2, 1.23) :: Row(3, 2.34) :: Row(4, 3.45) :: Nil)

    checkAnswer(
      Seq[(java.lang.Long, java.lang.Double)]((null, 1.23), (3L, null), (4L, 3.45))
        .toDF("a", "b")
        .na
        .fill(5),
      Row(5, 1.23) :: Row(3, 5.0) :: Row(4, 3.45) :: Nil)
  }

  test("fill with map") {
    val sparkSession = spark
    import sparkSession.implicits._

    val df = Seq[(
        String,
        String,
        java.lang.Integer,
        java.lang.Long,
        java.lang.Float,
        java.lang.Double,
        java.lang.Boolean)]((null, null, null, null, null, null, null))
      .toDF(
        "stringFieldA",
        "stringFieldB",
        "integerField",
        "longField",
        "floatField",
        "doubleField",
        "booleanField")

    val fillMap = Map(
      "stringFieldA" -> "test",
      "integerField" -> 1,
      "longField" -> 2L,
      "floatField" -> 3.3f,
      "doubleField" -> 4.4d,
      "booleanField" -> false)

    val expectedRow = Row("test", null, 1, 2L, 3.3f, 4.4d, false)
    checkAnswer(df.na.fill(fillMap), expectedRow)
    checkAnswer(df.na.fill(fillMap.asJava), expectedRow) // Test Java version

    // Ensure replacement values are cast to the column data type.
    checkAnswer(
      df.na.fill(
        Map("integerField" -> 1d, "longField" -> 2d, "floatField" -> 3d, "doubleField" -> 4d)),
      Row(null, null, 1, 2L, 3f, 4d, null))

    // Ensure column types do not change. Columns that have null values replaced
    // will no longer be flagged as nullable, so do not compare schemas directly.
    assert(
      df.na.fill(fillMap).schema.fields.map(_.dataType) ===
        df.schema.fields.map(_.dataType))
  }

  test("fill with col(*)") {
    val df = createDF()
    // If columns are specified with "*", they are ignored.
    checkAnswer(df.na.fill("new name", Seq("*")), df.collect())
  }

  test("drop with col(*)") {
    val df = createDF()
    val ex = intercept[AnalysisException] {
      df.na.drop("any", Seq("*")).collect()
    }
    assert(ex.getMessage.contains("UNRESOLVED_COLUMN.WITH_SUGGESTION"))
  }

  test("fill with nested columns") {
    val df = createDFWithNestedColumns
    checkAnswer(df.na.fill("a1", Seq("c1.c1-1")), df)
  }

  test("drop with nested columns") {
    val df = createDFWithNestedColumns

    // Rows with the specified nested columns whose null values are dropped.
    assert(df.count() == 3)
    checkAnswer(df.na.drop("any", Seq("c1.c1-1")), Seq(Row(Row("b1", "b2"))))
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
    withSQLConf(SqlApiConf.ANSI_ENABLED_KEY -> false.toString) {
      checkAnswer(
        createNaNDF().na.replace("*", Map(1.0f -> Float.NaN)),
        Row(0, 0L, 0.toShort, 0.toByte, Float.NaN, Double.NaN) ::
          Row(0, 0L, 0.toShort, 0.toByte, Float.NaN, Double.NaN) :: Nil)
    }
  }

  test("replace double with nan") {
    withSQLConf(SqlApiConf.ANSI_ENABLED_KEY -> false.toString) {
      checkAnswer(
        createNaNDF().na.replace("*", Map(1.0 -> Double.NaN)),
        Row(0, 0L, 0.toShort, 0.toByte, Float.NaN, Double.NaN) ::
          Row(0, 0L, 0.toShort, 0.toByte, Float.NaN, Double.NaN) :: Nil)
    }
  }

}
