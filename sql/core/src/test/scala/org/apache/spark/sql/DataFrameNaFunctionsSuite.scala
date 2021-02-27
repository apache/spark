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

import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.test.SharedSparkSession
import org.apache.spark.sql.types.{StringType, StructType}

class DataFrameNaFunctionsSuite extends QueryTest with SharedSparkSession {
  import testImplicits._

  def createDF(): DataFrame = {
    Seq[(String, java.lang.Integer, java.lang.Double)](
      ("Bob", 16, 176.5),
      ("Alice", null, 164.3),
      ("David", 60, null),
      ("Nina", 25, Double.NaN),
      ("Amy", null, null),
      (null, null, null)
      ).toDF("name", "age", "height")
  }

  def createNaNDF(): DataFrame = {
    Seq[(java.lang.Integer, java.lang.Long, java.lang.Short,
      java.lang.Byte, java.lang.Float, java.lang.Double)](
      (1, 1L, 1.toShort, 1.toByte, 1.0f, 1.0),
      (0, 0L, 0.toShort, 0.toByte, Float.NaN, Double.NaN)
    ).toDF("int", "long", "short", "byte", "float", "double")
  }

  def createDFWithNestedColumns: DataFrame = {
    val schema = new StructType()
      .add("c1", new StructType()
        .add("c1-1", StringType)
        .add("c1-2", StringType))
    val data = Seq(Row(Row(null, "a2")), Row(Row("b1", "b2")), Row(null))
    spark.createDataFrame(
      spark.sparkContext.parallelize(data), schema)
  }

  test("drop") {
    val input = createDF()
    val rows = input.collect()

    checkAnswer(
      input.na.drop("name" :: Nil).select("name"),
      Row("Bob") :: Row("Alice") :: Row("David") :: Row("Nina") :: Row("Amy") :: Nil)

    checkAnswer(
      input.na.drop("age" :: Nil).select("name"),
      Row("Bob") :: Row("David") :: Row("Nina") :: Nil)

    checkAnswer(
      input.na.drop("age" :: "height" :: Nil),
      rows(0) :: Nil)

    checkAnswer(
      input.na.drop(),
      rows(0))

    // dropna on an a dataframe with no column should return an empty data frame.
    val empty = input.sparkSession.emptyDataFrame.select()
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

    checkAnswer(
      input.na.drop("any"),
      rows(0) :: Nil)

    checkAnswer(
      input.na.drop("any", Seq("age", "height")),
      rows(0) :: Nil)

    checkAnswer(
      input.na.drop("all", Seq("age", "height")).select("name"),
      Row("Bob") :: Row("Alice") :: Row("David") :: Row("Nina") :: Nil)
  }

  test("drop with threshold") {
    val input = createDF()
    val rows = input.collect()

    checkAnswer(
      input.na.drop(2, Seq("age", "height")),
      rows(0) :: Nil)

    checkAnswer(
      input.na.drop(3, Seq("name", "age", "height")),
      rows(0))

    // Make sure the columns are properly named.
    assert(input.na.drop(2, Seq("age", "height")).columns.toSeq === input.columns.toSeq)
  }

  test("fill") {
    val input = createDF()

    val boolInput = Seq[(String, java.lang.Boolean)](
      ("Bob", false),
      ("Alice", null),
      ("Mallory", true),
      (null, null)
    ).toDF("name", "spy")

    withSQLConf(SQLConf.SUPPORT_QUOTED_REGEX_COLUMN_NAME.key -> "false") {
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
          .toDF("a", "b").na.fill(0),
        Row(1, 2) :: Row(-1, -2) :: Row(9123146099426677101L, 9123146560113991650L) :: Nil
      )

      checkAnswer(
        Seq[(java.lang.Long, java.lang.Double)]((null, 3.14), (9123146099426677101L, null),
          (9123146560113991650L, 1.6), (null, null)).toDF("a", "b").na.fill(0.2),
        Row(0, 3.14) :: Row(9123146099426677101L, 0.2) :: Row(9123146560113991650L, 1.6)
          :: Row(0, 0.2) :: Nil
      )

      checkAnswer(
        Seq[(java.lang.Long, java.lang.Float)]((null, 3.14f), (9123146099426677101L, null),
          (9123146560113991650L, 1.6f), (null, null)).toDF("a", "b").na.fill(0.2),
        Row(0, 3.14f) :: Row(9123146099426677101L, 0.2f) :: Row(9123146560113991650L, 1.6f)
          :: Row(0, 0.2f) :: Nil
      )

      checkAnswer(
        Seq[(java.lang.Long, java.lang.Double)]((null, 1.23), (3L, null), (4L, 3.45))
          .toDF("a", "b").na.fill(2.34),
        Row(2, 1.23) :: Row(3, 2.34) :: Row(4, 3.45) :: Nil
      )

      checkAnswer(
        Seq[(java.lang.Long, java.lang.Double)]((null, 1.23), (3L, null), (4L, 3.45))
          .toDF("a", "b").na.fill(5),
        Row(5, 1.23) :: Row(3, 5.0) :: Row(4, 3.45) :: Nil
      )
    }
  }

  test("fill with map") {
    withSQLConf(SQLConf.SUPPORT_QUOTED_REGEX_COLUMN_NAME.key -> "false") {
      val df = Seq[(String, String, java.lang.Integer, java.lang.Long,
        java.lang.Float, java.lang.Double, java.lang.Boolean)](
        (null, null, null, null, null, null, null))
        .toDF("stringFieldA", "stringFieldB", "integerField", "longField",
          "floatField", "doubleField", "booleanField")

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
      checkAnswer(df.na.fill(Map(
        "integerField" -> 1d,
        "longField" -> 2d,
        "floatField" -> 3d,
        "doubleField" -> 4d)),
        Row(null, null, 1, 2L, 3f, 4d, null))

      // Ensure column types do not change. Columns that have null values replaced
      // will no longer be flagged as nullable, so do not compare schemas directly.
      assert(df.na.fill(fillMap).schema.fields.map(_.dataType) === df.schema.fields.map(_.dataType))
    }
  }

  def createDFsWithSameFieldsName(): (DataFrame, DataFrame) = {
    val df1 = Seq(
      ("f1-1", "f2", null),
      ("f1-2", null, null),
      ("f1-3", "f2", "f3-1"),
      ("f1-4", "f2", "f3-1")
    ).toDF("f1", "f2", "f3")
    val df2 = Seq(
      ("f1-1", null, null),
      ("f1-2", "f2", null),
      ("f1-3", "f2", "f4-1")
    ).toDF("f1", "f2", "f4")
    (df1, df2)
  }

  test("fill unambiguous field for join operation") {
    val (df1, df2) = createDFsWithSameFieldsName()
    val joined_df = df1.join(df2, Seq("f1"), joinType = "left_outer")
    checkAnswer(joined_df.na.fill("", cols = Seq("f4")),
      Row("f1-1", "f2", null, null, "") ::
        Row("f1-2", null, null, "f2", "") ::
        Row("f1-3", "f2", "f3-1", "f2", "f4-1") ::
        Row("f1-4", "f2", "f3-1", null, "") :: Nil)
  }

  test("fill ambiguous field for join operation") {
    val (df1, df2) = createDFsWithSameFieldsName()
    val joined_df = df1.join(df2, Seq("f1"), joinType = "left_outer")

    val message = intercept[AnalysisException] {
      joined_df.na.fill("", cols = Seq("f2"))
    }.getMessage
    assert(message.contains("Reference 'f2' is ambiguous"))
  }

  test("fill with col(*)") {
    val df = createDF()
    // If columns are specified with "*", they are ignored.
    checkAnswer(df.na.fill("new name", Seq("*")), df.collect())
  }

  test("drop with col(*)") {
    val df = createDF()
    val exception = intercept[AnalysisException] {
      df.na.drop("any", Seq("*"))
    }
    assert(exception.getMessage.contains("Cannot resolve column name \"*\""))
  }

  test("fill with nested columns") {
    val df = createDFWithNestedColumns

    // Nested columns are ignored for fill().
    checkAnswer(df.na.fill("a1", Seq("c1.c1-1")), df)
  }

  test("drop with nested columns") {
    val df = createDFWithNestedColumns

    // Rows with the specified nested columns whose null values are dropped.
    assert(df.count == 3)
    checkAnswer(
      df.na.drop("any", Seq("c1.c1-1")),
      Seq(Row(Row("b1", "b2"))))
  }

  test("replace") {
    val input = createDF()

    // Replace two numeric columns: age and height
    val out = input.na.replace(Seq("age", "height"), Map(
      16 -> 61,
      60 -> 6,
      164.3 -> 461.3  // Alice is really tall
    )).collect()

    assert(out(0) === Row("Bob", 61, 176.5))
    assert(out(1) === Row("Alice", null, 461.3))
    assert(out(2) === Row("David", 6, null))
    assert(out(3).get(2).asInstanceOf[Double].isNaN)
    assert(out(4) === Row("Amy", null, null))
    assert(out(5) === Row(null, null, null))

    // Replace only the age column
    val out1 = input.na.replace("age", Map(
      16 -> 61,
      60 -> 6,
      164.3 -> 461.3  // Alice is really tall
    )).collect()

    assert(out1(0) === Row("Bob", 61, 176.5))
    assert(out1(1) === Row("Alice", null, 164.3))
    assert(out1(2) === Row("David", 6, null))
    assert(out1(3).get(2).asInstanceOf[Double].isNaN)
    assert(out1(4) === Row("Amy", null, null))
    assert(out1(5) === Row(null, null, null))
  }

  test("replace with null") {
    val input = Seq[(String, java.lang.Double, java.lang.Boolean)](
      ("Bob", 176.5, true),
      ("Alice", 164.3, false),
      ("David", null, true)
    ).toDF("name", "height", "married")

    // Replace String with String and null
    checkAnswer(
      input.na.replace("name", Map(
        "Bob" -> "Bravo",
        "Alice" -> null
      )),
      Row("Bravo", 176.5, true) ::
        Row(null, 164.3, false) ::
        Row("David", null, true) :: Nil)

    // Replace Double with null
    checkAnswer(
      input.na.replace("height", Map[Any, Any](
        164.3 -> null
      )),
      Row("Bob", 176.5, true) ::
        Row("Alice", null, false) ::
        Row("David", null, true) :: Nil)

    // Replace Boolean with null
    checkAnswer(
      input.na.replace("*", Map[Any, Any](
        false -> null
      )),
      Row("Bob", 176.5, true) ::
        Row("Alice", 164.3, null) ::
        Row("David", null, true) :: Nil)

    // Replace String with null and then drop rows containing null
    checkAnswer(
      input.na.replace("name", Map(
        "Bob" -> null
      )).na.drop("name" :: Nil).select("name"),
      Row("Alice") :: Row("David") :: Nil)
  }

  test("SPARK-29890: duplicate names are allowed for fill() if column names are not specified.") {
    val left = Seq(("1", null), ("3", "4")).toDF("col1", "col2")
    val right = Seq(("1", "2"), ("3", null)).toDF("col1", "col2")
    val df = left.join(right, Seq("col1"))

    // If column names are specified, the following fails due to ambiguity.
    val exception = intercept[AnalysisException] {
      df.na.fill("hello", Seq("col2"))
    }
    assert(exception.getMessage.contains("Reference 'col2' is ambiguous"))

    // If column names are not specified, fill() is applied to all the eligible columns.
    checkAnswer(
      df.na.fill("hello"),
      Row("1", "hello", "2") :: Row("3", "4", "hello") :: Nil)
  }

  test("SPARK-30065: duplicate names are allowed for drop() if column names are not specified.") {
    val left = Seq(("1", null), ("3", "4"), ("5", "6")).toDF("col1", "col2")
    val right = Seq(("1", "2"), ("3", null), ("5", "6")).toDF("col1", "col2")
    val df = left.join(right, Seq("col1"))

    // If column names are specified, the following fails due to ambiguity.
    val exception = intercept[AnalysisException] {
      df.na.drop("any", Seq("col2"))
    }
    assert(exception.getMessage.contains("Reference 'col2' is ambiguous"))

    // If column names are not specified, drop() is applied to all the eligible rows.
    checkAnswer(
      df.na.drop("any"),
      Row("5", "6", "6") :: Nil)
  }

  test("replace nan with float") {
    checkAnswer(
      createNaNDF().na.replace("*", Map(
        Float.NaN -> 10.0f
      )),
      Row(1, 1L, 1.toShort, 1.toByte, 1.0f, 1.0) ::
      Row(0, 0L, 0.toShort, 0.toByte, 10.0f, 10.0) :: Nil)
  }

  test("replace nan with double") {
    checkAnswer(
      createNaNDF().na.replace("*", Map(
        Double.NaN -> 10.0
      )),
      Row(1, 1L, 1.toShort, 1.toByte, 1.0f, 1.0) ::
      Row(0, 0L, 0.toShort, 0.toByte, 10.0f, 10.0) :: Nil)
  }

  test("replace float with nan") {
    checkAnswer(
      createNaNDF().na.replace("*", Map(
        1.0f -> Float.NaN
      )),
      Row(0, 0L, 0.toShort, 0.toByte, Float.NaN, Double.NaN) ::
      Row(0, 0L, 0.toShort, 0.toByte, Float.NaN, Double.NaN) :: Nil)
  }

  test("replace double with nan") {
    checkAnswer(
      createNaNDF().na.replace("*", Map(
        1.0 -> Double.NaN
      )),
      Row(0, 0L, 0.toShort, 0.toByte, Float.NaN, Double.NaN) ::
      Row(0, 0L, 0.toShort, 0.toByte, Float.NaN, Double.NaN) :: Nil)
  }

  test("SPARK-34417 - test fillMap() for column with a dot in the name") {
    val na = "n/a"
    checkAnswer(
      Seq(("abc", 23L), ("def", 44L), (null, 0L)).toDF("ColWith.Dot", "Col")
        .na.fill(Map("`ColWith.Dot`" -> na)),
      Row("abc", 23) :: Row("def", 44L) :: Row(na, 0L) :: Nil)
  }

  test("SPARK-34417 - test fillMap() for qualified-column with a dot in the name") {
    val na = "n/a"
    checkAnswer(
      Seq(("abc", 23L), ("def", 44L), (null, 0L)).toDF("ColWith.Dot", "Col").as("testDF")
        .na.fill(Map("testDF.`ColWith.Dot`" -> na)),
      Row("abc", 23) :: Row("def", 44L) :: Row(na, 0L) :: Nil)
  }

  test("SPARK-34417 - test fillMap() for column without a dot in the name" +
    " and dataframe with another column having a dot in the name") {
    val na = "n/a"
    checkAnswer(
      Seq(("abc", 23L), ("def", 44L), (null, 0L)).toDF("Col", "ColWith.Dot")
        .na.fill(Map("Col" -> na)),
      Row("abc", 23) :: Row("def", 44L) :: Row(na, 0L) :: Nil)
  }
}
