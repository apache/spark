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
import org.apache.spark.sql.test.SharedSQLContext

class DataFrameNaFunctionsSuite extends QueryTest with SharedSQLContext {
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
}
