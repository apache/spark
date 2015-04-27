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

import scala.collection.JavaConversions._

import org.apache.spark.sql.test.TestSQLContext.implicits._


class DataFrameNaFunctionsSuite extends QueryTest {

  def createDF(): DataFrame = {
    Seq[(String, java.lang.Integer, java.lang.Double)](
      ("Bob", 16, 176.5),
      ("Alice", null, 164.3),
      ("David", 60, null),
      ("Amy", null, null),
      (null, null, null)).toDF("name", "age", "height")
  }

  test("drop") {
    val input = createDF()
    val rows = input.collect()

    checkAnswer(
      input.na.drop("name" :: Nil),
      rows(0) :: rows(1) :: rows(2) :: rows(3) :: Nil)

    checkAnswer(
      input.na.drop("age" :: Nil),
      rows(0) :: rows(2) :: Nil)

    checkAnswer(
      input.na.drop("age" :: "height" :: Nil),
      rows(0) :: Nil)

    checkAnswer(
      input.na.drop(),
      rows(0))

    // dropna on an a dataframe with no column should return an empty data frame.
    val empty = input.sqlContext.emptyDataFrame.select()
    assert(empty.na.drop().count() === 0L)

    // Make sure the columns are properly named.
    assert(input.na.drop().columns.toSeq === input.columns.toSeq)
  }

  test("drop with how") {
    val input = createDF()
    val rows = input.collect()

    checkAnswer(
      input.na.drop("all"),
      rows(0) :: rows(1) :: rows(2) :: rows(3) :: Nil)

    checkAnswer(
      input.na.drop("any"),
      rows(0) :: Nil)

    checkAnswer(
      input.na.drop("any", Seq("age", "height")),
      rows(0) :: Nil)

    checkAnswer(
      input.na.drop("all", Seq("age", "height")),
      rows(0) :: rows(1) :: rows(2) :: Nil)
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

    val fillNumeric = input.na.fill(50.6)
    checkAnswer(
      fillNumeric,
      Row("Bob", 16, 176.5) ::
        Row("Alice", 50, 164.3) ::
        Row("David", 60, 50.6) ::
        Row("Amy", 50, 50.6) ::
        Row(null, 50, 50.6) :: Nil)

    // Make sure the columns are properly named.
    assert(fillNumeric.columns.toSeq === input.columns.toSeq)

    // string
    checkAnswer(
      input.na.fill("unknown").select("name"),
      Row("Bob") :: Row("Alice") :: Row("David") :: Row("Amy") :: Row("unknown") :: Nil)
    assert(input.na.fill("unknown").columns.toSeq === input.columns.toSeq)

    // fill double with subset columns
    checkAnswer(
      input.na.fill(50.6, "age" :: Nil),
      Row("Bob", 16, 176.5) ::
        Row("Alice", 50, 164.3) ::
        Row("David", 60, null) ::
        Row("Amy", 50, null) ::
        Row(null, 50, null) :: Nil)

    // fill string with subset columns
    checkAnswer(
      Seq[(String, String)]((null, null)).toDF("col1", "col2").na.fill("test", "col1" :: Nil),
      Row("test", null))
  }

  test("fill with map") {
    val df = Seq[(String, String, java.lang.Long, java.lang.Double)](
      (null, null, null, null)).toDF("a", "b", "c", "d")
    checkAnswer(
      df.na.fill(Map(
        "a" -> "test",
        "c" -> 1,
        "d" -> 2.2
      )),
      Row("test", null, 1, 2.2))

    // Test Java version
    checkAnswer(
      df.na.fill(mapAsJavaMap(Map(
        "a" -> "test",
        "c" -> 1,
        "d" -> 2.2
      ))),
      Row("test", null, 1, 2.2))
  }

  test("replace") {
    val input = createDF()

    // Replace two numeric columns: age and height
    val out = input.na.replace(Seq("age", "height"), Map(
      16 -> 61,
      60 -> 6,
      164.3 -> 461.3  // Alice is really tall
    ))

    checkAnswer(
      out,
      Row("Bob", 61, 176.5) ::
        Row("Alice", null, 461.3) ::
        Row("David", 6, null) ::
        Row("Amy", null, null) ::
        Row(null, null, null) :: Nil)

    // Replace only the age column
    val out1 = input.na.replace("age", Map(
      16 -> 61,
      60 -> 6,
      164.3 -> 461.3  // Alice is really tall
    ))

    checkAnswer(
      out1,
      Row("Bob", 61, 176.5) ::
        Row("Alice", null, 164.3) ::
        Row("David", 6, null) ::
        Row("Amy", null, null) ::
        Row(null, null, null) :: Nil)
  }
}
