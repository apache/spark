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

import org.apache.spark.SparkFunSuite
import org.apache.spark.internal.Logging
import org.apache.spark.sql.{DataFrame, Dataset, Diff, DiffOptions, Encoders, Row}
import org.apache.spark.sql.expressions.SparkUserDefinedFunction
import org.apache.spark.sql.functions._
import org.apache.spark.sql.test.SharedSparkSession
import org.apache.spark.sql.types.StructType

case class Empty()
case class Value(id: Int, value: Option[String])
case class Value2(id: Int, seq: Option[Int], value: Option[String])

case class DiffAs(diff: String,
                  id: Int,
                  left_value: Option[String],
                  right_value: Option[String])
case class DiffAsCustom(action: String,
                        id: Int,
                        before_value: Option[String],
                        after_value: Option[String])

class DiffSuite extends SparkFunSuite with SharedSparkSession with Logging {

  import testImplicits._

  lazy val left: Dataset[Value] = Seq(
    Value(1, Some("one")),
    Value(2, Some("two")),
    Value(3, Some("three"))
  ).toDS()

  lazy val right: Dataset[Value] = Seq(
    Value(1, Some("one")),
    Value(2, Some("Two")),
    Value(4, Some("four"))
  ).toDS()

  lazy val expectedDiffColumns: Seq[String] =
    Seq("diff", "id", "left_value", "right_value")

  lazy val expectedDiff: Seq[Row] = Seq(
    Row("N", 1, "one", "one"),
    Row("C", 2, "two", "Two"),
    Row("D", 3, "three", null),
    Row("I", 4, null, "four")
  )

  lazy val expectedDiffAs: Seq[DiffAs] = expectedDiff.map( r =>
    DiffAs(r.getString(0), r.getInt(1), Option(r.getString(2)), Option(r.getString(3)))
  )

  test("diff with no id column") {
    val expected = Seq(
      Row("N", 1, "one"),
      Row("D", 2, "two"),
      Row("I", 2, "Two"),
      Row("D", 3, "three"),
      Row("I", 4, "four")
    )

    val actual = left.diff(right).orderBy("id", "diff")

    assert(actual.columns === Seq("diff", "id", "value"))
    assert(actual.collect() === expected)
  }

  test("diff with one id column") {
    val actual = left.diff(right, "id").orderBy("id")

    assert(actual.columns === expectedDiffColumns)
    assert(actual.collect() === expectedDiff)
  }

  test("diff with two id columns") {
    val left = Seq(
      Value2(1, Some(1), Some("one")),
      Value2(2, Some(1), Some("two.one")),
      Value2(2, Some(2), Some("two.two")),
      Value2(3, Some(1), Some("three"))
    ).toDS()

    val right = Seq(
      Value2(1, Some(1), Some("one")),
      Value2(2, Some(1), Some("two.one")),
      Value2(2, Some(2), Some("two.Two")),
      Value2(4, Some(1), Some("four"))
    ).toDS()

    val expected = Seq(
      Row("N", 1, 1, "one", "one"),
      Row("N", 2, 1, "two.one", "two.one"),
      Row("C", 2, 2, "two.two", "two.Two"),
      Row("D", 3, 1, "three", null),
      Row("I", 4, 1, null, "four")
    )

    val actual = left.diff(right, "id", "seq")
      .orderBy("id", "seq")

    assert(actual.columns === Seq("diff", "id", "seq", "left_value", "right_value"))
    assert(actual.collect() === expected)
  }

  test("diff with all id columns") {
    val expected = Seq(
      Row("N", 1, "one"),
      Row("D", 2, "two"),
      Row("I", 2, "Two"),
      Row("D", 3, "three"),
      Row("I", 4, "four")
    )

    val actual = left.diff(right, "id", "value")
      .orderBy("id", "diff")

    assert(actual.columns === Seq("diff", "id", "value"))
    assert(actual.collect() === expected)
  }

  test("diff with null values") {
    val left = Seq(
      Value(1, None),
      Value(2, None),
      Value(3, Some("three")),
      Value(4, None)
    ).toDS()

    val right = Seq(
      Value(1, None),
      Value(2, Some("two")),
      Value(3, None),
      Value(5, None)
    ).toDS()

    val expected = Seq(
      Row("N", 1, null, null),
      Row("C", 2, null, "two"),
      Row("C", 3, "three", null),
      Row("D", 4, null, null),
      Row("I", 5, null, null)
    )

    val actual = left.diff(right, "id").orderBy("id")

    assert(actual.columns === Seq("diff", "id", "left_value", "right_value"))
    assert(actual.collect() === expected)
  }

  test("diff with null id values") {
    val left = Seq(
      Value2(1, None, Some("one")),
      Value2(2, Some(1), Some("two.one")),
      Value2(2, Some(2), Some("two.two")),
      Value2(3, None, Some("three"))
    ).toDS()

    val right = Seq(
      Value2(1, None, Some("one")),
      Value2(2, Some(1), Some("two.one")),
      Value2(2, Some(2), Some("two.Two")),
      Value2(4, None, Some("four"))
    ).toDS()

    val expected = Seq(
      Row("N", 1, None.orNull, "one", "one"),
      Row("N", 2, 1, "two.one", "two.one"),
      Row("C", 2, 2, "two.two", "two.Two"),
      Row("D", 3, None.orNull, "three", None.orNull),
      Row("I", 4, None.orNull, None.orNull, "four")
    )

    val actual = left.diff(right, "id", "seq")
      .orderBy("id", "seq")

    assert(actual.columns === Seq("diff", "id", "seq", "left_value", "right_value"))
    assert(actual.collect() === expected)
  }

  /**
   * Tests the column order of the produced diff DataFrame.
   */
  test("diff column order") {
    // left has same schema as right but different column order
    val left = Seq(
      // value1, id, value2, seq, value3
      ("val1.1.1", 1, "val1.1.2", 1, "val1.1.3"),
      ("val1.2.1", 1, "val1.2.2", 2, "val1.2.3"),
      ("val2.1.1", 2, "val2.1.2", 1, "val2.1.3")
    ).toDF("value1", "id", "value2", "seq", "value3")
    val right = Seq(
      // value2, seq, value3, id, value1
      ("val1.1.2", 1, "val1.1.3", 1, "val1.1.1"),
      ("val1.2.2", 2, "val1.2.3 changed", 1, "val1.2.1"),
      ("val2.2.2", 2, "val2.2.3", 2, "val2.2.1")
    ).toDF("value2", "seq", "value3", "id", "value1")

    // diffing left to right provides schema of result DataFrame different to right-to-left diff
    {
      val expected = Seq(
        Row("N", 1, 1, "val1.1.1", "val1.1.1", "val1.1.2", "val1.1.2", "val1.1.3", "val1.1.3"),
        Row("C", 1, 2, "val1.2.1", "val1.2.1", "val1.2.2", "val1.2.2", "val1.2.3", "val1.2.3 changed"),
        Row("D", 2, 1, "val2.1.1", null, "val2.1.2", null, "val2.1.3", null),
        Row("I", 2, 2, null, "val2.2.1", null, "val2.2.2", null, "val2.2.3")
      )
      val expectedColumns = Seq(
        "diff",
        "id", "seq",
        "left_value1", "right_value1",
        "left_value2", "right_value2",
        "left_value3", "right_value3"
      )

      val actual = left.diff(right, "id", "seq").orderBy("id", "seq")

      assert(actual.columns === expectedColumns)
      assert(actual.collect() === expected)
    }

    // diffing right to left provides different schema of result DataFrame
    {
      val expected = Seq(
        Row("N", 1, 1, "val1.1.2", "val1.1.2", "val1.1.3", "val1.1.3", "val1.1.1", "val1.1.1"),
        Row("C", 1, 2, "val1.2.2", "val1.2.2", "val1.2.3 changed", "val1.2.3", "val1.2.1", "val1.2.1"),
        Row("I", 2, 1, null, "val2.1.2", null, "val2.1.3", null, "val2.1.1"),
        Row("D", 2, 2, "val2.2.2", null, "val2.2.3", null, "val2.2.1", null)
      )
      val expectedColumns = Seq(
        "diff",
        "id", "seq",
        "left_value2", "right_value2",
        "left_value3", "right_value3",
        "left_value1", "right_value1"
      )

      val actual = right.diff(left, "id", "seq").orderBy("id", "seq")

      assert(actual.columns === expectedColumns)
      assert(actual.collect() === expected)
    }

    // diffing left to right without id columns takes column order of left
    {
      val expected = Seq(
        Row("N", "val1.1.1", 1, "val1.1.2", 1, "val1.1.3"),
        Row("D", "val1.2.1", 1, "val1.2.2", 2, "val1.2.3"),
        Row("I", "val1.2.1", 1, "val1.2.2", 2, "val1.2.3 changed"),
        Row("D", "val2.1.1", 2, "val2.1.2", 1, "val2.1.3"),
        Row("I", "val2.2.1", 2, "val2.2.2", 2, "val2.2.3")
      )
      val expectedColumns = Seq(
        "diff", "value1", "id", "value2", "seq", "value3"
      )

      val actual = left.diff(right).orderBy("id", "seq", "diff")

      assert(actual.columns === expectedColumns)
      assert(actual.collect() === expected)
    }

    // diffing right to left without id columns takes column order of right
    {
      val expected = Seq(
        Row("N", "val1.1.1", 1, "val1.1.2", 1, "val1.1.3"),
        Row("D", "val1.2.1", 1, "val1.2.2", 2, "val1.2.3"),
        Row("I", "val1.2.1", 1, "val1.2.2", 2, "val1.2.3 changed"),
        Row("D", "val2.1.1", 2, "val2.1.2", 1, "val2.1.3"),
        Row("I", "val2.2.1", 2, "val2.2.2", 2, "val2.2.3")
      )
      val expectedColumns = Seq(
        "diff", "value1", "id", "value2", "seq", "value3"
      )

      val actual = left.diff(right).orderBy("id", "seq", "diff")

      assert(actual.columns === expectedColumns)
      assert(actual.collect() === expected)
    }
  }

  test("diff DataFrames") {
    val actual = left.toDF().diff(right.toDF(), "id").orderBy("id")

    assert(actual.columns === expectedDiffColumns)
    assert(actual.collect() === expectedDiff)
  }

  test("diff with custom diff options") {
    val options = DiffOptions("action", "before", "after", "new", "change", "del", "eq")

    val expected = Seq(
      Row("eq", 1, "one", "one"),
      Row("change", 2, "two", "Two"),
      Row("del", 3, "three", null),
      Row("new", 4, null, "four")
    )

    val actual = left.diff(right, options, "id")
      .orderBy("id", "action")

    assert(actual.columns === Seq("action", "id", "before_value", "after_value"))
    assert(actual.collect() === expected)
  }

  test("diff of empty schema") {
    val left = Seq(Empty()).toDS()
    val right = Seq(Empty()).toDS()

    val message = intercept[IllegalArgumentException]{ left.diff(right) }.getMessage

    assert(message === "requirement failed: The schema must not be empty")
  }

  test("diff with different types") {
    // different value types only compiles with DataFrames
    val left = Seq((1, "str")).toDF("id", "value")
    val right = Seq((1, 2)).toDF("id", "value")

    val message = intercept[IllegalArgumentException]{ left.diff(right) }.getMessage

    assert(message === "requirement failed: The datasets do not have the same schema.\n" +
      "Left extra columns: value (StringType)\n" +
      "Right extra columns: value (IntegerType)")
  }

  test("diff with different nullability") {
    val leftSchema = StructType(left.schema.fields.map(_.copy(nullable = true)))
    val rightSchema = StructType(right.schema.fields.map(_.copy(nullable = false)))

    // different value types only compiles with DataFrames
    val left2 = sqlContext.createDataFrame(left.toDF().rdd, leftSchema)
    val right2 = sqlContext.createDataFrame(right.toDF().rdd, rightSchema)

    val actual = left2.diff(right2, "id").orderBy("id")

    assert(actual.columns === expectedDiffColumns)
    assert(actual.collect() === expectedDiff)
  }

  test("diff with different column names") {
    // different column names only compiles with DataFrames
    val left = Seq((1, "str")).toDF("id", "value")
    val right = Seq((1, "str")).toDF("id", "comment")

    val message = intercept[IllegalArgumentException] {
      left.diff(right, "id")
    }.getMessage

    assert(message === "requirement failed: The datasets do not have the same schema.\n" +
      "Left extra columns: value (StringType)\n" +
      "Right extra columns: comment (StringType)")
  }

  test("diff of non-existing id column") {
    val message = intercept[IllegalArgumentException] {
      left.diff(right, "does not exists")
    }.getMessage

    assert(message === "requirement failed: Some id columns do not exist: does not exists")
  }

  test("diff with different number of column") {
    // different column names only compiles with DataFrames
    val left = Seq((1, "str")).toDF("id", "value")
    val right = Seq((1, 1, "str")).toDF("id", "seq", "value")

    val message = intercept[IllegalArgumentException] {
      left.diff(right, "id")
    }.getMessage

    assert(message === "requirement failed: The number of columns doesn't match.\n" +
      "Left column names (2): id, value\n" +
      "Right column names (3): id, seq, value")
  }

  test("diff as U") {
    val actual = left.diffAs[DiffAs](right, "id").orderBy("id")

    assert(actual.columns === Seq("diff", "id", "left_value", "right_value"))
    assert(actual.collect() === expectedDiffAs)
  }

  test("diff as U with encoder") {
    val encoder = Encoders.product[DiffAs]

    val actual = left.diffAs(right, encoder, "id").orderBy("id")

    assert(actual.columns === Seq("diff", "id", "left_value", "right_value"))
    assert(actual.collect() === expectedDiffAs)
  }

  test("diff as U with encoder and custom options") {
    val options = DiffOptions("action", "before", "after", "new", "change", "del", "eq")
    val encoder = Encoders.product[DiffAsCustom]

    val actions = Seq(
      (DiffOptions.default.insertDiffValue, "new"),
      (DiffOptions.default.changeDiffValue, "change"),
      (DiffOptions.default.deleteDiffValue, "del"),
      (DiffOptions.default.nochangeDiffValue, "eq"),
    ).toDF("diff", "action")

    val expected = expectedDiffAs.toDS()
      .join(actions, "diff")
      .select($"action", $"id", $"left_value".as("before_value"), $"right_value".as("after_value"))
      .as[DiffAsCustom]
      .collect()

    val actual = left.diffAs(right, options, encoder, "id").orderBy("id")

    assert(actual.columns === Seq("action", "id", "before_value", "after_value"))
    assert(actual.collect() === expected)
  }

}

