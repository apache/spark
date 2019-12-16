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
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.test.SharedSparkSession
import org.apache.spark.sql.types.StructType

case class Empty()
case class Value(id: Int, value: Option[String])
case class Value2(id: Int, seq: Option[Int], value: Option[String])
case class Value3(id: Int, left_value: String, right_value: String, value: String)
case class Value4(id: Int, diff: String)
case class Value5(first_id: Int, id: String)
case class Value6(id: Int, label: String)

case class DiffAs(diff: String,
                  id: Int,
                  left_value: Option[String],
                  right_value: Option[String])
case class DiffAsCustom(action: String,
                        id: Int,
                        before_value: Option[String],
                        after_value: Option[String])
case class DiffAsSubset(diff: String,
                        id: Int,
                        left_value: Option[String])
case class DiffAsExtra(diff: String,
                       id: Int,
                       left_value: Option[String],
                       right_value: Option[String],
                       extra: String)

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

  lazy val expectedReverseDiff: Seq[Row] = Seq(
    Row("N", 1, "one", "one"),
    Row("C", 2, "Two", "two"),
    Row("I", 3, null, "three"),
    Row("D", 4, "four", null)
  )

  lazy val expectedDiffAs: Seq[DiffAs] = expectedDiff.map( r =>
    DiffAs(r.getString(0), r.getInt(1), Option(r.getString(2)), Option(r.getString(3)))
  )

  test("distinct string for") {
    assert(Diff.distinctStringNameFor(Seq.empty[String]) === "_")
    assert(Diff.distinctStringNameFor(Seq("a")) === "__")
    assert(Diff.distinctStringNameFor(Seq("abc")) === "____")
    assert(Diff.distinctStringNameFor(Seq("a", "bc", "def")) === "____")
  }

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
    val reverse = right.diff(left, "id").orderBy("id")

    assert(actual.columns === expectedDiffColumns)
    assert(actual.collect() === expectedDiff)
    assert(reverse.columns === expectedDiffColumns)
    assert(reverse.collect() === expectedReverseDiff)
  }

  test("diff with one ID column case-insensitive") {
    val actual = left.diff(right, "ID").orderBy("ID")
    val reverse = right.diff(left, "ID").orderBy("ID")

    assert(actual.columns === Seq("diff", "ID", "left_value", "right_value"))
    assert(actual.collect() === expectedDiff)
    assert(reverse.columns === Seq("diff", "ID", "left_value", "right_value"))
    assert(reverse.collect() === expectedReverseDiff)
  }

  test("diff with one id column case-sensitive") {
    withSQLConf(SQLConf.CASE_SENSITIVE.key -> "true") {
      val actual = left.diff(right, "id").orderBy("id")
      val reverse = right.diff(left, "id").orderBy("id")

      assert(actual.columns === expectedDiffColumns)
      assert(actual.collect() === expectedDiff)
      assert(reverse.columns === expectedDiffColumns)
      assert(reverse.collect() === expectedReverseDiff)
    }
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
    val reverse = right.toDF().diff(left.toDF(), "id").orderBy("id")

    assert(actual.columns === expectedDiffColumns)
    assert(actual.collect() === expectedDiff)
    assert(reverse.columns === expectedDiffColumns)
    assert(reverse.collect() === expectedReverseDiff)
  }

  test("diff with output columns in T") {
    val left = Seq(Value3(1, "left", "right", "value")).toDS()
    val right = Seq(Value3(1, "Left", "Right", "Value")).toDS()

    val actual = left.diff(right, "id")
    val expectedColumns = Seq(
      "diff",
      "id",
      "left_left_value", "right_left_value",
      "left_right_value", "right_right_value",
      "left_value", "right_value"
    )
    val expectedDiff = Seq(
      Row("C", 1, "left", "Left", "right", "Right", "value", "Value")
    )

    assert(actual.columns === expectedColumns)
    assert(actual.collect() === expectedDiff)
  }

  test("diff with id column diff in T") {
    val left = Seq(Value4(1, "diff")).toDS()
    val right = Seq(Value4(1, "Diff")).toDS()

    doTestRequirement(left.diff(right),
      "The id columns must not contain the diff column name 'diff': id, diff")
    doTestRequirement(left.diff(right, "diff"),
      "The id columns must not contain the diff column name 'diff': diff")
    doTestRequirement(left.diff(right, "diff", "id"),
      "The id columns must not contain the diff column name 'diff': diff, id")
  }

  test("diff with non-id column diff in T") {
    val left = Seq(Value4(1, "diff")).toDS()
    val right = Seq(Value4(1, "Diff")).toDS()

    val actual = left.diff(right, "id")
    val expectedColumns = Seq(
      "diff",
      "id",
      "left_diff", "right_diff"
    )
    val expectedDiff = Seq(
      Row("C", 1, "diff", "Diff")
    )

    assert(actual.columns === expectedColumns)
    assert(actual.collect() === expectedDiff)
  }

  test("diff where non-id column produces diff column name") {
    val options = DiffOptions.default.copy(diffColumn = "a_label", leftColumnPrefix = "a", rightColumnPrefix = "b")

    val left = Seq(Value6(1, "label")).toDS()
    val right = Seq(Value6(1, "Label")).toDS()

    doTestRequirement(left.diff(right, options, "id"),
      "The column prefixes 'a' and 'b', together with these non-id columns " +
        "must not produce the diff column name 'a_label': label")
  }

  test("diff where non-id column produces id column name") {
    val options = DiffOptions.default.copy(leftColumnPrefix = "first", rightColumnPrefix = "second")

    val left = Seq(Value5(1, "value")).toDS()
    val right = Seq(Value5(1, "Value")).toDS()

    doTestRequirement(left.diff(right, options, "first_id"),
      "The column prefixes 'first' and 'second', together with these non-id columns " +
        "must not produce any id column name 'first_id': id")
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

  test("diff options with empty diff column name") {
    val default = DiffOptions.default
    doTestRequirement(default.copy(diffColumn = ""),
      "Diff column name must not be empty")
  }

  test("diff options left and right prefixes") {
    val default = DiffOptions.default
    doTestRequirement(default.copy(leftColumnPrefix = ""),
      "Left column prefix must not be empty")
    doTestRequirement(default.copy(rightColumnPrefix = ""),
      "Right column prefix must not be empty")
    val prefix = "prefix"
    doTestRequirement(default.copy(leftColumnPrefix = prefix, rightColumnPrefix = prefix),
      s"Left and right column prefix must be distinct: $prefix")
  }

  test("diff options diff value") {
    val default = DiffOptions.default

    doTestRequirement(default.copy(insertDiffValue = ""),
      s"Insert diff value must not be empty")
    doTestRequirement(default.copy(changeDiffValue = ""),
      s"Change diff value must not be empty")
    doTestRequirement(default.copy(deleteDiffValue = ""),
      s"Delete diff value must not be empty")
    doTestRequirement(default.copy(nochangeDiffValue = ""),
      s"No-change diff value must not be empty")

    val value = "value"
    doTestRequirement(default.copy(insertDiffValue = value, changeDiffValue = value),
      s"Diff values must be distinct: List($value, $value, D, N)")
    doTestRequirement(default.copy(insertDiffValue = value, deleteDiffValue = value),
      s"Diff values must be distinct: List($value, C, $value, N)")
    doTestRequirement(default.copy(insertDiffValue = value, nochangeDiffValue = value),
      s"Diff values must be distinct: List($value, C, D, $value)")
    doTestRequirement(default.copy(changeDiffValue = value, deleteDiffValue = value),
      s"Diff values must be distinct: List(I, $value, $value, N)")
    doTestRequirement(default.copy(changeDiffValue = value, nochangeDiffValue = value),
      s"Diff values must be distinct: List(I, $value, D, $value)")
    doTestRequirement(default.copy(deleteDiffValue = value, nochangeDiffValue = value),
      s"Diff values must be distinct: List(I, C, $value, $value)")
  }

  test("diff of empty schema") {
    val left = Seq(Empty()).toDS()
    val right = Seq(Empty()).toDS()

    doTestRequirement(left.diff(right), "The schema must not be empty")
  }

  test("diff with different types") {
    // different value types only compiles with DataFrames
    val left = Seq((1, "str")).toDF("id", "value")
    val right = Seq((1, 2)).toDF("id", "value")

    doTestRequirement(left.diff(right),
      "The datasets do not have the same schema.\n" +
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
    val reverse = right2.diff(left2, "id").orderBy("id")

    assert(actual.columns === expectedDiffColumns)
    assert(actual.collect() === expectedDiff)
    assert(reverse.columns === expectedDiffColumns)
    assert(reverse.collect() === expectedReverseDiff)
  }

  test("diff with different column names") {
    // different column names only compiles with DataFrames
    val left = Seq((1, "str")).toDF("id", "value")
    val right = Seq((1, "str")).toDF("id", "comment")

    doTestRequirement(left.diff(right, "id"),
      "The datasets do not have the same schema.\n" +
        "Left extra columns: value (StringType)\n" +
        "Right extra columns: comment (StringType)")
  }

  test("diff with case-insensitive column names") {
    // different column names only compiles with DataFrames
    val left = this.left.toDF("id", "value")
    val right = this.right.toDF("ID", "VaLuE")

    val actual = left.diff(right, "id").orderBy("id")
    val reverse = right.diff(left, "id").orderBy("id")

    assert(actual.columns === expectedDiffColumns)
    assert(actual.collect() === expectedDiff)
    assert(reverse.columns === Seq("diff", "id", "left_VaLuE", "right_VaLuE"))
    assert(reverse.collect() === expectedReverseDiff)
  }

  test("diff with case-sensitive column names") {
    // different column names only compiles with DataFrames
    val left = this.left.toDF("id", "value")
    val right = this.right.toDF("ID", "VaLuE")

    withSQLConf(SQLConf.CASE_SENSITIVE.key -> "true") {
      doTestRequirement(left.diff(right, "id"),
        "The datasets do not have the same schema.\n" +
          "Left extra columns: id (IntegerType), value (StringType)\n" +
          "Right extra columns: ID (IntegerType), VaLuE (StringType)")
    }
  }

  test("diff of non-existing id column") {
    doTestRequirement(left.diff(right, "does not exists"),
      "Some id columns do not exist: does not exists")
  }

  test("diff with different number of column") {
    // different column names only compiles with DataFrames
    val left = Seq((1, "str")).toDF("id", "value")
    val right = Seq((1, 1, "str")).toDF("id", "seq", "value")

    doTestRequirement(left.diff(right, "id"),
      "The number of columns doesn't match.\n" +
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

  test("diff as U with subset of columns") {
    val expected = expectedDiff.map(row => DiffAsSubset(row.getString(0), row.getInt(1), Option(row.getString(2))))

    val actual = left.diffAs[DiffAsSubset](right, "id").orderBy("id")

    assert(Seq("diff", "id", "left_value").forall(column => actual.columns.contains(column)))
    assert(actual.collect() === expected)
  }

  test("diff as U with extra column") {
    doTestRequirement(left.diffAs[DiffAsExtra](right, "id"),
      "Diff encoder's columns must be part of the diff result schema, these columns are unexpected: extra")
  }

  def doTestRequirement(f : => Any, expected: String): Unit = {
    assert(intercept[IllegalArgumentException](f).getMessage === s"requirement failed: $expected")
  }

}

