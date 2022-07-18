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

import org.apache.spark.sql.errors.QueryErrorsSuiteBase
import org.apache.spark.sql.functions.{length, struct, sum}
import org.apache.spark.sql.test.SharedSparkSession
import org.apache.spark.sql.types._

/**
 * Comprehensive tests for Dataset.unpivot.
 */
class DatasetUnpivotSuite extends QueryTest
  with QueryErrorsSuiteBase
  with SharedSparkSession {
  import testImplicits._

  lazy val wideDataDs: Dataset[WideData] = Seq(
    WideData(1, "one", "One", Some(1), Some(1L)),
    WideData(2, "two", null, None, Some(2L)),
    WideData(3, null, "three", Some(3), None),
    WideData(4, null, null, None, None)
  ).toDS()

  val longDataRows = Seq(
    Row(1, "str1", "one"),
    Row(1, "str2", "One"),
    Row(2, "str1", "two"),
    Row(2, "str2", null),
    Row(3, "str1", null),
    Row(3, "str2", "three"),
    Row(4, "str1", null),
    Row(4, "str2", null)
  )

  val longDataWithoutIdRows: Seq[Row] =
    longDataRows.map(row => Row(row.getString(1), row.getString(2)))

  val longSchema: StructType = StructType(Seq(
    StructField("id", IntegerType, nullable = false),
    StructField("var", StringType, nullable = false),
    StructField("val", StringType, nullable = true)
  ))

  lazy val wideStructDataDs: DataFrame = wideDataDs.select(
    struct($"id").as("an"),
    struct(
      $"str1".as("one"),
      $"str2".as("two")
    ).as("str")
  )
  val longStructDataRows: Seq[Row] = longDataRows.map(row =>
    Row(
      row.getInt(0),
      row.getString(1) match {
        case "str1" => "one"
        case "str2" => "two"
      },
      row.getString(2))
  )

  test("overloaded unpivot without values") {
    val ds = wideDataDs.select($"id", $"str1", $"str2")
    checkAnswer(
      ds.unpivot(Array($"id"), "var", "val"),
      ds.unpivot(Array($"id"), Array.empty, "var", "val"))
  }

  test("unpivot with single id") {
    val unpivoted = wideDataDs
      .unpivot(
        Array($"id"),
        Array($"str1", $"str2"),
        variableColumnName = "var",
        valueColumnName = "val")
    assert(unpivoted.schema === longSchema)
    checkAnswer(unpivoted, longDataRows)
  }

  test("unpivot with two ids") {
    val unpivotedRows = Seq(
      Row(1, 1, "str1", "one"),
      Row(1, 1, "str2", "One"),
      Row(2, null, "str1", "two"),
      Row(2, null, "str2", null),
      Row(3, 3, "str1", null),
      Row(3, 3, "str2", "three"),
      Row(4, null, "str1", null),
      Row(4, null, "str2", null))

    val unpivoted = wideDataDs
      .unpivot(
        Array($"id", $"int1"),
        Array($"str1", $"str2"),
        variableColumnName = "var",
        valueColumnName = "val")
    assert(unpivoted.schema === StructType(Seq(
      StructField("id", IntegerType, nullable = false),
      StructField("int1", IntegerType, nullable = true),
      StructField("var", StringType, nullable = false),
      StructField("val", StringType, nullable = true))))
    checkAnswer(unpivoted, unpivotedRows)
  }

  test("unpivot without ids") {
    val unpivoted = wideDataDs
      .unpivot(
        Array.empty,
        Array($"str1", $"str2"),
        variableColumnName = "var",
        valueColumnName = "val")
    assert(unpivoted.schema === StructType(Seq(
      StructField("var", StringType, nullable = false),
      StructField("val", StringType, nullable = true))))
    checkAnswer(unpivoted, longDataWithoutIdRows)
  }

  test("unpivot without values") {
    val unpivoted = wideDataDs.select($"id", $"str1", $"str2")
      .unpivot(
        Array($"id"),
        variableColumnName = "var",
        valueColumnName = "val")
    assert(unpivoted.schema === longSchema)
    checkAnswer(unpivoted, longDataRows)

    val unpivoted2 = wideDataDs.select($"id", $"str1", $"str2")
      .unpivot(
        Array($"id"),
        Array.empty,
        variableColumnName = "var",
        valueColumnName = "val")
    assert(unpivoted2.schema === longSchema)
    checkAnswer(unpivoted2, longDataRows)

    val unpivotedRows = Seq(
      Row(1, "id", 1L),
      Row(1, "int1", 1L),
      Row(1, "long1", 1L),
      Row(2, "id", 2L),
      Row(2, "int1", null),
      Row(2, "long1", 2L),
      Row(3, "id", 3L),
      Row(3, "int1", 3L),
      Row(3, "long1", null),
      Row(4, "id", 4L),
      Row(4, "int1", null),
      Row(4, "long1", null)
    )

    val unpivoted3 = wideDataDs.select($"id", $"int1", $"long1")
      .unpivot(
        Array($"id" * 2),
        Array.empty,
        variableColumnName = "var",
        valueColumnName = "val")
    assert(unpivoted3.schema === StructType(Seq(
      StructField("(id * 2)", IntegerType, nullable = false),
      StructField("var", StringType, nullable = false),
      StructField("val", LongType, nullable = true)
    )))
    checkAnswer(unpivoted3, unpivotedRows.map(row =>
      Row(row.getInt(0) * 2, row.get(1), row.get(2))))

    val unpivoted4 = wideDataDs.select($"id", $"int1", $"long1")
      .unpivot(
        Array($"id".as("uid")),
        Array.empty,
        variableColumnName = "var",
        valueColumnName = "val")
    assert(unpivoted4.schema === StructType(Seq(
      StructField("uid", IntegerType, nullable = false),
      StructField("var", StringType, nullable = false),
      StructField("val", LongType, nullable = true)
    )))
    checkAnswer(unpivoted4, unpivotedRows)
  }

  test("unpivot without ids or values") {
    val unpivoted = wideDataDs.select($"str1", $"str2")
      .unpivot(
        Array.empty,
        Array.empty,
        variableColumnName = "var",
        valueColumnName = "val")
    assert(unpivoted.schema === StructType(Seq(
      StructField("var", StringType, nullable = false),
      StructField("val", StringType, nullable = true))))
    checkAnswer(unpivoted, longDataWithoutIdRows)
  }

  test("unpivot with star values") {
    val unpivoted = wideDataDs.select($"str1", $"str2")
      .unpivot(
        Array.empty,
        Array($"*"),
        variableColumnName = "var",
        valueColumnName = "val")
    assert(unpivoted.schema === StructType(Seq(
      StructField("var", StringType, nullable = false),
      StructField("val", StringType, nullable = true))))
    checkAnswer(unpivoted, longDataWithoutIdRows)
  }

  test("unpivot with id and star values") {
    val unpivoted = wideDataDs.select($"id", $"int1", $"long1")
      .unpivot(
        Array($"id"),
        Array($"*"),
        variableColumnName = "var",
        valueColumnName = "val")

    assert(unpivoted.schema === StructType(Seq(
      StructField("id", IntegerType, nullable = false),
      StructField("var", StringType, nullable = false),
      StructField("val", LongType, nullable = true))))

    checkAnswer(unpivoted, wideDataDs.collect().flatMap { row => Seq(
      Row(row.id, "id", row.id),
      Row(row.id, "int1", row.int1.orNull),
      Row(row.id, "long1", row.long1.orNull)
    )})
  }

  test("unpivot with expressions") {
    // ids and values are all expressions (computed)
    val unpivoted = wideDataDs
      .unpivot(
        Array(($"id" * 10).as("primary"), $"str1".as("secondary")),
        Array(($"int1" + $"long1").as("sum"), length($"str2").as("len")),
        variableColumnName = "var",
        valueColumnName = "val")

    assert(unpivoted.schema === StructType(Seq(
      StructField("primary", IntegerType, nullable = false),
      StructField("secondary", StringType, nullable = true),
      StructField("var", StringType, nullable = false),
      StructField("val", LongType, nullable = true))))

    checkAnswer(unpivoted, wideDataDs.collect().flatMap { row =>
      Seq(
        Row(
          row.id * 10,
          row.str1,
          "sum",
          // sum of int1 and long1 when both are set, or null otherwise
          row.int1.flatMap(i => row.long1.map(l => i + l)).orNull),
        Row(
          row.id * 10,
          row.str1,
          "len",
          // length of str2 if set, or null otherwise
          Option(row.str2).map(_.length).orNull)
      )
    })
  }

  test("unpivot with variable / value columns") {
    // with value column `variable` and `value`
    val unpivoted = wideDataDs
      .withColumnRenamed("str1", "var")
      .withColumnRenamed("str2", "val")
      .unpivot(
        Array($"id"),
        Array($"var", $"val"),
        variableColumnName = "var",
        valueColumnName = "val")
    checkAnswer(unpivoted, longDataRows.map(row => Row(
      row.getInt(0),
      row.getString(1) match {
        case "str1" => "var"
        case "str2" => "val"
      },
      row.getString(2))))
  }

  test("unpivot with incompatible value types") {
    val e = intercept[AnalysisException] {
      wideDataDs
        .select(
          $"id",
          $"str1",
          $"int1", $"int1".as("int2"), $"int1".as("int3"), $"int1".as("int4"),
          $"long1", $"long1".as("long2")
        )
        .unpivot(
          Array($"id"),
          Array(),
          variableColumnName = "var",
          valueColumnName = "val"
        )
    }
    checkErrorClass(
      exception = e,
      errorClass = "UNPIVOT_VALUE_DATA_TYPE_MISMATCH",
      msg = "Unpivot value columns must share a least common type, some types do not: \\[" +
        "\"STRING\" \\(`str1#\\d+`\\), " +
        "\"INT\" \\(`int1#\\d+`, `int2#\\d+`, `int3#\\d+`, ...\\), " +
        "\"BIGINT\" \\(`long1#\\d+L`, `long2#\\d+L`\\)\\];(\n.*)*",
      matchMsg = true)
  }

  test("unpivot with compatible value types") {
    val unpivoted = wideDataDs.unpivot(
      Array($"id"),
      Array($"int1", $"long1"),
      variableColumnName = "var",
      valueColumnName = "val")
    assert(unpivoted.schema === StructType(Seq(
      StructField("id", IntegerType, nullable = false),
      StructField("var", StringType, nullable = false),
      StructField("val", LongType, nullable = true)
    )))

    val unpivotedRows = Seq(
      Row(1, "int1", 1L),
      Row(1, "long1", 1L),
      Row(2, "int1", null),
      Row(2, "long1", 2L),
      Row(3, "int1", 3L),
      Row(3, "long1", null),
      Row(4, "int1", null),
      Row(4, "long1", null)
    )
    checkAnswer(unpivoted, unpivotedRows)
  }

  test("unpivot and drop nulls") {
    checkAnswer(
      wideDataDs
        .unpivot(Array($"id"), Array($"str1", $"str2"), "var", "val")
        .where($"val".isNotNull),
      longDataRows.filter(_.getString(2) != null))
  }

  test("unpivot with invalid arguments") {
    // unpivoting where id column does not exist
    val e1 = intercept[AnalysisException] {
      wideDataDs.unpivot(
        Array($"1", $"2"),
        Array($"str1", $"str2"),
        variableColumnName = "var",
        valueColumnName = "val"
      )
    }
    checkErrorClass(
      exception = e1,
      errorClass = "UNRESOLVED_COLUMN",
      msg = "A column or function parameter with name `1` cannot be resolved\\. " +
        "Did you mean one of the following\\? \\[`id`, `int1`, `str1`, `str2`, `long1`\\];(\n.*)*",
      matchMsg = true)

    // unpivoting where value column does not exist
    val e2 = intercept[AnalysisException] {
      wideDataDs.unpivot(
        Array($"id"),
        Array($"does", $"not", $"exist"),
        variableColumnName = "var",
        valueColumnName = "val"
      )
    }
    checkErrorClass(
      exception = e2,
      errorClass = "UNRESOLVED_COLUMN",
      msg = "A column or function parameter with name `does` cannot be resolved\\. " +
        "Did you mean one of the following\\? \\[`id`, `int1`, `long1`, `str1`, `str2`\\];(\n.*)*",
      matchMsg = true)

    // unpivoting with empty list of value columns
    // where potential value columns are of incompatible types
    val e3 = intercept[AnalysisException] {
      wideDataDs.unpivot(
        Array.empty,
        Array.empty,
        variableColumnName = "var",
        valueColumnName = "val"
      )
    }
    checkErrorClass(
      exception = e3,
      errorClass = "UNPIVOT_VALUE_DATA_TYPE_MISMATCH",
      msg = "Unpivot value columns must share a least common type, some types do not: \\[" +
        "\"INT\" \\(`id#\\d+`, `int1#\\d+`\\), " +
        "\"STRING\" \\(`str1#\\d+`, `str2#\\d+`\\), " +
        "\"BIGINT\" \\(`long1#\\d+L`\\)\\];(\n.*)*",
      matchMsg = true)

    // unpivoting with star id columns so that no value columns are left
    val e4 = intercept[AnalysisException] {
      wideDataDs.unpivot(
        Array($"*"),
        Array.empty,
        variableColumnName = "var",
        valueColumnName = "val"
      )
    }
    checkErrorClass(
      exception = e4,
      errorClass = "UNPIVOT_REQUIRES_VALUE_COLUMNS",
      msg = "At least one value column needs to be specified for UNPIVOT, " +
        "all columns specified as ids;(\\n.*)*",
      matchMsg = true)

    // unpivoting with star value columns
    // where potential value columns are of incompatible types
    val e5 = intercept[AnalysisException] {
      wideDataDs.unpivot(
        Array.empty,
        Array($"*"),
        variableColumnName = "var",
        valueColumnName = "val"
      )
    }
    checkErrorClass(
      exception = e5,
      errorClass = "UNPIVOT_VALUE_DATA_TYPE_MISMATCH",
      msg = "Unpivot value columns must share a least common type, some types do not: \\[" +
        "\"INT\" \\(`id#\\d+`, `int1#\\d+`\\), " +
        "\"STRING\" \\(`str1#\\d+`, `str2#\\d+`\\), " +
        "\"BIGINT\" \\(`long1#\\d+L`\\)\\];(\n.*)*",
      matchMsg = true)

    // unpivoting without giving values and no non-id columns
    val e6 = intercept[AnalysisException] {
      wideDataDs.select($"id", $"str1", $"str2").unpivot(
        Array($"id", $"str1", $"str2"),
        Array.empty,
        variableColumnName = "var",
        valueColumnName = "val"
      )
    }
    checkErrorClass(
      exception = e6,
      errorClass = "UNPIVOT_REQUIRES_VALUE_COLUMNS",
      msg = "At least one value column needs to be specified for UNPIVOT, " +
        "all columns specified as ids;(\\n.*)*",
      matchMsg = true)
  }

  test("unpivot after pivot") {
    // see test "pivot courses" in DataFramePivotSuite
    val pivoted = courseSales.groupBy("year").pivot("course", Array("dotNET", "Java"))
      .agg(sum($"earnings"))
    val unpivoted = pivoted.unpivot(Array($"year"), "course", "earnings")
    val expected = courseSales.groupBy("year", "course").sum("earnings")
    checkAnswer(unpivoted, expected)
  }

  test("unpivot of unpivot") {
    checkAnswer(
      wideDataDs
        .unpivot(Array($"id"), Array($"str1", $"str2"), "var", "val")
        .unpivot(Array($"id"), Array($"var", $"val"), "col", "value"),
      longDataRows.flatMap(row => Seq(
        Row(row.getInt(0), "var", row.getString(1)),
        Row(row.getInt(0), "val", row.getString(2)))))
  }

  test("unpivot with dot and backtick") {
    val ds = wideDataDs
      .withColumnRenamed("id", "an.id")
      .withColumnRenamed("str1", "str.one")
      .withColumnRenamed("str2", "str.two")

    val unpivoted = ds.unpivot(
        Array($"`an.id`"),
        Array($"`str.one`", $"`str.two`"),
        variableColumnName = "var",
        valueColumnName = "val")
    checkAnswer(unpivoted, longDataRows.map(row => Row(
        row.getInt(0),
        row.getString(1) match {
          case "str1" => "str.one"
          case "str2" => "str.two"
        },
        row.getString(2))))

    // without backticks, this references struct fields, which do not exist
    val e = intercept[AnalysisException] {
      ds.unpivot(
        Array($"an.id"),
        Array($"str.one", $"str.two"),
        variableColumnName = "var",
        valueColumnName = "val"
      )
    }
    checkErrorClass(
      exception = e,
      errorClass = "UNRESOLVED_COLUMN",
      // expected message is wrong: https://issues.apache.org/jira/browse/SPARK-39783
      msg = "A column or function parameter with name `an`\\.`id` cannot be resolved\\. " +
        "Did you mean one of the following\\? " +
        "\\[`an`.`id`, `int1`, `long1`, `str`.`one`, `str`.`two`\\];(\n.*)*",
      matchMsg = true)
  }

  test("unpivot with struct fields") {
    checkAnswer(
      wideStructDataDs.unpivot(
        Array($"an.id"),
        Array($"str.one", $"str.two"),
        "var",
        "val"),
      longStructDataRows)
  }

  test("unpivot with struct ids star") {
    checkAnswer(
      wideStructDataDs.unpivot(
        Array($"an.*"),
        Array($"str.one", $"str.two"),
        "var",
        "val"),
      longStructDataRows)
  }

  test("unpivot with struct values star") {
    checkAnswer(
      wideStructDataDs.unpivot(
        Array($"an.id"),
        Array($"str.*"),
        "var",
        "val"),
      longStructDataRows)
  }
}

case class WideData(id: Int, str1: String, str2: String, int1: Option[Int], long1: Option[Long])
