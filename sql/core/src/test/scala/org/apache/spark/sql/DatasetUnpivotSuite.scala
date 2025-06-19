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

import org.apache.spark.sql.functions.{length, struct, sum}
import org.apache.spark.sql.test.SharedSparkSession
import org.apache.spark.sql.types._
import org.apache.spark.util.ArrayImplicits._

/**
 * Comprehensive tests for Dataset.unpivot.
 */
class DatasetUnpivotSuite extends QueryTest
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
      ds.unpivot(Array($"id"), Array($"str1", $"str2"), "var", "val"))
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

    val e = intercept[AnalysisException] {
      wideDataDs.select($"id", $"str1", $"str2")
        .unpivot(
          Array($"id"),
          Array.empty,
          variableColumnName = "var",
          valueColumnName = "val")
    }
    checkError(
      exception = e,
      condition = "UNPIVOT_REQUIRES_VALUE_COLUMNS",
      parameters = Map())

    // ids expressions are not allowed when no values are given
    val e2 = intercept[AnalysisException] {
      wideDataDs.select($"id", $"int1", $"long1")
        .unpivot(
          Array($"id" * 2),
          variableColumnName = "var",
          valueColumnName = "val")
    }
    checkError(
      exception = e2,
      condition = "UNPIVOT_REQUIRES_ATTRIBUTES",
      parameters = Map(
        "given" -> "id",
        "empty" -> "value",
        "expressions" -> "\"(id * 2) AS `(id * 2)`\"")
    )

    val e3 = intercept[AnalysisException] {
      wideDataDs.select($"id", $"int1", $"long1")
        .unpivot(
          Array($"id".as("uid")),
          variableColumnName = "var",
          valueColumnName = "val")
    }
    checkError(
      exception = e3,
      condition = "UNPIVOT_REQUIRES_ATTRIBUTES",
      parameters = Map(
        "given" -> "id",
        "empty" -> "value",
        "expressions" -> "\"id AS uid\"")
    )
  }

  test("unpivot without ids or values") {
    val unpivoted = wideDataDs.select($"str1", $"str2")
      .unpivot(
        Array.empty,
        variableColumnName = "var",
        valueColumnName = "val")
    assert(unpivoted.schema === StructType(Seq(
      StructField("var", StringType, nullable = false),
      StructField("val", StringType, nullable = true))))
    checkAnswer(unpivoted, longDataWithoutIdRows)

    val e = intercept[AnalysisException] {
      wideDataDs.select($"str1", $"str2")
        .unpivot(
          Array.empty,
          Array.empty,
          variableColumnName = "var",
          valueColumnName = "val")
    }
    checkError(
      exception = e,
      condition = "UNPIVOT_REQUIRES_VALUE_COLUMNS",
      parameters = Map())
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

    checkAnswer(unpivoted, wideDataDs.collect().flatMap { row =>
      Seq(
        Row(row.id, "id", row.id),
        Row(row.id, "int1", row.int1.orNull),
        Row(row.id, "long1", row.long1.orNull)
      )
    }.toImmutableArraySeq)
  }

  test("unpivot with id and value expressions") {
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
    }.toImmutableArraySeq)
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
          variableColumnName = "var",
          valueColumnName = "val"
        )
    }
    checkError(
      exception = e,
      condition = "UNPIVOT_VALUE_DATA_TYPE_MISMATCH",
      parameters = Map(
        "types" -> (
          """"BIGINT" (`long1`, `long2`), """ +
            """"INT" (`int1`, `int2`, `int3`, ...), """ +
            """"STRING" (`str1`)"""
          )
      )
    )
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
    checkError(
      exception = e1,
      condition = "UNRESOLVED_COLUMN.WITH_SUGGESTION",
      parameters = Map(
        "objectName" -> "`1`",
        "proposal" -> "`id`, `int1`, `str1`, `long1`, `str2`"),
      context = ExpectedContext(fragment = "$", callSitePattern = getCurrentClassCallSitePattern))

    // unpivoting where value column does not exist
    val e2 = intercept[AnalysisException] {
      wideDataDs.unpivot(
        Array($"id"),
        Array($"does", $"not", $"exist"),
        variableColumnName = "var",
        valueColumnName = "val"
      )
    }
    checkError(
      exception = e2,
      condition = "UNRESOLVED_COLUMN.WITH_SUGGESTION",
      parameters = Map(
        "objectName" -> "`does`",
        "proposal" -> "`id`, `int1`, `long1`, `str1`, `str2`"),
      context = ExpectedContext(fragment = "$", callSitePattern = getCurrentClassCallSitePattern))

    // unpivoting without values where potential value columns are of incompatible types
    val e3 = intercept[AnalysisException] {
      wideDataDs.unpivot(
        Array.empty,
        variableColumnName = "var",
        valueColumnName = "val"
      )
    }
    checkError(
      exception = e3,
      condition = "UNPIVOT_VALUE_DATA_TYPE_MISMATCH",
      parameters = Map(
        "types" -> """"BIGINT" (`long1`), "INT" (`id`, `int1`), "STRING" (`str1`, `str2`)"""
      )
    )

    // unpivoting with star id columns so that no value columns are left
    val e4 = intercept[AnalysisException] {
      wideDataDs.unpivot(
        Array($"*"),
        variableColumnName = "var",
        valueColumnName = "val"
      )
    }
    checkError(
      exception = e4,
      condition = "UNPIVOT_REQUIRES_VALUE_COLUMNS",
      parameters = Map()
    )

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
    checkError(
      exception = e5,
      condition = "UNPIVOT_VALUE_DATA_TYPE_MISMATCH",
      parameters = Map(
        "types" -> """"BIGINT" (`long1`), "INT" (`id`, `int1`), "STRING" (`str1`, `str2`)"""
      )
    )

    // unpivoting without giving values and no non-id columns
    val e6 = intercept[AnalysisException] {
      wideDataDs.select($"id", $"str1", $"str2").unpivot(
        Array($"id", $"str1", $"str2"),
        variableColumnName = "var",
        valueColumnName = "val"
      )
    }
    checkError(
      exception = e6,
      condition = "UNPIVOT_REQUIRES_VALUE_COLUMNS",
      parameters = Map.empty
    )
  }

  test("unpivot after pivot") {
    // see test "pivot courses" in DataFramePivotSuite
    val pivoted = courseSales.groupBy("year")
      .pivot("course", Array("dotNET", "Java").toImmutableArraySeq)
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
    checkError(
      exception = e,
      condition = "UNRESOLVED_COLUMN.WITH_SUGGESTION",
      parameters = Map(
        "objectName" -> "`an`.`id`",
        "proposal" -> "`an.id`, `int1`, `long1`, `str.one`, `str.two`"),
      context = ExpectedContext(fragment = "$", callSitePattern = getCurrentClassCallSitePattern))
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

  test("unpivot sql with struct fields") {
    // accessing struct fields in FROM clause works
    checkAnswer(
      spark.sql("""SELECT * FROM (
                  |  SELECT course, `the.earnings`.* FROM (
                  |    SELECT col1 AS course,
                  |           struct(col2 AS `2012`, col3 AS `2013`, col4 AS `2014`) `the.earnings`
                  |    FROM VALUES ("dotNET", 15000, 48000.0, 22500L)
                  |  )
                  |)
                  |UNPIVOT (
                  |  earningsYear FOR year IN (`2012`, `2013`, `2014`)
                  |);
                  |""".stripMargin),
      Seq(
        ("dotNET", "2012", 15000),
        ("dotNET", "2013", 48000),
        ("dotNET", "2014", 22500)
      ).toDF("course", "year", "earningsYear")
    )

    checkAnswer(
      spark.sql("""SELECT * FROM (
                  |  SELECT course, `the.earnings`.* FROM (
                  |    SELECT col1 AS course,
                  |           named_struct(
                  |             'earnings2012', col2, 'earnings2013', col4, 'earnings2014', col6,
                  |             'sales2012', col3, 'sales2013', col5, 'sales2014', col7
                  |           ) AS `the.earnings`
                  |    FROM VALUES ("dotNET", 15000, NULL, 48000.0, 1, 22500L, 1)
                  |  )
                  |)
                  |UNPIVOT (
                  |  (earnings, sales) FOR year IN (
                  |    (`earnings2012`, `sales2012`) `2012`,
                  |    (`earnings2013`, `sales2013`) `2013`,
                  |    (`earnings2014`, `sales2014`) `2014`
                  |  )
                  |);
                  |""".stripMargin),
      Seq(
        ("dotNET", "2012", 15000, null),
        ("dotNET", "2013", 48000, Some(1)),
        ("dotNET", "2014", 22500, Some(1))
      ).toDF("course", "year", "earnings", "sales")
    )

    // accessing struct fields as unpivot columns does not work
    val e = intercept[AnalysisException] {
      spark.sql("""SELECT * FROM (
                  |  SELECT col1 AS course,
                  |         struct(col2 AS `2012`, col3 AS `2013`, col4 AS `2014`) AS `the.earnings`
                  |  FROM VALUES ("dotNET", 15000, 48000, 22500)
                  |)
                  |UNPIVOT (
                  |  earningsYear FOR year IN (`the.earnings`.`2012`,
                  |                            `the.earnings`.`2013`,
                  |                            `the.earnings`.`2014`)
                  |);
                  |""".stripMargin)
    }
    checkError(
      exception = e,
      condition = "UNPIVOT_REQUIRES_ATTRIBUTES",
      parameters = Map(
        "given" -> "value",
        "empty" -> "id",
        "expressions" -> (
          "\"the.earnings.2012 AS `2012`\", " +
            "\"the.earnings.2013 AS `2013`\", " +
            "\"the.earnings.2014 AS `2014`\"")
      ))

    val e2 = intercept[AnalysisException] {
      spark.sql("""SELECT * FROM (
                  |  SELECT col1 AS course,
                  |         named_struct('2012', col2, '2013', col4, '2014', col6) `the.earnings`,
                  |         named_struct('2012', col3, '2013', col5, '2014', col7) `the.sales`
                  |  FROM VALUES ("dotNET", 15000, NULL, 48000, 1, 22500, 1)
                  |)
                  |UNPIVOT (
                  |  (earnings, sales) FOR year IN (
                  |    (`the.earnings`.`2012`, `the.sales`.`2012`) `2012`,
                  |    (`the.earnings`.`2013`, `the.sales`.`2013`) `2013`,
                  |    (`the.earnings`.`2014`, `the.sales`.`2014`) `2014`
                  |  )
                  |);
                  |""".stripMargin)
    }
    checkError(
      exception = e2,
      condition = "UNPIVOT_REQUIRES_ATTRIBUTES",
      parameters = Map(
        "given" -> "value",
        "empty" -> "id",
        "expressions" -> (
          "\"the.earnings.2012 AS `2012`\", " +
            "\"the.sales.2012 AS `2012`\", " +
            "\"the.earnings.2013 AS `2013`\", " +
            "\"the.sales.2013 AS `2013`\", " +
            "\"the.earnings.2014 AS `2014`\", " +
            "\"the.sales.2014 AS `2014`\"")
      ))
  }

  test("unpivot sql with unpivot value number mismatch") {
    Seq("col1", "col1, col2, col3").foreach { columns =>
      withClue(columns) {
        val e = intercept[AnalysisException] {
          spark.sql(s"""SELECT * FROM VALUES (1, 2, 3)
                       |UNPIVOT (
                       |  (val1, val2) FOR col IN (($columns))
                       |);
                       |""".stripMargin)
        }
        checkError(
          exception = e,
          condition = "UNPIVOT_VALUE_SIZE_MISMATCH",
          parameters = Map("names" -> "2"))
      }
    }
  }

}

case class WideData(id: Int, str1: String, str2: String, int1: Option[Int], long1: Option[Long])
