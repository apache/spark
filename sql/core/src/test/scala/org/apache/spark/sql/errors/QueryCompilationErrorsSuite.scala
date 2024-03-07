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

package org.apache.spark.sql.errors

import java.util.IllegalFormatException

import org.apache.spark.{SPARK_DOC_ROOT, SparkIllegalArgumentException, SparkUnsupportedOperationException}
import org.apache.spark.sql._
import org.apache.spark.sql.api.java.{UDF1, UDF2, UDF23Test}
import org.apache.spark.sql.catalyst.expressions.{Coalesce, Literal, UnsafeRow}
import org.apache.spark.sql.catalyst.parser.ParseException
import org.apache.spark.sql.execution.datasources.parquet.SparkToParquetSchemaConverter
import org.apache.spark.sql.execution.datasources.v2.jdbc.JDBCTableCatalog
import org.apache.spark.sql.expressions.SparkUserDefinedFunction
import org.apache.spark.sql.functions._
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.test.SharedSparkSession
import org.apache.spark.sql.types._
import org.apache.spark.util.Utils

case class StringLongClass(a: String, b: Long)

case class StringIntClass(a: String, b: Int)

case class ComplexClass(a: Long, b: StringLongClass)

case class ArrayClass(arr: Seq[StringIntClass])

class QueryCompilationErrorsSuite
  extends QueryTest
  with QueryErrorsBase
  with SharedSparkSession {
  import testImplicits._

  test("CANNOT_UP_CAST_DATATYPE: invalid upcast data type") {
    val e1 = intercept[AnalysisException] {
      sql("select 'value1' as a, 1L as b").as[StringIntClass]
    }
    checkError(
      exception = e1,
      errorClass = "CANNOT_UP_CAST_DATATYPE",
      parameters = Map("expression" -> "b", "sourceType" -> "\"BIGINT\"", "targetType" -> "\"INT\"",
        "details" -> (
        s"""
           |The type path of the target object is:
           |- field (class: "int", name: "b")
           |- root class: "org.apache.spark.sql.errors.StringIntClass"
           |You can either add an explicit cast to the input data or choose a higher precision type
         """.stripMargin.trim + " of the field in the target object")))

    val e2 = intercept[AnalysisException] {
      sql("select 1L as a," +
        " named_struct('a', 'value1', 'b', cast(1.0 as decimal(38,18))) as b")
        .as[ComplexClass]
    }
    checkError(
      exception = e2,
      errorClass = "CANNOT_UP_CAST_DATATYPE",
      parameters = Map("expression" -> "b.`b`", "sourceType" -> "\"DECIMAL(38,18)\"",
        "targetType" -> "\"BIGINT\"",
        "details" -> (
        s"""
           |The type path of the target object is:
           |- field (class: "long", name: "b")
           |- field (class: "org.apache.spark.sql.errors.StringLongClass", name: "b")
           |- root class: "org.apache.spark.sql.errors.ComplexClass"
           |You can either add an explicit cast to the input data or choose a higher precision type
         """.stripMargin.trim + " of the field in the target object")))
  }

  test("UNSUPPORTED_GROUPING_EXPRESSION: filter with grouping/grouping_Id expression") {
    val df = Seq(
      (536361, "85123A", 2, 17850),
      (536362, "85123B", 4, 17850),
      (536363, "86123A", 6, 17851)
    ).toDF("InvoiceNo", "StockCode", "Quantity", "CustomerID")
    Seq("grouping", "grouping_id").foreach { grouping =>
      val e = intercept[AnalysisException] {
        df.groupBy("CustomerId").agg(Map("Quantity" -> "max"))
          .filter(s"$grouping(CustomerId)=17850")
      }
      checkError(
        exception = e,
        errorClass = "UNSUPPORTED_GROUPING_EXPRESSION",
        parameters = Map[String, String]())
    }
  }

  test("UNSUPPORTED_GROUPING_EXPRESSION: Sort with grouping/grouping_Id expression") {
    val df = Seq(
      (536361, "85123A", 2, 17850),
      (536362, "85123B", 4, 17850),
      (536363, "86123A", 6, 17851)
    ).toDF("InvoiceNo", "StockCode", "Quantity", "CustomerID")
    Seq(grouping("CustomerId"), grouping_id("CustomerId")).foreach { grouping =>
      val e = intercept[AnalysisException] {
        df.groupBy("CustomerId").agg(Map("Quantity" -> "max")).
          sort(grouping)
      }
      checkError(
        exception = e,
        errorClass = "UNSUPPORTED_GROUPING_EXPRESSION",
        parameters = Map[String, String]())
    }
  }

  test("INVALID_PARAMETER_VALUE.ZERO_INDEX: the argument_index of string format is invalid") {
    withSQLConf(SQLConf.ALLOW_ZERO_INDEX_IN_FORMAT_STRING.key -> "false") {
      checkError(
        exception = intercept[AnalysisException] {
          sql("select format_string('%0$s', 'Hello')")
        },
        errorClass = "INVALID_PARAMETER_VALUE.ZERO_INDEX",
        parameters = Map(
          "parameter" -> "`strfmt`",
          "functionName" -> "`format_string`"),
        context = ExpectedContext(
          fragment = "format_string('%0$s', 'Hello')", start = 7, stop = 36))
    }
    withSQLConf(SQLConf.ALLOW_ZERO_INDEX_IN_FORMAT_STRING.key -> "true") {
      intercept[IllegalFormatException] {
        sql("select format_string('%0$s', 'Hello')").collect()
      }
    }
  }

  test("INVALID_PANDAS_UDF_PLACEMENT: Using aggregate function with grouped aggregate pandas UDF") {
    import IntegratedUDFTestUtils._
    assume(shouldTestPandasUDFs)

    val df = Seq(
      (536361, "85123A", 2, 17850),
      (536362, "85123B", 4, 17850),
      (536363, "86123A", 6, 17851)
    ).toDF("InvoiceNo", "StockCode", "Quantity", "CustomerID")
    val e = intercept[AnalysisException] {
      val pandasTestUDF1 = TestGroupedAggPandasUDF(name = "pandas_udf_1")
      val pandasTestUDF2 = TestGroupedAggPandasUDF(name = "pandas_udf_2")
      df.groupBy("CustomerId")
        .agg(pandasTestUDF1(df("Quantity")), pandasTestUDF2(df("Quantity")), sum(df("Quantity")))
        .collect()
    }

    checkError(
      exception = e,
      errorClass = "INVALID_PANDAS_UDF_PLACEMENT",
      parameters = Map("functionList" -> "`pandas_udf_1`, `pandas_udf_2`"))
  }

  test("UNSUPPORTED_FEATURE: Using Python UDF with unsupported join condition") {
    import IntegratedUDFTestUtils._
    assume(shouldTestPythonUDFs)

    val df1 = Seq(
      (536361, "85123A", 2, 17850),
      (536362, "85123B", 4, 17850),
      (536363, "86123A", 6, 17851)
    ).toDF("InvoiceNo", "StockCode", "Quantity", "CustomerID")
    val df2 = Seq(
      ("Bob", 17850),
      ("Alice", 17850),
      ("Tom", 17851)
    ).toDF("CustomerName", "CustomerID")

    val e = intercept[AnalysisException] {
      val pythonTestUDF = TestPythonUDF(name = "python_udf", Some(BooleanType))
      df1.join(
        df2, pythonTestUDF(df1("CustomerID") === df2("CustomerID")), "leftouter").collect()
    }

    checkError(
      exception = e,
      errorClass = "UNSUPPORTED_FEATURE.PYTHON_UDF_IN_ON_CLAUSE",
      parameters = Map("joinType" -> "LEFT OUTER"),
      sqlState = Some("0A000"))
  }

  test("UNSUPPORTED_FEATURE: Using pandas UDF aggregate expression with pivot") {
    import IntegratedUDFTestUtils._
    assume(shouldTestPandasUDFs)

    val df = Seq(
      (536361, "85123A", 2, 17850),
      (536362, "85123B", 4, 17850),
      (536363, "86123A", 6, 17851)
    ).toDF("InvoiceNo", "StockCode", "Quantity", "CustomerID")

    val e = intercept[AnalysisException] {
      val pandasTestUDF = TestGroupedAggPandasUDF(name = "pandas_udf")
      df.groupBy(df("CustomerID")).pivot(df("CustomerID")).agg(pandasTestUDF(df("Quantity")))
    }

    checkError(
      exception = e,
      errorClass = "UNSUPPORTED_FEATURE.PANDAS_UDAF_IN_PIVOT",
      parameters = Map[String, String](),
      sqlState = "0A000")
  }

  test("NO_HANDLER_FOR_UDAF: No handler for UDAF error") {
    val functionName = "myCast"
    withUserDefinedFunction(functionName -> true) {
      sql(
        s"""
          |CREATE TEMPORARY FUNCTION $functionName
          |AS 'org.apache.spark.sql.errors.MyCastToString'
          |""".stripMargin)

      val e = intercept[AnalysisException] (
        sql(s"SELECT $functionName(123) as value")
      )
      checkError(
        exception = e,
        errorClass = "NO_HANDLER_FOR_UDAF",
        parameters = Map("functionName" -> "org.apache.spark.sql.errors.MyCastToString"),
        context = ExpectedContext(
          fragment = "myCast(123)", start = 7, stop = 17))
    }
  }

  test("UNTYPED_SCALA_UDF: use untyped Scala UDF should fail by default") {
    checkError(
      exception = intercept[AnalysisException](udf((x: Int) => x, IntegerType)),
      errorClass = "UNTYPED_SCALA_UDF",
      parameters = Map[String, String]())
  }

  test("NO_UDF_INTERFACE: java udf class does not implement any udf interface") {
    val className = "org.apache.spark.sql.errors.MyCastToString"
    val e = intercept[AnalysisException](
      spark.udf.registerJava(
        "myCast",
        className,
        StringType)
    )
    checkError(
      exception = e,
      errorClass = "NO_UDF_INTERFACE",
      parameters = Map("className" -> className))
  }

  test("MULTI_UDF_INTERFACE_ERROR: java udf implement multi UDF interface") {
    val className = "org.apache.spark.sql.errors.MySum"
    val e = intercept[AnalysisException](
      spark.udf.registerJava(
        "mySum",
        className,
        StringType)
    )
    checkError(
      exception = e,
      errorClass = "MULTI_UDF_INTERFACE_ERROR",
      parameters = Map("className" -> className))
  }

  test("UNSUPPORTED_FEATURE: java udf with too many type arguments") {
    val className = "org.apache.spark.sql.errors.MultiIntSum"
    val e = intercept[AnalysisException](
      spark.udf.registerJava(
        "mySum",
        className,
        StringType)
    )
    checkError(
      exception = e,
      errorClass = "UNSUPPORTED_FEATURE.TOO_MANY_TYPE_ARGUMENTS_FOR_UDF_CLASS",
      parameters = Map("num" -> "24"),
      sqlState = "0A000")
  }

  test("GROUPING_COLUMN_MISMATCH: not found the grouping column") {
    val groupingColMismatchEx = intercept[AnalysisException] {
      courseSales.cube("course", "year").agg(grouping("earnings")).explain()
    }
    checkError(
      exception = groupingColMismatchEx,
      errorClass = "GROUPING_COLUMN_MISMATCH",
      parameters = Map("grouping" -> "earnings.*", "groupingColumns" -> "course.*,year.*"),
      sqlState = Some("42803"),
      matchPVals = true)
  }

  test("GROUPING_ID_COLUMN_MISMATCH: columns of grouping_id does not match") {
    val groupingIdColMismatchEx = intercept[AnalysisException] {
      courseSales.cube("course", "year").agg(grouping_id("earnings")).explain()
    }
    checkError(
      exception = groupingIdColMismatchEx,
      errorClass = "GROUPING_ID_COLUMN_MISMATCH",
      parameters = Map("groupingIdColumn" -> "earnings.*",
      "groupByColumns" -> "course.*,year.*"),
      sqlState = Some("42803"),
      matchPVals = true)
  }

  test("GROUPING_SIZE_LIMIT_EXCEEDED: max size of grouping set") {
    withTempView("t") {
      sql("CREATE TEMPORARY VIEW t AS SELECT * FROM " +
        s"VALUES(${(0 until 65).map { _ => 1 }.mkString(", ")}, 3) AS " +
        s"t(${(0 until 65).map { i => s"k$i" }.mkString(", ")}, v)")

      def testGroupingIDs(numGroupingSet: Int, expectedIds: Seq[Any] = Nil): Unit = {
        val groupingCols = (0 until numGroupingSet).map { i => s"k$i" }
        val df = sql("SELECT GROUPING_ID(), SUM(v) FROM t GROUP BY " +
          s"GROUPING SETS ((${groupingCols.mkString(",")}), (${groupingCols.init.mkString(",")}))")
        checkAnswer(df, expectedIds.map { id => Row(id, 3) })
      }

      withSQLConf(SQLConf.LEGACY_INTEGER_GROUPING_ID.key -> "true") {
        checkError(
          exception = intercept[AnalysisException] { testGroupingIDs(33) },
          errorClass = "GROUPING_SIZE_LIMIT_EXCEEDED",
          parameters = Map("maxSize" -> "32"))
      }

      withSQLConf(SQLConf.LEGACY_INTEGER_GROUPING_ID.key -> "false") {
        checkError(
          exception = intercept[AnalysisException] { testGroupingIDs(65) },
          errorClass = "GROUPING_SIZE_LIMIT_EXCEEDED",
          parameters = Map("maxSize" -> "64"))
      }
    }
  }

  test("FORBIDDEN_OPERATION: desc partition on a temporary view") {
    val tableName: String = "t"
    val tempViewName: String = "tempView"

    withTable(tableName) {
      sql(
        s"""
          |CREATE TABLE $tableName (a STRING, b INT, c STRING, d STRING)
          |USING parquet
          |PARTITIONED BY (c, d)
          |""".stripMargin)

      withTempView(tempViewName) {
        sql(s"CREATE TEMPORARY VIEW $tempViewName as SELECT * FROM $tableName")

        checkError(
          exception = intercept[AnalysisException] {
            sql(s"DESC TABLE $tempViewName PARTITION (c='Us', d=1)")
          },
          errorClass = "FORBIDDEN_OPERATION",
          parameters = Map("statement" -> "DESC PARTITION",
            "objectType" -> "TEMPORARY VIEW", "objectName" -> s"`$tempViewName`"))
      }
    }
  }

  test("FORBIDDEN_OPERATION: desc partition on a view") {
    val tableName: String = "t"
    val viewName: String = "view"

    withTable(tableName) {
      sql(
        s"""
           |CREATE TABLE $tableName (a STRING, b INT, c STRING, d STRING)
           |USING parquet
           |PARTITIONED BY (c, d)
           |""".stripMargin)

      withView(viewName) {
        sql(s"CREATE VIEW $viewName as SELECT * FROM $tableName")

        checkError(
          exception = intercept[AnalysisException] {
            sql(s"DESC TABLE $viewName PARTITION (c='Us', d=1)")
          },
          errorClass = "FORBIDDEN_OPERATION",
          parameters = Map("statement" -> "DESC PARTITION",
          "objectType" -> "VIEW", "objectName" -> s"`$viewName`"))
      }
    }
  }

  test("SECOND_FUNCTION_ARGUMENT_NOT_INTEGER: " +
    "the second argument of 'date_add' function needs to be an integer") {
    withSQLConf(SQLConf.ANSI_ENABLED.key -> "false") {
      checkError(
        exception = intercept[AnalysisException] {
          sql("select date_add('1982-08-15', 'x')").collect()
        },
        errorClass = "SECOND_FUNCTION_ARGUMENT_NOT_INTEGER",
        parameters = Map("functionName" -> "date_add"),
        sqlState = "22023")
    }
  }

  test("INVALID_JSON_SCHEMA_MAP_TYPE: only STRING as a key type for MAP") {
    val schema = StructType(
      StructField("map", MapType(IntegerType, IntegerType, true), false) :: Nil)

    checkError(
      exception = intercept[AnalysisException] {
        spark.read.schema(schema).json(spark.emptyDataset[String])
      },
      errorClass = "INVALID_JSON_SCHEMA_MAP_TYPE",
      parameters = Map("jsonSchema" -> "\"STRUCT<map: MAP<INT, INT> NOT NULL>\"")
    )
  }

  test("UNRESOLVED_MAP_KEY: string type literal should be quoted") {
    checkAnswer(sql("select m['a'] from (select map('a', 'b') as m, 'aa' as aa)"), Row("b"))
    val query = "select m[a] from (select map('a', 'b') as m, 'aa' as aa)"
    checkError(
      exception = intercept[AnalysisException] {sql(query)},
      errorClass = "UNRESOLVED_MAP_KEY.WITH_SUGGESTION",
      sqlState = None,
      parameters = Map("objectName" -> "`a`",
        "proposal" -> "`aa`, `m`"),
      context = ExpectedContext(
        fragment = "a",
        start = 9,
        stop = 9)
    )
  }

  test("UNRESOLVED_MAP_KEY: proposal columns containing quoted dots") {
    val query = "select m[a] from (select map('a', 'b') as m, 'aa' as `a.a`)"
    checkError(
      exception = intercept[AnalysisException] {sql(query)},
      errorClass = "UNRESOLVED_MAP_KEY.WITH_SUGGESTION",
      sqlState = None,
      parameters = Map(
        "objectName" -> "`a`",
        "proposal" -> "`m`, `a.a`"),
      context = ExpectedContext(
        fragment = "a",
        start = 9,
        stop = 9)
    )
  }

  test("UNRESOLVED_COLUMN: SELECT distinct does not work correctly " +
    "if order by missing attribute") {
    checkAnswer(
      sql(
        """select distinct struct.a, struct.b
          |from (
          |  select named_struct('a', 1, 'b', 2, 'c', 3) as struct
          |  union all
          |  select named_struct('a', 1, 'b', 2, 'c', 4) as struct) tmp
          |order by a, b
          |""".stripMargin), Row(1, 2) :: Nil)

    checkError(
      exception = intercept[AnalysisException] {
        sql(
          """select distinct struct.a, struct.b
            |from (
            |  select named_struct('a', 1, 'b', 2, 'c', 3) as struct
            |  union all
            |  select named_struct('a', 1, 'b', 2, 'c', 4) as struct) tmp
            |order by struct.a, struct.b
            |""".stripMargin)
      },
      errorClass = "UNRESOLVED_COLUMN.WITH_SUGGESTION",
      sqlState = None,
      parameters = Map(
        "objectName" -> "`struct`.`a`",
        "proposal" -> "`a`, `b`"
      ),
      context = ExpectedContext(
        fragment = "struct.a",
        start = 180,
        stop = 187)
    )
  }

  test("UNRESOLVED_COLUMN - SPARK-21335: support un-aliased subquery") {
    withTempView("v") {
      Seq(1 -> "a").toDF("i", "j").createOrReplaceTempView("v")
      checkAnswer(sql("SELECT i from (SELECT i FROM v)"), Row(1))

      val query = "SELECT v.i from (SELECT i FROM v)"
      checkError(
        exception = intercept[AnalysisException](sql(query)),
        errorClass = "UNRESOLVED_COLUMN.WITH_SUGGESTION",
        sqlState = None,
        parameters = Map(
          "objectName" -> "`v`.`i`",
          "proposal" -> "`i`"),
        context = ExpectedContext(
          fragment = "v.i",
          start = 7,
          stop = 9))

      checkAnswer(sql("SELECT __auto_generated_subquery_name.i from (SELECT i FROM v)"), Row(1))
      checkAnswer(sql("SELECT i from (SELECT i FROM v)"), Row(1))
    }
  }

  test("AMBIGUOUS_ALIAS_IN_NESTED_CTE: Nested CTEs with same name and " +
    "ctePrecedencePolicy = EXCEPTION") {
    withTable("t") {
      withSQLConf(SQLConf.LEGACY_CTE_PRECEDENCE_POLICY.key -> "EXCEPTION") {
        val query =
          """
            |WITH
            |    t AS (SELECT 1),
            |    t2 AS (
            |        WITH t AS (SELECT 2)
            |        SELECT * FROM t)
            |SELECT * FROM t2;
            |""".stripMargin

        checkError(
          exception = intercept[AnalysisException] {
            sql(query)
          },
          errorClass = "AMBIGUOUS_ALIAS_IN_NESTED_CTE",
          parameters = Map(
            "name" -> "`t`",
            "config" -> toSQLConf(SQLConf.LEGACY_CTE_PRECEDENCE_POLICY.key),
            "docroot" -> SPARK_DOC_ROOT))
      }
    }
  }

  test("AMBIGUOUS_COLUMN_OR_FIELD: alter column matching multi fields in the struct") {
    withTable("t") {
      withSQLConf(SQLConf.CASE_SENSITIVE.key -> "true") {
        sql("CREATE TABLE t(c struct<X:String, x:String>) USING parquet")
      }

      val query = "ALTER TABLE t CHANGE COLUMN c.X COMMENT 'new comment'"
      checkError(
        exception = intercept[AnalysisException] {
          sql(query)
        },
        errorClass = "AMBIGUOUS_COLUMN_OR_FIELD",
        parameters = Map("name" -> "`c`.`X`", "n" -> "2"),
        context = ExpectedContext(
          fragment = query, start = 0, stop = 52))
    }
  }

  test("PIVOT_VALUE_DATA_TYPE_MISMATCH: can't cast pivot value data type (struct) " +
    "to pivot column data type (int)") {
    val df = Seq(
      ("dotNET", 2012, 10000),
      ("Java", 2012, 20000),
      ("dotNET", 2012, 5000),
      ("dotNET", 2013, 48000),
      ("Java", 2013, 30000)
    ).toDF("course", "year", "earnings")

    checkError(
      exception = intercept[AnalysisException] {
        df.groupBy(df("course")).pivot(df("year"), Seq(
          struct(lit("dotnet"), lit("Experts")),
          struct(lit("java"), lit("Dummies")))).
          agg(sum($"earnings")).collect()
      },
      errorClass = "PIVOT_VALUE_DATA_TYPE_MISMATCH",
      parameters = Map("value" -> "struct(col1, dotnet, col2, Experts)",
        "valueType" -> "struct<col1:string,col2:string>",
        "pivotType" -> "int"))
  }

  test("INVALID_FIELD_NAME: add a nested field for not struct parent") {
    withTable("t") {
      sql("CREATE TABLE t(c struct<x:string>, m string) USING parquet")

      val e = intercept[AnalysisException] {
        sql("ALTER TABLE t ADD COLUMNS (m.n int)")
      }
      checkError(
        exception = e,
        errorClass = "INVALID_FIELD_NAME",
        parameters = Map("fieldName" -> "`m`.`n`", "path" -> "`m`"),
        context = ExpectedContext(
          fragment = "m.n int", start = 27, stop = 33))
    }
  }

  test("NON_LITERAL_PIVOT_VALUES: literal expressions required for pivot values") {
    val df = Seq(
      ("dotNET", 2012, 10000),
      ("Java", 2012, 20000),
      ("dotNET", 2012, 5000),
      ("dotNET", 2013, 48000),
      ("Java", 2013, 30000)
    ).toDF("course", "year", "earnings")

    checkError(
      exception = intercept[AnalysisException] {
        df.groupBy(df("course")).
          pivot(df("year"), Seq($"earnings")).
          agg(sum($"earnings")).collect()
      },
      errorClass = "NON_LITERAL_PIVOT_VALUES",
      parameters = Map("expression" -> "\"earnings\""))
  }

  test("UNSUPPORTED_DESERIALIZER: data type mismatch") {
    val e = intercept[AnalysisException] {
      sql("select 1 as arr").as[ArrayClass]
    }
    checkError(
      exception = e,
      errorClass = "UNSUPPORTED_DESERIALIZER.DATA_TYPE_MISMATCH",
      parameters = Map("desiredType" -> "\"ARRAY\"", "dataType" -> "\"INT\""))
  }

  test("UNSUPPORTED_DESERIALIZER: " +
    "the real number of fields doesn't match encoder schema") {
    val ds = Seq(ClassData("a", 1), ClassData("b", 2)).toDS()

    val e1 = intercept[AnalysisException] {
      ds.as[(String, Int, Long)]
    }
    checkError(
      exception = e1,
      errorClass = "UNSUPPORTED_DESERIALIZER.FIELD_NUMBER_MISMATCH",
      parameters = Map(
        "schema" -> "\"STRUCT<a: STRING, b: INT NOT NULL>\"",
        "ordinal" -> "3"))

    val e2 = intercept[AnalysisException] {
      ds.as[Tuple1[String]]
    }
    checkError(
      exception = e2,
      errorClass = "UNSUPPORTED_DESERIALIZER.FIELD_NUMBER_MISMATCH",
      parameters = Map("schema" -> "\"STRUCT<a: STRING, b: INT NOT NULL>\"",
        "ordinal" -> "1"))
  }

  test("UNSUPPORTED_GENERATOR: " +
    "generators are not supported when it's nested in expressions") {
    val e = intercept[AnalysisException](
      sql("""select explode(Array(1, 2, 3)) + 1""").collect()
    )

    checkError(
      exception = e,
      errorClass = "UNSUPPORTED_GENERATOR.NESTED_IN_EXPRESSIONS",
      parameters = Map("expression" -> "\"(explode(array(1, 2, 3)) + 1)\""))
  }

  test("UNSUPPORTED_GENERATOR: only one generator allowed") {
    val e = intercept[AnalysisException](
      sql("""select explode(Array(1, 2, 3)), explode(Array(1, 2, 3))""").collect()
    )

    checkError(
      exception = e,
      errorClass = "UNSUPPORTED_GENERATOR.MULTI_GENERATOR",
      parameters = Map("clause" -> "SELECT", "num" -> "2",
        "generators" -> "\"explode(array(1, 2, 3))\", \"explode(array(1, 2, 3))\""))
  }

  test("UNSUPPORTED_GENERATOR: generators are not supported outside the SELECT clause") {
    val e = intercept[AnalysisException](
      sql("""select 1 from t order by explode(Array(1, 2, 3))""").collect()
    )

    checkError(
      exception = e,
      errorClass = "UNSUPPORTED_GENERATOR.OUTSIDE_SELECT",
      parameters = Map("plan" -> "'Sort [explode(array(1, 2, 3)) ASC NULLS FIRST], true"))
  }

  test("UNSUPPORTED_GENERATOR: not a generator") {
    val e = intercept[AnalysisException](
      sql(
        """
          |SELECT explodedvalue.*
          |FROM VALUES array(1, 2, 3) AS (value)
          |LATERAL VIEW array_contains(value, 1) AS explodedvalue""".stripMargin).collect()
    )

    checkError(
      exception = e,
      errorClass = "UNSUPPORTED_GENERATOR.NOT_GENERATOR",
      sqlState = None,
      parameters = Map(
        "functionName" -> "`array_contains`",
        "classCanonicalName" -> "org.apache.spark.sql.catalyst.expressions.ArrayContains"),
      context = ExpectedContext(
        fragment = "LATERAL VIEW array_contains(value, 1) AS explodedvalue",
        start = 62, stop = 115))
  }

  test("DATATYPE_MISMATCH.INVALID_JSON_SCHEMA: invalid top type passed to from_json()") {
    checkError(
      exception = intercept[AnalysisException] {
        Seq("""{"a":1}""").toDF("a").select(from_json($"a", IntegerType)).collect()
      },
      errorClass = "DATATYPE_MISMATCH.INVALID_JSON_SCHEMA",
      parameters = Map("schema" -> "\"INT\"", "sqlExpr" -> "\"from_json(a)\""),
      context =
        ExpectedContext(fragment = "from_json", callSitePattern = getCurrentClassCallSitePattern))
  }

  test("WRONG_NUM_ARGS.WITHOUT_SUGGESTION: wrong args of CAST(parameter types contains DataType)") {
    checkError(
      exception = intercept[AnalysisException] {
        sql("SELECT CAST(1)")
      },
      errorClass = "WRONG_NUM_ARGS.WITHOUT_SUGGESTION",
      parameters = Map(
        "functionName" -> "`cast`",
        "expectedNum" -> "0",
        "actualNum" -> "1",
        "docroot" -> SPARK_DOC_ROOT),
      context = ExpectedContext("", "", 7, 13, "CAST(1)")
    )
  }

  test("IDENTIFIER_TOO_MANY_NAME_PARTS: " +
    "create temp view doesn't support identifiers consisting of more than 2 parts") {
    checkError(
      exception = intercept[ParseException] {
        sql("CREATE TEMPORARY VIEW db_name.schema_name.view_name AS SELECT '1' as test_column")
      },
      errorClass = "IDENTIFIER_TOO_MANY_NAME_PARTS",
      sqlState = "42601",
      parameters = Map("identifier" -> "`db_name`.`schema_name`.`view_name`")
    )
  }

  test("IDENTIFIER_TOO_MANY_NAME_PARTS: " +
    "alter table doesn't support identifiers consisting of more than 2 parts") {
    val tableName: String = "t"
    withTable(tableName) {
      sql(
        s"""
           |CREATE TABLE $tableName (a STRING, b INT, c STRING, d STRING)
           |USING parquet
           |PARTITIONED BY (c, d)
           |""".stripMargin)

      checkError(
        exception = intercept[AnalysisException] {
          sql(s"ALTER TABLE $tableName RENAME TO db_name.schema_name.new_table_name")
        },
        errorClass = "IDENTIFIER_TOO_MANY_NAME_PARTS",
        sqlState = "42601",
        parameters = Map("identifier" -> "`db_name`.`schema_name`.`new_table_name`")
      )
    }
  }

  test("AMBIGUOUS_REFERENCE_TO_FIELDS: select ambiguous field from struct") {
    val data = Seq(
      Row(Row("test1", "test1")),
      Row(Row("test2", "test2")))

    val schema = new StructType()
      .add("name",
        new StructType()
          .add("firstname", StringType)
          .add("firstname", StringType))

    val df = spark.createDataFrame(spark.sparkContext.parallelize(data), schema)

    checkError(
      exception = intercept[AnalysisException] {
        df.select($"name.firstname")
      },
      errorClass = "AMBIGUOUS_REFERENCE_TO_FIELDS",
      sqlState = "42000",
      parameters = Map("field" -> "`firstname`", "count" -> "2"),
      context = ExpectedContext(fragment = "$", callSitePattern = getCurrentClassCallSitePattern)
    )
  }

  test("INVALID_EXTRACT_BASE_FIELD_TYPE: select ambiguous field from struct") {
    val df = Seq("test").toDF("firstname")

    checkError(
      exception = intercept[AnalysisException] {
        df.select($"firstname.test_field")
      },
      errorClass = "INVALID_EXTRACT_BASE_FIELD_TYPE",
      sqlState = "42000",
      parameters = Map("base" -> "\"firstname\"", "other" -> "\"STRING\""),
      context = ExpectedContext(fragment = "$", callSitePattern = getCurrentClassCallSitePattern)
    )
  }

  test("INVALID_EXTRACT_FIELD_TYPE: extract not string literal field") {
    val structureData = Seq(
      Row(Row(Row("test1", "test1"))),
      Row(Row(Row("test2", "test2"))))

    val structureSchema = new StructType()
      .add("name",
        new StructType()
          .add("inner",
            new StructType()
              .add("firstname", StringType)
              .add("lastname", StringType)))

    val df = spark.createDataFrame(spark.sparkContext.parallelize(structureData), structureSchema)

    checkError(
      exception = intercept[AnalysisException] {
        df.select(struct($"name"(struct("test"))))
      },
      errorClass = "INVALID_EXTRACT_FIELD_TYPE",
      sqlState = "42000",
      parameters = Map("extraction" -> "\"struct(test)\""))

    checkError(
      exception = intercept[AnalysisException] {
        df.select($"name"(array("test")))
      },
      errorClass = "INVALID_EXTRACT_FIELD_TYPE",
      sqlState = "42000",
      parameters = Map("extraction" -> "\"array(test)\""))
  }

  test("CREATE NAMESPACE with LOCATION for JDBC catalog should throw an error") {
    withTempDir { tempDir =>
      val url = s"jdbc:h2:${tempDir.getCanonicalPath};user=testUser;password=testPass"
      Utils.classForName("org.h2.Driver")
      withSQLConf(
        "spark.sql.catalog.h2" -> classOf[JDBCTableCatalog].getName,
        "spark.sql.catalog.h2.url" -> url,
        "spark.sql.catalog.h2.driver" -> "org.h2.Driver") {
        checkError(
          exception = intercept[AnalysisException] {
            sql("CREATE NAMESPACE h2.test_namespace LOCATION './samplepath'")
          },
          errorClass = "NOT_SUPPORTED_IN_JDBC_CATALOG.COMMAND",
          sqlState = "0A000",
          parameters = Map("cmd" -> toSQLStmt("CREATE NAMESPACE ... LOCATION ...")))
      }
    }
  }

  test("ALTER NAMESPACE with property other than COMMENT " +
    "for JDBC catalog should throw an exception") {
    withTempDir { tempDir =>
      val url = s"jdbc:h2:${tempDir.getCanonicalPath};user=testUser;password=testPass"
      Utils.classForName("org.h2.Driver")
      withSQLConf(
        "spark.sql.catalog.h2" -> classOf[JDBCTableCatalog].getName,
        "spark.sql.catalog.h2.url" -> url,
        "spark.sql.catalog.h2.driver" -> "org.h2.Driver") {
        val namespace = "h2.test_namespace"
        withNamespace(namespace) {
          sql(s"CREATE NAMESPACE $namespace")
          checkError(
            exception = intercept[AnalysisException] {
              sql(s"ALTER NAMESPACE h2.test_namespace SET LOCATION '/tmp/loc_test_2'")
            },
            errorClass = "NOT_SUPPORTED_IN_JDBC_CATALOG.COMMAND_WITH_PROPERTY",
            sqlState = "0A000",
            parameters = Map(
              "cmd" -> toSQLStmt("SET NAMESPACE"),
              "property" -> toSQLConf("location")))

          checkError(
            exception = intercept[AnalysisException] {
              sql(s"ALTER NAMESPACE h2.test_namespace SET PROPERTIES('a'='b')")
            },
            errorClass = "NOT_SUPPORTED_IN_JDBC_CATALOG.COMMAND_WITH_PROPERTY",
            sqlState = "0A000",
            parameters = Map(
              "cmd" -> toSQLStmt("SET NAMESPACE"),
              "property" -> toSQLConf("a")))
        }
      }
    }
  }

  test("ALTER TABLE UNSET nonexistent property should throw an exception") {
    val tableName = "test_table"
    withTable(tableName) {
      sql(s"CREATE TABLE $tableName (a STRING, b INT) USING parquet")

      checkError(
        exception = intercept[AnalysisException] {
          sql(s"ALTER TABLE $tableName UNSET TBLPROPERTIES ('test_prop1', 'test_prop2', 'comment')")
        },
        errorClass = "UNSET_NONEXISTENT_PROPERTIES",
        parameters = Map(
          "properties" -> "`test_prop1`, `test_prop2`",
          "table" -> "`spark_catalog`.`default`.`test_table`")
      )
    }
  }

  test("SPARK-43841: Unresolved attribute in select of full outer join with USING") {
    withTempView("v1", "v2") {
      sql("create or replace temp view v1 as values (1, 2) as (c1, c2)")
      sql("create or replace temp view v2 as values (2, 3) as (c1, c2)")

      val query =
        """select b
          |from v1
          |full outer join v2
          |using (c1)
          |""".stripMargin

      checkError(
        exception = intercept[AnalysisException] {
          sql(query)
        },
        errorClass = "UNRESOLVED_COLUMN.WITH_SUGGESTION",
        parameters = Map(
          "proposal" -> "`c1`, `v1`.`c2`, `v2`.`c2`",
          "objectName" -> "`b`"),
        context = ExpectedContext(
          fragment = "b",
          start = 7, stop = 7)
      )
    }
  }

  test("ComplexTypeMergingExpression should throw exception if no children") {
    val coalesce = Coalesce(Seq.empty)

    checkError(
      exception = intercept[SparkIllegalArgumentException] {
        coalesce.dataType
      },
      errorClass = "COMPLEX_EXPRESSION_UNSUPPORTED_INPUT.NO_INPUTS",
      parameters = Map("expression" -> "\"coalesce()\""))
  }

  test("ComplexTypeMergingExpression should throw " +
    "exception if children have different data types") {
    val coalesce = Coalesce(Seq(Literal(1), Literal("a"), Literal("a")))

    checkError(
      exception = intercept[SparkIllegalArgumentException] {
        coalesce.dataType
      },
      errorClass = "COMPLEX_EXPRESSION_UNSUPPORTED_INPUT.MISMATCHED_TYPES",
      parameters = Map(
        "expression" -> "\"coalesce(1, a, a)\"",
        "inputTypes" -> "[\"INT\", \"STRING\", \"STRING\"]"))
  }

  test("UNSUPPORTED_CALL: call the unsupported method update()") {
    checkError(
      exception = intercept[SparkUnsupportedOperationException] {
        new UnsafeRow(1).update(0, 1)
      },
      errorClass = "UNSUPPORTED_CALL.WITHOUT_SUGGESTION",
      parameters = Map(
        "methodName" -> "update",
        "className" -> "org.apache.spark.sql.catalyst.expressions.UnsafeRow"))
  }

  test("SPARK-47102: the collation feature is off without collate builder call") {
    withSQLConf(SQLConf.COLLATION_ENABLED.key -> "false") {
      Seq(
        "CREATE TABLE t(col STRING COLLATE 'UNICODE_CI') USING parquet",
        "CREATE TABLE t(col STRING COLLATE 'UNKNOWN_COLLATION_STRING') USING parquet",
        "SELECT 'aaa' COLLATE 'UNICODE_CI'",
        "select collation('aaa')"
      ).foreach { sqlText =>
        checkError(
          exception = intercept[AnalysisException](sql(sqlText)),
          errorClass = "UNSUPPORTED_FEATURE.COLLATION")
      }
    }
  }

  test("SPARK-47102: the collation feature is off with collate builder call") {
    withSQLConf(SQLConf.COLLATION_ENABLED.key -> "false") {
      Seq(
        "SELECT collate('aaa', 'UNICODE_CI')",
        "SELECT collate('aaa', 'UNKNOWN_COLLATION_STRING')"
      ).foreach { sqlText =>
        checkError(
          exception = intercept[AnalysisException](sql(sqlText)),
          errorClass = "UNSUPPORTED_FEATURE.COLLATION",
          parameters = Map.empty,
          context = ExpectedContext(
            fragment = sqlText.substring(7), start = 7, stop = sqlText.length - 1))
      }
    }
  }

  test("INTERNAL_ERROR: Convert unsupported data type from Spark to Parquet") {
    val converter = new SparkToParquetSchemaConverter
    val dummyDataType = new DataType {
      override def defaultSize: Int = 0

      override def simpleString: String = "Dummy"

      override private[spark] def asNullable = NullType
    }
    checkError(
      exception = intercept[AnalysisException] {
        converter.convertField(StructField("test", dummyDataType))
      },
      errorClass = "INTERNAL_ERROR",
      parameters = Map("message" -> "Cannot convert Spark data type \"DUMMY\" to any Parquet type.")
    )
  }
}

class MyCastToString extends SparkUserDefinedFunction(
  (input: Any) => if (input == null) {
    null
  } else {
    input.toString
  },
  StringType,
  inputEncoders = Seq.fill(1)(None))

class MySum extends UDF1[Int, Int] with UDF2[Int, Int, Int] {
  override def call(t1: Int): Int = t1

  override def call(t1: Int, t2: Int): Int = t1 + t2
}

class MultiIntSum extends
  UDF23Test[Int, Int, Int, Int, Int, Int, Int, Int, Int, Int, Int, Int,
    Int, Int, Int, Int, Int, Int, Int, Int, Int, Int, Int, Int] {
  // scalastyle:off argcount
  override def call(
      t1: Int, t2: Int, t3: Int, t4: Int, t5: Int, t6: Int, t7: Int, t8: Int,
      t9: Int, t10: Int, t11: Int, t12: Int, t13: Int, t14: Int, t15: Int, t16: Int,
      t17: Int, t18: Int, t19: Int, t20: Int, t21: Int, t22: Int, t23: Int): Int = {
    t1 + t2 + t3 + t4 + t5 + t6 + t7 + t8 + t9 + t10 +
      t11 + t12 + t13 + t14 + t15 + t16 + t17 + t18 + t19 + t20 + t21 + t22 + t23
  }
  // scalastyle:on argcount
}
