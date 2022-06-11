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

import org.apache.spark.sql.{AnalysisException, IntegratedUDFTestUtils, QueryTest}
import org.apache.spark.sql.functions.{grouping, grouping_id, sum}
import org.apache.spark.sql.internal.SQLConf

case class StringLongClass(a: String, b: Long)

case class StringIntClass(a: String, b: Int)

case class ComplexClass(a: Long, b: StringLongClass)

class QueryCompilationErrorsSuite extends QueryTest with QueryErrorsSuiteBase {
  import testImplicits._

  test("CANNOT_UP_CAST_DATATYPE: invalid upcast data type") {
    val e1 = intercept[AnalysisException] {
      sql("select 'value1' as a, 1L as b").as[StringIntClass]
    }
    checkErrorClass(
      exception = e1,
      errorClass = "CANNOT_UP_CAST_DATATYPE",
      msg =
        s"""
           |Cannot up cast b from "BIGINT" to "INT".
           |The type path of the target object is:
           |- field (class: "scala.Int", name: "b")
           |- root class: "org.apache.spark.sql.errors.StringIntClass"
           |You can either add an explicit cast to the input data or choose a higher precision type
       """.stripMargin.trim + " of the field in the target object")

    val e2 = intercept[AnalysisException] {
      sql("select 1L as a," +
        " named_struct('a', 'value1', 'b', cast(1.0 as decimal(38,18))) as b")
        .as[ComplexClass]
    }
    checkErrorClass(
      exception = e2,
      errorClass = "CANNOT_UP_CAST_DATATYPE",
      msg =
        s"""
           |Cannot up cast b.`b` from "DECIMAL(38,18)" to "BIGINT".
           |The type path of the target object is:
           |- field (class: "scala.Long", name: "b")
           |- field (class: "org.apache.spark.sql.errors.StringLongClass", name: "b")
           |- root class: "org.apache.spark.sql.errors.ComplexClass"
           |You can either add an explicit cast to the input data or choose a higher precision type
       """.stripMargin.trim + " of the field in the target object")
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
      checkErrorClass(
        exception = e,
        errorClass = "UNSUPPORTED_GROUPING_EXPRESSION",
        msg = "grouping()/grouping_id() can only be used with GroupingSets/Cube/Rollup")
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
      checkErrorClass(
        exception = e,
        errorClass = "UNSUPPORTED_GROUPING_EXPRESSION",
        msg = "grouping()/grouping_id() can only be used with GroupingSets/Cube/Rollup")
    }
  }

  test("INVALID_PARAMETER_VALUE: the argument_index of string format is invalid") {
    withSQLConf(SQLConf.ALLOW_ZERO_INDEX_IN_FORMAT_STRING.key -> "false") {
      val e = intercept[AnalysisException] {
        sql("select format_string('%0$s', 'Hello')")
      }
      checkErrorClass(
        exception = e,
        errorClass = "INVALID_PARAMETER_VALUE",
        msg = "The value of parameter(s) 'strfmt' in `format_string` is invalid: " +
          "expects %1$, %2$ and so on, but got %0$.; line 1 pos 7")
    }
  }

  test("CANNOT_USE_MIXTURE: Using aggregate function with grouped aggregate pandas UDF") {
    import IntegratedUDFTestUtils._
    assume(shouldTestGroupedAggPandasUDFs)

    val df = Seq(
      (536361, "85123A", 2, 17850),
      (536362, "85123B", 4, 17850),
      (536363, "86123A", 6, 17851)
    ).toDF("InvoiceNo", "StockCode", "Quantity", "CustomerID")
    val e = intercept[AnalysisException] {
      val pandasTestUDF = TestGroupedAggPandasUDF(name = "pandas_udf")
      df.groupBy("CustomerId")
        .agg(pandasTestUDF(df("Quantity")), sum(df("Quantity"))).collect()
    }
    checkErrorClass(
      exception = e,
      errorClass = "CANNOT_USE_MIXTURE",
      msg = "Cannot use a mixture of aggregate function and group aggregate pandas UDF")
  }

  test("UNSUPPORTED_FEATURE: Using Python UDF with unsupported join condition") {
    import IntegratedUDFTestUtils._

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
      val pythonTestUDF = TestPythonUDF(name = "python_udf")
      df1.join(
        df2, pythonTestUDF(df1("CustomerID") === df2("CustomerID")), "leftouter").collect()
    }
    checkErrorClass(
      exception = e,
      errorClass = "UNSUPPORTED_FEATURE",
      sqlState = Some("0A000"),
      msg = "The feature is not supported: " +
        "Using PythonUDF in join condition of join type LEFT OUTER is not supported.")
  }

  test("UNSUPPORTED_FEATURE: Using pandas UDF aggregate expression with pivot") {
    import IntegratedUDFTestUtils._
    assume(shouldTestGroupedAggPandasUDFs)

    val df = Seq(
      (536361, "85123A", 2, 17850),
      (536362, "85123B", 4, 17850),
      (536363, "86123A", 6, 17851)
    ).toDF("InvoiceNo", "StockCode", "Quantity", "CustomerID")

    val e = intercept[AnalysisException] {
      val pandasTestUDF = TestGroupedAggPandasUDF(name = "pandas_udf")
      df.groupBy(df("CustomerID")).pivot(df("CustomerID")).agg(pandasTestUDF(df("Quantity")))
    }
    checkErrorClass(
      exception = e,
      errorClass = "UNSUPPORTED_FEATURE",
      sqlState = Some("0A000"),
      msg = "The feature is not supported: " +
        "Pandas UDF aggregate expressions don't support pivot.")
  }

  test("GROUPING_COLUMN_MISMATCH: not found the grouping column") {
    val groupingColMismatchEx = intercept[AnalysisException] {
      courseSales.cube("course", "year").agg(grouping("earnings")).explain()
    }
    checkErrorClass(
      exception = groupingColMismatchEx,
      errorClass = "GROUPING_COLUMN_MISMATCH",
      sqlState = Some("42000"),
      msg =
        "Column of grouping \\(earnings.*\\) can't be found in grouping columns course.*,year.*",
      matchMsg = true)
  }

  test("GROUPING_ID_COLUMN_MISMATCH: columns of grouping_id does not match") {
    val groupingIdColMismatchEx = intercept[AnalysisException] {
      courseSales.cube("course", "year").agg(grouping_id("earnings")).explain()
    }
    checkErrorClass(
      exception = groupingIdColMismatchEx,
      errorClass = "GROUPING_ID_COLUMN_MISMATCH",
      sqlState = Some("42000"),
      msg = "Columns of grouping_id \\(earnings.*\\) does not match " +
        "grouping columns \\(course.*,year.*\\)",
      matchMsg = true)
  }
}
