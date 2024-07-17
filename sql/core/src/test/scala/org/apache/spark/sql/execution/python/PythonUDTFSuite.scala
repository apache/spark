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

package org.apache.spark.sql.execution.python

import org.apache.spark.api.python.PythonEvalType
import org.apache.spark.sql.{AnalysisException, IntegratedUDFTestUtils, QueryTest, Row}
import org.apache.spark.sql.catalyst.expressions.{Add, Alias, Expression, FunctionTableSubqueryArgumentExpression, Literal}
import org.apache.spark.sql.catalyst.parser.ParseException
import org.apache.spark.sql.catalyst.plans.logical.{LocalRelation, LogicalPlan, OneRowRelation, Project, Repartition, RepartitionByExpression, Sort, SubqueryAlias}
import org.apache.spark.sql.functions.lit
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.test.SharedSparkSession
import org.apache.spark.sql.types.StructType

class PythonUDTFSuite extends QueryTest with SharedSparkSession {

  import testImplicits._

  import IntegratedUDFTestUtils._

  private val pythonScript: String =
    """
      |class SimpleUDTF:
      |    def eval(self, a: int, b: int):
      |        yield a, b, a + b
      |        yield a, b, a - b
      |        yield a, b, b - a
      |""".stripMargin

  private val variantInputScript: String =
    """
      |class InputVariantUDTF:
      |    def eval(self, a):
      |        yield str(a)
      |""".stripMargin

  private val variantStr =
    "VariantVal(VariantVal(bytes([2, 1, 0, 0, 2, 5, 98]), bytes([1, 1, 0, 1, 97])))"

  private val variantOutputScript: String =
    """
      |class SimpleOutputVariantUDTF:
      |    from pyspark.sql.types import VariantVal
      |    def eval(self):
      |        yield {0}
      |""".format(variantStr).stripMargin

  private val arrayOfVariantOutputScript: String =
    """
      |class OutputArrayOfVariantUDTF:
      |    from pyspark.sql.types import VariantVal
      |    def eval(self):
      |        yield [{0}]
      |""".format(variantStr).stripMargin

  private val returnType: StructType = StructType.fromDDL("a int, b int, c int")

  private val pythonUDTF: UserDefinedPythonTableFunction =
    createUserDefinedPythonTableFunction("SimpleUDTF", pythonScript, Some(returnType))

  private val pythonUDTFCountSumLast: UserDefinedPythonTableFunction =
    createUserDefinedPythonTableFunction(
      UDTFCountSumLast.name, UDTFCountSumLast.pythonScript, None)

  private val pythonUDTFWithSinglePartition: UserDefinedPythonTableFunction =
    createUserDefinedPythonTableFunction(
      UDTFWithSinglePartition.name, UDTFWithSinglePartition.pythonScript, None)

  private val pythonUDTFPartitionByOrderBy: UserDefinedPythonTableFunction =
    createUserDefinedPythonTableFunction(
      UDTFPartitionByOrderBy.name, UDTFPartitionByOrderBy.pythonScript, None)

  private val arrowPythonUDTF: UserDefinedPythonTableFunction =
    createUserDefinedPythonTableFunction(
      "SimpleUDTF",
      pythonScript,
      Some(returnType),
      evalType = PythonEvalType.SQL_ARROW_TABLE_UDF)

  private val pythonUDTFForwardStateFromAnalyze: UserDefinedPythonTableFunction =
    createUserDefinedPythonTableFunction(
      UDTFForwardStateFromAnalyze.name,
      UDTFForwardStateFromAnalyze.pythonScript, None)

  private val variantInputUDTF: UserDefinedPythonTableFunction =
    createUserDefinedPythonTableFunction(
      "InputVariantUDTF",
      variantInputScript,
      Some(StructType.fromDDL("o STRING"))
    )

  private val variantOutputUDTF: UserDefinedPythonTableFunction =
    createUserDefinedPythonTableFunction(
      "SimpleOutputVariantUDTF",
      variantOutputScript,
      Some(StructType.fromDDL("v VARIANT"))
    )

  private val arrayOfVariantOutputUDTF: UserDefinedPythonTableFunction =
    createUserDefinedPythonTableFunction(
      "OutputArrayOfVariantUDTF",
      arrayOfVariantOutputScript,
      Some(StructType.fromDDL("v ARRAY<VARIANT>"))
    )

  test("Simple PythonUDTF") {
    assume(shouldTestPythonUDFs)
    val df = pythonUDTF(spark, lit(1), lit(2))
    checkAnswer(df, Seq(Row(1, 2, -1), Row(1, 2, 1), Row(1, 2, 3)))
  }

  test("Simple variant input UDTF") {
    assume(shouldTestPythonUDFs)
    withTempView("t") {
      spark.udtf.registerPython("variantInputUDTF", variantInputUDTF)
      spark.range(0, 10).selectExpr("parse_json(cast(id as string)) v").createOrReplaceTempView("t")
      checkError(
        exception = intercept[AnalysisException] {
          spark.sql("select udtf.* from t, lateral variantInputUDTF(v) udtf").collect()
        },
        errorClass = "DATATYPE_MISMATCH.UNSUPPORTED_UDF_INPUT_TYPE",
        parameters = Map(
          "sqlExpr" -> """"InputVariantUDTF\(outer\(v#\d+\)\)"""",
          "dataType" -> "VARIANT"),
        matchPVals = true,
        queryContext = Array(ExpectedContext(
          fragment = "variantInputUDTF(v) udtf",
          start = 30,
          stop = 53)))
    }
  }

  test("Complex variant input UDTF") {
    assume(shouldTestPythonUDFs)
    withTempView("t") {
      spark.udtf.registerPython("variantInputUDTF", variantInputUDTF)
      spark.range(0, 10)
        .selectExpr("map(id, parse_json(cast(id as string))) map_v")
        .createOrReplaceTempView("t")
      checkError(
        exception = intercept[AnalysisException] {
          spark.sql("select udtf.* from t, lateral variantInputUDTF(map_v) udtf").collect()
        },
        errorClass = "DATATYPE_MISMATCH.UNSUPPORTED_UDF_INPUT_TYPE",
        parameters = Map(
          "sqlExpr" -> """"InputVariantUDTF\(outer\(map_v#\d+\)\)"""",
          "dataType" -> "MAP<BIGINT, VARIANT>"),
        matchPVals = true,
        queryContext = Array(ExpectedContext(
          fragment = "variantInputUDTF(map_v) udtf",
          start = 30,
          stop = 57)))
    }
  }

  test("Simple variant output UDTF") {
    assume(shouldTestPythonUDFs)
      spark.udtf.registerPython("variantOutUDTF", variantOutputUDTF)
      checkError(
        exception = intercept[AnalysisException] {
          spark.sql("select * from variantOutUDTF()").collect()
        },
        errorClass = "DATATYPE_MISMATCH.UNSUPPORTED_UDF_OUTPUT_TYPE",
        parameters = Map(
          "sqlExpr" -> "\"SimpleOutputVariantUDTF()\"",
          "dataType" -> "VARIANT"),
        context = ExpectedContext(
          fragment = "variantOutUDTF()",
          start = 14,
          stop = 29))
  }

  test("Complex variant output UDTF") {
    assume(shouldTestPythonUDFs)
      spark.udtf.registerPython("arrayOfVariantOutUDTF", arrayOfVariantOutputUDTF)
      checkError(
        exception = intercept[AnalysisException] {
          spark.sql("select * from arrayOfVariantOutUDTF()").collect()
        },
        errorClass = "DATATYPE_MISMATCH.UNSUPPORTED_UDF_OUTPUT_TYPE",
        parameters = Map(
          "sqlExpr" -> "\"OutputArrayOfVariantUDTF()\"",
          "dataType" -> "ARRAY<VARIANT>"),
        context = ExpectedContext(
          fragment = "arrayOfVariantOutUDTF()",
          start = 14,
          stop = 36))
  }

  test("PythonUDTF with lateral join") {
    assume(shouldTestPythonUDFs)
    withTempView("t") {
      spark.udtf.registerPython("testUDTF", pythonUDTF)
      Seq((0, 1), (1, 2)).toDF("a", "b").createOrReplaceTempView("t")
      checkAnswer(
        sql("SELECT f.* FROM t, LATERAL testUDTF(a, b) f"),
        sql("SELECT * FROM t, LATERAL explode(array(a + b, a - b, b - a)) t(c)"))
    }
  }

  test("PythonUDTF in correlated subquery") {
    assume(shouldTestPythonUDFs)
    withTempView("t") {
      spark.udtf.registerPython("testUDTF", pythonUDTF)
      Seq((0, 1), (1, 2)).toDF("a", "b").createOrReplaceTempView("t")
      checkAnswer(
        sql("SELECT (SELECT sum(f.b) AS r FROM testUDTF(1, 2) f WHERE f.a = t.a) FROM t"),
        Seq(Row(6), Row(null)))
    }
  }

  test("Arrow optimized UDTF") {
    assume(shouldTestPandasUDFs)
    val df = arrowPythonUDTF(spark, lit(1), lit(2))
    checkAnswer(df, Seq(Row(1, 2, -1), Row(1, 2, 1), Row(1, 2, 3)))
  }

  test("arrow optimized UDTF with lateral join") {
    assume(shouldTestPandasUDFs)
    withTempView("t") {
      spark.udtf.registerPython("testUDTF", arrowPythonUDTF)
      Seq((0, 1), (1, 2)).toDF("a", "b").createOrReplaceTempView("t")
      checkAnswer(
        sql("SELECT t.*, f.c FROM t, LATERAL testUDTF(a, b) f"),
        sql("SELECT * FROM t, LATERAL explode(array(a + b, a - b, b - a)) t(c)"))
    }
  }

  test("non-deterministic UDTF should pass check analysis") {
    assume(shouldTestPythonUDFs)
    withSQLConf(SQLConf.OPTIMIZE_ONE_ROW_RELATION_SUBQUERY.key -> "true") {
      spark.udtf.registerPython("testUDTF", pythonUDTF)
      withTempView("t") {
        Seq((0, 1), (1, 2)).toDF("a", "b").createOrReplaceTempView("t")
        val df = sql("SELECT f.* FROM t, LATERAL testUDTF(a, b) f")
        df.queryExecution.assertAnalyzed()
      }
    }
  }

  test("SPARK-44503: Specify PARTITION BY and ORDER BY for TABLE arguments") {
    // Positive tests
    assume(shouldTestPythonUDFs)
    def failure(plan: LogicalPlan): Unit = {
      fail(s"Unexpected plan: $plan")
    }

    spark.udtf.registerPython("testUDTF", pythonUDTF)
    sql(
      """
        |SELECT * FROM testUDTF(
        |  TABLE(VALUES (1), (1) AS tab(x))
        |  PARTITION BY X)
        |""".stripMargin).queryExecution.analyzed
      .collectFirst { case r: RepartitionByExpression => r }.get match {
      case RepartitionByExpression(
        _, Project(
          _, SubqueryAlias(
            _, _: LocalRelation)), _, _) =>
      case other =>
        failure(other)
    }
    sql(
      """
        |SELECT * FROM testUDTF(
        |  TABLE(VALUES (1), (1) AS tab(x))
        |  WITH SINGLE PARTITION)
        |""".stripMargin).queryExecution.analyzed
      .collectFirst { case r: Repartition => r }.get match {
      case Repartition(
        1, true, SubqueryAlias(
          _, _: LocalRelation)) =>
      case other =>
        failure(other)
    }
    sql(
      """
        |SELECT * FROM testUDTF(
        |  TABLE(VALUES ('abcd', 2), ('xycd', 4) AS tab(x, y))
        |  PARTITION BY SUBSTR(X, 2) ORDER BY (X, Y))
        |""".stripMargin).queryExecution.analyzed
      .collectFirst { case r: Sort => r }.get match {
      case Sort(
        _, false, RepartitionByExpression(
          _, Project(
            _, SubqueryAlias(
              _, _: LocalRelation)), _, _)) =>
      case other =>
        failure(other)
    }
    sql(
      """
        |SELECT * FROM testUDTF(
        |  TABLE(VALUES ('abcd', 2), ('xycd', 4) AS tab(x, y))
        |  WITH SINGLE PARTITION ORDER BY (X, Y))
        |""".stripMargin).queryExecution.analyzed
      .collectFirst { case r: Sort => r }.get match {
      case Sort(
        _, false, Repartition(
          1, true, SubqueryAlias(
            _, _: LocalRelation))) =>
      case other =>
        failure(other)
    }
    withTable("t") {
      sql("create table t(col array<int>) using parquet")
      val query = "select * from explode(table(t))"
      checkErrorMatchPVals(
        exception = intercept[AnalysisException](sql(query)),
        errorClass = "UNSUPPORTED_SUBQUERY_EXPRESSION_CATEGORY.UNSUPPORTED_TABLE_ARGUMENT",
        sqlState = None,
        parameters = Map("treeNode" -> "(?s).*"),
        context = ExpectedContext(
          fragment = "table(t)",
          start = 22,
          stop = 29))
    }

    spark.udtf.registerPython(UDTFCountSumLast.name, pythonUDTFCountSumLast)
    var plan = sql(
      s"""
        |WITH t AS (
        |  VALUES (0, 1), (1, 2), (1, 3) t(partition_col, input)
        |)
        |SELECT count, total, last
        |FROM ${UDTFCountSumLast.name}(TABLE(t) WITH SINGLE PARTITION)
        |ORDER BY 1, 2
        |""".stripMargin).queryExecution.analyzed
    plan.collectFirst { case r: Repartition => r } match {
      case Some(Repartition(1, true, _)) =>
      case _ =>
        failure(plan)
    }

    spark.udtf.registerPython(UDTFWithSinglePartition.name, pythonUDTFWithSinglePartition)
    plan = sql(
      s"""
        |WITH t AS (
        |    SELECT id AS partition_col, 1 AS input FROM range(1, 21)
        |    UNION ALL
        |    SELECT id AS partition_col, 2 AS input FROM range(1, 21)
        |)
        |SELECT count, total, last
        |FROM ${UDTFWithSinglePartition.name}(0, TABLE(t))
        |ORDER BY 1, 2
        |""".stripMargin).queryExecution.analyzed
    plan.collectFirst { case r: Repartition => r } match {
      case Some(Repartition(1, true, _)) =>
      case _ =>
        failure(plan)
    }

    spark.udtf.registerPython(UDTFPartitionByOrderBy.name, pythonUDTFPartitionByOrderBy)
    plan = sql(
      s"""
        |WITH t AS (
        |    SELECT id AS partition_col, 1 AS input FROM range(1, 21)
        |    UNION ALL
        |    SELECT id AS partition_col, 2 AS input FROM range(1, 21)
        |)
        |SELECT partition_col, count, total, last
        |FROM ${UDTFPartitionByOrderBy.name}(TABLE(t))
        |ORDER BY 1, 2
        |""".stripMargin).queryExecution.analyzed
    plan.collectFirst { case r: RepartitionByExpression => r } match {
      case Some(_: RepartitionByExpression) =>
      case _ =>
        failure(plan)
    }
  }

  test("SPARK-44503: Compute partition child indexes for various UDTF argument lists") {
    // Each of the following tests calls the PythonUDTF.partitionChildIndexes with a list of
    // expressions and then checks the PARTITION BY child expression indexes that come out.
    val projectList = Seq(
      Alias(Literal(42), "a")(),
      Alias(Literal(43), "b")())
    val projectTwoValues = Project(
      projectList = projectList,
      child = OneRowRelation())
    // There are no UDTF TABLE arguments, so there are no PARTITION BY child expression indexes.
    def partitionChildIndexes(udtfArguments: Seq[Expression]): Seq[Int] =
      udtfArguments.flatMap {
        case f: FunctionTableSubqueryArgumentExpression =>
          f.partitioningExpressionIndexes
        case _ =>
          Seq()
      }
    assert(partitionChildIndexes(Seq(
      Literal(41))) ==
      Seq.empty[Int])
    assert(partitionChildIndexes(Seq(
      Literal(41),
      Literal("abc"))) ==
      Seq.empty[Int])
    // The UDTF TABLE argument has no PARTITION BY expressions, so there are no PARTITION BY child
    // expression indexes.
    assert(partitionChildIndexes(Seq(
      FunctionTableSubqueryArgumentExpression(
        plan = projectTwoValues))) ==
      Seq.empty[Int])
    // The UDTF TABLE argument has two PARTITION BY expressions which are equal to the output
    // attributes from the provided relation, in order. Therefore the PARTITION BY child expression
    // indexes are 0 and 1.
    assert(partitionChildIndexes(Seq(
      FunctionTableSubqueryArgumentExpression(
        plan = projectTwoValues,
        partitionByExpressions = projectTwoValues.output))) ==
      Seq(0, 1))
    // The UDTF TABLE argument has one PARTITION BY expression which is equal to the first output
    // attribute from the provided relation. Therefore the PARTITION BY child expression index is 0.
    assert(partitionChildIndexes(Seq(
      FunctionTableSubqueryArgumentExpression(
        plan = projectTwoValues,
        partitionByExpressions = Seq(projectList.head.toAttribute)))) ==
      Seq(0))
    // The UDTF TABLE argument has one PARTITION BY expression which is equal to the second output
    // attribute from the provided relation. Therefore the PARTITION BY child expression index is 1.
    assert(partitionChildIndexes(Seq(
      FunctionTableSubqueryArgumentExpression(
        plan = projectTwoValues,
        partitionByExpressions = Seq(projectList.last.toAttribute)))) ==
      Seq(1))
    // The UDTF has one scalar argument, then one TABLE argument, then another scalar argument. The
    // TABLE argument has two PARTITION BY expressions which are equal to the output attributes from
    // the provided relation, in order. Therefore the PARTITION BY child expression indexes are 0
    // and 1.
    assert(partitionChildIndexes(Seq(
      Literal(41),
      FunctionTableSubqueryArgumentExpression(
        plan = projectTwoValues,
        partitionByExpressions = projectTwoValues.output),
      Literal("abc"))) ==
      Seq(0, 1))
    // Same as above, but the PARTITION BY expressions are new expressions which must be projected
    // after all the attributes from the relation provided to the UDTF TABLE argument. Therefore the
    // PARTITION BY child indexes are 3 and 4 because they begin at an offset of 2 from the
    // zero-based start of the list of values provided to the UDTF 'eval' method.
    assert(partitionChildIndexes(Seq(
      Literal(41),
      FunctionTableSubqueryArgumentExpression(
        plan = projectTwoValues,
        partitionByExpressions = Seq(Literal(42), Literal(43))),
      Literal("abc"))) ==
      Seq(2, 3))
    // Same as above, but the PARTITION BY list comprises just one addition expression.
    assert(partitionChildIndexes(Seq(
      Literal(41),
      FunctionTableSubqueryArgumentExpression(
        plan = projectTwoValues,
        partitionByExpressions = Seq(Add(projectList.head.toAttribute, Literal(1)))),
      Literal("abc"))) ==
      Seq(2))
    // Same as above, but the PARTITION BY list comprises one literal value and one addition
    // expression.
    assert(partitionChildIndexes(Seq(
      Literal(41),
      FunctionTableSubqueryArgumentExpression(
        plan = projectTwoValues,
        partitionByExpressions = Seq(Literal(42), Add(projectList.head.toAttribute, Literal(1)))),
      Literal("abc"))) ==
      Seq(2, 3))
  }

  test("SPARK-45402: Add UDTF API for 'analyze' to return a buffer to consume on class creation") {
    spark.udtf.registerPython(
      UDTFForwardStateFromAnalyze.name,
      pythonUDTFForwardStateFromAnalyze)
    withTable("t") {
      sql("create table t(col array<int>) using parquet")
      val query = s"select * from ${UDTFForwardStateFromAnalyze.name}('abc')"
      checkAnswer(
        sql(query),
        Row("abc"))
    }
  }

  test("SPARK-48180: Analyzer bug with multiple ORDER BY items for input table argument") {
    assume(shouldTestPythonUDFs)
    spark.udtf.registerPython("testUDTF", pythonUDTF)
    checkError(
      exception = intercept[ParseException](sql(
        """
          |SELECT * FROM testUDTF(
          |  TABLE(SELECT 1 AS device_id, 2 AS data_ds)
          |  WITH SINGLE PARTITION
          |  ORDER BY device_id, data_ds)
          |""".stripMargin)),
      errorClass = "_LEGACY_ERROR_TEMP_0064",
      parameters = Map("msg" ->
        ("The table function call includes a table argument with an invalid " +
          "partitioning/ordering specification: the ORDER BY clause included multiple " +
          "expressions without parentheses surrounding them; please add parentheses around these " +
          "expressions and then retry the query again")),
      context = ExpectedContext(
        fragment = "TABLE(SELECT 1 AS device_id, 2 AS data_ds)\n  " +
          "WITH SINGLE PARTITION\n  " +
          "ORDER BY device_id, data_ds",
        start = 27,
        stop = 122))
  }
}
