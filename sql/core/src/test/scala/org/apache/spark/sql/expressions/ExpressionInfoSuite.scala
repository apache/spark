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

package org.apache.spark.sql.expressions

import org.apache.spark.{SparkFunSuite, SparkIllegalArgumentException}
import org.apache.spark.sql.catalyst.{FunctionIdentifier, InternalRow}
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.execution.HiveResult.hiveResultString
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.test.SharedSparkSession
import org.apache.spark.tags.SlowSQLTest
import org.apache.spark.util.{ThreadUtils, Utils}

@SlowSQLTest
class ExpressionInfoSuite extends SparkFunSuite with SharedSparkSession {

  test("Replace _FUNC_ in ExpressionInfo") {
    val info = spark.sessionState.catalog.lookupFunctionInfo(FunctionIdentifier("upper"))
    assert(info.getName === "upper")
    assert(info.getClassName === "org.apache.spark.sql.catalyst.expressions.Upper")
    assert(info.getUsage === "upper(str) - Returns `str` with all characters changed to uppercase.")
    assert(info.getExamples.contains("> SELECT upper('SparkSql');"))
    assert(info.getSince === "1.0.1")
    assert(info.getNote === "")
    assert(info.getExtended.contains("> SELECT upper('SparkSql');"))
  }

  test("group info in ExpressionInfo") {
    val info = spark.sessionState.catalog.lookupFunctionInfo(FunctionIdentifier("sum"))
    assert(info.getGroup === "agg_funcs")
    Seq("agg_funcs", "array_funcs", "binary_funcs", "bitwise_funcs", "collection_funcs",
      "predicate_funcs", "conditional_funcs", "conversion_funcs", "csv_funcs", "datetime_funcs",
      "generator_funcs", "hash_funcs", "json_funcs", "lambda_funcs", "map_funcs", "math_funcs",
      "misc_funcs", "string_funcs", "struct_funcs", "window_funcs", "xml_funcs")

    Seq("agg_funcs", "array_funcs", "datetime_funcs", "json_funcs", "map_funcs", "window_funcs")
        .foreach { groupName =>
      val info = new ExpressionInfo(
        "testClass", null, "testName", null, "", "", "", groupName, "", "", "")
      assert(info.getGroup === groupName)
    }

    val validGroups = Seq(
      "agg_funcs", "array_funcs", "binary_funcs", "bitwise_funcs", "collection_funcs",
      "predicate_funcs", "conditional_funcs", "conversion_funcs", "csv_funcs", "datetime_funcs",
      "generator_funcs", "hash_funcs", "json_funcs", "lambda_funcs", "map_funcs", "math_funcs",
      "misc_funcs", "string_funcs", "struct_funcs", "window_funcs", "xml_funcs", "table_funcs",
      "url_funcs", "variant_funcs").sorted
    val invalidGroupName = "invalid_group_funcs"
    checkError(
      exception = intercept[SparkIllegalArgumentException] {
        new ExpressionInfo(
          "testClass", null, "testName", null, "", "", "", invalidGroupName, "", "", "")
      },
      condition = "_LEGACY_ERROR_TEMP_3202",
      parameters = Map(
        "exprName" -> "testName",
        "group" -> invalidGroupName,
        "validGroups" -> validGroups.mkString("[", ", ", "]")))
  }

  test("source in ExpressionInfo") {
    val info = spark.sessionState.catalog.lookupFunctionInfo(FunctionIdentifier("sum"))
    assert(info.getSource === "built-in")

    val validSources = Seq(
      "built-in", "hive", "python_udf", "scala_udf", "java_udf", "python_udtf", "internal",
      "sql_udf")
    validSources.foreach { source =>
      val info = new ExpressionInfo(
        "testClass", null, "testName", null, "", "", "", "", "", "", source)
      assert(info.getSource === source)
    }
    val invalidSource = "invalid_source"
    checkError(
      exception = intercept[SparkIllegalArgumentException] {
        new ExpressionInfo(
          "testClass", null, "testName", null, "", "", "", "", "", "", invalidSource)
      },
      condition = "_LEGACY_ERROR_TEMP_3203",
      parameters = Map(
        "exprName" -> "testName",
        "source" -> invalidSource,
        "validSources" -> validSources.sorted.mkString("[", ", ", "]")))
  }

  test("error handling in ExpressionInfo") {
    val invalidNote = "  invalid note"
    checkError(
      exception = intercept[SparkIllegalArgumentException] {
        new ExpressionInfo("testClass", null, "testName", null, "", "", invalidNote, "", "", "", "")
      },
      condition = "_LEGACY_ERROR_TEMP_3201",
      parameters = Map("exprName" -> "testName", "note" -> invalidNote))

    val invalidSince = "-3.0.0"
    checkError(
      exception = intercept[SparkIllegalArgumentException] {
        new ExpressionInfo(
          "testClass", null, "testName", null, "", "", "", "", invalidSince, "", "")
      },
      condition = "_LEGACY_ERROR_TEMP_3204",
      parameters = Map("since" -> invalidSince, "exprName" -> "testName"))

    val invalidDeprecated = "  invalid deprecated"
    checkError(
      exception = intercept[SparkIllegalArgumentException] {
        new ExpressionInfo(
          "testClass", null, "testName", null, "", "", "", "", "", invalidDeprecated, "")
      },
      condition = "_LEGACY_ERROR_TEMP_3205",
      parameters = Map("exprName" -> "testName", "deprecated" -> invalidDeprecated))
  }

  test("using _FUNC_ instead of function names in examples") {
    val exampleRe = "(>.*;)".r
    val setStmtRe = "(?i)^(>\\s+set\\s+).+".r
    val ignoreSet = Set(
      // Examples for CaseWhen show simpler syntax:
      // `CASE WHEN ... THEN ... WHEN ... THEN ... END`
      "org.apache.spark.sql.catalyst.expressions.CaseWhen",
      // _FUNC_ is replaced by `locate` but `locate(... IN ...)` is not supported
      "org.apache.spark.sql.catalyst.expressions.StringLocate",
      // _FUNC_ is replaced by `%` which causes a parsing error on `SELECT %(2, 1.8)`
      "org.apache.spark.sql.catalyst.expressions.Remainder",
      // Examples demonstrate alternative names, see SPARK-20749
      "org.apache.spark.sql.catalyst.expressions.Length",
      // Examples demonstrate alternative syntax, see SPARK-45574
      "org.apache.spark.sql.catalyst.expressions.Cast",
      // Examples demonstrate alternative syntax, see SPARK-47012
      "org.apache.spark.sql.catalyst.expressions.Collate",
      classOf[ShiftLeft].getName,
      classOf[ShiftRight].getName,
      classOf[ShiftRightUnsigned].getName
    )
    spark.sessionState.functionRegistry.listFunction().foreach { funcId =>
      val info = spark.sessionState.catalog.lookupFunctionInfo(funcId)
      val className = info.getClassName
      withClue(s"Expression class '$className'") {
        val exprExamples = info.getOriginalExamples
        if (!exprExamples.isEmpty && !ignoreSet.contains(className)) {
          assert(exampleRe.findAllIn(exprExamples).iterator.to(Iterable)
            .filter(setStmtRe.findFirstIn(_).isEmpty) // Ignore SET commands
            .forall(_.contains("_FUNC_")))
        }
      }
    }
  }

  test("SPARK-32870: Default expressions in FunctionRegistry should have their " +
    "usage, examples, since, and group filled") {
    val ignoreSet = Set(
      // Cast aliases do not need examples
      "org.apache.spark.sql.catalyst.expressions.Cast")

    spark.sessionState.functionRegistry.listFunction().foreach { funcId =>
      val info = spark.sessionState.catalog.lookupFunctionInfo(funcId)
      if (!ignoreSet.contains(info.getClassName)) {
        withClue(s"Function '${info.getName}', Expression class '${info.getClassName}'") {
          assert(info.getUsage.nonEmpty)
          assert(info.getExamples.startsWith("\n    Examples:\n"))
          assert(info.getExamples.endsWith("\n  "))
          assert(info.getSince.matches("[0-9]+\\.[0-9]+\\.[0-9]+"))
          assert(info.getGroup.nonEmpty)

          if (info.getArguments.nonEmpty) {
            assert(info.getArguments.startsWith("\n    Arguments:\n"))
            assert(info.getArguments.endsWith("\n  "))
          }
        }
      }
    }
  }

  test("check outputs of expression examples") {
    def unindentAndTrim(s: String): String = {
      s.replaceAll("\n\\s+", "\n").trim
    }
    val beginSqlStmtRe = "\n      > ".r
    val endSqlStmtRe = ";\n".r
    def checkExampleSyntax(example: String): Unit = {
      val beginStmtNum = beginSqlStmtRe.findAllIn(example).length
      val endStmtNum = endSqlStmtRe.findAllIn(example).length
      assert(beginStmtNum === endStmtNum,
        "The number of ` > ` does not match to the number of `;`")
    }
    val exampleRe = """^(.+);\n(?s)(.+)$""".r
    val ignoreSet = Set(
      // One of examples shows getting the current timestamp
      "org.apache.spark.sql.catalyst.expressions.UnixTimestamp",
      "org.apache.spark.sql.catalyst.expressions.CurrentDate",
      "org.apache.spark.sql.catalyst.expressions.CurDateExpressionBuilder",
      "org.apache.spark.sql.catalyst.expressions.CurrentTimestamp",
      "org.apache.spark.sql.catalyst.expressions.CurrentTimeZone",
      "org.apache.spark.sql.catalyst.expressions.Now",
      "org.apache.spark.sql.catalyst.expressions.LocalTimestamp",
      // Random output without a seed
      "org.apache.spark.sql.catalyst.expressions.Rand",
      "org.apache.spark.sql.catalyst.expressions.Randn",
      "org.apache.spark.sql.catalyst.expressions.Shuffle",
      "org.apache.spark.sql.catalyst.expressions.Uuid",
      // Other nondeterministic expressions
      "org.apache.spark.sql.catalyst.expressions.MonotonicallyIncreasingID",
      "org.apache.spark.sql.catalyst.expressions.SparkPartitionID",
      "org.apache.spark.sql.catalyst.expressions.InputFileName",
      "org.apache.spark.sql.catalyst.expressions.InputFileBlockStart",
      "org.apache.spark.sql.catalyst.expressions.InputFileBlockLength",
      // The example calls methods that return unstable results.
      "org.apache.spark.sql.catalyst.expressions.CallMethodViaReflection",
      "org.apache.spark.sql.catalyst.expressions.TryReflect",
      "org.apache.spark.sql.catalyst.expressions.SparkVersion",
      // Throws an error
      "org.apache.spark.sql.catalyst.expressions.RaiseErrorExpressionBuilder",
      "org.apache.spark.sql.catalyst.expressions.AssertTrue",
      // Requires dynamic class loading not available in this test suite.
      "org.apache.spark.sql.catalyst.expressions.FromAvro",
      "org.apache.spark.sql.catalyst.expressions.ToAvro",
      "org.apache.spark.sql.catalyst.expressions.SchemaOfAvro",
      "org.apache.spark.sql.catalyst.expressions.FromProtobuf",
      "org.apache.spark.sql.catalyst.expressions.ToProtobuf",
      classOf[CurrentUser].getName,
      // The encrypt expression includes a random initialization vector to its encrypted result
      classOf[AesEncrypt].getName)

    ThreadUtils.parmap(
      spark.sessionState.functionRegistry.listFunction(),
      prefix = "ExpressionInfoSuite-check-outputs-of-expression-examples",
      maxThreads = Runtime.getRuntime.availableProcessors
    ) { funcId =>
      // Examples can change settings. We clone the session to prevent tests clashing.
      val clonedSpark = spark.cloneSession()
      // Coalescing partitions can change result order, so disable it.
      clonedSpark.conf.set(SQLConf.COALESCE_PARTITIONS_ENABLED.key, false)
      val info = clonedSpark.sessionState.catalog.lookupFunctionInfo(funcId)
      val className = info.getClassName
      if (!ignoreSet.contains(className)) {
        withClue(s"Function '${info.getName}', Expression class '$className'") {
          val example = info.getExamples
          checkExampleSyntax(example)
          example.split("  > ").toList.foreach {
            case exampleRe(sql, output) =>
              val df = clonedSpark.sql(sql)
              val actual = unindentAndTrim(
                hiveResultString(df.queryExecution.executedPlan).mkString("\n"))
              val expected = unindentAndTrim(output)
              assert(actual === expected)
            case _ =>
          }
        }
      }
    }
  }

  test("Check whether SQL expressions should extend NullIntolerant") {
    // Only check expressions extended from these expressions because these expressions are
    // NullIntolerant by default.
    val exprTypesToCheck = Seq(classOf[UnaryExpression], classOf[BinaryExpression],
      classOf[TernaryExpression], classOf[QuaternaryExpression], classOf[SeptenaryExpression])

    // Do not check these expressions, because these expressions override the eval method
    val ignoreSet = Set(
      // Throws an exception, even if input is null
      classOf[RaiseError]
    )

    val candidateExprsToCheck = spark.sessionState.functionRegistry.listFunction()
      .map(spark.sessionState.catalog.lookupFunctionInfo).map(_.getClassName)
      .filterNot(c => ignoreSet.exists(_.getName.equals(c)))
      .map(name => Utils.classForName(name))
      .filterNot(classOf[NonSQLExpression].isAssignableFrom)
      // BinaryArithmetic overrides the eval method
      .filterNot(classOf[BinaryArithmetic].isAssignableFrom)

    exprTypesToCheck.foreach { superClass =>
      candidateExprsToCheck.filter(superClass.isAssignableFrom).foreach { clazz =>
        val isEvalOverrode = clazz.getMethod("eval", classOf[InternalRow]) !=
          superClass.getMethod("eval", classOf[InternalRow])
        val isNullIntolerantOverridden = clazz.getMethod("nullIntolerant") !=
          classOf[Expression].getMethod("nullIntolerant")
        if (isEvalOverrode && isNullIntolerantOverridden) {
          fail(s"${clazz.getName} should not override nullIntolerant, " +
            s"or add ${clazz.getName} in the ignoreSet of this test.")
        } else if (!isEvalOverrode && !isNullIntolerantOverridden) {
          fail(s"${clazz.getName} should override nullIntolerant.")
        } else {
          assert((!isEvalOverrode && isNullIntolerantOverridden) ||
            (isEvalOverrode && !isNullIntolerantOverridden))
        }
      }
    }
  }

  test("Check source for Built-in and Scala UDF") {
    import org.apache.spark.sql.IntegratedUDFTestUtils
    val catalog = spark.sessionState.catalog
    assert(catalog.lookupFunctionInfo(FunctionIdentifier("sum")).getSource === "built-in")

    val scalaUDF = IntegratedUDFTestUtils.TestScalaUDF("scalaUDF")
    IntegratedUDFTestUtils.registerTestUDF(scalaUDF, spark)
    val scalaInfo = catalog.lookupFunctionInfo(FunctionIdentifier(scalaUDF.name))
    assert(scalaInfo.getSource === "scala_udf")
  }

  test("Check source for Python UDF") {
    import org.apache.spark.sql.IntegratedUDFTestUtils
    assume(IntegratedUDFTestUtils.shouldTestPythonUDFs)

    val catalog = spark.sessionState.catalog
    val pythonUDF = IntegratedUDFTestUtils.TestPythonUDF("pythonUDF")
    IntegratedUDFTestUtils.registerTestUDF(pythonUDF, spark)
    val pythonInfo = catalog.lookupFunctionInfo(FunctionIdentifier(pythonUDF.name))
    assert(pythonInfo.getSource === "python_udf")
  }
}
