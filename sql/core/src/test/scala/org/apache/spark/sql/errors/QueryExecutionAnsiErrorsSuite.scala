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

import org.apache.spark._
import org.apache.spark.scheduler.{SparkListener, SparkListenerTaskStart}
import org.apache.spark.sql.{QueryTest, SparkSession}
import org.apache.spark.sql.catalyst.expressions.{CaseWhen, Cast, CheckOverflowInTableInsert, ExpressionProxy, Literal, SubExprEvaluationRuntime}
import org.apache.spark.sql.catalyst.plans.logical.OneRowRelation
import org.apache.spark.sql.functions._
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.test.{SharedSparkSession, TestSparkSession}
import org.apache.spark.sql.types.ByteType

// Test suite for all the execution errors that requires enable ANSI SQL mode.
class QueryExecutionAnsiErrorsSuite extends QueryTest
  with SharedSparkSession {
  import testImplicits._

  override def sparkConf: SparkConf = super.sparkConf.set(SQLConf.ANSI_ENABLED.key, "true")

  override def createSparkSession: TestSparkSession = {
    SparkSession.cleanupAnyExistingSession()
    new TestSparkSession(sparkConf, maxLocalTaskFailures = 2)
  }

  private val ansiConf = "\"" + SQLConf.ANSI_ENABLED.key + "\""

  test("CAST_OVERFLOW: from timestamp to int") {
    checkError(
      exception = intercept[SparkArithmeticException] {
        sql("select CAST(TIMESTAMP '9999-12-31T12:13:14.56789Z' AS INT)").collect()
      },
      condition = "CAST_OVERFLOW",
      parameters = Map("value" -> "TIMESTAMP '9999-12-31 04:13:14.56789'",
        "sourceType" -> "\"TIMESTAMP\"",
        "targetType" -> "\"INT\""),
      sqlState = "22003")
  }

  test("DIVIDE_BY_ZERO: can't divide an integer by zero") {
    checkError(
      exception = intercept[SparkArithmeticException] {
        sql("select 6/0").collect()
      },
      condition = "DIVIDE_BY_ZERO",
      sqlState = "22012",
      parameters = Map("config" -> ansiConf),
      context = ExpectedContext(fragment = "6/0", start = 7, stop = 9))

    checkError(
      exception = intercept[SparkArithmeticException] {
        OneRowRelation().select(lit(5) / lit(0)).collect()
      },
      condition = "DIVIDE_BY_ZERO",
      sqlState = "22012",
      parameters = Map("config" -> ansiConf),
      context = ExpectedContext(fragment = "div", callSitePattern = getCurrentClassCallSitePattern))

    checkError(
      exception = intercept[SparkArithmeticException] {
        OneRowRelation().select(lit(5).divide(lit(0))).collect()
      },
      condition = "DIVIDE_BY_ZERO",
      sqlState = "22012",
      parameters = Map("config" -> ansiConf),
      context = ExpectedContext(
        fragment = "divide",
        callSitePattern = getCurrentClassCallSitePattern))
  }

  test("INTERVAL_DIVIDED_BY_ZERO: interval divided by zero") {
    checkError(
      exception = intercept[SparkArithmeticException] {
        sql("select interval 1 day / 0").collect()
      },
      condition = "INTERVAL_DIVIDED_BY_ZERO",
      sqlState = "22012",
      parameters = Map.empty[String, String],
      context = ExpectedContext(fragment = "interval 1 day / 0", start = 7, stop = 24))
  }

  test("INVALID_FRACTION_OF_SECOND: in the function make_timestamp") {
    checkError(
      exception = intercept[SparkDateTimeException] {
        sql("select make_timestamp(2012, 11, 30, 9, 19, 60.1)").collect()
      },
      condition = "INVALID_FRACTION_OF_SECOND",
      sqlState = "22023",
      parameters = Map(
        "secAndMicros" -> "60.100000"
      ))
  }

  test("NUMERIC_VALUE_OUT_OF_RANGE: cast string to decimal") {
    checkError(
      exception = intercept[SparkArithmeticException] {
        sql("select CAST('66666666666666.666' AS DECIMAL(8, 1))").collect()
      },
      condition = "NUMERIC_VALUE_OUT_OF_RANGE.WITH_SUGGESTION",
      sqlState = "22003",
      parameters = Map(
        "value" -> "66666666666666.666",
        "precision" -> "8",
        "scale" -> "1",
        "config" -> ansiConf),
      context = ExpectedContext(
        fragment = "CAST('66666666666666.666' AS DECIMAL(8, 1))",
        start = 7,
        stop = 49))

    checkError(
      exception = intercept[SparkArithmeticException] {
        OneRowRelation().select(lit("66666666666666.666").cast("DECIMAL(8, 1)")).collect()
      },
      condition = "NUMERIC_VALUE_OUT_OF_RANGE.WITH_SUGGESTION",
      sqlState = "22003",
      parameters = Map(
        "value" -> "66666666666666.666",
        "precision" -> "8",
        "scale" -> "1",
        "config" -> ansiConf),
      context = ExpectedContext(
        fragment = "cast",
        callSitePattern = getCurrentClassCallSitePattern))
  }

  test("INVALID_ARRAY_INDEX: get element from array") {
    checkError(
      exception = intercept[SparkArrayIndexOutOfBoundsException] {
        sql("select array(1, 2, 3, 4, 5)[8]").collect()
      },
      condition = "INVALID_ARRAY_INDEX",
      parameters = Map("indexValue" -> "8", "arraySize" -> "5", "ansiConfig" -> ansiConf),
      context = ExpectedContext(fragment = "array(1, 2, 3, 4, 5)[8]", start = 7, stop = 29))

    checkError(
      exception = intercept[SparkArrayIndexOutOfBoundsException] {
        OneRowRelation().select(lit(Array(1, 2, 3, 4, 5))(8)).collect()
      },
      condition = "INVALID_ARRAY_INDEX",
      parameters = Map("indexValue" -> "8", "arraySize" -> "5", "ansiConfig" -> ansiConf),
      context = ExpectedContext(
        fragment = "apply",
        callSitePattern = getCurrentClassCallSitePattern))
  }

  test("INVALID_ARRAY_INDEX_IN_ELEMENT_AT: element_at from array") {
    checkError(
      exception = intercept[SparkArrayIndexOutOfBoundsException] {
        sql("select element_at(array(1, 2, 3, 4, 5), 8)").collect()
      },
      condition = "INVALID_ARRAY_INDEX_IN_ELEMENT_AT",
      parameters = Map("indexValue" -> "8", "arraySize" -> "5", "ansiConfig" -> ansiConf),
      context = ExpectedContext(
        fragment = "element_at(array(1, 2, 3, 4, 5), 8)",
        start = 7,
        stop = 41))

    checkError(
      exception = intercept[SparkArrayIndexOutOfBoundsException] {
        OneRowRelation().select(element_at(lit(Array(1, 2, 3, 4, 5)), 8)).collect()
      },
      condition = "INVALID_ARRAY_INDEX_IN_ELEMENT_AT",
      parameters = Map("indexValue" -> "8", "arraySize" -> "5", "ansiConfig" -> ansiConf),
      context =
        ExpectedContext(fragment = "element_at", callSitePattern = getCurrentClassCallSitePattern))
  }

  test("INVALID_INDEX_OF_ZERO: element_at from array by index zero") {
    checkError(
      exception = intercept[SparkRuntimeException](
        sql("select element_at(array(1, 2, 3, 4, 5), 0)").collect()
      ),
      condition = "INVALID_INDEX_OF_ZERO",
      parameters = Map.empty,
      context = ExpectedContext(
        fragment = "element_at(array(1, 2, 3, 4, 5), 0)",
        start = 7,
        stop = 41)
    )

    checkError(
      exception = intercept[SparkRuntimeException](
        OneRowRelation().select(element_at(lit(Array(1, 2, 3, 4, 5)), 0)).collect()
      ),
      condition = "INVALID_INDEX_OF_ZERO",
      parameters = Map.empty,
      context =
        ExpectedContext(fragment = "element_at", callSitePattern = getCurrentClassCallSitePattern))
  }

  test("CAST_INVALID_INPUT: cast string to double") {
    checkError(
      exception = intercept[SparkNumberFormatException] {
        sql("select CAST('111111111111xe23' AS DOUBLE)").collect()
      },
      condition = "CAST_INVALID_INPUT",
      parameters = Map(
        "expression" -> "'111111111111xe23'",
        "sourceType" -> "\"STRING\"",
        "targetType" -> "\"DOUBLE\""),
      context = ExpectedContext(
        fragment = "CAST('111111111111xe23' AS DOUBLE)",
        start = 7,
        stop = 40))

    checkError(
      exception = intercept[SparkNumberFormatException] {
        OneRowRelation().select(lit("111111111111xe23").cast("DOUBLE")).collect()
      },
      condition = "CAST_INVALID_INPUT",
      parameters = Map(
        "expression" -> "'111111111111xe23'",
        "sourceType" -> "\"STRING\"",
        "targetType" -> "\"DOUBLE\""),
      context = ExpectedContext(
        fragment = "cast",
        callSitePattern = getCurrentClassCallSitePattern))
  }

  test("CANNOT_PARSE_TIMESTAMP: parse string to timestamp") {
    checkError(
      exception = intercept[SparkDateTimeException] {
        sql("select to_timestamp('abc', 'yyyy-MM-dd HH:mm:ss')").collect()
      },
      condition = "CANNOT_PARSE_TIMESTAMP",
      parameters = Map(
        "message" -> "Text 'abc' could not be parsed at index 0",
        "ansiConfig" -> ansiConf)
    )
  }

  test("CAST_OVERFLOW_IN_TABLE_INSERT: overflow during table insertion") {
    Seq("TINYINT", "SMALLINT", "INT", "BIGINT", "DECIMAL(7,2)").foreach { targetType =>
      val tableName = "overflowTable"
      withTable(tableName) {
        sql(s"CREATE TABLE $tableName(i $targetType) USING parquet")
        checkError(
          exception = intercept[SparkArithmeticException] {
            sql(s"insert into $tableName values 12345678901234567890D")
          },
          condition = "CAST_OVERFLOW_IN_TABLE_INSERT",
          parameters = Map(
            "sourceType" -> "\"DOUBLE\"",
            "targetType" -> ("\"" + targetType + "\""),
            "columnName" -> "`i`")
        )
      }
    }
  }

  test("SPARK-42286: CheckOverflowInTableInsert with CaseWhen should throw an exception") {
    val caseWhen = CaseWhen(
      Seq((Literal(true), Cast(Literal.apply(12345678901234567890D), ByteType))), None)
    checkError(
      exception = intercept[SparkArithmeticException] {
        CheckOverflowInTableInsert(caseWhen, "col").eval(null)
      },
      condition = "CAST_OVERFLOW",
      parameters = Map("value" -> "1.2345678901234567E19D",
        "sourceType" -> "\"DOUBLE\"",
        "targetType" -> ("\"TINYINT\""))
    )
  }

  test("SPARK-42286: End-to-end query with Case When throwing CAST_OVERFLOW exception") {
    withTable("t1", "t2") {
      sql("CREATE TABLE t1 (x double) USING parquet")
      sql("insert into t1 values (1.2345678901234567E19D)")
      sql("CREATE TABLE t2 (x tinyint) USING parquet")
      val insertCmd = "insert into t2 select 0 - (case when x = 1.2345678901234567E19D " +
        "then 1.2345678901234567E19D else x end) from t1 where x = 1.2345678901234567E19D;"
      checkError(
        exception = intercept[SparkArithmeticException] {
          sql(insertCmd).collect()
        },
        condition = "CAST_OVERFLOW",
        parameters = Map("value" -> "-1.2345678901234567E19D",
          "sourceType" -> "\"DOUBLE\"",
          "targetType" -> "\"TINYINT\""),
        sqlState = "22003")
    }
  }

  test("SPARK-39981: interpreted CheckOverflowInTableInsert should throw an exception") {
    checkError(
      exception = intercept[SparkArithmeticException] {
        CheckOverflowInTableInsert(
          Cast(Literal.apply(12345678901234567890D), ByteType), "col").eval(null)
      }.asInstanceOf[SparkThrowable],
      condition = "CAST_OVERFLOW_IN_TABLE_INSERT",
      parameters = Map(
        "sourceType" -> "\"DOUBLE\"",
        "targetType" -> ("\"TINYINT\""),
        "columnName" -> "`col`")
    )
  }

  test("SPARK-41991: interpreted CheckOverflowInTableInsert with ExpressionProxy should " +
    "throw an exception") {
    val runtime = new SubExprEvaluationRuntime(1)
    val proxy = ExpressionProxy(Cast(Literal.apply(12345678901234567890D), ByteType), 0, runtime)
    checkError(
      exception = intercept[SparkArithmeticException] {
        CheckOverflowInTableInsert(proxy, "col").eval(null)
      }.asInstanceOf[SparkThrowable],
      condition = "CAST_OVERFLOW_IN_TABLE_INSERT",
      parameters = Map(
        "sourceType" -> "\"DOUBLE\"",
        "targetType" -> ("\"TINYINT\""),
        "columnName" -> "`col`")
    )
  }

  test("SPARK-46922: user-facing runtime errors") {
    withSQLConf(SQLConf.ANSI_ENABLED.key -> "true") {
      var numTaskStarted = 0
      val listener = new SparkListener {
        override def onTaskStart(taskStart: SparkListenerTaskStart): Unit = {
          numTaskStarted += 1
        }
      }
      sparkContext.addSparkListener(listener)
      try {
        val df1 = spark.range(0, 10, 1, 1).map { v =>
          if (v > 5) throw new RuntimeException("test error") else v
        }
        // If error is not user-facing, it will be wrapped by `SparkException` with "Job aborted".
        val e1 = intercept[SparkException](df1.collect())
        assert(e1.getMessage.contains("Job aborted"))
        sparkContext.listenerBus.waitUntilEmpty()
        // In this test suite, Spark re-tries the task 2 times.
        assert(numTaskStarted == 2)
        numTaskStarted = 0

        val df2 = spark.range(0, 10, 1, 2).map { v =>
          if (v > 5) throw new RuntimeException("test error") else v
        }
        val e2 = intercept[SparkException](df2.collect())
        assert(e2.getMessage.contains("Job aborted"))
        sparkContext.listenerBus.waitUntilEmpty()
        // In this test suite, Spark re-tries the task 2 times, the input data has 2 partitions, but
        // only the first task will fail (contains value 0), so in total 3 tasks started.
        assert(numTaskStarted == 3)
        numTaskStarted = 0

        val df3 = spark.range(0, 10, 1, 1).select(lit(1) / $"id")
        checkError(
          // If error is user-facing, it will be thrown directly.
          exception = intercept[SparkArithmeticException](df3.collect()),
          condition = "DIVIDE_BY_ZERO",
          parameters = Map("config" -> ansiConf),
          context = ExpectedContext(
            fragment = "div",
            callSitePattern = getCurrentClassCallSitePattern
          )
        )
        sparkContext.listenerBus.waitUntilEmpty()
        // TODO (SPARK-46951): Spark should not re-try tasks for this error.
        assert(numTaskStarted == 2)
        numTaskStarted = 0

        val df4 = spark.range(0, 10, 1, 2).select(lit(1) / $"id")
        checkError(
          exception = intercept[SparkArithmeticException](df4.collect()),
          condition = "DIVIDE_BY_ZERO",
          parameters = Map("config" -> ansiConf),
          context = ExpectedContext(
            fragment = "div",
            callSitePattern = getCurrentClassCallSitePattern
          )
        )
        sparkContext.listenerBus.waitUntilEmpty()
        // TODO (SPARK-46951): Spark should not re-try tasks for this error.
        assert(numTaskStarted == 3)
      } finally {
        sparkContext.removeSparkListener(listener)
      }
    }
  }

  test("SPARK-49773: INVALID_TIMEZONE for bad timezone") {
    checkError(
      exception = intercept[SparkDateTimeException] {
        sql("select make_timestamp(1, 2, 28, 23, 1, 1, -100)").collect()
      },
      condition = "INVALID_TIMEZONE",
      parameters = Map("timeZone" -> "-100")
    )
  }
}
