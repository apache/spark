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

import org.apache.spark.sql.{AnalysisException, IntegratedUDFTestUtils, QueryTest, Row}
import org.apache.spark.sql.functions.{array, avg, col, count, transform}
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.test.SharedSparkSession
import org.apache.spark.sql.types.LongType

class PythonUDFSuite extends QueryTest with SharedSparkSession {
  import testImplicits._

  import IntegratedUDFTestUtils._

  val scalaTestUDF = TestScalaUDF(name = "scalaUDF", Some(LongType))
  val pythonTestUDF = TestPythonUDF(name = "pyUDF", Some(LongType))

  lazy val base = Seq(
    (Some(1), Some(1)), (Some(1), Some(2)), (Some(2), Some(1)),
    (Some(2), Some(2)), (Some(3), Some(1)), (Some(3), Some(2)),
    (None, Some(1)), (Some(3), None), (None, None)).toDF("a", "b")

  test("SPARK-28445: PythonUDF as grouping key and aggregate expressions") {
    assume(shouldTestPythonUDFs)
    val df1 = base.groupBy(scalaTestUDF(base("a") + 1))
      .agg(scalaTestUDF(base("a") + 1), scalaTestUDF(count(base("b"))))
    val df2 = base.groupBy(pythonTestUDF(base("a") + 1))
      .agg(pythonTestUDF(base("a") + 1), pythonTestUDF(count(base("b"))))
    checkAnswer(df1, df2)
  }

  test("SPARK-28445: PythonUDF as grouping key and used in aggregate expressions") {
    assume(shouldTestPythonUDFs)
    val df1 = base.groupBy(scalaTestUDF(base("a") + 1))
      .agg(scalaTestUDF(base("a") + 1) + 1, scalaTestUDF(count(base("b"))))
    val df2 = base.groupBy(pythonTestUDF(base("a") + 1))
      .agg(pythonTestUDF(base("a") + 1) + 1, pythonTestUDF(count(base("b"))))
    checkAnswer(df1, df2)
  }

  test("SPARK-28445: PythonUDF in aggregate expression has grouping key in its arguments") {
    assume(shouldTestPythonUDFs)
    val df1 = base.groupBy(scalaTestUDF(base("a") + 1))
      .agg(scalaTestUDF(scalaTestUDF(base("a") + 1)), scalaTestUDF(count(base("b"))))
    val df2 = base.groupBy(pythonTestUDF(base("a") + 1))
      .agg(pythonTestUDF(pythonTestUDF(base("a") + 1)), pythonTestUDF(count(base("b"))))
    checkAnswer(df1, df2)
  }

  test("SPARK-28445: PythonUDF over grouping key is argument to aggregate function") {
    assume(shouldTestPythonUDFs)
    val df1 = base.groupBy(scalaTestUDF(base("a") + 1))
      .agg(scalaTestUDF(scalaTestUDF(base("a") + 1)),
        scalaTestUDF(count(scalaTestUDF(base("a") + 1))))
    val df2 = base.groupBy(pythonTestUDF(base("a") + 1))
      .agg(pythonTestUDF(pythonTestUDF(base("a") + 1)),
        pythonTestUDF(count(pythonTestUDF(base("a") + 1))))
    checkAnswer(df1, df2)
  }

  test("SPARK-39962: Global aggregation of Pandas UDF should respect the column order") {
    assume(shouldTestPandasUDFs)
    val df = Seq[(java.lang.Integer, java.lang.Integer)]((1, null)).toDF("a", "b")

    val pandasTestUDF = TestGroupedAggPandasUDF(name = "pandas_udf")
    val reorderedDf = df.select("b", "a")
    val actual = reorderedDf.agg(
      pandasTestUDF(reorderedDf("a")), pandasTestUDF(reorderedDf("b")))
    val expected = df.agg(pandasTestUDF(df("a")), pandasTestUDF(df("b")))

    checkAnswer(actual, expected)
  }

  test("SPARK-34265: Instrument Python UDF execution using SQL Metrics") {
    assume(shouldTestPythonUDFs)
    val pythonSQLMetrics = List(
      "data sent to Python workers",
      "data returned from Python workers",
      "number of output rows",
      "time to initialize Python workers",
      "time to start Python workers",
      "time to run Python workers")

    val df = base.groupBy(pythonTestUDF(base("a") + 1))
      .agg(pythonTestUDF(pythonTestUDF(base("a") + 1)))
    df.count()

    val statusStore = spark.sharedState.statusStore
    val lastExecId = statusStore.executionsList().last.executionId
    val executionMetrics = statusStore.execution(lastExecId).get.metrics.mkString
    for (metric <- pythonSQLMetrics) {
      assert(executionMetrics.contains(metric))
    }
  }

  test("PythonUDAF pretty name") {
    assume(shouldTestPandasUDFs)
    val udfName = "pandas_udf"
    val df = spark.range(1)
    val pandasTestUDF = TestGroupedAggPandasUDF(name = udfName)
    assert(df.agg(pandasTestUDF(df("id"))).schema.fieldNames.exists(_.startsWith(udfName)))
  }

  test("SPARK-48706: Negative test case for Python UDF in higher order functions") {
    assume(shouldTestPythonUDFs)
    checkError(
      exception = intercept[AnalysisException] {
        spark.range(1).select(transform(array("id"), x => pythonTestUDF(x))).collect()
      },
      condition = "UNSUPPORTED_FEATURE.LAMBDA_FUNCTION_WITH_PYTHON_UDF",
      parameters = Map("funcName" -> "\"pyUDF(namedlambdavariable())\""),
      context = ExpectedContext(
        "transform", s".*${this.getClass.getSimpleName}.*"))
  }

  test("SPARK-48666: Python UDF execution against partitioned column") {
    assume(shouldTestPythonUDFs)
    withTable("t") {
      spark.range(1).selectExpr("id AS t", "(id + 1) AS p").write.partitionBy("p").saveAsTable("t")
      val table = spark.table("t")
      val newTable = table.withColumn("new_column", pythonTestUDF(table("p")))
      val df = newTable.as("t1").join(
        newTable.as("t2"), col("t1.new_column") === col("t2.new_column"))
      checkAnswer(df, Row(0, 1, 1, 0, 1, 1))
    }
  }

  test("SPARK-53311: Nondeterministic Python UDF pull out in aggregate with grouping") {
    assume(shouldTestPythonUDFs)

    // nondeterministic UDF
    val pythonUDF = TestPythonUDF(name = "foo", Some(LongType), deterministic = false)

    // This query should work without throwing an analysis exception
    // The UDF foo(value) appears in both grouping expressions and aggregate expressions
    // The fix ensures that both instances are properly mapped to the same attribute
    val df = spark.range(1)
      .selectExpr("id", "id % 3 as value")
      .groupBy(pythonUDF(col("value")))
      .agg(avg("id"), pythonUDF(col("value")))

    checkAnswer(df, Row(0, 0.0, 0))
  }

  test("SPARK-55046: pythonProcessingTime metric is available for Python UDFs") {
    assume(shouldTestPythonUDFs)
    val pythonSQLMetrics = List(
      "data sent to Python workers",
      "data returned from Python workers",
      "number of output rows",
      "time to initialize Python workers",
      "time to start Python workers",
      "time to run Python workers",
      "time to execute Python code")

    val df = base.groupBy(pythonTestUDF(base("a") + 1))
      .agg(pythonTestUDF(pythonTestUDF(base("a") + 1)))
    df.count()

    val statusStore = spark.sharedState.statusStore
    val lastExecId = statusStore.executionsList().last.executionId
    val executionMetrics = statusStore.execution(lastExecId).get.metrics.mkString
    for (metric <- pythonSQLMetrics) {
      assert(executionMetrics.contains(metric),
        s"Expected metric '$metric' not found in execution metrics")
    }
  }

  test(
    "SPARK-55046: pythonProcessingTime reflects actual UDF computation time"
  ) {
    assume(shouldTestPythonUDFs)
    val df = spark.range(10000)
    val result = df.select(pythonTestUDF(col("id")))
    result.collect()

    val pythonExec = result.queryExecution.executedPlan.collectFirst {
      case p: BatchEvalPythonExec => p
    }.getOrElse {
      fail("Expected BatchEvalPythonExec in executed plan")
    }

    val processingTime = pythonExec.metrics.get("pythonProcessingTime").map(_.value).getOrElse(0L)
    val pythonTotalTime = pythonExec.metrics.get("pythonTotalTime").map(_.value).getOrElse(0L)

    // Processing time should be non-zero
    assert(processingTime > 0,
      s"pythonProcessingTime should be > 0, but was $processingTime")

    // Python total time should also be non-zero and >= processing time
    assert(pythonTotalTime > 0 && pythonTotalTime >= processingTime,
      s"pythonTotalTime should be > 0, but was $pythonTotalTime")
  }

  test("SPARK-55046:pythonProcessingTime metric for ArrowEvalPythonExec") {
    assume(shouldTestPythonUDFs)
    withSQLConf(SQLConf.ARROW_PYSPARK_EXECUTION_ENABLED.key -> "true") {
      val df = spark.range(100)
      val result = df.select(pythonTestUDF(col("id")))
      result.collect()

      val arrowExec = result.queryExecution.executedPlan.collectFirst {
        case p: ArrowEvalPythonExec => p
      }

      // If Arrow execution is available, verify the metric, otherwise, skip the test
      arrowExec.foreach { exec =>
        val processingTime = exec.metrics.get("pythonProcessingTime").map(_.value).getOrElse(0L)
        val pythonTotalTime = exec.metrics.get("pythonTotalTime").map(_.value).getOrElse(0L)

        assert(processingTime > 0,
          s"pythonProcessingTime should be > 0 for ArrowEvalPythonExec, but was $processingTime")
        assert(pythonTotalTime > 0 && pythonTotalTime >= processingTime,
          s"pythonTotalTime should be > 0 for ArrowEvalPythonExec, but was $pythonTotalTime")
      }
    }
  }

  test("SPARK-55046: pythonProcessingTime metric for BatchEvalPythonUDTFExec") {
    assume(shouldTestPythonUDFs)
    val udtf = TestPythonUDTF(name = "test_udtf")

    spark.udtf.registerPython(udtf.name, udtf.udtf)
    withTempView("t") {
      try {
        spark.range(1000).selectExpr("id % 100 as a", "id % 50 as b")
          .createOrReplaceTempView("t")
        val result = sql(s"SELECT f.* FROM t, LATERAL ${udtf.name}(a, b) f")
        result.collect()

        val udtfExec = result.queryExecution.executedPlan.collectFirst {
          case p: BatchEvalPythonUDTFExec => p
        }.getOrElse {
          fail("Expected BatchEvalPythonUDTFExec in executed plan")
        }

        // Verify the metric exists and has a positive value
        val processingTime = udtfExec.metrics.get("pythonProcessingTime").map(_.value).getOrElse(0L)
        val pythonTotalTime = udtfExec.metrics.get("pythonTotalTime").map(_.value).getOrElse(0L)

        assert(processingTime > 0,
          s"pythonProcessingTime should be > 0 for BatchEvalPythonUDTFExec," +
            s" but was $processingTime")
        assert(pythonTotalTime > 0 && pythonTotalTime >= processingTime,
          s"pythonTotalTime should be > 0 for BatchEvalPythonUDTFExec," +
            s" but was $pythonTotalTime")
      } finally {
        spark.sessionState.catalog.dropTempFunction(udtf.name, ignoreIfNotExists = true)
      }
    }
  }
}
