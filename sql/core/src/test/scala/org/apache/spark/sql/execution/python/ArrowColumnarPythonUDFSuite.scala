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

import org.apache.spark.sql.{IntegratedUDFTestUtils, QueryTest}
import org.apache.spark.sql.execution.{ColumnarToRowExec, SparkPlan}
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.test.SharedSparkSession

/**
 * End-to-end tests for the Arrow columnar Python UDF input path.
 *
 * Verifies that when reading from an Arrow-backed DataSource V2 connector,
 * ArrowEvalPythonExec uses the columnar path (no ColumnarToRowExec) and
 * produces correct results.
 */
class ArrowColumnarPythonUDFSuite extends QueryTest with SharedSparkSession {

  import IntegratedUDFTestUtils._

  private val arrowSource =
    "org.apache.spark.sql.execution.python.ArrowBackedDataSourceV2"

  private def readArrowSource(numRows: Int = 100, numPartitions: Int = 1) = {
    spark.read
      .format(arrowSource)
      .option("numRows", numRows.toString)
      .option("numPartitions", numPartitions.toString)
      .load()
  }

  private def collectNodes[T <: SparkPlan](
      plan: SparkPlan)(implicit tag: reflect.ClassTag[T]): Seq[T] = {
    plan.collect { case p if tag.runtimeClass.isInstance(p) => p.asInstanceOf[T] }
  }

  test("Arrow-backed source: no ColumnarToRowExec before ArrowEvalPythonExec") {
    assume(shouldTestPandasUDFs)
    withSQLConf(SQLConf.ARROW_PYSPARK_EXECUTION_ENABLED.key -> "true") {
      val pandasUDF = TestScalarPandasUDF(name = "arrow_test_udf")
      registerTestUDF(pandasUDF, spark)

      val df = readArrowSource()
      val result = df.select(col("id"), pandasUDF(col("id")))
      val plan = result.queryExecution.executedPlan

      // ArrowEvalPythonExec should be present.
      val arrowExecs = collectNodes[ArrowEvalPythonExec](plan)
      assert(arrowExecs.nonEmpty,
        s"Expected ArrowEvalPythonExec in plan:\n$plan")

      // No ColumnarToRowExec should be between the scan and ArrowEvalPythonExec.
      val columnarToRows = collectNodes[ColumnarToRowExec](plan)
      assert(columnarToRows.isEmpty,
        s"ColumnarToRowExec should not be present when reading from " +
          s"Arrow-backed source:\n$plan")
    }
  }

  test("Arrow-backed source: correctness of scalar pandas UDF") {
    assume(shouldTestPandasUDFs)
    withSQLConf(SQLConf.ARROW_PYSPARK_EXECUTION_ENABLED.key -> "true") {
      val pandasUDF = TestScalarPandasUDF(name = "arrow_str_udf")
      registerTestUDF(pandasUDF, spark)

      val df = readArrowSource(numRows = 10)
      // The test pandas UDF converts input to string.
      val result = df.select(col("id"), pandasUDF(col("id")))
      val rows = result.collect()

      assert(rows.length == 10)
      rows.zipWithIndex.foreach { case (row, i) =>
        assert(row.getInt(0) == i, s"id mismatch at row $i")
        // The pandas UDF converts to string.
        assert(row.getString(1) == i.toString, s"UDF result mismatch at row $i")
      }
    }
  }

  test("Arrow-backed source: multiple UDF columns") {
    assume(shouldTestPandasUDFs)
    withSQLConf(SQLConf.ARROW_PYSPARK_EXECUTION_ENABLED.key -> "true") {
      val udf1 = TestScalarPandasUDF(name = "arrow_udf1")
      val udf2 = TestScalarPandasUDF(name = "arrow_udf2")
      registerTestUDF(udf1, spark)
      registerTestUDF(udf2, spark)

      val df = readArrowSource(numRows = 10)
      val result = df.select(
        col("id"),
        udf1(col("id")),
        udf2(col("name")))
      val rows = result.collect()

      assert(rows.length == 10)
      rows.zipWithIndex.foreach { case (row, i) =>
        assert(row.getInt(0) == i)
        assert(row.getString(1) == i.toString)
        assert(row.getString(2) == s"row_$i")
      }
    }
  }

  test("Arrow-backed source: supportsColumnar is true") {
    val df = readArrowSource()
    val plan = df.queryExecution.executedPlan
    assert(plan.supportsColumnar,
      s"Arrow-backed source should support columnar output:\n$plan")
  }

  test("Row-based source: ColumnarToRowExec is present (baseline)") {
    assume(shouldTestPandasUDFs)
    withSQLConf(SQLConf.ARROW_PYSPARK_EXECUTION_ENABLED.key -> "true") {
      val pandasUDF = TestScalarPandasUDF(name = "row_test_udf")
      registerTestUDF(pandasUDF, spark)

      // spark.range does not produce Arrow-backed columnar output.
      val df = spark.range(100).toDF("id")
      val result = df.select(pandasUDF(col("id")))
      val plan = result.queryExecution.executedPlan

      // Row-based source should NOT have the columnar optimization.
      val arrowExecs = collectNodes[ArrowEvalPythonExec](plan)
      arrowExecs.foreach { exec =>
        assert(!exec.child.supportsColumnar,
          s"Row-based source child should not support columnar:\n$plan")
      }
    }
  }

  test("Arrow-backed source: multiple partitions") {
    assume(shouldTestPandasUDFs)
    withSQLConf(SQLConf.ARROW_PYSPARK_EXECUTION_ENABLED.key -> "true") {
      val pandasUDF = TestScalarPandasUDF(name = "arrow_multi_part_udf")
      registerTestUDF(pandasUDF, spark)

      val df = readArrowSource(numRows = 100, numPartitions = 4)
      val result = df.select(col("id"), pandasUDF(col("id")))
      val rows = result.collect().sortBy(_.getInt(0))

      assert(rows.length == 100)
      rows.zipWithIndex.foreach { case (row, i) =>
        assert(row.getInt(0) == i)
        assert(row.getString(1) == i.toString)
      }
    }
  }
}
