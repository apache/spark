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

import org.apache.spark.SparkConf
import org.apache.spark.sql.IntegratedUDFTestUtils
import org.apache.spark.sql.execution.SparkPlan
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.test.SharedSparkSession
import org.apache.spark.sql.types.StringType

/**
 * End-to-end tests for the Arrow columnar Python UDF input path.
 *
 * Verifies that when reading from an Arrow-backed DataSource V2 connector,
 * ArrowEvalPythonExec uses the columnar path (no ColumnarToRowExec) and
 * produces correct results.
 */
class ArrowColumnarPythonUDFSuite extends SharedSparkSession {

  import IntegratedUDFTestUtils._

  // The tests of the columnar input path enable it, since it is disabled by default.
  override protected def sparkConf: SparkConf =
    super.sparkConf.set(SQLConf.ARROW_PYSPARK_UDF_COLUMNAR_INPUT_ENABLED.key, "true")

  private val arrowSource =
    "org.apache.spark.sql.execution.python.ArrowBackedDataSourceV2"

  private def readArrowSource(
      numRows: Int = 100,
      numPartitions: Int = 1,
      reuseVectors: Boolean = false,
      batchSize: Int = ArrowBackedDataSourceV2.BATCH_SIZE) = {
    spark.read
      .format(arrowSource)
      .option("numRows", numRows.toString)
      .option("numPartitions", numPartitions.toString)
      .option("reuseVectors", reuseVectors.toString)
      .option("batchSize", batchSize.toString)
      .load()
  }

  private def collectNodes[T <: SparkPlan](
      plan: SparkPlan)(implicit tag: reflect.ClassTag[T]): Seq[T] = {
    plan.collect {
      case p if tag.runtimeClass.isInstance(p) => p.asInstanceOf[T]
    }
  }

  test("Arrow-backed source: no ColumnarToRowExec before ArrowEvalPythonExec") {
    assume(shouldTestPandasUDFs)
    withSQLConf(SQLConf.ARROW_PYSPARK_EXECUTION_ENABLED.key -> "true") {
      val pandasUDF = TestScalarPandasUDF(
        name = "arrow_test_udf", returnType = Some(StringType))
      registerTestUDF(pandasUDF, spark)

      val df = readArrowSource()
      // Select all columns to avoid column pruning inserting a
      // ProjectExec (which does not support columnar) between the
      // scan and ArrowEvalPythonExec.
      val result = df.selectExpr(
        "id", "name", "value", "data",
        "arrow_test_udf(id) as udf_id")
      val plan = result.queryExecution.executedPlan

      // ArrowEvalPythonExec should be present.
      val arrowExecs = collectNodes[ArrowEvalPythonExec](plan)
      assert(arrowExecs.nonEmpty,
        s"Expected ArrowEvalPythonExec in plan:\n$plan")

      // The direct child of ArrowEvalPythonExec should support
      // columnar (the scan), with no ColumnarToRowExec in between.
      val arrowExec = arrowExecs.head
      assert(arrowExec.child.supportsColumnar,
        "ArrowEvalPythonExec child should support columnar " +
          s"when reading from Arrow-backed source:\n$plan")
    }
  }

  test("Arrow-backed source: correctness of scalar pandas UDF") {
    assume(shouldTestPandasUDFs)
    withSQLConf(SQLConf.ARROW_PYSPARK_EXECUTION_ENABLED.key -> "true") {
      val pandasUDF = TestScalarPandasUDF(
        name = "arrow_str_udf", returnType = Some(StringType))
      registerTestUDF(pandasUDF, spark)

      val df = readArrowSource(numRows = 10)
      // The test pandas UDF converts input to string.
      val result = df.selectExpr("id", "arrow_str_udf(id) as udf_id")
      val rows = result.collect()

      assert(rows.length == 10)
      rows.zipWithIndex.foreach { case (row, i) =>
        assert(row.getInt(0) == i, s"id mismatch at row $i")
        assert(row.getString(1) == i.toString,
          s"UDF result mismatch at row $i")
      }
    }
  }

  test("Arrow-backed source that reuses its vectors: correct with the default configuration") {
    assume(shouldTestPandasUDFs)
    // The Arrow columnar path keeps the pass-through columns of a batch until its result comes
    // back from Python, which is only correct if the batch stays valid until then. A reader may
    // refill its vectors for the next batch, or release them once it is closed, as soon as it is
    // exhausted, so columnar input is disabled by default.
    withSQLConf(
        SQLConf.ARROW_PYSPARK_EXECUTION_ENABLED.key -> "true",
        SQLConf.ARROW_PYSPARK_UDF_COLUMNAR_INPUT_ENABLED.key ->
          SQLConf.ARROW_PYSPARK_UDF_COLUMNAR_INPUT_ENABLED.defaultValueString) {
      val pandasUDF = TestScalarPandasUDF(
        name = "arrow_reuse_udf", returnType = Some(StringType))
      registerTestUDF(pandasUDF, spark)

      // Spark writes batches to Python before the results of the earlier ones come back. With
      // two batches, the reader is closed once it is exhausted, before their results come back.
      // With many, the reader refills its vectors with the next batches before then. The test
      // UDF casts its input to string: on a string column, the cast is removed, so that the
      // batches would take the Arrow columnar path if columnar input were enabled.
      Seq(200, 200000).foreach { numRows =>
        val result = readArrowSource(numRows = numRows, reuseVectors = true, batchSize = 100)
          .selectExpr("id", "name", "value", "data", "arrow_reuse_udf(name) as udf_name")
        val arrowExec = collectNodes[ArrowEvalPythonExec](result.queryExecution.executedPlan)
        assert(arrowExec.nonEmpty && !arrowExec.head.supportsColumnar)

        val rows = result.collect()
        assert(rows.map(_.getInt(0)).toSeq == (0 until numRows))
        rows.foreach { row =>
          val id = row.getInt(0)
          assert(row.getString(1) == s"row_$id", s"name mismatch for id $id")
          assert(row.getString(3) == s"data_$id", s"data mismatch for id $id")
          assert(row.getString(4) == s"row_$id", s"UDF result mismatch for id $id")
        }
      }
    }
  }

  test("Arrow-backed source: multiple UDF columns") {
    assume(shouldTestPandasUDFs)
    withSQLConf(SQLConf.ARROW_PYSPARK_EXECUTION_ENABLED.key -> "true") {
      val udf1 = TestScalarPandasUDF(
        name = "arrow_udf1", returnType = Some(StringType))
      val udf2 = TestScalarPandasUDF(
        name = "arrow_udf2", returnType = Some(StringType))
      registerTestUDF(udf1, spark)
      registerTestUDF(udf2, spark)

      val df = readArrowSource(numRows = 10)
      val result = df.selectExpr(
        "id", "arrow_udf1(id) as u1", "arrow_udf2(name) as u2")
      val rows = result.collect()

      assert(rows.length == 10)
      rows.zipWithIndex.foreach { case (row, i) =>
        assert(row.getInt(0) == i)
        assert(row.getString(1) == i.toString)
        assert(row.getString(2) == s"row_$i")
      }
    }
  }

  test("Arrow-backed source: scan supportsColumnar is true") {
    val df = readArrowSource()
    val plan = df.queryExecution.executedPlan
    // The top-level plan may be ColumnarToRowExec (for Spark's row output),
    // but the scan underneath should support columnar.
    val scans = plan.collect {
      case p if p.supportsColumnar => p
    }
    assert(scans.nonEmpty,
      s"Arrow-backed source should have a columnar-capable scan:\n$plan")
  }

  test("Row-based source: child does not support columnar (baseline)") {
    assume(shouldTestPandasUDFs)
    withSQLConf(SQLConf.ARROW_PYSPARK_EXECUTION_ENABLED.key -> "true") {
      val pandasUDF = TestScalarPandasUDF(
        name = "row_test_udf", returnType = Some(StringType))
      registerTestUDF(pandasUDF, spark)

      // spark.range does not produce Arrow-backed columnar output.
      val df = spark.range(100).toDF("id")
      val result = df.selectExpr("row_test_udf(id)")
      val plan = result.queryExecution.executedPlan

      val arrowExecs = collectNodes[ArrowEvalPythonExec](plan)
      arrowExecs.foreach { exec =>
        assert(!exec.child.supportsColumnar,
          "Row-based source child should not support columnar:" +
            s"\n$plan")
      }
    }
  }

  test("Arrow-backed source: multiple partitions") {
    assume(shouldTestPandasUDFs)
    withSQLConf(SQLConf.ARROW_PYSPARK_EXECUTION_ENABLED.key -> "true") {
      val pandasUDF = TestScalarPandasUDF(
        name = "arrow_mp_udf", returnType = Some(StringType))
      registerTestUDF(pandasUDF, spark)

      val df = readArrowSource(numRows = 100, numPartitions = 4)
      val result = df.selectExpr("id", "arrow_mp_udf(id) as udf_id")
      val rows = result.collect().sortBy(_.getInt(0))

      assert(rows.length == 100)
      rows.zipWithIndex.foreach { case (row, i) =>
        assert(row.getInt(0) == i)
        assert(row.getString(1) == i.toString)
      }
    }
  }
}
