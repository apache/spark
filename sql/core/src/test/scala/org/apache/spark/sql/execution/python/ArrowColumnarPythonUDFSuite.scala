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
import org.apache.spark.api.python.PythonEvalType
import org.apache.spark.internal.config.Python.PYTHON_UDF_PIPELINED_EXECUTION
import org.apache.spark.sql.IntegratedUDFTestUtils
import org.apache.spark.sql.execution.{ColumnarToRowExec, SparkPlan, WholeStageCodegenExec}
import org.apache.spark.sql.execution.columnar.InMemoryRelation
import org.apache.spark.sql.execution.vectorized.OnHeapColumnVector
import org.apache.spark.sql.internal.{SQLConf, StaticSQLConf}
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

  // The cache serializer is static. The tests of the columnar input path enable it.
  override protected def sparkConf: SparkConf = super.sparkConf
    .set(StaticSQLConf.SPARK_CACHE_SERIALIZER.key,
      "org.apache.spark.sql.execution.columnar.ArrowCachedBatchSerializer")
    .set(SQLConf.ARROW_PYSPARK_UDF_COLUMNAR_INPUT_ENABLED.key, "true")

  // InMemoryRelation caches the serializer in a process-wide field on first use, so reset it to
  // pick up the Arrow serializer configured above, and again so as not to leak it to later suites.
  override def beforeAll(): Unit = {
    super.beforeAll()
    InMemoryRelation.clearSerializer()
  }

  override def afterAll(): Unit = {
    InMemoryRelation.clearSerializer()
    super.afterAll()
  }

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

  private def singleArrowEvalPythonExec(plan: SparkPlan): ArrowEvalPythonExec = {
    val execs = collectNodes[ArrowEvalPythonExec](plan)
    assert(execs.length == 1, s"Expected one ArrowEvalPythonExec in plan:\n$plan")
    execs.head
  }

  /**
   * Whether `exec` takes the Arrow columnar path, which outputs the input columns as they are,
   * while the other paths output on-heap vectors.
   */
  private def takesArrowColumnarPath(exec: ArrowEvalPythonExec): Boolean = {
    assert(exec.supportsColumnar)
    val columnClasses = exec.executeColumnar().mapPartitionsInternal { batches =>
      batches.map(_.column(0).getClass.getName)
    }.collect().toSet
    assert(columnClasses.size == 1, s"Expected one path for all batches: $columnClasses")
    columnClasses.head != classOf[OnHeapColumnVector].getName
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

  test("Arrow-backed source that reuses its vectors: Arrow columnar path not taken") {
    assume(shouldTestPandasUDFs)
    withSQLConf(SQLConf.ARROW_PYSPARK_EXECUTION_ENABLED.key -> "true") {
      val pandasUDF = TestScalarPandasUDF(
        name = "arrow_reuse_udf", returnType = Some(StringType))
      registerTestUDF(pandasUDF, spark)

      // The Arrow columnar path would keep the pass-through columns of a batch until its result
      // comes back from Python. The reader marks its vectors as reused, since it refills them for
      // the next batch and closes them once it is closed, as soon as it is exhausted, so the
      // batches take the row queue path. With two batches, the reader is closed before their
      // results come back. With many, it refills its vectors with the next batches before then.
      // The test UDF casts its input to string: on a string column, the cast is removed, so the
      // inputs are column references.
      Seq(200, 200000).foreach { numRows =>
        val result = readArrowSource(numRows = numRows, reuseVectors = true, batchSize = 100)
          .selectExpr("id", "name", "value", "data", "arrow_reuse_udf(name) as udf_name")
        val arrowExec = singleArrowEvalPythonExec(result.queryExecution.executedPlan)
        assert(!takesArrowColumnarPath(arrowExec))

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

  test("Arrow-backed source with fresh vectors: held without a copy and released once consumed") {
    assume(shouldTestPandasUDFs)
    withSQLConf(SQLConf.ARROW_PYSPARK_EXECUTION_ENABLED.key -> "true") {
      val pandasUDF = TestScalarPandasUDF(
        name = "arrow_fresh_udf", returnType = Some(StringType))
      registerTestUDF(pandasUDF, spark)

      val numRows = 50000
      val numBatches = 500
      val result = readArrowSource(numRows = numRows, batchSize = numRows / numBatches)
        .selectExpr("id", "name", "value", "data", "arrow_fresh_udf(name) as udf_name")
      // The pass-through columns are output as the reader returned them.
      assert(takesArrowColumnarPath(
        singleArrowEvalPythonExec(result.queryExecution.executedPlan)))

      ArrowBackedDataSourceV2.resetBatchCounters()
      val rows = result.collect()
      assert(rows.map(_.getInt(0)).toSeq == (0 until numRows))
      rows.foreach { row =>
        val id = row.getInt(0)
        assert(row.getString(1) == s"row_$id", s"name mismatch for id $id")
        assert(row.getDouble(2) == id * 0.1, s"value mismatch for id $id")
        assert(row.getString(3).length == 100, s"data mismatch for id $id")
        assert(row.getString(4) == s"row_$id", s"UDF result mismatch for id $id")
      }
      // The consumer of the output releases each input batch once its rows are consumed, before
      // the task completes. How many are held at a time depends on how far the writer runs
      // ahead of the results, which may be all of them for a small input.
      assert(ArrowBackedDataSourceV2.returnedBatches.get == numBatches)
      assert(ArrowBackedDataSourceV2.releasedBatches.get == numBatches)
    }
  }

  test("Arrow cache: the pass-through columns match the UDF results") {
    assume(shouldTestPandasUDFs)
    withSQLConf(
        SQLConf.ARROW_PYSPARK_EXECUTION_ENABLED.key -> "true",
        SQLConf.COLUMN_BATCH_SIZE.key -> "1000") {
      val pandasUDF = TestScalarPandasUDF(
        name = "arrow_cache_udf", returnType = Some(StringType))
      registerTestUDF(pandasUDF, spark)

      val numRows = 2000000L
      val cached = spark.range(0, numRows, 1, 1).selectExpr("id", "cast(id as string) as s")
        .cache()
      try {
        val result = cached.selectExpr("id", "s", "arrow_cache_udf(s) as u")
        // The cache loads every batch into a root of its own and leaves its release to the
        // consumer, so the batches take the Arrow columnar path.
        val arrowExec = singleArrowEvalPythonExec(result.queryExecution.executedPlan)
        assert(arrowExec.child.supportsColumnar)
        assert(takesArrowColumnarPath(arrowExec))

        val counts = result
          .selectExpr("count(*)", "count_if(s = cast(id as string) and u = s)")
          .head()
        assert(counts.getLong(0) == numRows)
        assert(counts.getLong(1) == numRows)
      } finally {
        cached.unpersist(blocking = true)
      }
    }
  }

  test("Stacked Python UDFs over an Arrow-backed source") {
    assume(shouldTestPythonUDFs && shouldTestPandasUDFs)
    withSQLConf(SQLConf.ARROW_PYSPARK_EXECUTION_ENABLED.key -> "true") {
      // UDFs of different eval types are not chained, so they are evaluated by two stacked
      // ArrowEvalPythonExecs.
      val innerUDF = TestPythonUDF(name = "arrow_inner_udf", returnType = Some(StringType),
        pythonEvalType = PythonEvalType.SQL_ARROW_BATCHED_UDF)
      val outerUDF = TestScalarPandasUDF(name = "arrow_outer_udf", returnType = Some(StringType))
      registerTestUDF(innerUDF, spark)
      registerTestUDF(outerUDF, spark)

      val numRows = 20000
      val result = readArrowSource(numRows = numRows, batchSize = 100)
        .selectExpr("id", "name", "value", "data", "arrow_outer_udf(arrow_inner_udf(name)) as u")
      val plan = result.queryExecution.executedPlan
      val execs = collectNodes[ArrowEvalPythonExec](plan)
      assert(execs.length == 2, s"Expected two ArrowEvalPythonExecs in plan:\n$plan")
      val outerExec = execs.find(_.child.isInstanceOf[ArrowEvalPythonExec]).getOrElse(
        fail(s"Expected stacked ArrowEvalPythonExecs in plan:\n$plan"))
      // The inner one holds the pass-through columns of the reader, while the results of Python
      // are loaded into the same vectors for every batch, so the outer one does not hold them.
      assert(takesArrowColumnarPath(outerExec.child.asInstanceOf[ArrowEvalPythonExec]))
      assert(!takesArrowColumnarPath(outerExec))

      val rows = result.collect()
      assert(rows.map(_.getInt(0)).toSeq == (0 until numRows))
      rows.foreach { row =>
        val id = row.getInt(0)
        assert(row.getString(1) == s"row_$id", s"name mismatch for id $id")
        assert(row.getString(3).length == 100, s"data mismatch for id $id")
        assert(row.getString(4) == s"row_$id", s"UDF result mismatch for id $id")
      }
    }
  }

  test("ColumnarToRow releases each batch once its rows are consumed") {
    Seq(true, false).foreach { codegen =>
      withSQLConf(SQLConf.WHOLESTAGE_CODEGEN_ENABLED.key -> codegen.toString) {
        val numRows = 50000
        val numBatches = 500
        val df = readArrowSource(numRows = numRows, batchSize = numRows / numBatches)
        val plan = df.queryExecution.executedPlan
        assert(collectNodes[ColumnarToRowExec](plan).nonEmpty)
        assert(collectNodes[WholeStageCodegenExec](plan).nonEmpty == codegen)

        ArrowBackedDataSourceV2.resetBatchCounters()
        val rows = df.collect()
        assert(rows.map(_.getInt(0)).toSeq == (0 until numRows))
        assert(rows.forall(row => row.getString(1) == s"row_${row.getInt(0)}"))
        // A batch is released before the next one is read.
        assert(ArrowBackedDataSourceV2.returnedBatches.get == numBatches)
        assert(ArrowBackedDataSourceV2.releasedBatches.get == numBatches)
        assert(ArrowBackedDataSourceV2.maxUnreleasedBatches.get == 1)
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

/** Runs the tests of [[ArrowColumnarPythonUDFSuite]] with pipelined Python UDF execution. */
class ArrowColumnarPythonUDFPipelinedSuite extends ArrowColumnarPythonUDFSuite {
  override protected def sparkConf: SparkConf =
    super.sparkConf.set(PYTHON_UDF_PIPELINED_EXECUTION.key, "true")
}
