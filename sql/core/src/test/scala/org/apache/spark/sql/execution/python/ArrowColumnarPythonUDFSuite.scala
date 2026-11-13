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
import org.apache.spark.api.python.{ChainedPythonFunctions, PythonEvalType}
import org.apache.spark.internal.config.Python.PYTHON_UDF_PIPELINED_EXECUTION
import org.apache.spark.sql.{DataFrame, IntegratedUDFTestUtils, Row}
import org.apache.spark.sql.catalyst.expressions.{AttributeReference, UnsafeRow}
import org.apache.spark.sql.execution.{ColumnarToRowExec, ReusedSubqueryExec, SparkPlan, SQLExecution,
  WholeStageCodegenExec}
import org.apache.spark.sql.execution.vectorized.ReusedArrowColumnVector
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.test.SharedSparkSession
import org.apache.spark.sql.types.{StringType, StructType}

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
      batchSize: Int = ArrowBackedDataSourceV2.BATCH_SIZE,
      arrowIdColumn: Boolean = true,
      emptyBatches: Boolean = false,
      pruneColumns: Boolean = false) = {
    spark.read
      .format(arrowSource)
      .option("numRows", numRows.toString)
      .option("numPartitions", numPartitions.toString)
      .option("reuseVectors", reuseVectors.toString)
      .option("batchSize", batchSize.toString)
      .option("arrowIdColumn", arrowIdColumn.toString)
      .option("emptyBatches", emptyBatches.toString)
      .option("pruneColumns", pruneColumns.toString)
      .load()
  }

  private def singleArrowEvalPythonExec(plan: SparkPlan): ArrowEvalPythonExec = {
    val execs = collectNodes[ArrowEvalPythonExec](plan)
    assert(execs.length == 1, s"Expected one ArrowEvalPythonExec in plan:\n$plan")
    execs.head
  }

  /** The numbers of rows of the output batches of `exec`, in order. */
  private def outputBatchSizes(exec: ArrowEvalPythonExec): Seq[Int] = {
    assert(exec.supportsColumnar)
    // The evaluator is created in the tasks, which read the session configurations propagated.
    SQLExecution.withSQLConfPropagated(spark) {
      exec.executeColumnar().mapPartitionsInternal(_.map(_.numRows())).collect().toSeq
    }
  }

  /** Whether a ColumnarToRowExec converts the output of an ArrowEvalPythonExec. */
  private def hasColumnarToRowAbove(plan: SparkPlan): Boolean =
    collectNodes[ColumnarToRowExec](plan).exists(
      _.find(_.isInstanceOf[ArrowEvalPythonExec]).isDefined)

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

  test("SPARK-60325: Arrow-backed source that reuses its vectors: correct results") {
    assume(shouldTestPandasUDFs)
    // The results of a batch come back from Python after the reader refills its vectors for the
    // next batch, and closes them once it is exhausted. With columnar input, the pass-through
    // columns are copied before that, while the default reads the scan as rows. With two batches,
    // the reader is closed before their results come back. With many, it refills its vectors with
    // the next batches before then. The test UDF casts its input to string: on a string column,
    // the cast is removed, so the inputs are column references.
    Seq(true, false).foreach { columnarInput =>
      withSQLConf(
          SQLConf.ARROW_PYSPARK_EXECUTION_ENABLED.key -> "true",
          SQLConf.ARROW_PYSPARK_UDF_COLUMNAR_INPUT_ENABLED.key -> columnarInput.toString) {
        val pandasUDF = TestScalarPandasUDF(
          name = "arrow_reuse_udf", returnType = Some(StringType))
        registerTestUDF(pandasUDF, spark)

        Seq(200, 20000).foreach { numRows =>
          val result = readArrowSource(numRows = numRows, reuseVectors = true, batchSize = 100)
            .selectExpr("id", "name", "value", "data", "arrow_reuse_udf(name) as udf_name")
          val arrowExec = singleArrowEvalPythonExec(result.queryExecution.executedPlan)
          assert(arrowExec.child.supportsColumnar == columnarInput)

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
  }

  test("SPARK-60325: The results of Python are sliced with columnar input") {
    assume(shouldTestPandasUDFs)
    // The results are joined with the pass-through columns by row, so that they can be sliced.
    Seq(
      SQLConf.ARROW_EXECUTION_MAX_RECORDS_PER_OUTPUT_BATCH.key,
      SQLConf.ARROW_EXECUTION_MAX_BYTES_PER_OUTPUT_BATCH.key).foreach { key =>
      withSQLConf(SQLConf.ARROW_PYSPARK_EXECUTION_ENABLED.key -> "true", key -> "3") {
        val pandasUDF = TestScalarPandasUDF(
          name = "arrow_slice_udf", returnType = Some(StringType))
        registerTestUDF(pandasUDF, spark)

        val numRows = 1000
        val result = readArrowSource(numRows = numRows, batchSize = 100)
          .selectExpr("id", "name", "value", "data", "arrow_slice_udf(name) as udf_name")
        val arrowExec = singleArrowEvalPythonExec(result.queryExecution.executedPlan)
        assert(arrowExec.supportsColumnar)

        // How the results are sliced is not visible in the output, so the limits of the runner
        // are checked.
        class LimitsRunner extends ColumnarArrowPythonWithNamedArgumentRunner(
            Seq((ChainedPythonFunctions(Seq(arrowExec.udfs.head.func)), 0L)),
            arrowExec.evalType, Array(Array.empty[EvalPythonExec.ArgumentMetadata]),
            new StructType(), "UTC", false, Map.empty, Map.empty, None, None, Array.empty[Int]) {
          def outputBatchLimits: (Int, Int) =
            (arrowMaxRecordsPerOutputBatch, arrowMaxBytesPerOutputBatch)
        }
        val (maxRecords, maxBytes) = new LimitsRunner().outputBatchLimits
        assert(maxRecords == 3 || maxBytes == 3)

        val rows = result.collect()
        assert(rows.map(_.getInt(0)).toSeq == (0 until numRows))
        assert(rows.forall(row => row.getString(4) == s"row_${row.getInt(0)}"))
      }
    }
  }

  test("SPARK-60325: The output batches of columnar input follow the limits of Arrow batches") {
    assume(shouldTestPandasUDFs)
    val pandasUDF = TestScalarPandasUDF(name = "arrow_limit_udf", returnType = Some(StringType))
    registerTestUDF(pandasUDF, spark)

    val numRows = 5000
    def outputBatches(): (ArrowEvalPythonExec, DataFrame) = {
      val result = readArrowSource(numRows = numRows, batchSize = 500)
        .selectExpr("id", "name", "value", "data", "arrow_limit_udf(name) as udf_name")
      (singleArrowEvalPythonExec(result.queryExecution.executedPlan), result)
    }
    def checkRows(result: DataFrame): Unit = {
      val rows = result.collect()
      assert(rows.map(_.getInt(0)).toSeq == (0 until numRows))
      assert(rows.forall(row => row.getString(4) == s"row_${row.getInt(0)}"))
    }

    withSQLConf(SQLConf.ARROW_EXECUTION_MAX_RECORDS_PER_BATCH.key -> "700") {
      val (arrowExec, result) = outputBatches()
      assert(outputBatchSizes(arrowExec) == Seq.fill(7)(700) :+ 100)
      checkRows(result)
    }

    // No limit on the rows of a batch.
    Seq("0", "-1").foreach { maxRecords =>
      withSQLConf(SQLConf.ARROW_EXECUTION_MAX_RECORDS_PER_BATCH.key -> maxRecords) {
        val (arrowExec, result) = outputBatches()
        assert(outputBatchSizes(arrowExec) == Seq(numRows))
        checkRows(result)
      }
    }

    // Every row has more than 100 bytes, in the `data` column alone.
    val maxBytes = 4096
    withSQLConf(SQLConf.ARROW_EXECUTION_MAX_BYTES_PER_BATCH.key -> maxBytes.toString) {
      val (arrowExec, result) = outputBatches()
      val batchSizes = outputBatchSizes(arrowExec)
      assert(batchSizes.sum == numRows)
      assert(batchSizes.forall(size => size > 0 && size <= maxBytes / 100 + 1), batchSizes)
      checkRows(result)
    }
  }

  test("SPARK-60325: Input batches without rows are released and skipped") {
    assume(shouldTestPandasUDFs)
    withSQLConf(SQLConf.ARROW_PYSPARK_EXECUTION_ENABLED.key -> "true") {
      val pandasUDF = TestScalarPandasUDF(
        name = "arrow_empty_udf", returnType = Some(StringType))
      registerTestUDF(pandasUDF, spark)

      val numRows = 2000
      val numBatches = 20
      // Returns the data sent to Python for the batches of the reader, with and without the
      // batches without rows that it can also return, first, in the middle and last.
      def dataSent(input: String, emptyBatches: Boolean): Long = {
        val result = readArrowSource(numRows = numRows, batchSize = numRows / numBatches,
            emptyBatches = emptyBatches)
          .selectExpr("id", "name", "value", "data", s"arrow_empty_udf($input) as udf_name")
        val arrowExec = singleArrowEvalPythonExec(result.queryExecution.executedPlan)
        val numEmpty = if (emptyBatches) ArrowBackedDataSourceV2.NUM_EMPTY_BATCHES else 0

        ArrowBackedDataSourceV2.resetBatchCounters()
        val rows = result.collect()
        assert(rows.map(_.getInt(0)).toSeq == (0 until numRows))
        assert(ArrowBackedDataSourceV2.returnedBatches.get == numBatches + numEmpty)
        assert(ArrowBackedDataSourceV2.releasedBatches.get == numBatches + numEmpty)
        arrowExec.metrics("pythonDataSent").value
      }

      // The UDF inputs are column references, or an expression on the row fallback path.
      Seq("name", "concat(name, '_')").foreach { input =>
        assert(dataSent(input, emptyBatches = true) == dataSent(input, emptyBatches = false))
      }
    }
  }

  test("SPARK-60325: Stacked Python UDFs over an Arrow-backed source") {
    assume(shouldTestPythonUDFs && shouldTestPandasUDFs)
    withSQLConf(SQLConf.ARROW_PYSPARK_EXECUTION_ENABLED.key -> "true") {
      // UDFs of different eval types are not chained, so they are evaluated by two stacked
      // ArrowEvalPythonExecs.
      val innerUDF = TestPythonUDF(name = "arrow_inner_udf", returnType = Some(StringType),
        pythonEvalType = PythonEvalType.SQL_ARROW_BATCHED_UDF)
      val outerUDF = TestScalarPandasUDF(name = "arrow_outer_udf", returnType = Some(StringType))
      registerTestUDF(innerUDF, spark)
      registerTestUDF(outerUDF, spark)

      val numRows = 2000
      val result = readArrowSource(numRows = numRows, batchSize = 100)
        .selectExpr("id", "name", "value", "data", "arrow_outer_udf(arrow_inner_udf(name)) as u")
      val plan = result.queryExecution.executedPlan
      val execs = collectNodes[ArrowEvalPythonExec](plan)
      assert(execs.length == 2, s"Expected two ArrowEvalPythonExecs in plan:\n$plan")
      val outerExec = execs.find(_.child.isInstanceOf[ArrowEvalPythonExec]).getOrElse(
        fail(s"Expected stacked ArrowEvalPythonExecs in plan:\n$plan"))
      // The results of Python are loaded into the same vectors for every batch, so the outer one
      // must copy the columns that it passes through.
      assert(outerExec.supportsColumnar && outerExec.child.supportsColumnar)

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

  test("SPARK-60306: ColumnarToRow releases each batch once its rows are consumed") {
    Seq(true, false).foreach { codegen =>
      withSQLConf(SQLConf.WHOLESTAGE_CODEGEN_ENABLED.key -> codegen.toString) {
        val numRows = 2000
        val numBatches = 20
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

  test("SPARK-60306: Arrow Python UDF over many batches releases each input batch") {
    assume(shouldTestPandasUDFs)
    val pandasUDF = TestScalarPandasUDF(
      name = "arrow_release_udf", returnType = Some(StringType))
    registerTestUDF(pandasUDF, spark)

    val numRows = 20000
    val numBatches = 20
    // On a string column, the cast of the test UDF is removed, so the UDF inputs are column
    // references. If the `id` column is not an ArrowColumnVector, the batch is written to Python
    // row by row, and on an expression, the row fallback path is taken. All of them release each
    // batch once it has been written to Python.
    for {
      codegen <- Seq(true, false)
      (input, suffix, arrowIdColumn) <- Seq(
        ("name", "", true), ("name", "", false), ("concat(name, '_')", "_", true))
    } withSQLConf(
        SQLConf.ARROW_PYSPARK_EXECUTION_ENABLED.key -> "true",
        SQLConf.WHOLESTAGE_CODEGEN_ENABLED.key -> codegen.toString) {
        val result = readArrowSource(numRows = numRows, batchSize = numRows / numBatches,
            arrowIdColumn = arrowIdColumn)
          .selectExpr("id", "name", "value", "data", s"arrow_release_udf($input) as udf_out")
        val plan = result.queryExecution.executedPlan
        val arrowExec = collectNodes[ArrowEvalPythonExec](plan)
        assert(arrowExec.nonEmpty && arrowExec.head.supportsColumnar)
        assert(arrowExec.head.udfs.forall(_.children.forall(_.isInstanceOf[AttributeReference]))
          == suffix.isEmpty, s"Unexpected UDF inputs:\n$plan")
        assert(!hasColumnarToRowAbove(plan), s"Unexpected ColumnarToRowExec:\n$plan")
        ArrowBackedDataSourceV2.resetBatchCounters()
        val rows = result.collect()
        assert(rows.map(_.getInt(0)).toSeq == (0 until numRows))
        rows.foreach { row =>
          val id = row.getInt(0)
          assert(row.getString(1) == s"row_$id", s"name mismatch for id $id")
          assert(row.getDouble(2) == id * 0.1, s"value mismatch for id $id")
          assert(row.getString(3).length == 100, s"data mismatch for id $id")
          assert(row.getString(4) == s"row_$id$suffix", s"UDF result mismatch for id $id")
        }
        // A batch is released before the next one is read.
        assert(ArrowBackedDataSourceV2.returnedBatches.get == numBatches)
        assert(ArrowBackedDataSourceV2.releasedBatches.get == numBatches)
        assert(ArrowBackedDataSourceV2.maxUnreleasedBatches.get == 1)

        // A limit stops consuming the output batches early, possibly before the input batches
        // that have been read are all released: the source closes them when the task completes,
        // and closing its allocator then fails the task if any of their buffers is referenced.
        val limited = result.limit(10)
        assert(collectNodes[ArrowEvalPythonExec](limited.queryExecution.executedPlan).nonEmpty)
        assert(limited.collect().map(_.getString(4)).toSeq ==
          (0 until 10).map(id => s"row_$id$suffix"))
      }
  }

  test("SPARK-60306: A plan built with columnar input runs as built after the configuration " +
      "changes") {
    assume(shouldTestPandasUDFs)
    withSQLConf(SQLConf.ARROW_PYSPARK_EXECUTION_ENABLED.key -> "true") {
      val pandasUDF = TestScalarPandasUDF(
        name = "arrow_replan_udf", returnType = Some(StringType))
      registerTestUDF(pandasUDF, spark)

      // The physical plan is built with columnar input over the columnar-only scan, and first
      // executed after columnar input is disabled. The row-based parent calls `doExecute` of the
      // evaluation, which must still read the batches of the scan.
      val numRows = 100
      val result = readArrowSource(numRows = numRows)
        .selectExpr("id", "name", "value", "data", "arrow_replan_udf(name) as udf_name")
      val plan = result.queryExecution.executedPlan
      val arrowExec = singleArrowEvalPythonExec(plan)
      assert(arrowExec.supportsColumnar && arrowExec.supportsRowBased)
      assert(!hasColumnarToRowAbove(plan), s"Unexpected ColumnarToRowExec:\n$plan")
      withSQLConf(SQLConf.ARROW_PYSPARK_UDF_COLUMNAR_INPUT_ENABLED.key -> "false") {
        assert(singleArrowEvalPythonExec(result.queryExecution.executedPlan).supportsColumnar)
        val rows = result.collect()
        assert(rows.map(_.getInt(0)).toSeq == (0 until numRows))
        assert(rows.forall(row => row.getString(4) == s"row_${row.getInt(0)}"))
      }
    }
  }

  test("SPARK-61516: An evaluation rebuilt after the transitions runs as built after the " +
      "configuration changes") {
    assume(shouldTestPandasUDFs)
    val pandasUDF = TestScalarPandasUDF(name = "arrow_rebuilt_udf", returnType = Some(StringType))
    registerTestUDF(pandasUDF, spark)

    withTempView("t_max") {
      spark.range(1).selectExpr("cast(id as string) as s").createOrReplaceTempView("t_max")
      // Without adaptive execution, the reuse of subqueries runs after the transitions are
      // inserted, and rebuilds the evaluation, whose UDF arguments have the same scalar subquery
      // twice, to reuse it. The rebuilt evaluation is executed after columnar input is disabled,
      // and must still read the batches of the columnar-only scan.
      withSQLConf(
          SQLConf.ARROW_PYSPARK_EXECUTION_ENABLED.key -> "true",
          SQLConf.ADAPTIVE_EXECUTION_ENABLED.key -> "false") {
        val numRows = 100
        val subquery = "(SELECT max(s) FROM t_max)"
        val result = readArrowSource(numRows = numRows).selectExpr("id", "name", "value", "data",
          s"arrow_rebuilt_udf(concat(name, $subquery)) as a",
          s"arrow_rebuilt_udf(concat(data, $subquery)) as b")
        val plan = result.queryExecution.executedPlan
        val arrowExec = singleArrowEvalPythonExec(plan)
        assert(arrowExec.subqueries.exists(_.isInstanceOf[ReusedSubqueryExec]),
          s"Expected a reused subquery in the evaluation:\n$plan")
        // Checked through the plan only, so that the executed evaluation is not asked anything
        // before the configuration changes.
        assert(!arrowExec.child.exists(_.isInstanceOf[ColumnarToRowExec]))
        assert(!hasColumnarToRowAbove(plan), s"Unexpected ColumnarToRowExec:\n$plan")
        withSQLConf(SQLConf.ARROW_PYSPARK_UDF_COLUMNAR_INPUT_ENABLED.key -> "false") {
          val rows = result.collect()
          assert(rows.map(_.getInt(0)).toSeq == (0 until numRows))
          rows.foreach { row =>
            val id = row.getInt(0)
            assert(row.getString(4) == s"row_${id}0", s"UDF result mismatch for id $id")
            assert(row.getString(5).length == 101, s"UDF result mismatch for id $id")
          }
        }
      }
    }
  }

  test("A row-based parent gets the joined rows of an evaluation with columnar input directly") {
    assume(shouldTestPythonUDFs && shouldTestPandasUDFs)
    val pandasUDF = TestScalarPandasUDF(name = "arrow_rows_udf", returnType = Some(StringType))
    val innerUDF = TestPythonUDF(name = "arrow_rows_inner_udf", returnType = Some(StringType),
      pythonEvalType = PythonEvalType.SQL_ARROW_BATCHED_UDF)
    registerTestUDF(pandasUDF, spark)
    registerTestUDF(innerUDF, spark)

    val numRows = 2000
    val numBatches = 20
    val expected = (0 until numRows).map(id => s"row_$id")
    def query(passThrough: Boolean, udf: String): DataFrame = {
      val columns = if (passThrough) Seq("id", s"$udf as u") else Seq(s"$udf as u")
      readArrowSource(numRows = numRows, batchSize = numRows / numBatches, pruneColumns = true)
        .selectExpr(columns: _*)
    }
    def checkRows(passThrough: Boolean, rows: Array[Row], expected: Seq[String]): Unit = {
      if (passThrough) {
        assert(rows.map(_.getString(1)).toSeq == expected)
      } else {
        assert(rows.map(_.getString(0)).toSeq == expected)
      }
    }

    // UDFs of different eval types are evaluated by two stacked evaluations, the inner one with
    // a columnar parent. Without pass-through columns, the topmost evaluation outputs the result
    // batches of Python, which a ColumnarToRowExec reads in the stage of the parent.
    for {
      passThrough <- Seq(true, false)
      (udf, numEvals) <- Seq(
        ("arrow_rows_udf(name)", 1), ("arrow_rows_udf(arrow_rows_inner_udf(name))", 2))
    } withSQLConf(SQLConf.ARROW_PYSPARK_EXECUTION_ENABLED.key -> "true") {
      withClue(s"passThrough=$passThrough, udf=$udf: ") {
        val result = query(passThrough, udf)
        val plan = result.queryExecution.executedPlan
        val execs = collectNodes[ArrowEvalPythonExec](plan)
        assert(execs.length == numEvals, s"Unexpected evaluations:\n$plan")
        assert(execs.forall(_.supportsColumnar))
        assert(execs.forall(_.requiredChildOutput.nonEmpty == passThrough))
        // The topmost evaluation, with the row-based parent.
        val topExec = execs.head
        assert(topExec.supportsRowBased == passThrough)
        assert(hasColumnarToRowAbove(plan) == !passThrough, s"Unexpected plan:\n$plan")
        if (passThrough) {
          assert(collectNodes[ColumnarToRowExec](plan).isEmpty, s"Unexpected plan:\n$plan")
          // The evaluation returns unsafe rows, which a parent may expect.
          val unsafe = SQLExecution.withSQLConfPropagated(spark) {
            topExec.execute().mapPartitionsInternal(_.map(_.isInstanceOf[UnsafeRow])).collect()
          }
          assert(unsafe.length == numRows && unsafe.forall(identity))
        }

        ArrowBackedDataSourceV2.resetBatchCounters()
        val rows = result.collect()
        if (passThrough) assert(rows.map(_.getInt(0)).toSeq == (0 until numRows))
        checkRows(passThrough, rows, expected)
        // A batch is released before the next one is read.
        assert(ArrowBackedDataSourceV2.returnedBatches.get == numBatches)
        assert(ArrowBackedDataSourceV2.releasedBatches.get == numBatches)
        assert(ArrowBackedDataSourceV2.maxUnreleasedBatches.get == 1)

        // A filter and a limit above, which stop reading the row output early. A limit alone
        // would be pushed below the evaluation. The filter is pushed below the projection, so
        // the evaluation under it passes the arguments of the UDF through to the projection,
        // which evaluates it again over the rows that the limit lets through.
        val limited = result.where("u like 'row_1%'").limit(10)
        val limitedPlan = limited.queryExecution.executedPlan
        assert(collectNodes[ArrowEvalPythonExec](limitedPlan).exists { e =>
          e.supportsColumnar && e.supportsRowBased && e.requiredChildOutput.nonEmpty
        }, s"Unexpected plan:\n$limitedPlan")
        assert(!hasColumnarToRowAbove(limitedPlan), s"Unexpected plan:\n$limitedPlan")
        checkRows(passThrough, limited.collect(), expected.filter(_.startsWith("row_1")).take(10))
      }
    }

    // The evaluators are also run through `mapPartitionsWithEvaluator`.
    withSQLConf(
        SQLConf.ARROW_PYSPARK_EXECUTION_ENABLED.key -> "true",
        SQLConf.USE_PARTITION_EVALUATOR.key -> "true") {
      Seq(true, false).foreach { passThrough =>
        checkRows(passThrough, query(passThrough, "arrow_rows_udf(name)").collect(), expected)
      }
    }
  }

  test("Without pass-through columns, the results of Python are the output batches") {
    assume(shouldTestPandasUDFs)
    val pandasUDF = TestScalarPandasUDF(name = "arrow_direct_udf", returnType = Some(StringType))
    registerTestUDF(pandasUDF, spark)

    val numRows = 2000
    val numBatches = 20
    // On a string column, the cast of the test UDF is removed, so the UDF inputs are column
    // references; on an expression, the row fallback path is taken.
    for {
      codegen <- Seq(true, false)
      reuseVectors <- Seq(true, false)
      (input, suffix) <- Seq(("name", ""), ("concat(name, '_')", "_"))
    } withSQLConf(
        SQLConf.ARROW_PYSPARK_EXECUTION_ENABLED.key -> "true",
        SQLConf.WHOLESTAGE_CODEGEN_ENABLED.key -> codegen.toString,
        // The joined rows would be sliced to this, but the results of Python are not.
        SQLConf.ARROW_EXECUTION_MAX_RECORDS_PER_BATCH.key -> "30") {
      withClue(s"codegen=$codegen, reuseVectors=$reuseVectors, input=$input: ") {
        // The scan reads only the arguments, so that it stays columnar under the evaluation.
        val result = readArrowSource(numRows = numRows, batchSize = numRows / numBatches,
            reuseVectors = reuseVectors, pruneColumns = true)
          .selectExpr(s"arrow_direct_udf($input) as u")
        val arrowExec = singleArrowEvalPythonExec(result.queryExecution.executedPlan)
        assert(arrowExec.requiredChildOutput.isEmpty)
        assert(arrowExec.output == arrowExec.resultAttrs)
        assert(arrowExec.child.output.map(_.name) == Seq("name"))
        // A row-based parent reads the result batches through a ColumnarToRowExec.
        assert(arrowExec.supportsColumnar && !arrowExec.supportsRowBased)
        assert(hasColumnarToRowAbove(result.queryExecution.executedPlan))

        // Each input batch has one result batch from Python, over the vectors of its reader.
        val batches = SQLExecution.withSQLConfPropagated(spark) {
          arrowExec.executeColumnar().mapPartitionsInternal(_.map { batch =>
            (batch.numRows(), batch.column(0).isInstanceOf[ReusedArrowColumnVector])
          }).collect().toSeq
        }
        assert(batches == Seq.fill(numBatches)((numRows / numBatches, true)))

        ArrowBackedDataSourceV2.resetBatchCounters()
        assert(result.collect().map(_.getString(0)).toSeq ==
          (0 until numRows).map(id => s"row_$id$suffix"))
        if (!reuseVectors) {
          // A batch is released before the next one is read.
          assert(ArrowBackedDataSourceV2.returnedBatches.get == numBatches)
          assert(ArrowBackedDataSourceV2.releasedBatches.get == numBatches)
          assert(ArrowBackedDataSourceV2.maxUnreleasedBatches.get == 1)
        }

        // A filter and a projection above, and a limit.
        assert(result.where("u like 'row_1%'").selectExpr("concat(u, '!')").collect()
          .map(_.getString(0)).toSet ==
          (0 until numRows).map(id => s"row_$id$suffix").filter(_.startsWith("row_1"))
            .map(_ + "!").toSet)
        assert(result.limit(10).collect().map(_.getString(0)).toSeq ==
          (0 until 10).map(id => s"row_$id$suffix"))
      }
    }
  }

  test("Only the pass-through columns used above are copied with columnar input") {
    assume(shouldTestPandasUDFs)
    withSQLConf(SQLConf.ARROW_PYSPARK_EXECUTION_ENABLED.key -> "true") {
      val pandasUDF = TestScalarPandasUDF(name = "arrow_some_udf", returnType = Some(StringType))
      registerTestUDF(pandasUDF, spark)

      val numRows = 2000
      Seq(true, false).foreach { reuseVectors =>
        val result = readArrowSource(numRows = numRows, batchSize = 100,
            reuseVectors = reuseVectors, pruneColumns = true)
          .selectExpr("id", "arrow_some_udf(name) as u")
        val arrowExec = singleArrowEvalPythonExec(result.queryExecution.executedPlan)
        assert(arrowExec.supportsColumnar)
        assert(arrowExec.requiredChildOutput.map(_.name) == Seq("id"))
        assert(arrowExec.child.output.map(_.name).toSet == Set("id", "name"))

        val rows = result.collect()
        assert(rows.map(_.getInt(0)).toSeq == (0 until numRows))
        assert(rows.forall(row => row.getString(1) == s"row_${row.getInt(0)}"))
      }
    }
  }

  test("Stacked Python UDFs without pass-through columns over an Arrow-backed source") {
    assume(shouldTestPythonUDFs && shouldTestPandasUDFs)
    withSQLConf(SQLConf.ARROW_PYSPARK_EXECUTION_ENABLED.key -> "true") {
      val innerUDF = TestPythonUDF(name = "arrow_inner_direct_udf",
        returnType = Some(StringType), pythonEvalType = PythonEvalType.SQL_ARROW_BATCHED_UDF)
      val outerUDF = TestScalarPandasUDF(
        name = "arrow_outer_direct_udf", returnType = Some(StringType))
      registerTestUDF(innerUDF, spark)
      registerTestUDF(outerUDF, spark)

      val numRows = 2000
      Seq(true, false).foreach { reuseVectors =>
        val result = readArrowSource(numRows = numRows, batchSize = 100,
            reuseVectors = reuseVectors, pruneColumns = true)
          .selectExpr("arrow_outer_direct_udf(arrow_inner_direct_udf(name)) as u")
        val plan = result.queryExecution.executedPlan
        val execs = collectNodes[ArrowEvalPythonExec](plan)
        assert(execs.length == 2, s"Expected two ArrowEvalPythonExecs in plan:\n$plan")
        assert(execs.forall(e => e.supportsColumnar && e.requiredChildOutput.isEmpty))
        // The outer one sends the result batches of the inner one to Python as they are.
        val outerExec = execs.find(_.child.isInstanceOf[ArrowEvalPythonExec]).getOrElse(
          fail(s"Expected stacked ArrowEvalPythonExecs in plan:\n$plan"))
        assert(outerExec.udfs.head.children.forall(_.isInstanceOf[AttributeReference]))
        assert(hasColumnarToRowAbove(plan), s"Expected a ColumnarToRowExec above:\n$plan")

        assert(result.collect().map(_.getString(0)).toSeq ==
          (0 until numRows).map(id => s"row_$id"))
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
