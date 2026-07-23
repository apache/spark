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

import scala.collection.mutable.ArrayBuffer
import scala.jdk.CollectionConverters._

import org.apache.spark.api.python.{PythonEvalType, SimplePythonFunction}
import org.apache.spark.sql.catalyst.{FunctionIdentifier, InternalRow}
import org.apache.spark.sql.catalyst.expressions.{And, AttributeReference, GenericInternalRow, GreaterThan, In}
import org.apache.spark.sql.connector.catalog.CatalogManager
import org.apache.spark.sql.execution.{FilterExec, InputAdapter, WholeStageCodegenExec}
import org.apache.spark.sql.execution.adaptive.AdaptiveSparkPlanHelper
import org.apache.spark.sql.execution.metric.{SQLMetric, SQLMetrics}
import org.apache.spark.sql.test.SharedSparkSession
import org.apache.spark.sql.types._
import org.apache.spark.unsafe.types.UTF8String

class BatchEvalPythonExecSuite extends SharedSparkSession
  with AdaptiveSparkPlanHelper {
  import testImplicits.newProductEncoder
  import testImplicits.localSeqToDatasetHolder

  override def beforeAll(): Unit = {
    super.beforeAll()
    spark.udf.registerPython("dummyPythonUDF", new MyDummyPythonUDF)
  }

  override def afterAll(): Unit = {
    try {
      // Registry requires 3-part identifier for session/temp functions
      val ident = FunctionIdentifier(
        "dummyPythonUDF",
        Some(CatalogManager.SESSION_NAMESPACE),
        Some(CatalogManager.SYSTEM_CATALOG_NAME))
      spark.sessionState.functionRegistry.dropFunction(ident)
    } finally {
      super.afterAll()
    }
  }

  test("SPARK-57593: getInputIterator records peak pickled batch size as a size metric") {
    EvaluatePython.registerPicklers()
    val schema = StructType(Seq(StructField("s", StringType)))
    // batchSize = 1 so each row is its own pickled batch; the widest row yields the peak.
    val batchSize = 1
    val rows = Seq(
      InternalRow(UTF8String.fromString("a")),
      InternalRow(UTF8String.fromString("b" * 1000)),
      InternalRow(UTF8String.fromString("c")))

    // Baseline: the peak pickled-batch size, computed without instrumentation.
    val expectedPeak = BatchEvalPythonExec
      .getInputIterator(rows.iterator, schema, batchSize, binaryAsBytes = false)
      .map(_.length.toLong)
      .max
    assert(expectedPeak > 0)

    val metric = SQLMetrics.createSizeMetric(spark.sparkContext, "peak pickled batch size in bytes")
    // Drain the iterator so every batch is pickled and measured.
    BatchEvalPythonExec
      .getInputIterator(rows.iterator, schema, batchSize, binaryAsBytes = false,
        pythonMetrics = Map("pythonPeakPickledBatchBytes" -> metric))
      .foreach(_ => ())

    assert(metric.value === expectedPeak)
  }

  test("SPARK-57593: ByteBoundedAsArrayIterator oversized-batch and estimated-bytes metrics") {
    def elems(sizes: Long*): Iterator[(Any, Long)] =
      sizes.iterator.map(s => (0.asInstanceOf[Any], s))
    def m(): SQLMetric = SQLMetrics.createMetric(spark.sparkContext, "m")

    // Cut once accumulated bytes >= 25 (after 3 rows of 10). estimatedInputBytes sums the
    // per-row estimate across the whole partition.
    val ov1 = m(); val est1 = m()
    val byBytes = new ByteBoundedAsArrayIterator(
      elems(10, 10, 10, 10, 10, 10, 10), maxRecordsPerBatch = 100, maxBytesPerBatch = 25L,
      Some(ov1), Some(est1)).toList.map(_.length)
    assert(byBytes === List(3, 3, 1))
    assert(ov1.value === 2)   // two batches were cut at the cap; the trailing 1-row batch was not
    assert(est1.value === 70) // sum of per-row estimates

    // Record cap dominates when smaller; the byte limit is never reached.
    val ov2 = m()
    val byCount = new ByteBoundedAsArrayIterator(
      elems(10, 10, 10, 10, 10), maxRecordsPerBatch = 2, maxBytesPerBatch = 1000L,
      Some(ov2), None).toList.map(_.length)
    assert(byCount === List(2, 2, 1))
    assert(ov2.value === 0)

    // A single row larger than the cap still forms a one-row batch (never zero rows).
    val ov3 = m()
    val giant = new ByteBoundedAsArrayIterator(
      elems(10000), maxRecordsPerBatch = 100, maxBytesPerBatch = 25L,
      Some(ov3), None).toList.map(_.length)
    assert(giant === List(1))
    assert(ov3.value === 1)

    // Size-0 estimates contribute nothing to the estimate sum or the byte cap; the cut happens
    // on the first row that pushes the estimate to the cap.
    val ov5 = m(); val est5 = m()
    val mixed = new ByteBoundedAsArrayIterator(
      elems(0, 0, 30, 0), maxRecordsPerBatch = 100, maxBytesPerBatch = 25L,
      Some(ov5), Some(est5)).toList.map(_.length)
    assert(mixed === List(3, 1))
    assert(est5.value === 30) // only the size-30 row contributes
    assert(ov5.value === 1)   // one batch cut at the cap
  }

  test("SPARK-57593: ByteBoundedAsArrayIterator honors the Iterator contract") {
    def elems(sizes: Long*): Iterator[(Any, Long)] =
      sizes.iterator.map(s => (0.asInstanceOf[Any], s))
    def iterOf(input: Iterator[(Any, Long)], maxRecords: Int = 100): Iterator[Array[Any]] =
      new ByteBoundedAsArrayIterator(
        input, maxRecordsPerBatch = maxRecords, maxBytesPerBatch = Int.MaxValue.toLong,
        None, None)

    // next() past the end throws NoSuchElementException, matching the row-count batching path.
    val it = iterOf(elems(10))
    assert(it.hasNext)
    assert(it.next().length === 1)
    assert(!it.hasNext)
    intercept[NoSuchElementException](it.next())

    // An empty input yields no batches and next() throws immediately.
    val empty = iterOf(elems())
    assert(!empty.hasNext)
    intercept[NoSuchElementException](empty.next())

    // A non-positive record limit is rejected up front.
    intercept[IllegalArgumentException](iterOf(elems(10), maxRecords = 0))

    // A non-positive byte cap is rejected up front too: only finite positive caps may reach this
    // class (a cap of 0 would otherwise silently degrade every batch to a single row).
    intercept[IllegalArgumentException](new ByteBoundedAsArrayIterator(
      elems(10), maxRecordsPerBatch = 100, maxBytesPerBatch = 0L, None, None))
  }

  test("SPARK-57593: getInputIterator byte-bounds pickle batches when a cap is configured") {
    EvaluatePython.registerPicklers()
    val schema = StructType(Seq(StructField("s", StringType)))
    // GenericInternalRow on purpose: production feeds this path from a MutableProjection whose
    // target is a GenericInternalRow (never an UnsafeRow), so the byte cap must work on it.
    def wideRows(): Iterator[InternalRow] = (0 until 10).iterator.map { _ =>
      new GenericInternalRow(Array[Any](UTF8String.fromString("x" * 1000)))
    }

    // Default: no byte cap, so all 10 rows form a single batch (batchSize = 100).
    val defaultBatches = BatchEvalPythonExec
      .getInputIterator(wideRows(), schema, batchSize = 100, binaryAsBytes = false).size
    assert(defaultBatches === 1)

    // Finite cap: the wide rows split into multiple smaller batches, and the estimate metric is
    // populated (regression test: the estimate must be non-zero for production row classes).
    val estMetric = SQLMetrics.createSizeMetric(spark.sparkContext, "est")
    val cappedBatches = BatchEvalPythonExec
      .getInputIterator(wideRows(), schema, batchSize = 100, binaryAsBytes = false,
        maxBytesPerBatch = 2048L,
        pythonMetrics = Map("pythonEstimatedInputBytes" -> estMetric)).size
    assert(cappedBatches > defaultBatches)
    assert(estMetric.value >= 10 * 1000L) // 10 rows of a 1000-char string, plus overhead
  }

  test("SPARK-57593: getInputIterator does not byte-bound when maxBytesPerBatch is -1 (no limit)") {
    EvaluatePython.registerPicklers()
    val schema = StructType(Seq(StructField("s", StringType)))
    // 250 wide rows: more than batchSize so row-count batching genuinely splits them, and each row
    // is wide enough that any finite byte cap would split them into far more batches (the sibling
    // test above cuts 10 of these at a 2048-byte cap).
    def wideRows(): Iterator[InternalRow] = (0 until 250).iterator.map { _ =>
      new GenericInternalRow(Array[Any](UTF8String.fromString("x" * 1000)))
    }

    // -1 (the default) routes to the row-count-only batcher: batches are cut purely at batchSize
    // (250 rows / 100 = 3 batches) with no byte bound, and the byte-only metrics stay unpopulated.
    val estMetric = SQLMetrics.createSizeMetric(spark.sparkContext, "est")
    val oversizedMetric = SQLMetrics.createMetric(spark.sparkContext, "ov")
    val batches = BatchEvalPythonExec
      .getInputIterator(wideRows(), schema, batchSize = 100, binaryAsBytes = false,
        maxBytesPerBatch = -1L,
        pythonMetrics = Map(
          "pythonEstimatedInputBytes" -> estMetric,
          "pythonOversizedBatchCount" -> oversizedMetric)).size
    // Row-count only: 3 batches (100 + 100 + 50). A finite byte cap would produce many more.
    assert(batches === 3)
    assert(estMetric.value === 0L)       // no per-row estimate accumulated on the no-limit path
    assert(oversizedMetric.value === 0L) // no batch cut at a byte limit
  }

  test("SPARK-57593: toJava accumulates the pickled-size estimate during conversion") {
    val acc = new PickledSizeAccumulator
    def sized(value: Any, dataType: DataType): Long = {
      EvaluatePython.toJava(value, dataType, binaryAsBytes = true, Some(acc))
      acc.getAndReset()
    }
    // Payload bytes dominate the estimate; UTF8String.numBytes is the exact UTF-8 byte count.
    val wide = sized(UTF8String.fromString("x" * 10000), StringType)
    assert(wide >= 10000L && wide <= 10000L + 64L)
    val binary = sized(Array.fill[Byte](5000)(1), BinaryType)
    assert(binary >= 5000L && binary <= 5000L + 64L)
    // Decimals expand into digit strings when pickled; the estimate tracks precision.
    val dec = sized(Decimal(BigDecimal("1234567890.123456789")), DecimalType(38, 18))
    assert(dec >= 19L)
    // Nulls and unknown leaf types yield small positive estimates -- never zero, so the cap
    // can never go blind on an unrecognized row shape.
    assert(sized(null, StringType) > 0L)
    assert(sized(new Object, CalendarIntervalType) > 0L)
    // Whole-row conversion (needConversion path): nested values are sized in the same pass,
    // and getAndReset() closes the per-row cycle.
    val schema = StructType(Seq(StructField("s", StringType), StructField("n", StringType)))
    val rowSize = sized(
      new GenericInternalRow(Array[Any](UTF8String.fromString("y" * 2000), null)), schema)
    assert(rowSize >= 2000L)
    assert(acc.getAndReset() === 0L) // reset really resets
  }

  test("SPARK-57593: toJava size estimate is positive for every data type (drift guard)") {
    // Enforces the accounting invariant on EvaluatePython.toJava: a (new) conversion case that
    // forgets to account to sizeAcc yields a zero estimate at value level and makes the byte cap
    // blind to that type. The type list comes from DataTypeTestUtils + composites, so the sweep
    // extends as Spark grows types, and values come from the seeded RandomDataGenerator.
    import org.apache.spark.sql.RandomDataGenerator
    import org.apache.spark.sql.catalyst.CatalystTypeConverters
    val acc = new PickledSizeAccumulator
    val sweep = (DataTypeTestUtils.atomicTypes.toSeq ++ Seq(
      ArrayType(StringType),
      MapType(StringType, DoubleType),
      StructType(Seq(StructField("s", StringType), StructField("d", DoubleType)))))
    var swept = 0
    sweep.foreach { dt =>
      RandomDataGenerator.forType(dt, nullable = false, new scala.util.Random(42)).foreach {
        gen =>
          val catalystValue = CatalystTypeConverters.createToCatalystConverter(dt)(gen())
          EvaluatePython.toJava(catalystValue, dt, binaryAsBytes = true, Some(acc))
          val estimate = acc.getAndReset()
          assert(estimate > 0L, s"toJava accounted nothing for $dt -- the byte cap is blind " +
            "to this type; add accounting to the corresponding case")
          swept += 1
      }
    }
    assert(swept > 10) // the sweep must be exercising a meaningful slice of the type space
  }

  test("Python UDF: push down deterministic FilterExec predicates") {
    val df = Seq(("Hello", 4)).toDF("a", "b")
      .where("dummyPythonUDF(b) and dummyPythonUDF(a) and a in (3, 4)")
    val qualifiedPlanNodes = df.queryExecution.executedPlan.collect {
      case f @ FilterExec(
          And(_: AttributeReference, _: AttributeReference),
          InputAdapter(_: BatchEvalPythonExec)) => f
      case b @ BatchEvalPythonExec(_, _, WholeStageCodegenExec(FilterExec(_: In, _))) => b
    }
    assert(qualifiedPlanNodes.size == 2)
  }

  test("Nested Python UDF: push down deterministic FilterExec predicates") {
    val df = Seq(("Hello", 4)).toDF("a", "b")
      .where("dummyPythonUDF(a, dummyPythonUDF(a, b)) and a in (3, 4)")
    val qualifiedPlanNodes = df.queryExecution.executedPlan.collect {
      case f @ FilterExec(_: AttributeReference, InputAdapter(_: BatchEvalPythonExec)) => f
      case b @ BatchEvalPythonExec(_, _, WholeStageCodegenExec(FilterExec(_: In, _))) => b
    }
    assert(qualifiedPlanNodes.size == 2)
  }

  test("Python UDF: no push down on non-deterministic") {
    val df = Seq(("Hello", 4)).toDF("a", "b")
      .where("b > 4 and dummyPythonUDF(a) and rand() > 0.3")
    val qualifiedPlanNodes = df.queryExecution.executedPlan.collect {
      case f @ FilterExec(
          And(_: AttributeReference, _: GreaterThan),
          InputAdapter(_: BatchEvalPythonExec)) => f
      case b @ BatchEvalPythonExec(_, _, WholeStageCodegenExec(_: FilterExec)) => b
    }
    assert(qualifiedPlanNodes.size == 2)
  }

  test("Python UDF: push down on deterministic predicates after the first non-deterministic") {
    val df = Seq(("Hello", 4)).toDF("a", "b")
      .where("dummyPythonUDF(a) and rand() > 0.3 and b > 4")

    val qualifiedPlanNodes = df.queryExecution.executedPlan.collect {
      case f @ FilterExec(
          And(_: AttributeReference, _: GreaterThan),
          InputAdapter(_: BatchEvalPythonExec)) => f
      case b @ BatchEvalPythonExec(_, _, WholeStageCodegenExec(_: FilterExec)) => b
    }
    assert(qualifiedPlanNodes.size == 2)
  }

  test("Python UDF refers to the attributes from more than one child") {
    val df = Seq(("Hello", 4)).toDF("a", "b")
    val df2 = Seq(("Hello", 4)).toDF("c", "d")
    val joinDF = df.crossJoin(df2).where("dummyPythonUDF(a, c) == dummyPythonUDF(d, c)")
    val qualifiedPlanNodes = collect(joinDF.queryExecution.executedPlan) {
      case b: BatchEvalPythonExec => b
    }
    assert(qualifiedPlanNodes.size == 1)
  }

  test("SPARK-28422: GROUPED_AGG pandas_udf should work without group by clause") {
    val aggPandasUdf = new MyDummyGroupedAggPandasUDF
    spark.udf.registerPython("dummyGroupedAggPandasUDF", aggPandasUdf)

    withTempView("table") {
      val df = spark.range(0, 100)
      df.createTempView("table")

      val agg1 = df.agg(aggPandasUdf(df("id")))
      val agg2 = sql("select dummyGroupedAggPandasUDF(id) from table")

      comparePlans(agg1.queryExecution.optimizedPlan, agg2.queryExecution.optimizedPlan)
    }
  }
}

// This Python UDF is dummy and just for testing. Unable to execute.
class DummyUDF extends SimplePythonFunction(
  command = Array[Byte](),
  envVars = Map("" -> "").asJava,
  pythonIncludes = ArrayBuffer("").asJava,
  pythonExec = "",
  pythonVer = "",
  broadcastVars = null,
  accumulator = null)

class MyDummyPythonUDF extends UserDefinedPythonFunction(
  name = "dummyUDF",
  func = new DummyUDF,
  dataType = BooleanType,
  pythonEvalType = PythonEvalType.SQL_BATCHED_UDF,
  udfDeterministic = true)

class MyDummyNondeterministicPythonUDF extends UserDefinedPythonFunction(
  name = "dummyNondeterministicUDF",
  func = new DummyUDF,
  dataType = BooleanType,
  pythonEvalType = PythonEvalType.SQL_BATCHED_UDF,
  udfDeterministic = false)

class MyDummyGroupedAggPandasUDF extends UserDefinedPythonFunction(
  name = "dummyGroupedAggPandasUDF",
  func = new DummyUDF,
  dataType = DoubleType,
  pythonEvalType = PythonEvalType.SQL_GROUPED_AGG_PANDAS_UDF,
  udfDeterministic = true)

class MyDummyScalarPandasUDF extends UserDefinedPythonFunction(
  name = "dummyScalarPandasUDF",
  func = new DummyUDF,
  dataType = BooleanType,
  pythonEvalType = PythonEvalType.SQL_SCALAR_PANDAS_UDF,
  udfDeterministic = true)
