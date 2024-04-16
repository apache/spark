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

package org.apache.spark.sql.streaming

import java.io.{File, InterruptedIOException, UncheckedIOException}
import java.nio.channels.ClosedByInterruptException
import java.time.ZoneId
import java.util.concurrent.{CountDownLatch, ExecutionException, TimeUnit}

import scala.concurrent.TimeoutException
import scala.reflect.ClassTag
import scala.util.control.ControlThrowable

import com.google.common.util.concurrent.UncheckedExecutionException
import org.apache.commons.io.FileUtils
import org.apache.hadoop.conf.Configuration
import org.scalatest.time.SpanSugar._

import org.apache.spark.{SparkConf, SparkContext, TaskContext, TestUtils}
import org.apache.spark.scheduler.{SparkListener, SparkListenerJobStart}
import org.apache.spark.sql._
import org.apache.spark.sql.catalyst.plans.logical.{Range, RepartitionByExpression}
import org.apache.spark.sql.catalyst.streaming.{InternalOutputModes, StreamingRelationV2}
import org.apache.spark.sql.catalyst.types.DataTypeUtils.toAttributes
import org.apache.spark.sql.catalyst.util.DateTimeUtils
import org.apache.spark.sql.execution.{LocalLimitExec, SimpleMode, SparkPlan}
import org.apache.spark.sql.execution.command.ExplainCommand
import org.apache.spark.sql.execution.streaming._
import org.apache.spark.sql.execution.streaming.sources.{ContinuousMemoryStream, MemorySink}
import org.apache.spark.sql.execution.streaming.state.{KeyStateEncoderSpec, StateStore, StateStoreConf, StateStoreId, StateStoreProvider}
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.sources.StreamSourceProvider
import org.apache.spark.sql.streaming.util.{BlockOnStopSourceProvider, StreamManualClock}
import org.apache.spark.sql.types.{IntegerType, LongType, StructField, StructType}
import org.apache.spark.tags.SlowSQLTest
import org.apache.spark.util.Utils

@SlowSQLTest
class StreamSuite extends StreamTest {

  import testImplicits._

  test("map with recovery") {
    val inputData = MemoryStream[Int]
    val mapped = inputData.toDS().map(_ + 1)

    testStream(mapped)(
      AddData(inputData, 1, 2, 3),
      StartStream(),
      CheckAnswer(2, 3, 4),
      StopStream,
      AddData(inputData, 4, 5, 6),
      StartStream(),
      CheckAnswer(2, 3, 4, 5, 6, 7))
  }

  test("join") {
    // Make a table and ensure it will be broadcast.
    val smallTable = Seq((1, "one"), (2, "two"), (4, "four")).toDF("number", "word")

    // Join the input stream with a table.
    val inputData = MemoryStream[Int]
    val joined = inputData.toDS().toDF().join(smallTable, $"value" === $"number")

    testStream(joined)(
      AddData(inputData, 1, 2, 3),
      CheckAnswer(Row(1, 1, "one"), Row(2, 2, "two")),
      AddData(inputData, 4),
      CheckAnswer(Row(1, 1, "one"), Row(2, 2, "two"), Row(4, 4, "four")))
  }

  test("StreamingRelation.computeStats") {
    withTempDir { dir =>
      val df = spark.readStream.format("csv").schema(StructType(Seq())).load(dir.getCanonicalPath)
      val streamingRelation = df.logicalPlan collect {
        case s: StreamingRelation => s
      }
      assert(streamingRelation.nonEmpty, "cannot find StreamingRelation")
      assert(
        streamingRelation.head.computeStats().sizeInBytes ==
          spark.sessionState.conf.defaultSizeInBytes)
    }
  }

  test("StreamingRelationV2.computeStats") {
    val streamingRelation = spark.readStream.format("rate").load().logicalPlan collect {
      case s: StreamingRelationV2 => s
    }
    assert(streamingRelation.nonEmpty, "cannot find StreamingRelationV2")
    assert(
      streamingRelation.head.computeStats().sizeInBytes ==
        spark.sessionState.conf.defaultSizeInBytes)
  }

  test("StreamingExecutionRelation.computeStats") {
    val memoryStream = MemoryStream[Int]
    val executionRelation = StreamingExecutionRelation(
      memoryStream, toAttributes(memoryStream.encoder.schema), None)(
      memoryStream.sqlContext.sparkSession)
    assert(executionRelation.computeStats().sizeInBytes ==
      spark.sessionState.conf.defaultSizeInBytes)
  }

  test("explain join with a normal source") {
    // This test triggers CostBasedJoinReorder to call `computeStats`
    withSQLConf(SQLConf.CBO_ENABLED.key -> "true", SQLConf.JOIN_REORDER_ENABLED.key -> "true") {
      val smallTable = Seq((1, "one"), (2, "two"), (4, "four")).toDF("number", "word")
      val smallTable2 = Seq((1, "one"), (2, "two"), (4, "four")).toDF("number", "word")
      val smallTable3 = Seq((1, "one"), (2, "two"), (4, "four")).toDF("number", "word")

      // Join the input stream with a table.
      val df = spark.readStream.format("rate").load()
      val joined = df.join(smallTable, smallTable("number") === $"value")
        .join(smallTable2, smallTable2("number") === $"value")
        .join(smallTable3, smallTable3("number") === $"value")

      val outputStream = new java.io.ByteArrayOutputStream()
      Console.withOut(outputStream) {
        joined.explain(true)
      }
      assert(outputStream.toString.contains("StreamingRelation"))
    }
  }

  test("explain join with MemoryStream") {
    // This test triggers CostBasedJoinReorder to call `computeStats`
    // Because MemoryStream doesn't use DataSource code path, we need a separate test.
    withSQLConf(SQLConf.CBO_ENABLED.key -> "true", SQLConf.JOIN_REORDER_ENABLED.key -> "true") {
      val smallTable = Seq((1, "one"), (2, "two"), (4, "four")).toDF("number", "word")
      val smallTable2 = Seq((1, "one"), (2, "two"), (4, "four")).toDF("number", "word")
      val smallTable3 = Seq((1, "one"), (2, "two"), (4, "four")).toDF("number", "word")

      // Join the input stream with a table.
      val df = MemoryStream[Int].toDF()
      val joined = df.join(smallTable, smallTable("number") === $"value")
        .join(smallTable2, smallTable2("number") === $"value")
        .join(smallTable3, smallTable3("number") === $"value")

      val outputStream = new java.io.ByteArrayOutputStream()
      Console.withOut(outputStream) {
        joined.explain(true)
      }
      assert(outputStream.toString.contains("StreamingRelation"))
    }
  }

  test("SPARK-20432: union one stream with itself") {
    val df = spark.readStream.format(classOf[FakeDefaultSource].getName).load().select("a")
    val unioned = df.union(df)
    withTempDir { outputDir =>
      withTempDir { checkpointDir =>
        val query =
          unioned
            .writeStream.format("parquet")
            .option("checkpointLocation", checkpointDir.getAbsolutePath)
            .start(outputDir.getAbsolutePath)
        try {
          query.processAllAvailable()
          val outputDf = spark.read.parquet(outputDir.getAbsolutePath).as[Long]
          checkDatasetUnorderly[Long](outputDf, (0L to 10L) ++ (0L to 10L): _*)
        } finally {
          query.stop()
        }
      }
    }
  }

  test("union two streams") {
    val inputData1 = MemoryStream[Int]
    val inputData2 = MemoryStream[Int]

    val unioned = inputData1.toDS().union(inputData2.toDS())

    testStream(unioned)(
      AddData(inputData1, 1, 3, 5),
      CheckAnswer(1, 3, 5),
      AddData(inputData2, 2, 4, 6),
      CheckAnswer(1, 2, 3, 4, 5, 6),
      StopStream,
      AddData(inputData1, 7),
      StartStream(),
      AddData(inputData2, 8),
      CheckAnswer(1, 2, 3, 4, 5, 6, 7, 8))
  }

  test("sql queries") {
    withTempView("stream") {
      val inputData = MemoryStream[Int]
      inputData.toDF().createOrReplaceTempView("stream")
      val evens = sql("SELECT * FROM stream WHERE value % 2 = 0")

      testStream(evens)(
        AddData(inputData, 1, 2, 3, 4),
        CheckAnswer(2, 4))
    }
  }

  test("DataFrame reuse") {
    def assertDF(df: DataFrame): Unit = {
      withTempDir { outputDir =>
        withTempDir { checkpointDir =>
          val query = df.writeStream.format("parquet")
            .option("checkpointLocation", checkpointDir.getAbsolutePath)
            .start(outputDir.getAbsolutePath)
          try {
            query.processAllAvailable()
            // Parquet write page-level CRC checksums will change the file size and
            // affect the data order when reading these files. Please see PARQUET-1746 for details.
            val outputDf = spark.read.parquet(outputDir.getAbsolutePath).sort($"a").as[Long]
            checkDataset[Long](outputDf, 0L to 10L: _*)
          } finally {
            query.stop()
          }
        }
      }
    }

    val df = spark.readStream.format(classOf[FakeDefaultSource].getName).load()
    Seq("", "parquet").foreach { useV1Source =>
      withSQLConf(SQLConf.USE_V1_SOURCE_LIST.key -> useV1Source) {
        assertDF(df)
        assertDF(df)
      }
    }
  }

  test("Within the same streaming query, one StreamingRelation should only be transformed to one " +
    "StreamingExecutionRelation") {
    val df = spark.readStream.format(classOf[FakeDefaultSource].getName).load()
    var query: StreamExecution = null
    try {
      query =
        df.union(df)
          .writeStream
          .format("memory")
          .queryName("memory")
          .start()
          .asInstanceOf[StreamingQueryWrapper]
          .streamingQuery
      query.awaitInitialization(streamingTimeout.toMillis)
      val executionRelations =
        query
          .logicalPlan
          .collect { case ser: StreamingExecutionRelation => ser }
      assert(executionRelations.size === 2)
      assert(executionRelations.distinct.size === 1)
    } finally {
      if (query != null) {
        query.stop()
      }
    }
  }

  test("unsupported queries") {
    val streamInput = MemoryStream[Int]
    val batchInput = Seq(1, 2, 3).toDS()

    def assertError(expectedMsgs: Seq[String])(body: => Unit): Unit = {
      val e = intercept[AnalysisException] {
        body
      }
      expectedMsgs.foreach { s => assert(e.getMessage.contains(s)) }
    }

    // Running streaming plan as a batch query
    assertError("start" :: Nil) {
      streamInput.toDS().map { i => i }.count()
    }

    // Running non-streaming plan with as a streaming query
    assertError("without streaming sources" :: "start" :: Nil) {
      val ds = batchInput.map { i => i }
      testStream(ds)()
    }

    // Running streaming plan that cannot be incrementalized
    assertError("not supported" :: "streaming" :: Nil) {
      val ds = streamInput.toDS().map { i => i }.sort()
      testStream(ds)()
    }
  }

  test("minimize delay between batch construction and execution") {

    // For each batch, we would retrieve new data's offsets and log them before we run the execution
    // This checks whether the key of the offset log is the expected batch id
    def CheckOffsetLogLatestBatchId(expectedId: Int): AssertOnQuery =
      AssertOnQuery(_.offsetLog.getLatest().get._1 == expectedId,
        s"offsetLog's latest should be $expectedId")

    // Check the latest batchid in the commit log
    def CheckCommitLogLatestBatchId(expectedId: Int): AssertOnQuery =
      AssertOnQuery(_.commitLog.getLatest().get._1 == expectedId,
        s"commitLog's latest should be $expectedId")

    // Ensure that there has not been an incremental execution after restart
    def CheckNoIncrementalExecutionCurrentBatchId(): AssertOnQuery =
      AssertOnQuery(_.lastExecution == null, s"lastExecution not expected to run")

    // For each batch, we would log the state change during the execution
    // This checks whether the key of the state change log is the expected batch id
    def CheckIncrementalExecutionCurrentBatchId(expectedId: Int): AssertOnQuery =
      AssertOnQuery(_.lastExecution.currentBatchId == expectedId,
        s"lastExecution's currentBatchId should be $expectedId")

    // For each batch, we would log the sink change after the execution
    // This checks whether the key of the sink change log is the expected batch id
    def CheckSinkLatestBatchId(expectedId: Int): AssertOnQuery =
      AssertOnQuery(_.sink.asInstanceOf[MemorySink].latestBatchId.get == expectedId,
        s"sink's lastBatchId should be $expectedId")

    val inputData = MemoryStream[Int]
    testStream(inputData.toDS())(
      StartStream(Trigger.ProcessingTime("10 seconds"), new StreamManualClock),

      /* -- batch 0 ----------------------- */
      // Add some data in batch 0
      AddData(inputData, 1, 2, 3),
      AdvanceManualClock(10 * 1000), // 10 seconds

      /* -- batch 1 ----------------------- */
      // Check the results of batch 0
      CheckAnswer(1, 2, 3),
      CheckIncrementalExecutionCurrentBatchId(0),
      CheckCommitLogLatestBatchId(0),
      CheckOffsetLogLatestBatchId(0),
      CheckSinkLatestBatchId(0),
      // Add some data in batch 1
      AddData(inputData, 4, 5, 6),
      AdvanceManualClock(10 * 1000),

      /* -- batch _ ----------------------- */
      // Check the results of batch 1
      CheckAnswer(1, 2, 3, 4, 5, 6),
      CheckIncrementalExecutionCurrentBatchId(1),
      CheckCommitLogLatestBatchId(1),
      CheckOffsetLogLatestBatchId(1),
      CheckSinkLatestBatchId(1),

      AdvanceManualClock(10 * 1000),
      AdvanceManualClock(10 * 1000),
      AdvanceManualClock(10 * 1000),

      /* -- batch __ ---------------------- */
      // Check the results of batch 1 again; this is to make sure that, when there's no new data,
      // the currentId does not get logged (e.g. as 2) even if the clock has advanced many times
      CheckAnswer(1, 2, 3, 4, 5, 6),
      CheckIncrementalExecutionCurrentBatchId(1),
      CheckCommitLogLatestBatchId(1),
      CheckOffsetLogLatestBatchId(1),
      CheckSinkLatestBatchId(1),

      /* Stop then restart the Stream  */
      StopStream,
      StartStream(Trigger.ProcessingTime("10 seconds"), new StreamManualClock(60 * 1000)),

      /* -- batch 1 no rerun ----------------- */
      // batch 1 would not re-run because the latest batch id logged in commit log is 1
      AdvanceManualClock(10 * 1000),
      CheckNoIncrementalExecutionCurrentBatchId(),

      /* -- batch 2 ----------------------- */
      // Check the results of batch 1
      CheckAnswer(1, 2, 3, 4, 5, 6),
      CheckCommitLogLatestBatchId(1),
      CheckOffsetLogLatestBatchId(1),
      CheckSinkLatestBatchId(1),
      // Add some data in batch 2
      AddData(inputData, 7, 8, 9),
      AdvanceManualClock(10 * 1000),

      /* -- batch 3 ----------------------- */
      // Check the results of batch 2
      CheckAnswer(1, 2, 3, 4, 5, 6, 7, 8, 9),
      CheckIncrementalExecutionCurrentBatchId(2),
      CheckCommitLogLatestBatchId(2),
      CheckOffsetLogLatestBatchId(2),
      CheckSinkLatestBatchId(2))
  }

  test("insert an extraStrategy") {
    try {
      spark.experimental.extraStrategies = TestStrategy :: Nil

      val inputData = MemoryStream[(String, Int)]
      val df = inputData.toDS().map(_._1).toDF("a")

      testStream(df)(
        AddData(inputData, ("so slow", 1)),
        CheckAnswer("so fast"))
    } finally {
      spark.experimental.extraStrategies = Nil
    }
  }

  testQuietly("handle fatal errors thrown from the stream thread") {
    for (e <- Seq(
      new VirtualMachineError {},
      new ThreadDeath,
      new LinkageError,
      new ControlThrowable {}
    )) {
      val source = new Source {
        override def getOffset: Option[Offset] = {
          throw e
        }

        override def getBatch(start: Option[Offset], end: Offset): DataFrame = {
          throw e
        }

        override def schema: StructType = StructType(Array(StructField("value", IntegerType)))

        override def stop(): Unit = {}
      }
      val df = Dataset[Int](
        sqlContext.sparkSession,
        StreamingExecutionRelation(source, sqlContext.sparkSession))
      testStream(df)(
        // `ExpectFailure(isFatalError = true)` verifies two things:
        // - Fatal errors can be propagated to `StreamingQuery.exception` and
        //   `StreamingQuery.awaitTermination` like non fatal errors.
        // - Fatal errors can be caught by UncaughtExceptionHandler.
        ExpectFailure(isFatalError = true)(ClassTag(e.getClass))
      )
    }
  }

  test("output mode API in Scala") {
    assert(OutputMode.Append === InternalOutputModes.Append)
    assert(OutputMode.Complete === InternalOutputModes.Complete)
    assert(OutputMode.Update === InternalOutputModes.Update)
  }

  override protected def sparkConf: SparkConf = super.sparkConf
    .set("spark.redaction.string.regex", "file:/[\\w_]+")

  test("explain - redaction") {
    val replacement = "*********"

    val inputData = MemoryStream[String]
    val df = inputData.toDS().map(_ + "foo").groupBy("value").agg(count("*"))
    // Test StreamingQuery.display
    val q = df.writeStream.queryName("memory_explain").outputMode("complete").format("memory")
      .start()
      .asInstanceOf[StreamingQueryWrapper]
      .streamingQuery
    try {
      inputData.addData("abc")
      q.processAllAvailable()

      val explainWithoutExtended = q.explainInternal(false)
      assert(explainWithoutExtended.contains(replacement))
      assert(explainWithoutExtended.contains("StateStoreRestore"))
      assert(!explainWithoutExtended.contains("file:/"))

      val explainWithExtended = q.explainInternal(true)
      assert(explainWithExtended.contains(replacement))
      assert(explainWithExtended.contains("StateStoreRestore"))
      assert(!explainWithoutExtended.contains("file:/"))
    } finally {
      q.stop()
    }
  }

  test("explain") {
    val inputData = MemoryStream[String]
    val df = inputData.toDS().map(_ + "foo").groupBy("value").agg(count("*"))

    // Test `df.explain`
    val explain = ExplainCommand(df.queryExecution.logical, SimpleMode)
    val explainString =
      spark.sessionState
        .executePlan(explain)
        .executedPlan
        .executeCollect()
        .map(_.getString(0))
        .mkString("\n")
    assert(explainString.contains("StateStoreRestore"))
    assert(explainString.contains("StreamingRelation"))
    assert(!explainString.contains("LocalTableScan"))

    // Test StreamingQuery.display
    val q = df.writeStream.queryName("memory_explain").outputMode("complete").format("memory")
      .start()
      .asInstanceOf[StreamingQueryWrapper]
      .streamingQuery
    try {
      assert("No physical plan. Waiting for data." === q.explainInternal(false))
      assert("No physical plan. Waiting for data." === q.explainInternal(true))

      inputData.addData("abc")
      q.processAllAvailable()

      val explainWithoutExtended = q.explainInternal(false)
      // `extended = false` only displays the physical plan.
      assert("StreamingDataSourceV2ScanRelation".r
        .findAllMatchIn(explainWithoutExtended).size === 0)
      assert("BatchScan".r
        .findAllMatchIn(explainWithoutExtended).size === 1)
      // Use "StateStoreRestore" to verify that it does output a streaming physical plan
      assert(explainWithoutExtended.contains("StateStoreRestore"))

      val explainWithExtended = q.explainInternal(true)
      // `extended = true` displays 3 logical plans (Parsed/Optimized/Optimized) and 1 physical
      // plan.
      assert("StreamingDataSourceV2ScanRelation".r
        .findAllMatchIn(explainWithExtended).size === 3)
      assert("BatchScan".r
        .findAllMatchIn(explainWithExtended).size === 1)
      // Use "StateStoreRestore" to verify that it does output a streaming physical plan
      assert(explainWithExtended.contains("StateStoreRestore"))
    } finally {
      q.stop()
    }
  }

  test("explain-continuous") {
    val inputData = ContinuousMemoryStream[Int]
    val df = inputData.toDS().map(_ * 2).filter(_ > 5)

    // Test `df.explain`
    val explain = ExplainCommand(df.queryExecution.logical, SimpleMode)
    val explainString =
      spark.sessionState
        .executePlan(explain)
        .executedPlan
        .executeCollect()
        .map(_.getString(0))
        .mkString("\n")
    assert(explainString.contains("Filter"))
    assert(explainString.contains("MapElements"))
    assert(!explainString.contains("LocalTableScan"))

    // Test StreamingQuery.display
    val q = df.writeStream.queryName("memory_continuous_explain")
      .outputMode(OutputMode.Update()).format("memory")
      .trigger(Trigger.Continuous("1 seconds"))
      .start()
      .asInstanceOf[StreamingQueryWrapper]
      .streamingQuery
    try {
      // in continuous mode, the query will be run even there's no data
      // sleep a bit to ensure initialization
      eventually(timeout(2.seconds), interval(100.milliseconds)) {
        assert(q.lastExecution != null)
      }

      val explainWithoutExtended = q.explainInternal(false)

      // `extended = false` only displays the physical plan.
      assert("StreamingDataSourceV2ScanRelation".r
        .findAllMatchIn(explainWithoutExtended).size === 0)
      assert("ContinuousScan".r
        .findAllMatchIn(explainWithoutExtended).size === 1)

      val explainWithExtended = q.explainInternal(true)
      // `extended = true` displays 3 logical plans (Parsed/Optimized/Optimized) and 1 physical
      // plan.
      assert("StreamingDataSourceV2ScanRelation".r
        .findAllMatchIn(explainWithExtended).size === 3)
      assert("ContinuousScan".r
        .findAllMatchIn(explainWithExtended).size === 1)
    } finally {
      q.stop()
    }
  }

  test("codegen-microbatch") {
    val inputData = MemoryStream[Int]
    val df = inputData.toDS().map(_ * 2).filter(_ > 5)

    // Test StreamingQuery.codegen
    val q = df.writeStream.queryName("memory_microbatch_codegen")
      .outputMode(OutputMode.Update)
      .format("memory")
      .trigger(Trigger.ProcessingTime("1 seconds"))
      .start()

    try {
      import org.apache.spark.sql.execution.debug._
      assert("No physical plan. Waiting for data." === codegenString(q))
      assert(codegenStringSeq(q).isEmpty)

      inputData.addData(1, 2, 3, 4, 5)
      q.processAllAvailable()

      assertDebugCodegenResult(q)
    } finally {
      q.stop()
    }
  }

  test("codegen-continuous") {
    val inputData = ContinuousMemoryStream[Int]
    val df = inputData.toDS().map(_ * 2).filter(_ > 5)

    // Test StreamingQuery.codegen
    val q = df.writeStream.queryName("memory_continuous_codegen")
      .outputMode(OutputMode.Update)
      .format("memory")
      .trigger(Trigger.Continuous("1 seconds"))
      .start()

    try {
      // in continuous mode, the query will be run even there's no data
      // sleep a bit to ensure initialization
      eventually(timeout(2.seconds), interval(100.milliseconds)) {
        assert(q.asInstanceOf[StreamingQueryWrapper].streamingQuery.lastExecution != null)
      }

      assertDebugCodegenResult(q)
    } finally {
      q.stop()
    }
  }

  private def assertDebugCodegenResult(query: StreamingQuery): Unit = {
    import org.apache.spark.sql.execution.debug._

    val codegenStr = codegenString(query)
    assert(codegenStr.contains("Found 1 WholeStageCodegen subtrees."))
    // assuming that code is generated for the test query
    assert(codegenStr.contains("Generated code:"))

    val codegenStrSeq = codegenStringSeq(query)
    assert(codegenStrSeq.nonEmpty)
    assert(codegenStrSeq.head._1.contains("*(1)"))
    assert(codegenStrSeq.head._2.contains("codegenStageId=1"))
  }

  test("SPARK-19065: dropDuplicates should not create expressions using the same id") {
    withTempPath { testPath =>
      val data = Seq((1, 2), (2, 3), (3, 4))
      data.toDS().write.mode("overwrite").json(testPath.getCanonicalPath)
      val schema = spark.read.json(testPath.getCanonicalPath).schema
      val query = spark
        .readStream
        .schema(schema)
        .json(testPath.getCanonicalPath)
        .dropDuplicates("_1")
        .writeStream
        .format("memory")
        .queryName("testquery")
        .outputMode("append")
        .start()
      try {
        query.processAllAvailable()
        if (query.exception.isDefined) {
          throw query.exception.get
        }
      } finally {
        query.stop()
      }
    }
  }

  test("handle InterruptedIOException when the streaming thread is interrupted") {
    // This test uses a fake source to throw the same InterruptedIOException as Hadoop when the
    // streaming thread is interrupted. We should handle it properly by not failing the query.
    ThrowingInterruptedIOException.createSourceLatch = new CountDownLatch(1)
    val query = spark
      .readStream
      .format(classOf[ThrowingInterruptedIOException].getName)
      .load()
      .writeStream
      .format("console")
      .start()
    assert(ThrowingInterruptedIOException.createSourceLatch
      .await(streamingTimeout.toMillis, TimeUnit.MILLISECONDS),
      "ThrowingInterruptedIOException.createSource wasn't called before timeout")
    query.stop()
    assert(query.exception.isEmpty)
  }

  test("SPARK-44044: non-time-window") {
    val inputData = MemoryStream[(Int, Int)]
    val e = intercept[AnalysisException] {
      val agg = inputData
        .toDF()
        .selectExpr("CAST(_1 AS timestamp) AS col1", "_2 AS col2")
        .withWatermark("col1", "10 seconds")
        .withColumn("rn_col", row_number().over(Window
          .partitionBy("col1")
          .orderBy(col("col2"))))
        .select("rn_col", "col1", "col2")
        .writeStream
        .format("console")
        .start()
    }
    checkError(
      e,
      "NON_TIME_WINDOW_NOT_SUPPORTED_IN_STREAMING",
      parameters = Map(
        "windowFunc" -> "ROW_NUMBER()",
        "columnName" -> "`rn_col`",
        "windowSpec" ->
          ("(PARTITION BY COL1 ORDER BY COL2 ASC NULLS FIRST ROWS BETWEEN UNBOUNDED PRECEDING " +
          "AND CURRENT ROW)")))
  }


  test("SPARK-19873: streaming aggregation with change in number of partitions") {
    val inputData = MemoryStream[(Int, Int)]
    val agg = inputData.toDS().groupBy("_1").count()

    testStream(agg, OutputMode.Complete())(
      AddData(inputData, (1, 0), (2, 0)),
      StartStream(additionalConfs = Map(SQLConf.SHUFFLE_PARTITIONS.key -> "2")),
      CheckAnswer((1, 1), (2, 1)),
      StopStream,
      AddData(inputData, (3, 0), (2, 0)),
      StartStream(additionalConfs = Map(SQLConf.SHUFFLE_PARTITIONS.key -> "5")),
      CheckAnswer((1, 1), (2, 2), (3, 1)),
      StopStream,
      AddData(inputData, (3, 0), (1, 0)),
      StartStream(additionalConfs = Map(SQLConf.SHUFFLE_PARTITIONS.key -> "1")),
      CheckAnswer((1, 2), (2, 2), (3, 2)))
  }

  testQuietly("recover from a Spark v2.1 checkpoint") {
    var inputData: MemoryStream[Int] = null
    var query: DataStreamWriter[Row] = null

    def prepareMemoryStream(): Unit = {
      inputData = MemoryStream[Int]
      inputData.addData(1, 2, 3, 4)
      inputData.addData(3, 4, 5, 6)
      inputData.addData(5, 6, 7, 8)

      query = inputData
        .toDF()
        .groupBy($"value")
        .agg(count("*"))
        .writeStream
        .outputMode("complete")
        .format("memory")
    }

    // Get an existing checkpoint generated by Spark v2.1.
    // v2.1 does not record # shuffle partitions in the offset metadata.
    val resourceUri =
      this.getClass.getResource("/structured-streaming/checkpoint-version-2.1.0").toURI
    val checkpointDir = new File(resourceUri)

    // 1 - Test if recovery from the checkpoint is successful.
    prepareMemoryStream()
    val dir1 = Utils.createTempDir().getCanonicalFile // not using withTempDir {}, makes test flaky
    // Copy the checkpoint to a temp dir to prevent changes to the original.
    // Not doing this will lead to the test passing on the first run, but fail subsequent runs.
    FileUtils.copyDirectory(checkpointDir, dir1)
    // Checkpoint data was generated by a query with 10 shuffle partitions.
    // In order to test reading from the checkpoint, the checkpoint must have two or more batches,
    // since the last batch may be rerun.
    withSQLConf(SQLConf.SHUFFLE_PARTITIONS.key -> "10") {
      var streamingQuery: StreamingQuery = null
      try {
        streamingQuery =
          query.queryName("counts").option("checkpointLocation", dir1.getCanonicalPath).start()
        streamingQuery.processAllAvailable()
        inputData.addData(9)
        streamingQuery.processAllAvailable()

        checkAnswer(spark.table("counts").toDF(),
          Row(1, 1L) :: Row(2, 1L) :: Row(3, 2L) :: Row(4, 2L) ::
          Row(5, 2L) :: Row(6, 2L) :: Row(7, 1L) :: Row(8, 1L) :: Row(9, 1L) :: Nil)
      } finally {
        if (streamingQuery ne null) {
          streamingQuery.stop()
        }
      }
    }

    // 2 - Check recovery with wrong num shuffle partitions
    prepareMemoryStream()
    val dir2 = Utils.createTempDir().getCanonicalFile
    FileUtils.copyDirectory(checkpointDir, dir2)
    // Since the number of partitions is greater than 10, should throw exception.
    withSQLConf(SQLConf.SHUFFLE_PARTITIONS.key -> "15") {
      var streamingQuery: StreamingQuery = null
      try {
        intercept[StreamingQueryException] {
          streamingQuery =
            query.queryName("badQuery").option("checkpointLocation", dir2.getCanonicalPath).start()
          streamingQuery.processAllAvailable()
        }
      } finally {
        if (streamingQuery ne null) {
          streamingQuery.stop()
        }
      }
    }
  }

  test("calling stop() on a query cancels related jobs") {
    val input = MemoryStream[Int]
    val query = input
      .toDS()
      .map { i =>
        while (!TaskContext.get().isInterrupted()) {
          // keep looping till interrupted by query.stop()
          Thread.sleep(100)
        }
        i
      }
      .writeStream
      .format("console")
      .start()

    input.addData(1)
    // wait for jobs to start
    eventually(timeout(streamingTimeout)) {
      assert(sparkContext.statusTracker.getActiveJobIds().nonEmpty)
    }

    query.stop()
    // make sure jobs are stopped
    eventually(timeout(streamingTimeout)) {
      assert(sparkContext.statusTracker.getActiveJobIds().isEmpty)
    }
  }

  test("batch id is updated correctly in the job description") {
    val queryName = "memStream"
    @volatile var jobDescription: String = null
    def assertDescContainsQueryNameAnd(batch: Integer): Unit = {
      // wait for listener event to be processed
      spark.sparkContext.listenerBus.waitUntilEmpty(streamingTimeout.toMillis)
      assert(jobDescription.contains(queryName) && jobDescription.contains(s"batch = $batch"))
    }

    spark.sparkContext.addSparkListener(new SparkListener {
      override def onJobStart(jobStart: SparkListenerJobStart): Unit = {
        jobDescription = jobStart.properties.getProperty(SparkContext.SPARK_JOB_DESCRIPTION)
      }
    })

    val input = MemoryStream[Int]
    val query = input
      .toDS()
      .map(_ + 1)
      .writeStream
      .format("memory")
      .queryName(queryName)
      .start()

    input.addData(1)
    query.processAllAvailable()
    assertDescContainsQueryNameAnd(batch = 0)
    input.addData(2, 3)
    query.processAllAvailable()
    assertDescContainsQueryNameAnd(batch = 1)
    input.addData(4)
    query.processAllAvailable()
    assertDescContainsQueryNameAnd(batch = 2)
    query.stop()
  }

  test("should resolve the checkpoint path") {
    withTempDir { dir =>
      val checkpointLocation = dir.getCanonicalPath
      assert(!checkpointLocation.startsWith("file:/"))
      val query = MemoryStream[Int].toDF()
        .writeStream
        .option("checkpointLocation", checkpointLocation)
        .format("console")
        .start()
      try {
        val resolvedCheckpointDir =
          query.asInstanceOf[StreamingQueryWrapper].streamingQuery.resolvedCheckpointRoot
        assert(resolvedCheckpointDir.startsWith("file:/"))
      } finally {
        query.stop()
      }
    }
  }

  testQuietly("specify custom state store provider") {
    val providerClassName = classOf[TestStateStoreProvider].getCanonicalName
    withSQLConf(SQLConf.STATE_STORE_PROVIDER_CLASS.key -> providerClassName) {
      val input = MemoryStream[Int]
      val df = input.toDS().groupBy().count()
      val query = df.writeStream.outputMode("complete").format("memory").queryName("name").start()
      input.addData(1, 2, 3)
      val e = intercept[Exception] {
        query.awaitTermination()
      }

      TestUtils.assertExceptionMsg(e, providerClassName)
      TestUtils.assertExceptionMsg(e, "instantiated")
    }
  }

  testQuietly("custom state store provider read from offset log") {
    val input = MemoryStream[Int]
    val df = input.toDS().groupBy().count()
    val providerConf1 = SQLConf.STATE_STORE_PROVIDER_CLASS.key ->
      "org.apache.spark.sql.execution.streaming.state.HDFSBackedStateStoreProvider"
    val providerConf2 = SQLConf.STATE_STORE_PROVIDER_CLASS.key ->
      classOf[TestStateStoreProvider].getCanonicalName

    def runQuery(queryName: String, checkpointLoc: String): Unit = {
      val query = df.writeStream
        .outputMode("complete")
        .format("memory")
        .queryName(queryName)
        .option("checkpointLocation", checkpointLoc)
        .start()
      input.addData(1, 2, 3)
      query.processAllAvailable()
      query.stop()
    }

    withTempDir { dir =>
      val checkpointLoc1 = new File(dir, "1").getCanonicalPath
      withSQLConf(providerConf1) {
        runQuery("query1", checkpointLoc1)  // generate checkpoints
      }

      val checkpointLoc2 = new File(dir, "2").getCanonicalPath
      withSQLConf(providerConf2) {
        // Verify new query will use new provider that throw error on loading
        intercept[Exception] {
          runQuery("query2", checkpointLoc2)
        }

        // Verify old query from checkpoint will still use old provider
        runQuery("query1", checkpointLoc1)
      }
    }
  }

  test("streaming limit without state") {
    val inputData1 = MemoryStream[Int]
    testStream(inputData1.toDF().limit(0))(
      AddData(inputData1, 1 to 8: _*),
      CheckAnswer())

    val inputData2 = MemoryStream[Int]
    testStream(inputData2.toDF().limit(4))(
      AddData(inputData2, 1 to 8: _*),
      CheckAnswer(1 to 4: _*))
  }

  test("streaming limit with state") {
    val inputData = MemoryStream[Int]
    testStream(inputData.toDF().limit(4))(
      AddData(inputData, 1 to 2: _*),
      CheckAnswer(1 to 2: _*),
      AddData(inputData, 3 to 6: _*),
      CheckAnswer(1 to 4: _*),
      AddData(inputData, 7 to 9: _*),
      CheckAnswer(1 to 4: _*))
  }

  test("streaming limit with other operators") {
    val inputData = MemoryStream[Int]
    testStream(inputData.toDF().where("value % 2 = 1").limit(4))(
      AddData(inputData, 1 to 5: _*),
      CheckAnswer(1, 3, 5),
      AddData(inputData, 6 to 9: _*),
      CheckAnswer(1, 3, 5, 7),
      AddData(inputData, 10 to 12: _*),
      CheckAnswer(1, 3, 5, 7))
  }

  test("streaming limit with multiple limits") {
    val inputData1 = MemoryStream[Int]
    testStream(inputData1.toDF().limit(4).limit(2))(
      AddData(inputData1, 1),
      CheckAnswer(1),
      AddData(inputData1, 2 to 8: _*),
      CheckAnswer(1, 2))

    val inputData2 = MemoryStream[Int]
    testStream(inputData2.toDF().limit(4).limit(100).limit(3))(
      AddData(inputData2, 1, 2),
      CheckAnswer(1, 2),
      AddData(inputData2, 3 to 8: _*),
      CheckAnswer(1 to 3: _*))
  }

  test("SPARK-30658: streaming limit before agg in complete mode") {
    val inputData = MemoryStream[Int]
    val limited = inputData.toDF().limit(5).groupBy("value").count()
    testStream(limited, OutputMode.Complete())(
      AddData(inputData, 1 to 3: _*),
      CheckAnswer(Row(1, 1), Row(2, 1), Row(3, 1)),
      AddData(inputData, 1 to 9: _*),
      CheckAnswer(Row(1, 2), Row(2, 2), Row(3, 1)))
  }

  test("SPARK-30658: streaming limits before and after agg in complete mode " +
    "(after limit < before limit)") {
    val inputData = MemoryStream[Int]
    val limited = inputData.toDF().limit(4).groupBy("value").count().orderBy("value").limit(3)
    testStream(limited, OutputMode.Complete())(
      StartStream(additionalConfs = Map(SQLConf.SHUFFLE_PARTITIONS.key -> "1")),
      AddData(inputData, 1 to 9: _*),
      // only 1 to 4 should be allowed to aggregate, and counts for only 1 to 3 should be output
      CheckAnswer(Row(1, 1), Row(2, 1), Row(3, 1)),
      AddData(inputData, 2 to 6: _*),
      // None of the new values should be allowed to aggregate, same 3 counts should be output
      CheckAnswer(Row(1, 1), Row(2, 1), Row(3, 1)))
  }

  test("SPARK-30658: streaming limits before and after agg in complete mode " +
    "(before limit < after limit)") {
    val inputData = MemoryStream[Int]
    val limited = inputData.toDF().limit(2).groupBy("value").count().orderBy("value").limit(3)
    testStream(limited, OutputMode.Complete())(
      StartStream(additionalConfs = Map(SQLConf.SHUFFLE_PARTITIONS.key -> "1")),
      AddData(inputData, 1 to 9: _*),
      CheckAnswer(Row(1, 1), Row(2, 1)),
      AddData(inputData, 2 to 6: _*),
      CheckAnswer(Row(1, 1), Row(2, 1)))
  }

  test("SPARK-30657: streaming limit after streaming dedup in append mode") {
    val inputData = MemoryStream[Int]
    val limited = inputData.toDF().dropDuplicates().limit(1)
    testStream(limited)(
      AddData(inputData, 1, 2),
      CheckAnswer(Row(1)),
      AddData(inputData, 3, 4),
      CheckAnswer(Row(1)))
  }

  test("streaming limit in update mode") {
    val inputData = MemoryStream[Int]
    val e = intercept[AnalysisException] {
      testStream(inputData.toDF().limit(5), OutputMode.Update())(
        AddData(inputData, 1 to 3: _*)
      )
    }
    assert(e.getMessage.contains(
      "Limits are not supported on streaming DataFrames/Datasets in Update output mode"))
  }

  test("streaming limit in multiple partitions") {
    val inputData = MemoryStream[Int]
    testStream(inputData.toDF().repartition(2).limit(7))(
      AddData(inputData, 1 to 10: _*),
      CheckAnswerRowsByFunc(
        rows => assert(rows.size == 7 && rows.forall(r => r.getInt(0) <= 10)),
        false),
      AddData(inputData, 11 to 20: _*),
      CheckAnswerRowsByFunc(
        rows => assert(rows.size == 7 && rows.forall(r => r.getInt(0) <= 10)),
        false))
  }

  test("streaming limit in multiple partitions by column") {
    val inputData = MemoryStream[(Int, Int)]
    val df = inputData.toDF().repartition(2, $"_2").limit(7)
    testStream(df)(
      AddData(inputData, (1, 0), (2, 0), (3, 1), (4, 1)),
      CheckAnswerRowsByFunc(
        rows => assert(rows.size == 4 && rows.forall(r => r.getInt(0) <= 4)),
        false),
      AddData(inputData, (5, 0), (6, 0), (7, 1), (8, 1)),
      CheckAnswerRowsByFunc(
        rows => assert(rows.size == 7 && rows.forall(r => r.getInt(0) <= 8)),
        false))
  }

  test("SPARK-30657: streaming limit should not apply on limits on state subplans") {
    val streamData = MemoryStream[Int]
    val streamingDF = streamData.toDF().toDF("value")
    val staticDF = spark.createDataset(Seq(1)).toDF("value").orderBy("value")
    testStream(streamingDF.join(staticDF.limit(1), "value"))(
      AddData(streamData, 1, 2, 3),
      CheckAnswer(Row(1)),
      AddData(streamData, 1, 3, 5),
      CheckAnswer(Row(1), Row(1)))
  }

  test("SPARK-30657: streaming limit optimization from StreamingLocalLimitExec to LocalLimitExec") {
    val inputData = MemoryStream[Int]
    val inputDF = inputData.toDF()

    /** Verify whether the local limit in the plan is a streaming limit or is a simple */
    def verifyLocalLimit(
        df: DataFrame,
        expectStreamingLimit: Boolean,
      outputMode: OutputMode = OutputMode.Append): Unit = {

      var execPlan: SparkPlan = null
      testStream(df, outputMode)(
        AddData(inputData, 1),
        AssertOnQuery { q =>
          q.processAllAvailable()
          execPlan = q.lastExecution.executedPlan
          true
        }
      )
      require(execPlan != null)

      val localLimits = execPlan.collect {
        case l: LocalLimitExec => l
        case l: StreamingLocalLimitExec => l
      }

      require(
        localLimits.size == 1,
        s"Cant verify local limit optimization with this plan:\n$execPlan")

      if (expectStreamingLimit) {
        assert(
          localLimits.head.isInstanceOf[StreamingLocalLimitExec],
          s"Local limit was not StreamingLocalLimitExec:\n$execPlan")
      } else {
        assert(
          localLimits.head.isInstanceOf[LocalLimitExec],
          s"Local limit was not LocalLimitExec:\n$execPlan")
      }
    }

    // Should not be optimized, so StreamingLocalLimitExec should be present
    verifyLocalLimit(inputDF.dropDuplicates().limit(1), expectStreamingLimit = true)

    // Should be optimized from StreamingLocalLimitExec to LocalLimitExec
    verifyLocalLimit(inputDF.limit(1), expectStreamingLimit = false)
    verifyLocalLimit(
      inputDF.limit(1).groupBy().count(),
      expectStreamingLimit = false,
      outputMode = OutputMode.Complete())

    // Should be optimized as repartition is sufficient to ensure that the iterators of
    // StreamingDeduplicationExec should be consumed completely by the repartition exchange.
    verifyLocalLimit(inputDF.dropDuplicates().repartition(1).limit(1), expectStreamingLimit = false)

    // Should be LocalLimitExec in the first place, not from optimization of StreamingLocalLimitExec
    val staticDF = spark.range(2).toDF("value").limit(1)
    verifyLocalLimit(inputDF.toDF("value").join(staticDF, "value"), expectStreamingLimit = false)

    verifyLocalLimit(
      inputDF.groupBy("value").count().limit(1),
      expectStreamingLimit = false,
      outputMode = OutputMode.Complete())
  }

  test("is_continuous_processing property should be false for microbatch processing") {
    val input = MemoryStream[Int]
    val df = input.toDS()
      .map(i => TaskContext.get().getLocalProperty(StreamExecution.IS_CONTINUOUS_PROCESSING))
    testStream(df) (
      AddData(input, 1),
      CheckAnswer("false")
    )
  }

  test("is_continuous_processing property should be true for continuous processing") {
    val input = ContinuousMemoryStream[Int]
    val stream = input.toDS()
      .map(i => TaskContext.get().getLocalProperty(StreamExecution.IS_CONTINUOUS_PROCESSING))
      .writeStream.format("memory")
      .queryName("output")
      .trigger(Trigger.Continuous("1 seconds"))
      .start()
    try {
      input.addData(1)
      stream.processAllAvailable()
    } finally {
      stream.stop()
    }

    checkAnswer(spark.sql("select * from output"), Row("true"))
  }

  for (e <- Seq(
    new InterruptedException,
    new InterruptedIOException,
    new ClosedByInterruptException,
    new UncheckedIOException("test", new ClosedByInterruptException),
    new ExecutionException("test", new InterruptedException),
    new UncheckedExecutionException("test", new InterruptedException)) ++
    Seq(
      classOf[InterruptedException].getName,
      classOf[InterruptedIOException].getName,
      classOf[ClosedByInterruptException].getName).map { s =>
    new py4j.Py4JException(
      s"""
        |py4j.protocol.Py4JJavaError: An error occurred while calling o44.count.
        |: $s
        |""".stripMargin)
    }) {
    test(s"view ${e.getClass.getSimpleName} [${e.getMessage}] as a normal query stop") {
      ThrowingExceptionInCreateSource.createSourceLatch = new CountDownLatch(1)
      ThrowingExceptionInCreateSource.exception = e
      val query = spark
        .readStream
        .format(classOf[ThrowingExceptionInCreateSource].getName)
        .load()
        .writeStream
        .format("console")
        .start()
      assert(ThrowingExceptionInCreateSource.createSourceLatch
        .await(streamingTimeout.toMillis, TimeUnit.MILLISECONDS),
        "ThrowingExceptionInCreateSource.createSource wasn't called before timeout")
      query.stop()
      assert(query.exception.isEmpty)
    }
  }

  test("SPARK-26379 Structured Streaming - Exception on adding current_timestamp " +
    " to Dataset - use v2 sink") {
    testCurrentTimestampOnStreamingQuery()
  }

  test("SPARK-26379 Structured Streaming - Exception on adding current_timestamp " +
    " to Dataset - use v1 sink") {
    testCurrentTimestampOnStreamingQuery()
  }

  private def testCurrentTimestampOnStreamingQuery(): Unit = {
    val input = MemoryStream[Int]
    val df = input.toDS().withColumn("cur_timestamp", lit(current_timestamp()))

    def assertBatchOutputAndUpdateLastTimestamp(
        rows: Seq[Row],
        curTimestamp: Long,
        curDate: Int,
        expectedValue: Int): Long = {
      assert(rows.size === 1)
      val row = rows.head
      assert(row.getInt(0) === expectedValue)
      assert(row.getTimestamp(1).getTime >= curTimestamp)
      row.getTimestamp(1).getTime
    }

    var lastTimestamp = System.currentTimeMillis()
    val currentDate = DateTimeUtils.microsToDays(
      DateTimeUtils.millisToMicros(lastTimestamp), ZoneId.systemDefault)
    testStream(df) (
      AddData(input, 1),
      CheckLastBatch { rows: Seq[Row] =>
        lastTimestamp = assertBatchOutputAndUpdateLastTimestamp(rows, lastTimestamp, currentDate, 1)
      },
      Execute { _ => Thread.sleep(1000) },
      AddData(input, 2),
      CheckLastBatch { rows: Seq[Row] =>
        lastTimestamp = assertBatchOutputAndUpdateLastTimestamp(rows, lastTimestamp, currentDate, 2)
      }
    )
  }

  // ProcessingTime trigger generates MicroBatchExecution, and ContinuousTrigger starts a
  // ContinuousExecution
  Seq(Trigger.ProcessingTime("1 second"), Trigger.Continuous("1 second")).foreach { trigger =>
    test(s"SPARK-30143: stop waits until timeout if blocked - trigger: $trigger") {
      BlockOnStopSourceProvider.enableBlocking()
      val sq = spark.readStream.format(classOf[BlockOnStopSourceProvider].getName)
        .load()
        .writeStream
        .format("console")
        .trigger(trigger)
        .start()
      failAfter(60.seconds) {
        val startTime = System.nanoTime()
        withSQLConf(SQLConf.STREAMING_STOP_TIMEOUT.key -> "2000") {
          val ex = intercept[TimeoutException] {
            sq.stop()
          }
          assert(ex.getMessage.contains(sq.id.toString))
        }
        val duration = (System.nanoTime() - startTime) / 1e6
        assert(duration >= 2000,
          s"Should have waited more than 2000 millis, but waited $duration millis")

        BlockOnStopSourceProvider.disableBlocking()
        withSQLConf(SQLConf.STREAMING_STOP_TIMEOUT.key -> "0") {
          sq.stop()
        }
      }
    }
  }

  test("SPARK-34482: correct active SparkSession for logicalPlan") {
    withSQLConf(
      SQLConf.ADAPTIVE_EXECUTION_ENABLED.key -> "true",
      SQLConf.COALESCE_PARTITIONS_INITIAL_PARTITION_NUM.key -> "10") {
      val df = spark.readStream.format(classOf[FakeDefaultSource].getName).load()
      var query: StreamExecution = null
      try {
        query =
          df.repartition($"a")
            .writeStream
            .format("memory")
            .queryName("memory")
            .start()
            .asInstanceOf[StreamingQueryWrapper]
            .streamingQuery
        query.awaitInitialization(streamingTimeout.toMillis)
        val plan = query.logicalPlan
        val numPartition = plan
          .find { _.isInstanceOf[RepartitionByExpression] }
          .map(_.asInstanceOf[RepartitionByExpression].numPartitions)
        // Before the fix of SPARK-34482, the numPartition is the value of
        // `COALESCE_PARTITIONS_INITIAL_PARTITION_NUM`.
        assert(numPartition.get === spark.sessionState.conf.getConf(SQLConf.SHUFFLE_PARTITIONS))
      } finally {
        if (query != null) {
          query.stop()
        }
      }
    }
  }
}

abstract class FakeSource extends StreamSourceProvider {
  private val fakeSchema = StructType(StructField("a", LongType) :: Nil)

  override def sourceSchema(
      spark: SQLContext,
      schema: Option[StructType],
      providerName: String,
      parameters: Map[String, String]): (String, StructType) = ("fakeSource", fakeSchema)
}

/** A fake StreamSourceProvider that creates a fake Source that cannot be reused. */
class FakeDefaultSource extends FakeSource {

  override def createSource(
      spark: SQLContext,
      metadataPath: String,
      schema: Option[StructType],
      providerName: String,
      parameters: Map[String, String]): Source = {
    // Create a fake Source that emits 0 to 10.
    new Source {
      private var offset = -1L

      override def schema: StructType = StructType(StructField("a", LongType) :: Nil)

      override def getOffset: Option[Offset] = {
        if (offset >= 10) {
          None
        } else {
          offset += 1
          Some(LongOffset(offset))
        }
      }

      override def getBatch(start: Option[Offset], end: Offset): DataFrame = {
        val startOffset = start.map(_.asInstanceOf[LongOffset].offset).getOrElse(-1L) + 1
        val ds = new Dataset[java.lang.Long](
          spark.sparkSession,
          Range(
            startOffset,
            end.asInstanceOf[LongOffset].offset + 1,
            1,
            Some(spark.sparkSession.sparkContext.defaultParallelism),
            isStreaming = true),
          Encoders.LONG)
        ds.toDF("a")
      }

      override def stop(): Unit = {}
    }
  }
}

/** A fake source that throws InterruptedIOException like Hadoop when it's interrupted. */
class ThrowingInterruptedIOException extends FakeSource {
  import ThrowingInterruptedIOException._

  override def createSource(
      spark: SQLContext,
      metadataPath: String,
      schema: Option[StructType],
      providerName: String,
      parameters: Map[String, String]): Source = {
    createSourceLatch.countDown()
    try {
      Thread.sleep(30000)
      throw new TimeoutException("sleep was not interrupted in 30 seconds")
    } catch {
      case ie: InterruptedException =>
        val iie = new InterruptedIOException(ie.toString)
        iie.initCause(ie)
        throw iie
    }
  }
}

object ThrowingInterruptedIOException {
  /**
   * A latch to allow the user to wait until `ThrowingInterruptedIOException.createSource` is
   * called.
   */
  @volatile var createSourceLatch: CountDownLatch = null
}

class TestStateStoreProvider extends StateStoreProvider {

  override def init(
      stateStoreId: StateStoreId,
      keySchema: StructType,
      valueSchema: StructType,
      keyStateEncoderSpec: KeyStateEncoderSpec,
      useColumnFamilies: Boolean,
      storeConfs: StateStoreConf,
      hadoopConf: Configuration,
      useMultipleValuesPerKey: Boolean = false): Unit = {
    throw new Exception("Successfully instantiated")
  }

  override def stateStoreId: StateStoreId = null

  override def close(): Unit = { }

  override def getStore(version: Long): StateStore = null
}

/** A fake source that throws `ThrowingExceptionInCreateSource.exception` in `createSource` */
class ThrowingExceptionInCreateSource extends FakeSource {

  override def createSource(
    spark: SQLContext,
    metadataPath: String,
    schema: Option[StructType],
    providerName: String,
    parameters: Map[String, String]): Source = {
    ThrowingExceptionInCreateSource.createSourceLatch.countDown()
    try {
      Thread.sleep(30000)
      throw new TimeoutException("sleep was not interrupted in 30 seconds")
    } catch {
      case _: InterruptedException =>
        throw ThrowingExceptionInCreateSource.exception
    }
  }
}

object ThrowingExceptionInCreateSource {
  /**
   * A latch to allow the user to wait until `ThrowingExceptionInCreateSource.createSource` is
   * called.
   */
  @volatile var createSourceLatch: CountDownLatch = null
  @volatile var exception: Exception = null
}
