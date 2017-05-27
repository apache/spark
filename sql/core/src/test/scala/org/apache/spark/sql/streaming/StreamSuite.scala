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

import java.io.{File, InterruptedIOException, IOException}
import java.util.concurrent.{CountDownLatch, TimeoutException, TimeUnit}

import scala.reflect.ClassTag
import scala.util.control.ControlThrowable

import org.apache.commons.io.FileUtils

import org.apache.spark.SparkContext
import org.apache.spark.scheduler.{SparkListener, SparkListenerJobStart}
import org.apache.spark.sql._
import org.apache.spark.sql.catalyst.streaming.InternalOutputModes
import org.apache.spark.sql.execution.command.ExplainCommand
import org.apache.spark.sql.execution.streaming._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.sources.StreamSourceProvider
import org.apache.spark.sql.streaming.util.StreamManualClock
import org.apache.spark.sql.types.{IntegerType, StructField, StructType}
import org.apache.spark.util.Utils

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
          checkDatasetUnorderly[Long](outputDf, (0L to 10L).union((0L to 10L)).toArray: _*)
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
    val inputData = MemoryStream[Int]
    inputData.toDF().createOrReplaceTempView("stream")
    val evens = sql("SELECT * FROM stream WHERE value % 2 = 0")

    testStream(evens)(
      AddData(inputData, 1, 2, 3, 4),
      CheckAnswer(2, 4))
  }

  test("DataFrame reuse") {
    def assertDF(df: DataFrame) {
      withTempDir { outputDir =>
        withTempDir { checkpointDir =>
          val query = df.writeStream.format("parquet")
            .option("checkpointLocation", checkpointDir.getAbsolutePath)
            .start(outputDir.getAbsolutePath)
          try {
            query.processAllAvailable()
            val outputDf = spark.read.parquet(outputDir.getAbsolutePath).as[Long]
            checkDataset[Long](outputDf, (0L to 10L).toArray: _*)
          } finally {
            query.stop()
          }
        }
      }
    }

    val df = spark.readStream.format(classOf[FakeDefaultSource].getName).load()
    assertDF(df)
    assertDF(df)
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
      streamInput.toDS.map { i => i }.count()
    }

    // Running non-streaming plan with as a streaming query
    assertError("without streaming sources" :: "start" :: Nil) {
      val ds = batchInput.map { i => i }
      testStream(ds)()
    }

    // Running streaming plan that cannot be incrementalized
    assertError("not supported" :: "streaming" :: Nil) {
      val ds = streamInput.toDS.map { i => i }.sort()
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
      AssertOnQuery(_.batchCommitLog.getLatest().get._1 == expectedId,
        s"commitLog's latest should be $expectedId")

    // Ensure that there has not been an incremental execution after restart
    def CheckNoIncrementalExecutionCurrentBatchId(): AssertOnQuery =
      AssertOnQuery(_.lastExecution == null, s"lastExecution not expected to run")

    // For each batch, we would log the state change during the execution
    // This checks whether the key of the state change log is the expected batch id
    def CheckIncrementalExecutionCurrentBatchId(expectedId: Int): AssertOnQuery =
      AssertOnQuery(_.lastExecution.asInstanceOf[IncrementalExecution].currentBatchId == expectedId,
        s"lastExecution's currentBatchId should be $expectedId")

    // For each batch, we would log the sink change after the execution
    // This checks whether the key of the sink change log is the expected batch id
    def CheckSinkLatestBatchId(expectedId: Int): AssertOnQuery =
      AssertOnQuery(_.sink.asInstanceOf[MemorySink].latestBatchId.get == expectedId,
        s"sink's lastBatchId should be $expectedId")

    val inputData = MemoryStream[Int]
    testStream(inputData.toDS())(
      StartStream(ProcessingTime("10 seconds"), new StreamManualClock),

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
      StartStream(ProcessingTime("10 seconds"), new StreamManualClock(60 * 1000)),

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
      val df = Dataset[Int](sqlContext.sparkSession, StreamingExecutionRelation(source))
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

  test("explain") {
    val inputData = MemoryStream[String]
    val df = inputData.toDS().map(_ + "foo").groupBy("value").agg(count("*"))

    // Test `df.explain`
    val explain = ExplainCommand(df.queryExecution.logical, extended = false)
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
      assert("LocalRelation".r.findAllMatchIn(explainWithoutExtended).size === 0)
      assert("LocalTableScan".r.findAllMatchIn(explainWithoutExtended).size === 1)
      // Use "StateStoreRestore" to verify that it does output a streaming physical plan
      assert(explainWithoutExtended.contains("StateStoreRestore"))

      val explainWithExtended = q.explainInternal(true)
      // `extended = true` displays 3 logical plans (Parsed/Optimized/Optimized) and 1 physical
      // plan.
      assert("LocalRelation".r.findAllMatchIn(explainWithExtended).size === 3)
      assert("LocalTableScan".r.findAllMatchIn(explainWithExtended).size === 1)
      // Use "StateStoreRestore" to verify that it does output a streaming physical plan
      assert(explainWithExtended.contains("StateStoreRestore"))
    } finally {
      q.stop()
    }
  }

  test("SPARK-19065: dropDuplicates should not create expressions using the same id") {
    withTempPath { testPath =>
      val data = Seq((1, 2), (2, 3), (3, 4))
      data.toDS.write.mode("overwrite").json(testPath.getCanonicalPath)
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

  test("handle IOException when the streaming thread is interrupted (pre Hadoop 2.8)") {
    // This test uses a fake source to throw the same IOException as pre Hadoop 2.8 when the
    // streaming thread is interrupted. We should handle it properly by not failing the query.
    ThrowingIOExceptionLikeHadoop12074.createSourceLatch = new CountDownLatch(1)
    val query = spark
      .readStream
      .format(classOf[ThrowingIOExceptionLikeHadoop12074].getName)
      .load()
      .writeStream
      .format("console")
      .start()
    assert(ThrowingIOExceptionLikeHadoop12074.createSourceLatch
      .await(streamingTimeout.toMillis, TimeUnit.MILLISECONDS),
      "ThrowingIOExceptionLikeHadoop12074.createSource wasn't called before timeout")
    query.stop()
    assert(query.exception.isEmpty)
  }

  test("handle InterruptedIOException when the streaming thread is interrupted (Hadoop 2.8+)") {
    // This test uses a fake source to throw the same InterruptedIOException as Hadoop 2.8+ when the
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

        QueryTest.checkAnswer(spark.table("counts").toDF(),
          Row("1", 1) :: Row("2", 1) :: Row("3", 2) :: Row("4", 2) ::
          Row("5", 2) :: Row("6", 2) :: Row("7", 1) :: Row("8", 1) :: Row("9", 1) :: Nil)
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
        while (!org.apache.spark.TaskContext.get().isInterrupted()) {
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
}

abstract class FakeSource extends StreamSourceProvider {
  private val fakeSchema = StructType(StructField("a", IntegerType) :: Nil)

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

      override def schema: StructType = StructType(StructField("a", IntegerType) :: Nil)

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
        spark.range(startOffset, end.asInstanceOf[LongOffset].offset + 1).toDF("a")
      }

      override def stop() {}
    }
  }
}

/** A fake source that throws the same IOException like pre Hadoop 2.8 when it's interrupted. */
class ThrowingIOExceptionLikeHadoop12074 extends FakeSource {
  import ThrowingIOExceptionLikeHadoop12074._

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
        throw new IOException(ie.toString)
    }
  }
}

object ThrowingIOExceptionLikeHadoop12074 {
  /**
   * A latch to allow the user to wait until `ThrowingIOExceptionLikeHadoop12074.createSource` is
   * called.
   */
  @volatile var createSourceLatch: CountDownLatch = null
}

/** A fake source that throws InterruptedIOException like Hadoop 2.8+ when it's interrupted. */
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
