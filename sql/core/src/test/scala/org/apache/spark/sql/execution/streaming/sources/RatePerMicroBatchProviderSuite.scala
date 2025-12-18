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

package org.apache.spark.sql.execution.streaming.sources

import org.scalatest.concurrent.PatienceConfiguration.Timeout

import org.apache.spark.{SparkThrowable, SparkUnsupportedOperationException}
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.execution.datasources.DataSource
import org.apache.spark.sql.functions.spark_partition_id
import org.apache.spark.sql.streaming.{StreamingQueryException, StreamTest, Trigger}
import org.apache.spark.sql.streaming.util.StreamManualClock

class RatePerMicroBatchProviderSuite extends StreamTest {

  import testImplicits._

  test("RatePerMicroBatchProvider in registry") {
    val ds = DataSource.lookupDataSource("rate-micro-batch", spark.sessionState.conf)
      .getConstructor().newInstance()
    assert(ds.isInstanceOf[RatePerMicroBatchProvider], "Could not find rate-micro-batch source")
  }

  test("basic") {
    val input = spark.readStream
      .format("rate-micro-batch")
      .option("rowsPerBatch", "10")
      .option("startTimestamp", "1000")
      .option("advanceMillisPerBatch", "50")
      .load()
    val clock = new StreamManualClock
    testStream(input)(
      StartStream(trigger = Trigger.ProcessingTime(10), triggerClock = clock),
      waitUntilBatchProcessed(clock),
      CheckLastBatch((0 until 10).map(v => new java.sql.Timestamp(1000L) -> v): _*),
      AdvanceManualClock(10),
      waitUntilBatchProcessed(clock),
      CheckLastBatch((10 until 20).map(v => new java.sql.Timestamp(1050L) -> v): _*),
      AdvanceManualClock(10),
      waitUntilBatchProcessed(clock),
      CheckLastBatch((20 until 30).map(v => new java.sql.Timestamp(1100L) -> v): _*)
    )
  }

  test("restart") {
    withTempDir { dir =>
      val input = spark.readStream
        .format("rate-micro-batch")
        .option("rowsPerBatch", "10")
        .load()
        .select($"value")

      val clock = new StreamManualClock
      testStream(input)(
        StartStream(checkpointLocation = dir.getAbsolutePath,
          trigger = Trigger.ProcessingTime(10), triggerClock = clock),
        waitUntilBatchProcessed(clock),
        CheckLastBatch(0 until 10: _*),
        AdvanceManualClock(10),
        waitUntilBatchProcessed(clock),
        CheckLastBatch(10 until 20: _*),
        StopStream
      )

      testStream(input)(
        StartStream(checkpointLocation = dir.getAbsolutePath,
          trigger = Trigger.ProcessingTime(10), triggerClock = clock),
        waitUntilBatchProcessed(clock),
        CheckLastBatch(20 until 30: _*)
      )
    }
  }

  test("Trigger.Once") {
    // NOTE: the test uses the deprecated Trigger.Once() by intention, do not change.
    testTrigger(Trigger.Once())
  }

  test("Trigger.AvailableNow") {
    testTrigger(Trigger.AvailableNow())
  }

  private def testTrigger(triggerToTest: Trigger): Unit = {
    withTempDir { dir =>
      val input = spark.readStream
        .format("rate-micro-batch")
        .option("rowsPerBatch", "10")
        .load()
        .select($"value")

      val clock = new StreamManualClock
      testStream(input)(
        StartStream(checkpointLocation = dir.getAbsolutePath,
          trigger = Trigger.ProcessingTime(10), triggerClock = clock),
        waitUntilBatchProcessed(clock),
        CheckLastBatch(0 until 10: _*),
        StopStream
      )

      testStream(input)(
        StartStream(checkpointLocation = dir.getAbsolutePath, trigger = triggerToTest,
          triggerClock = clock),
        ProcessAllAvailable(),
        CheckLastBatch(10 until 20: _*),
        StopStream
      )

      testStream(input)(
        StartStream(checkpointLocation = dir.getAbsolutePath,
          trigger = Trigger.ProcessingTime(10), triggerClock = clock),
        waitUntilBatchProcessed(clock),
        CheckLastBatch(20 until 30: _*),
        StopStream
      )
    }
  }

  private def waitUntilBatchProcessed(clock: StreamManualClock) = AssertOnQuery { q =>
    eventually(Timeout(streamingTimeout)) {
      if (q.exception.isEmpty) {
        assert(clock.isStreamWaitingAt(clock.getTimeMillis()))
      }
    }
    if (q.exception.isDefined) {
      throw q.exception.get
    }
    true
  }

  test("numPartitions") {
    val input = spark.readStream
      .format("rate-micro-batch")
      .option("rowsPerBatch", "10")
      .option("numPartitions", "6")
      .load()
      .select(spark_partition_id())
      .distinct()
    val clock = new StreamManualClock
    testStream(input)(
      StartStream(trigger = Trigger.ProcessingTime(10), triggerClock = clock),
      waitUntilBatchProcessed(clock),
      CheckLastBatch(0 until 6: _*)
    )
  }

  testQuietly("illegal option values") {
    def testIllegalOptionValue(
        option: String,
        value: String,
        expectedMessages: Seq[String]): Unit = {
      val e = intercept[IllegalArgumentException] {
        var stream = spark.readStream
          .format("rate-micro-batch")
          .option(option, value)

        if (option != "rowsPerBatch") {
          stream = stream.option("rowsPerBatch", "1")
        }

        stream.load()
          .writeStream
          .format("console")
          .start()
          .awaitTermination()
      }
      for (msg <- expectedMessages) {
        assert(e.getMessage.contains(msg))
      }
    }

    testIllegalOptionValue("rowsPerBatch", "-1", Seq("-1", "rowsPerBatch", "positive"))
    testIllegalOptionValue("rowsPerBatch", "0", Seq("0", "rowsPerBatch", "positive"))
    testIllegalOptionValue("numPartitions", "-1", Seq("-1", "numPartitions", "positive"))
    testIllegalOptionValue("numPartitions", "0", Seq("0", "numPartitions", "positive"))

    // RatePerMicroBatchProvider allows setting below options to 0
    testIllegalOptionValue("advanceMillisPerBatch", "-1",
      Seq("-1", "advanceMillisPerBatch", "non-negative"))
    testIllegalOptionValue("startTimestamp", "-1", Seq("-1", "startTimestamp", "non-negative"))
  }

  test("user-specified schema given") {
    checkError(
      exception = intercept[SparkUnsupportedOperationException] {
        spark.readStream
          .format("rate-micro-batch")
          .option("rowsPerBatch", "10")
          .schema(spark.range(1).schema)
          .load()
      },
      condition = "_LEGACY_ERROR_TEMP_2242",
      parameters = Map("provider" -> "RatePerMicroBatchProvider"))
  }

  test("malformed state when the query is restarted with a newer" +
    " timestamp and reprocess batch 0") {
    withTempDir { ckpt =>
      var firstFailure = true
      def foreachBatchFn(df: DataFrame, batchId: Long): Unit = {
        if (firstFailure) {
          firstFailure = false
          throw new Exception("fail this run")
        }
      }

      try {
        spark.readStream
          .format("rate-micro-batch")
          .option("rowsPerBatch", "1")
          .load()
          .writeStream
          .option("checkpointLocation", ckpt.getAbsolutePath)
          .foreachBatch(foreachBatchFn _)
          .start()
          .awaitTermination()
      } catch {
        case _: Throwable =>
        // ignore
      }

      val ex = intercept[StreamingQueryException] {
        spark.readStream
         .format("rate-micro-batch")
         .option("rowsPerBatch", "1")
         .option("startTimestamp", System.currentTimeMillis().toString)
         .load()
         .writeStream
         .option("checkpointLocation", ckpt.getAbsolutePath)
         .foreachBatch(foreachBatchFn _)
         .start()
         .awaitTermination()
      }
      assert(
        ex.getCause.asInstanceOf[SparkThrowable].getCondition ==
        "MALFORMED_STATE_IN_RATE_PER_MICRO_BATCH_SOURCE.INVALID_TIMESTAMP"
      )
    }
  }
}
