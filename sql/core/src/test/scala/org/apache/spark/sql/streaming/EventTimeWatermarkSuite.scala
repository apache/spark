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

import java.{util => ju}
import java.io.File
import java.text.SimpleDateFormat
import java.util.{Calendar, Date, Locale}
import java.util.concurrent.TimeUnit._

import org.apache.commons.io.FileUtils
import org.scalatest.BeforeAndAfter
import org.scalatest.matchers.must.Matchers
import org.scalatest.matchers.should.Matchers._

import org.apache.spark.SparkException
import org.apache.spark.internal.Logging
import org.apache.spark.sql.{AnalysisException, DataFrame, Dataset}
import org.apache.spark.sql.catalyst.plans.logical.EventTimeWatermark
import org.apache.spark.sql.catalyst.util.DateTimeConstants._
import org.apache.spark.sql.catalyst.util.DateTimeTestUtils.UTC
import org.apache.spark.sql.execution.streaming._
import org.apache.spark.sql.execution.streaming.sources.MemorySink
import org.apache.spark.sql.functions.{count, expr, timestamp_seconds, window}
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.streaming.OutputMode._
import org.apache.spark.tags.SlowSQLTest
import org.apache.spark.util.Utils

@SlowSQLTest
class EventTimeWatermarkSuite extends StreamTest with BeforeAndAfter with Matchers with Logging {

  import testImplicits._

  after {
    sqlContext.streams.active.foreach(_.stop())
  }

  private val epsilon = 10E-6

  test("EventTimeStats") {
    val stats = EventTimeStats(max = 100, min = 10, avg = 20.0, count = 5)
    stats.add(80L)
    stats.max should be (100)
    stats.min should be (10)
    stats.avg should be (30.0 +- epsilon)
    stats.count should be (6)

    val stats2 = EventTimeStats(80L, 5L, 15.0, 4)
    stats.merge(stats2)
    stats.max should be (100)
    stats.min should be (5)
    stats.avg should be (24.0 +- epsilon)
    stats.count should be (10)
  }

  test("EventTimeStats: avg on large values") {
    val largeValue = 10000000000L // 10B
    // Make sure `largeValue` will cause overflow if we use a Long sum to calc avg.
    assert(largeValue * largeValue != BigInt(largeValue) * BigInt(largeValue))
    val stats =
      EventTimeStats(max = largeValue, min = largeValue, avg = largeValue, count = largeValue - 1)
    stats.add(largeValue)
    stats.avg should be (largeValue.toDouble +- epsilon)

    val stats2 = EventTimeStats(
      max = largeValue + 1,
      min = largeValue,
      avg = largeValue + 1,
      count = largeValue)
    stats.merge(stats2)
    stats.avg should be ((largeValue + 0.5) +- epsilon)
  }

  test("EventTimeStats: zero merge zero") {
    val stats = EventTimeStats.zero
    val stats2 = EventTimeStats.zero
    stats.merge(stats2)
    stats should be (EventTimeStats.zero)
  }

  test("EventTimeStats: non-zero merge zero") {
    val stats = EventTimeStats(max = 10, min = 1, avg = 5.0, count = 3)
    val stats2 = EventTimeStats.zero
    stats.merge(stats2)
    stats.max should be (10L)
    stats.min should be (1L)
    stats.avg should be (5.0 +- epsilon)
    stats.count should be (3L)
  }

  test("EventTimeStats: zero merge non-zero") {
    val stats = EventTimeStats.zero
    val stats2 = EventTimeStats(max = 10, min = 1, avg = 5.0, count = 3)
    stats.merge(stats2)
    stats.max should be (10L)
    stats.min should be (1L)
    stats.avg should be (5.0 +- epsilon)
    stats.count should be (3L)
  }

  test("error on bad column") {
    val inputData = MemoryStream[Int].toDF()
    val e = intercept[AnalysisException] {
      inputData.withWatermark("badColumn", "1 minute")
    }
    assert(e.getMessage contains "badColumn")
  }

  test("error on wrong type") {
    val inputData = MemoryStream[Int].toDF()
    val e = intercept[AnalysisException] {
      inputData.withWatermark("value", "1 minute")
    }
    assert(e.getMessage contains "value")
    assert(e.getMessage contains "int")
  }

  test("event time and watermark metrics") {
    // No event time metrics when there is no watermarking
    val inputData1 = MemoryStream[Int]
    val aggWithoutWatermark = inputData1.toDF()
      .withColumn("eventTime", timestamp_seconds($"value"))
      .groupBy(window($"eventTime", "5 seconds") as Symbol("window"))
      .agg(count("*") as Symbol("count"))
      .select($"window".getField("start").cast("long").as[Long], $"count".as[Long])

    testStream(aggWithoutWatermark, outputMode = Complete)(
      AddData(inputData1, 15),
      CheckAnswer((15, 1)),
      assertEventStats { e => assert(e.isEmpty) },
      AddData(inputData1, 10, 12, 14),
      CheckAnswer((10, 3), (15, 1)),
      assertEventStats { e => assert(e.isEmpty) }
    )

    // All event time metrics where watermarking is set
    val inputData2 = MemoryStream[Int]
    val aggWithWatermark = inputData2.toDF()
        .withColumn("eventTime", timestamp_seconds($"value"))
        .withWatermark("eventTime", "10 seconds")
        .groupBy(window($"eventTime", "5 seconds") as Symbol("window"))
        .agg(count("*") as Symbol("count"))
        .select($"window".getField("start").cast("long").as[Long], $"count".as[Long])

    testStream(aggWithWatermark)(
      AddData(inputData2, 15),
      CheckAnswer(),
      assertEventStats(min = 15, max = 15, avg = 15, wtrmark = 0),
      AddData(inputData2, 10, 12, 14),
      CheckAnswer(),
      assertEventStats(min = 10, max = 14, avg = 12, wtrmark = 5),
      AddData(inputData2, 25),
      CheckAnswer((10, 3)),
      assertEventStats(min = 25, max = 25, avg = 25, wtrmark = 5)
    )
  }

  test("event time and watermark metrics with Trigger.Once (SPARK-24699)") {
    // All event time metrics where watermarking is set
    val inputData = MemoryStream[Int]
    val aggWithWatermark = inputData.toDF()
        .withColumn("eventTime", timestamp_seconds($"value"))
        .withWatermark("eventTime", "10 seconds")
        .groupBy(window($"eventTime", "5 seconds") as Symbol("window"))
        .agg(count("*") as Symbol("count"))
        .select($"window".getField("start").cast("long").as[Long], $"count".as[Long])

    // Unlike the ProcessingTime trigger, Trigger.Once only runs one trigger every time
    // the query is started and it does not run no-data batches. Hence the answer generated
    // by the updated watermark is only generated the next time the query is started.
    // Also, the data to process in the next trigger is added *before* starting the stream in
    // Trigger.Once to ensure that first and only trigger picks up the new data.

    // NOTE: the test uses the deprecated Trigger.Once() by intention, do not change.

    testStream(aggWithWatermark)(
      StartStream(Trigger.Once),  // to make sure the query is not running when adding data 1st time
      awaitTermination(),

      AddData(inputData, 15),
      StartStream(Trigger.Once),
      awaitTermination(),
      CheckNewAnswer(),
      assertEventStats(min = 15, max = 15, avg = 15, wtrmark = 0),
      // watermark should be updated to 15 - 10 = 5

      AddData(inputData, 10, 12, 14),
      StartStream(Trigger.Once),
      awaitTermination(),
      CheckNewAnswer(),
      assertEventStats(min = 10, max = 14, avg = 12, wtrmark = 5),
      // watermark should stay at 5

      AddData(inputData, 25),
      StartStream(Trigger.Once),
      awaitTermination(),
      CheckNewAnswer(),
      assertEventStats(min = 25, max = 25, avg = 25, wtrmark = 5),
      // watermark should be updated to 25 - 10 = 15

      AddData(inputData, 50),
      StartStream(Trigger.Once),
      awaitTermination(),
      CheckNewAnswer((10, 3)),   // watermark = 15 is used to generate this
      assertEventStats(min = 50, max = 50, avg = 50, wtrmark = 15),
      // watermark should be updated to 50 - 10 = 40

      AddData(inputData, 50),
      StartStream(Trigger.Once),
      awaitTermination(),
      CheckNewAnswer((15, 1), (25, 1)), // watermark = 40 is used to generate this
      assertEventStats(min = 50, max = 50, avg = 50, wtrmark = 40))
  }

  test("recovery from Spark ver 2.3.1 commit log without commit metadata (SPARK-24699)") {
    // All event time metrics where watermarking is set
    val inputData = MemoryStream[Int]
    val aggWithWatermark = inputData.toDF()
        .withColumn("eventTime", timestamp_seconds($"value"))
        .withWatermark("eventTime", "10 seconds")
        .groupBy(window($"eventTime", "5 seconds") as Symbol("window"))
        .agg(count("*") as Symbol("count"))
        .select($"window".getField("start").cast("long").as[Long], $"count".as[Long])


    val resourceUri = this.getClass.getResource(
      "/structured-streaming/checkpoint-version-2.3.1-without-commit-log-metadata/").toURI

    val checkpointDir = Utils.createTempDir().getCanonicalFile
    // Copy the checkpoint to a temp dir to prevent changes to the original.
    // Not doing this will lead to the test passing on the first run, but fail subsequent runs.
    FileUtils.copyDirectory(new File(resourceUri), checkpointDir)

    inputData.addData(15)
    inputData.addData(10, 12, 14)

    testStream(aggWithWatermark)(
      /*

      Note: The checkpoint was generated using the following input in Spark version 2.3.1

      StartStream(checkpointLocation = "./sql/core/src/test/resources/structured-streaming/" +
        "checkpoint-version-2.3.1-without-commit-log-metadata/")),
      AddData(inputData, 15),  // watermark should be updated to 15 - 10 = 5
      CheckAnswer(),
      AddData(inputData, 10, 12, 14),  // watermark should stay at 5
      CheckAnswer(),
      StopStream,

      // Offset log should have watermark recorded as 5.
      */

      StartStream(Trigger.AvailableNow),
      awaitTermination(),

      AddData(inputData, 25),
      StartStream(Trigger.AvailableNow, checkpointLocation = checkpointDir.getAbsolutePath),
      awaitTermination(),
      CheckNewAnswer((10, 3)), // watermark should be updated to 25 - 10 = 15

      AddData(inputData, 50),
      StartStream(Trigger.AvailableNow, checkpointLocation = checkpointDir.getAbsolutePath),
      awaitTermination(),
      CheckNewAnswer((15, 1), (25, 1)), // watermark should be updated to 50 - 10 = 40

      AddData(inputData, 50),
      StartStream(Trigger.AvailableNow, checkpointLocation = checkpointDir.getAbsolutePath),
      awaitTermination(),
      CheckNewAnswer()
    )
  }

  test("append mode") {
    val inputData = MemoryStream[Int]

    val windowedAggregation = inputData.toDF()
      .withColumn("eventTime", timestamp_seconds($"value"))
      .withWatermark("eventTime", "10 seconds")
      .groupBy(window($"eventTime", "5 seconds") as Symbol("window"))
      .agg(count("*") as Symbol("count"))
      .select($"window".getField("start").cast("long").as[Long], $"count".as[Long])

    testStream(windowedAggregation)(
      AddData(inputData, 10, 11, 12, 13, 14, 15),
      CheckNewAnswer(),
      AddData(inputData, 25),   // Advance watermark to 15 seconds
      CheckNewAnswer((10, 5)),
      assertNumStateRows(2),
      assertNumRowsDroppedByWatermark(0),
      AddData(inputData, 10),   // Should not emit anything as data less than watermark
      CheckNewAnswer(),
      assertNumStateRows(2),
      assertNumRowsDroppedByWatermark(1)
    )
  }

  test("update mode") {
    val inputData = MemoryStream[Int]
    spark.conf.set(SQLConf.SHUFFLE_PARTITIONS.key, "10")

    val windowedAggregation = inputData.toDF()
      .withColumn("eventTime", timestamp_seconds($"value"))
      .withWatermark("eventTime", "10 seconds")
      .groupBy(window($"eventTime", "5 seconds") as Symbol("window"))
      .agg(count("*") as Symbol("count"))
      .select($"window".getField("start").cast("long").as[Long], $"count".as[Long])

    testStream(windowedAggregation, OutputMode.Update)(
      AddData(inputData, 10, 11, 12, 13, 14, 15),
      CheckNewAnswer((10, 5), (15, 1)),
      AddData(inputData, 25),     // Advance watermark to 15 seconds
      CheckNewAnswer((25, 1)),
      assertNumStateRows(2),
      assertNumRowsDroppedByWatermark(0),
      AddData(inputData, 10, 25), // Ignore 10 as its less than watermark
      CheckNewAnswer((25, 2)),
      assertNumStateRows(2),
      assertNumRowsDroppedByWatermark(1),
      AddData(inputData, 10),     // Should not emit anything as data less than watermark
      CheckNewAnswer(),
      assertNumStateRows(2),
      assertNumRowsDroppedByWatermark(1)
    )
  }

  test("delay in months and years handled correctly") {
    val currentTimeMs = System.currentTimeMillis
    val currentTime = new Date(currentTimeMs)

    val input = MemoryStream[Long]
    val aggWithWatermark = input.toDF()
      .withColumn("eventTime", timestamp_seconds($"value"))
      .withWatermark("eventTime", "2 years 5 months")
      .groupBy(window($"eventTime", "5 seconds") as Symbol("window"))
      .agg(count("*") as Symbol("count"))
      .select($"window".getField("start").cast("long").as[Long], $"count".as[Long])

    def monthsSinceEpoch(date: Date): Int = {
      val cal = Calendar.getInstance()
      cal.setTime(date)
      cal.get(Calendar.YEAR) * 12 + cal.get(Calendar.MONTH)
    }

    testStream(aggWithWatermark)(
      AddData(input, MILLISECONDS.toSeconds(currentTimeMs)),
      CheckAnswer(),
      AddData(input, MILLISECONDS.toSeconds(currentTimeMs)),
      CheckAnswer(),
      assertEventStats { e =>
        assert(timestampFormat.parse(e.get("max")).getTime ===
          SECONDS.toMillis(MILLISECONDS.toSeconds((currentTimeMs))))
        val watermarkTime = timestampFormat.parse(e.get("watermark"))
        val monthDiff = monthsSinceEpoch(currentTime) - monthsSinceEpoch(watermarkTime)
        // monthsSinceEpoch is like `math.floor(num)`, so monthDiff has two possible values.
        assert(monthDiff === 29 || monthDiff === 30,
          s"currentTime: $currentTime, watermarkTime: $watermarkTime")
      }
    )
  }

  test("recovery") {
    val inputData = MemoryStream[Int]
    val df = inputData.toDF()
      .withColumn("eventTime", timestamp_seconds($"value"))
      .withWatermark("eventTime", "10 seconds")
      .groupBy(window($"eventTime", "5 seconds") as Symbol("window"))
      .agg(count("*") as Symbol("count"))
      .select($"window".getField("start").cast("long").as[Long], $"count".as[Long])

    testStream(df)(
      AddData(inputData, 10, 11, 12, 13, 14, 15),
      CheckAnswer(),
      AddData(inputData, 25), // Advance watermark to 15 seconds
      CheckAnswer((10, 5)),
      StopStream,
      AssertOnQuery { q => // purge commit and clear the sink
        val commit = q.commitLog.getLatest().map(_._1).getOrElse(-1L)
        q.commitLog.purge(commit)
        q.sink.asInstanceOf[MemorySink].clear()
        true
      },
      StartStream(),
      AddData(inputData, 10, 27, 30), // Advance watermark to 20 seconds, 10 should be ignored
      CheckAnswer((15, 1)),
      StopStream,
      StartStream(),
      AddData(inputData, 17), // Watermark should still be 20 seconds, 17 should be ignored
      CheckAnswer((15, 1)),
      AddData(inputData, 40), // Advance watermark to 30 seconds, emit first data 25
      CheckNewAnswer((25, 2))
    )
  }

  test("watermark with 2 streams") {
    import org.apache.spark.sql.functions.sum
    val first = MemoryStream[Int]

    val firstDf = first.toDF()
      .withColumn("eventTime", timestamp_seconds($"value"))
      .withWatermark("eventTime", "10 seconds")
      .select($"value")

    val second = MemoryStream[Int]

    val secondDf = second.toDF()
      .withColumn("eventTime", timestamp_seconds($"value"))
      .withWatermark("eventTime", "5 seconds")
      .select($"value")

    withTempDir { checkpointDir =>
      val unionWriter = firstDf.union(secondDf).agg(sum($"value"))
        .writeStream
        .option("checkpointLocation", checkpointDir.getCanonicalPath)
        .format("memory")
        .outputMode("complete")
        .queryName("test")

      val union = unionWriter.start()

      def getWatermarkAfterData(
                                 firstData: Seq[Int] = Seq.empty,
                                 secondData: Seq[Int] = Seq.empty,
                                 query: StreamingQuery = union): Long = {
        if (firstData.nonEmpty) first.addData(firstData)
        if (secondData.nonEmpty) second.addData(secondData)
        query.processAllAvailable()
        // add a dummy batch so lastExecution has the new watermark
        first.addData(0)
        query.processAllAvailable()
        // get last watermark
        val lastExecution = query.asInstanceOf[StreamingQueryWrapper].streamingQuery.lastExecution
        lastExecution.offsetSeqMetadata.batchWatermarkMs
      }

      // Global watermark starts at 0 until we get data from both sides
      assert(getWatermarkAfterData(firstData = Seq(11)) == 0)
      assert(getWatermarkAfterData(secondData = Seq(6)) == 1000)
      // Global watermark stays at left watermark 1 when right watermark moves to 2
      assert(getWatermarkAfterData(secondData = Seq(8)) == 1000)
      // Global watermark switches to right side value 2 when left watermark goes higher
      assert(getWatermarkAfterData(firstData = Seq(21)) == 3000)
      // Global watermark goes back to left
      assert(getWatermarkAfterData(secondData = Seq(17, 28, 39)) == 11000)
      // Global watermark stays on left as long as it's below right
      assert(getWatermarkAfterData(firstData = Seq(31)) == 21000)
      assert(getWatermarkAfterData(firstData = Seq(41)) == 31000)
      // Global watermark switches back to right again
      assert(getWatermarkAfterData(firstData = Seq(51)) == 34000)

      // Global watermark is updated correctly with simultaneous data from both sides
      assert(getWatermarkAfterData(firstData = Seq(100), secondData = Seq(100)) == 90000)
      assert(getWatermarkAfterData(firstData = Seq(120), secondData = Seq(110)) == 105000)
      assert(getWatermarkAfterData(firstData = Seq(130), secondData = Seq(125)) == 120000)

      // Global watermark doesn't decrement with simultaneous data
      assert(getWatermarkAfterData(firstData = Seq(100), secondData = Seq(100)) == 120000)
      assert(getWatermarkAfterData(firstData = Seq(140), secondData = Seq(100)) == 120000)
      assert(getWatermarkAfterData(firstData = Seq(100), secondData = Seq(135)) == 130000)

      // Global watermark recovers after restart, but left side watermark ahead of it does not.
      assert(getWatermarkAfterData(firstData = Seq(200), secondData = Seq(190)) == 185000)
      union.stop()
      val union2 = unionWriter.start()
      assert(getWatermarkAfterData(query = union2) == 185000)
      // Even though the left side was ahead of 185000 in the last execution, the watermark won't
      // increment until it gets past it in this execution.
      assert(getWatermarkAfterData(secondData = Seq(200), query = union2) == 185000)
      assert(getWatermarkAfterData(firstData = Seq(200), query = union2) == 190000)
    }
  }

  test("complete mode") {
    val inputData = MemoryStream[Int]

    val windowedAggregation = inputData.toDF()
        .withColumn("eventTime", timestamp_seconds($"value"))
        .withWatermark("eventTime", "10 seconds")
        .groupBy(window($"eventTime", "5 seconds") as Symbol("window"))
        .agg(count("*") as Symbol("count"))
        .select($"window".getField("start").cast("long").as[Long], $"count".as[Long])

    // No eviction when asked to compute complete results.
    testStream(windowedAggregation, OutputMode.Complete)(
      AddData(inputData, 10, 11, 12),
      CheckAnswer((10, 3)),
      AddData(inputData, 25),
      CheckAnswer((10, 3), (25, 1)),
      AddData(inputData, 25),
      CheckAnswer((10, 3), (25, 2)),
      AddData(inputData, 10),
      CheckAnswer((10, 4), (25, 2)),
      AddData(inputData, 25),
      CheckAnswer((10, 4), (25, 3))
    )
  }

  test("group by on raw timestamp") {
    val inputData = MemoryStream[Int]

    val windowedAggregation = inputData.toDF()
        .withColumn("eventTime", timestamp_seconds($"value"))
        .withWatermark("eventTime", "10 seconds")
        .groupBy($"eventTime")
        .agg(count("*") as Symbol("count"))
        .select($"eventTime".cast("long").as[Long], $"count".as[Long])

    testStream(windowedAggregation)(
      AddData(inputData, 10),
      CheckAnswer(),
      AddData(inputData, 25), // Advance watermark to 15 seconds
      CheckAnswer((10, 1))
    )
  }

  test("delay threshold should not be negative.") {
    val inputData = MemoryStream[Int].toDF()
    var e = intercept[IllegalArgumentException] {
      inputData.withWatermark("value", "-1 year")
    }
    assert(e.getMessage contains "should not be negative.")

    e = intercept[IllegalArgumentException] {
      inputData.withWatermark("value", "1 year -13 months")
    }
    assert(e.getMessage contains "should not be negative.")

    e = intercept[IllegalArgumentException] {
      inputData.withWatermark("value", "1 month -40 days")
    }
    assert(e.getMessage contains "should not be negative.")

    e = intercept[IllegalArgumentException] {
      inputData.withWatermark("value", "-10 seconds")
    }
    assert(e.getMessage contains "should not be negative.")
  }

  private def buildTestQueryForOverridingWatermark(): (MemoryStream[(Long, Long)], DataFrame) = {
    val input = MemoryStream[(Long, Long)]
    val df = input.toDF()
      .withColumn("first", timestamp_seconds($"_1"))
      .withColumn("second", timestamp_seconds($"_2"))
      .withWatermark("first", "1 minute")
      .select("*")
      .withWatermark("second", "2 minutes")
      .groupBy(window($"second", "1 minute"))
      .count()

    (input, df)
  }

  test("overriding watermark should not be allowed by default") {
    val (input, df) = buildTestQueryForOverridingWatermark()
    testStream(df)(
      AddData(input, (100L, 200L)),
      ExpectFailure[AnalysisException](assertFailure = exc => {
        assert(exc.getMessage.contains("Redefining watermark is disallowed."))
        assert(exc.getMessage.contains(SQLConf.STATEFUL_OPERATOR_ALLOW_MULTIPLE.key))
      })
    )
  }

  test("overriding watermark should not fail in compatibility mode") {
    withSQLConf(SQLConf.STATEFUL_OPERATOR_ALLOW_MULTIPLE.key -> "false") {
      val (input, df) = buildTestQueryForOverridingWatermark()
      testStream(df)(
        AddData(input, (100L, 200L)),
        CheckAnswer(),
        Execute { query =>
          val lastExecution = query.lastExecution
          val aggSaveOperator = lastExecution.executedPlan.collect {
            case j: StateStoreSaveExec => j
          }.head

          // - watermark from first definition = 100 - 60 = 40
          // - watermark from second definition = 200 - 120 = 80
          // - global watermark = min(40, 60) = 40
          //
          // As we see the result, even though we override the watermark definition, the old
          // definition of watermark still plays to calculate global watermark.
          //
          // This is conceptually the right behavior. For operators after the first watermark
          // definition, the column named "first" is considered as event time column, and for
          // operators after the second watermark definition, the column named "second" is
          // considered as event time column. The correct "single" value of watermark satisfying
          // all operators should be lower bound of both columns "first" and "second".
          //
          // That said, this easily leads to incorrect definition - e.g. re-define watermark
          // against the output of streaming aggregation for append mode. The global watermark
          // cannot advance. This is the reason we don't allow re-define watermark in new behavior.
          val expectedWatermarkMs = 40 * 1000

          assert(aggSaveOperator.eventTimeWatermarkForLateEvents === Some(expectedWatermarkMs))
          assert(aggSaveOperator.eventTimeWatermarkForEviction === Some(expectedWatermarkMs))

          val eventTimeCols = aggSaveOperator.keyExpressions.filter(
            _.metadata.contains(EventTimeWatermark.delayKey))
          assert(eventTimeCols.size === 1)
          assert(eventTimeCols.head.name === "window")
          // 2 minutes delay threshold
          assert(eventTimeCols.head.metadata.getLong(EventTimeWatermark.delayKey) === 120 * 1000)
        }
      )
    }
  }

  private def buildTestQueryForMultiEventTimeColumns()
    : (MemoryStream[(String, Long)], MemoryStream[(String, Long)], DataFrame) = {
    val input1 = MemoryStream[(String, Long)]
    val input2 = MemoryStream[(String, Long)]
    val df1 = input1.toDF()
      .selectExpr("_1 AS id1", "timestamp_seconds(_2) AS ts1")
      .withWatermark("ts1", "1 minute")

    val df2 = input2.toDF()
      .selectExpr("_1 AS id2", "timestamp_seconds(_2) AS ts2")
      .withWatermark("ts2", "2 minutes")

    val joined = df1.join(df2, expr("id1 = id2 AND ts1 = ts2 + INTERVAL 10 SECONDS"), "inner")
      .selectExpr("id1", "ts1", "ts2")
    // the output of join contains both ts1 and ts2
    val dedup = joined.dropDuplicates()
      .selectExpr("id1", "CAST(ts1 AS LONG) AS ts1", "CAST(ts2 AS LONG) AS ts2")

    (input1, input2, dedup)
  }

  test("multiple event time columns in an input DataFrame for stateful operator is " +
    "not allowed") {
    // for ease of verification, we change the session timezone to UTC
    withSQLConf(SQLConf.SESSION_LOCAL_TIMEZONE.key -> "UTC") {
      val (input1, input2, dedup) = buildTestQueryForMultiEventTimeColumns()
      testStream(dedup)(
        MultiAddData(
          (input1, Seq(("A", 200L), ("B", 300L))),
          (input2, Seq(("A", 190L), ("C", 350L)))
        ),
        ExpectFailure[SparkException](assertFailure = exc => {
          val cause = exc.getCause
          assert(cause.getMessage.contains("More than one event time columns are available."))
          assert(cause.getMessage.contains(
            "Please ensure there is at most one event time column per stream."))
        })
      )
    }
  }

  test("stateful operator should pick the first occurrence of event time column if there is " +
    "multiple event time columns in compatibility mode") {
    // for ease of verification, we change the session timezone to UTC
    withSQLConf(
      SQLConf.STATEFUL_OPERATOR_ALLOW_MULTIPLE.key -> "false",
      SQLConf.SESSION_LOCAL_TIMEZONE.key -> "UTC") {
      val (input1, input2, dedup) = buildTestQueryForMultiEventTimeColumns()
      testStream(dedup)(
        MultiAddData(
          (input1, Seq(("A", 200L), ("B", 300L))),
          (input2, Seq(("A", 190L), ("C", 350L)))
        ),
        CheckAnswer(("A", 200L, 190L))
      )
    }
  }

  test("EventTime watermark should be ignored in batch query.") {
    val df = testData
      .withColumn("eventTime", timestamp_seconds($"key"))
      .withWatermark("eventTime", "1 minute")
      .select("eventTime")
      .as[Long]

    checkDataset[Long](df, 1L to 100L: _*)
  }

  test("SPARK-21565: watermark operator accepts attributes from replacement") {
    withTempDir { dir =>
      dir.delete()

      val df = Seq(("a", 100.0, new java.sql.Timestamp(100L)))
        .toDF("symbol", "price", "eventTime")
      df.write.json(dir.getCanonicalPath)

      val input = spark.readStream.schema(df.schema)
        .json(dir.getCanonicalPath)

      val groupEvents = input
        .withWatermark("eventTime", "2 seconds")
        .groupBy("symbol", "eventTime")
        .agg(count("price") as Symbol("count"))
        .select("symbol", "eventTime", "count")
      val q = groupEvents.writeStream
        .outputMode("append")
        .format("console")
        .start()
      try {
        q.processAllAvailable()
      } finally {
        q.stop()
      }
    }
  }

  test("SPARK-27340 Alias on TimeWindow expression cause watermark metadata lost") {
    val inputData = MemoryStream[Int]
    val aliasWindow = inputData.toDF()
      .withColumn("eventTime", timestamp_seconds($"value"))
      .withWatermark("eventTime", "10 seconds")
      .select(window($"eventTime", "5 seconds") as Symbol("aliasWindow"))
    // Check the eventTime metadata is kept in the top level alias.
    assert(aliasWindow.logicalPlan.output.exists(
      _.metadata.contains(EventTimeWatermark.delayKey)))

    val windowedAggregation = aliasWindow
      .groupBy($"aliasWindow")
      .agg(count("*") as Symbol("count"))
      .select($"aliasWindow".getField("start").cast("long").as[Long], $"count".as[Long])

    testStream(windowedAggregation)(
      AddData(inputData, 10, 11, 12, 13, 14, 15),
      CheckNewAnswer(),
      AddData(inputData, 25), // Advance watermark to 15 seconds
      CheckNewAnswer((10, 5)),
      assertNumStateRows(2),
      AddData(inputData, 10), // Should not emit anything as data less than watermark
      CheckNewAnswer(),
      assertNumStateRows(2)
    )
  }

  test("test no-data flag") {
    val flagKey = SQLConf.STREAMING_NO_DATA_MICRO_BATCHES_ENABLED.key

    def testWithFlag(flag: Boolean): Unit = withClue(s"with $flagKey = $flag") {
      val inputData = MemoryStream[Int]
      val windowedAggregation = inputData.toDF()
        .withColumn("eventTime", timestamp_seconds($"value"))
        .withWatermark("eventTime", "10 seconds")
        .groupBy(window($"eventTime", "5 seconds") as Symbol("window"))
        .agg(count("*") as Symbol("count"))
        .select($"window".getField("start").cast("long").as[Long], $"count".as[Long])

      testStream(windowedAggregation)(
        StartStream(additionalConfs = Map(flagKey -> flag.toString)),
        AddData(inputData, 10, 11, 12, 13, 14, 15),
        CheckNewAnswer(),
        AddData(inputData, 25), // Advance watermark to 15 seconds
        // Check if there is new answer if flag is set, no new answer otherwise
        if (flag) CheckNewAnswer((10, 5)) else CheckNewAnswer()
      )
    }

    testWithFlag(true)
    testWithFlag(false)
  }

  test("MultipleWatermarkPolicy: max") {
    val input1 = MemoryStream[Int]
    val input2 = MemoryStream[Int]

    withSQLConf(SQLConf.STREAMING_MULTIPLE_WATERMARK_POLICY.key -> "max") {
      testStream(dfWithMultipleWatermarks(input1, input2))(
        MultiAddData(input1, 20)(input2, 30),
        CheckLastBatch(20, 30),
        checkWatermark(input1, 15), // max(20 - 10, 30 - 15) = 15
        StopStream,
        StartStream(),
        checkWatermark(input1, 15), // watermark recovered correctly
        MultiAddData(input1, 120)(input2, 130),
        CheckLastBatch(120, 130),
        checkWatermark(input1, 115), // max(120 - 10, 130 - 15) = 115, policy recovered correctly
        AddData(input1, 150),
        CheckLastBatch(150),
        checkWatermark(input1, 140)  // should advance even if one of the input has data
      )
    }
  }

  test("MultipleWatermarkPolicy: min") {
    val input1 = MemoryStream[Int]
    val input2 = MemoryStream[Int]

    withSQLConf(SQLConf.STREAMING_MULTIPLE_WATERMARK_POLICY.key -> "min") {
      testStream(dfWithMultipleWatermarks(input1, input2))(
        MultiAddData(input1, 20)(input2, 30),
        CheckLastBatch(20, 30),
        checkWatermark(input1, 10), // min(20 - 10, 30 - 15) = 10
        StopStream,
        StartStream(),
        checkWatermark(input1, 10), // watermark recovered correctly
        MultiAddData(input1, 120)(input2, 130),
        CheckLastBatch(120, 130),
        checkWatermark(input2, 110), // min(120 - 10, 130 - 15) = 110, policy recovered correctly
        AddData(input2, 150),
        CheckLastBatch(150),
        checkWatermark(input2, 110)  // does not advance when only one of the input has data
      )
    }
  }

  test("MultipleWatermarkPolicy: recovery from checkpoints ignores session conf") {
    val input1 = MemoryStream[Int]
    val input2 = MemoryStream[Int]

    val checkpointDir = Utils.createTempDir().getCanonicalFile
    withSQLConf(SQLConf.STREAMING_MULTIPLE_WATERMARK_POLICY.key -> "max") {
      testStream(dfWithMultipleWatermarks(input1, input2))(
        StartStream(checkpointLocation = checkpointDir.getAbsolutePath),
        MultiAddData(input1, 20)(input2, 30),
        CheckLastBatch(20, 30),
        checkWatermark(input1, 15) // max(20 - 10, 30 - 15) = 15
      )
    }

    withSQLConf(SQLConf.STREAMING_MULTIPLE_WATERMARK_POLICY.key -> "min") {
      testStream(dfWithMultipleWatermarks(input1, input2))(
        StartStream(checkpointLocation = checkpointDir.getAbsolutePath),
        checkWatermark(input1, 15), // watermark recovered correctly
        MultiAddData(input1, 120)(input2, 130),
        CheckLastBatch(120, 130),
        checkWatermark(input1, 115), // max(120 - 10, 130 - 15) = 115, policy recovered correctly
        AddData(input1, 150),
        CheckLastBatch(150),
        checkWatermark(input1, 140) // should advance even if one of the input has data
      )
    }
  }

  test("MultipleWatermarkPolicy: recovery from Spark ver 2.3.1 checkpoints ensures min policy") {
    val input1 = MemoryStream[Int]
    val input2 = MemoryStream[Int]

    val resourceUri = this.getClass.getResource(
      "/structured-streaming/checkpoint-version-2.3.1-for-multi-watermark-policy/").toURI

    val checkpointDir = Utils.createTempDir().getCanonicalFile
    // Copy the checkpoint to a temp dir to prevent changes to the original.
    // Not doing this will lead to the test passing on the first run, but fail subsequent runs.
    FileUtils.copyDirectory(new File(resourceUri), checkpointDir)

    input1.addData(20)
    input2.addData(30)
    input1.addData(10)

    withSQLConf(SQLConf.STREAMING_MULTIPLE_WATERMARK_POLICY.key -> "max") {
      testStream(dfWithMultipleWatermarks(input1, input2))(
        StartStream(checkpointLocation = checkpointDir.getAbsolutePath),
        Execute { _.processAllAvailable() },
        MultiAddData(input1, 120)(input2, 130),
        CheckLastBatch(120, 130),
        checkWatermark(input2, 110), // should calculate 'min' even if session conf has 'max' policy
        AddData(input2, 150),
        CheckLastBatch(150),
        checkWatermark(input2, 110)
      )
    }
  }

  test("MultipleWatermarkPolicy: fail on incorrect conf values") {
    val invalidValues = Seq("", "random")
    invalidValues.foreach { value =>
      val e = intercept[IllegalArgumentException] {
        spark.conf.set(SQLConf.STREAMING_MULTIPLE_WATERMARK_POLICY.key, value)
      }
      assert(e.getMessage.toLowerCase(Locale.ROOT).contains("valid values are 'min' and 'max'"))
    }
  }

  Seq("true", "false").foreach { legacyIntervalEnabled =>
    test("SPARK-35815: Support ANSI intervals for delay threshold " +
      s"(${SQLConf.LEGACY_INTERVAL_ENABLED.key}=$legacyIntervalEnabled)") {
      withSQLConf(SQLConf.LEGACY_INTERVAL_ENABLED.key -> legacyIntervalEnabled) {
        val DAYS_PER_MONTH = 31
        Seq(
          // Conventional form and some variants
          (Seq("3 days", "Interval 3 day", "inTerval '3' day"), 3 * MILLIS_PER_DAY),
          (Seq(" 5 hours", "INTERVAL 5 hour", "interval '5' hour"), 5 * MILLIS_PER_HOUR),
          (Seq("\t8 minutes", "interval 8 minute", "interval '8' minute"), 8 * MILLIS_PER_MINUTE),
          (Seq("10 seconds", "interval 10 second", "interval '10' second"),
            10 * MILLIS_PER_SECOND),
          (Seq("1 years", "interval 1 year", "interval '1' year"),
            MONTHS_PER_YEAR * DAYS_PER_MONTH * MILLIS_PER_DAY),
          (Seq("1 months", "interval 1 month", "interval '1' month"),
            DAYS_PER_MONTH * MILLIS_PER_DAY),
          (Seq(
            "1 day 2 hours 3 minutes 4 seconds",
            " interval 1 day 2 hours 3 minutes 4 seconds",
            "\tinterval '1' day '2' hours '3' minutes '4' seconds",
            "interval '1 2:3:4' day to second"),
            MILLIS_PER_DAY + 2 * MILLIS_PER_HOUR + 3 * MILLIS_PER_MINUTE + 4 * MILLIS_PER_SECOND),
          (Seq(
            " 1 year 2 months",
            "interval 1 year 2 month",
            "interval '1' year '2' month",
            "\tinterval '1-2' year to month"),
            (MONTHS_PER_YEAR * DAYS_PER_MONTH + 2 * DAYS_PER_MONTH) * MILLIS_PER_DAY)
        ).foreach { case (delayThresholdVariants, expectedMs) =>
          delayThresholdVariants.foreach { case delayThreshold =>
            val df = MemoryStream[Int].toDF
              .withColumn("eventTime", timestamp_seconds($"value"))
              .withWatermark("eventTime", delayThreshold)
            val eventTimeAttr = df.queryExecution.analyzed.output.find(a => a.name == "eventTime")
            assert(eventTimeAttr.isDefined)
            val metadata = eventTimeAttr.get.metadata
            assert(metadata.contains(EventTimeWatermark.delayKey))
            assert(metadata.getLong(EventTimeWatermark.delayKey) === expectedMs)
          }
        }

        // Invalid interval patterns
        Seq(
          "1 foo",
          "interva 2 day",
          "intrval '3' day",
          "interval 4 foo",
          "interval '5' foo",
          "interval '1 2:3:4' day to hour",
          "interval '1 2' year to month").foreach { delayThreshold =>
          intercept[AnalysisException] {
            val df = MemoryStream[Int].toDF
              .withColumn("eventTime", timestamp_seconds($"value"))
              .withWatermark("eventTime", delayThreshold)
          }
        }
      }
    }
  }

  private def dfWithMultipleWatermarks(
      input1: MemoryStream[Int],
      input2: MemoryStream[Int]): Dataset[_] = {
    val df1 = input1.toDF
      .withColumn("eventTime", timestamp_seconds($"value"))
      .withWatermark("eventTime", "10 seconds")
    val df2 = input2.toDF
      .withColumn("eventTime", timestamp_seconds($"value"))
      .withWatermark("eventTime", "15 seconds")
    df1.union(df2).select($"eventTime".cast("int"))
  }

  private def checkWatermark(input: MemoryStream[Int], watermark: Long) = Execute { q =>
    input.addData(1)
    q.processAllAvailable()
    assert(q.lastProgress.eventTime.get("watermark") == formatTimestamp(watermark))
  }

  private def assertNumStateRows(numTotalRows: Long): AssertOnQuery = AssertOnQuery { q =>
    q.processAllAvailable()
    val progressWithData = q.recentProgress.lastOption.get
    assert(progressWithData.stateOperators(0).numRowsTotal === numTotalRows)
    true
  }

  private def assertNumRowsDroppedByWatermark(
      numRowsDroppedByWatermark: Long): AssertOnQuery = AssertOnQuery { q =>
    q.processAllAvailable()
    val progressWithData = q.recentProgress.filterNot { p =>
      // filter out batches which are falling into one of types:
      // 1) doesn't execute the batch run
      // 2) empty input batch
      p.inputRowsPerSecond == 0
    }.lastOption.get
    assert(progressWithData.stateOperators(0).numRowsDroppedByWatermark
      === numRowsDroppedByWatermark)
    true
  }

  /** Assert event stats generated on that last batch with data in it */
  private def assertEventStats(body: ju.Map[String, String] => Unit): AssertOnQuery = {
    Execute("AssertEventStats") { q =>
      body(q.recentProgress.filter(_.numInputRows > 0).lastOption.get.eventTime)
    }
  }

  /** Assert event stats generated on that last batch with data in it */
  private def assertEventStats(min: Long, max: Long, avg: Double, wtrmark: Long): AssertOnQuery = {
    assertEventStats { e =>
      assert(e.get("min") === formatTimestamp(min), s"min value mismatch")
      assert(e.get("max") === formatTimestamp(max), s"max value mismatch")
      assert(e.get("avg") === formatTimestamp(avg.toLong), s"avg value mismatch")
      assert(e.get("watermark") === formatTimestamp(wtrmark), s"watermark value mismatch")
    }
  }

  private val timestampFormat = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSS'Z'") // ISO8601
  timestampFormat.setTimeZone(ju.TimeZone.getTimeZone(UTC))

  private def formatTimestamp(sec: Long): String = {
    timestampFormat.format(new ju.Date(sec * 1000))
  }

  private def awaitTermination(): AssertOnQuery = Execute("AwaitTermination") { q =>
    q.awaitTermination()
  }
}
