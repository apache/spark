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
import java.text.SimpleDateFormat
import java.util.Date

import org.scalatest.BeforeAndAfter

import org.apache.spark.internal.Logging
import org.apache.spark.sql.AnalysisException
import org.apache.spark.sql.catalyst.plans.logical.EventTimeWatermark
import org.apache.spark.sql.execution.streaming._
import org.apache.spark.sql.functions.{count, window}
import org.apache.spark.sql.streaming.OutputMode._

class EventTimeWatermarkSuite extends StreamTest with BeforeAndAfter with Logging {

  import testImplicits._

  after {
    sqlContext.streams.active.foreach(_.stop())
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
      .withColumn("eventTime", $"value".cast("timestamp"))
      .groupBy(window($"eventTime", "5 seconds") as 'window)
      .agg(count("*") as 'count)
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
        .withColumn("eventTime", $"value".cast("timestamp"))
        .withWatermark("eventTime", "10 seconds")
        .groupBy(window($"eventTime", "5 seconds") as 'window)
        .agg(count("*") as 'count)
        .select($"window".getField("start").cast("long").as[Long], $"count".as[Long])

    testStream(aggWithWatermark)(
      AddData(inputData2, 15),
      CheckAnswer(),
      assertEventStats { e =>
        assert(e.get("max") === formatTimestamp(15))
        assert(e.get("min") === formatTimestamp(15))
        assert(e.get("avg") === formatTimestamp(15))
        assert(e.get("watermark") === formatTimestamp(0))
      },
      AddData(inputData2, 10, 12, 14),
      CheckAnswer(),
      assertEventStats { e =>
        assert(e.get("max") === formatTimestamp(14))
        assert(e.get("min") === formatTimestamp(10))
        assert(e.get("avg") === formatTimestamp(12))
        assert(e.get("watermark") === formatTimestamp(5))
      },
      AddData(inputData2, 25),
      CheckAnswer(),
      assertEventStats { e =>
        assert(e.get("max") === formatTimestamp(25))
        assert(e.get("min") === formatTimestamp(25))
        assert(e.get("avg") === formatTimestamp(25))
        assert(e.get("watermark") === formatTimestamp(5))
      },
      AddData(inputData2, 25),
      CheckAnswer((10, 3)),
      assertEventStats { e =>
        assert(e.get("max") === formatTimestamp(25))
        assert(e.get("min") === formatTimestamp(25))
        assert(e.get("avg") === formatTimestamp(25))
        assert(e.get("watermark") === formatTimestamp(15))
      }
    )
  }

  test("append mode") {
    val inputData = MemoryStream[Int]

    val windowedAggregation = inputData.toDF()
      .withColumn("eventTime", $"value".cast("timestamp"))
      .withWatermark("eventTime", "10 seconds")
      .groupBy(window($"eventTime", "5 seconds") as 'window)
      .agg(count("*") as 'count)
      .select($"window".getField("start").cast("long").as[Long], $"count".as[Long])

    testStream(windowedAggregation)(
      AddData(inputData, 10, 11, 12, 13, 14, 15),
      CheckLastBatch(),
      AddData(inputData, 25),   // Advance watermark to 15 seconds
      CheckLastBatch(),
      assertNumStateRows(3),
      AddData(inputData, 25),   // Emit items less than watermark and drop their state
      CheckLastBatch((10, 5)),
      assertNumStateRows(2),
      AddData(inputData, 10),   // Should not emit anything as data less than watermark
      CheckLastBatch(),
      assertNumStateRows(2)
    )
  }

  test("update mode") {
    val inputData = MemoryStream[Int]
    spark.conf.set("spark.sql.shuffle.partitions", "10")

    val windowedAggregation = inputData.toDF()
      .withColumn("eventTime", $"value".cast("timestamp"))
      .withWatermark("eventTime", "10 seconds")
      .groupBy(window($"eventTime", "5 seconds") as 'window)
      .agg(count("*") as 'count)
      .select($"window".getField("start").cast("long").as[Long], $"count".as[Long])

    testStream(windowedAggregation, OutputMode.Update)(
      AddData(inputData, 10, 11, 12, 13, 14, 15),
      CheckLastBatch((10, 5), (15, 1)),
      AddData(inputData, 25),     // Advance watermark to 15 seconds
      CheckLastBatch((25, 1)),
      assertNumStateRows(3),
      AddData(inputData, 10, 25), // Ignore 10 as its less than watermark
      CheckLastBatch((25, 2)),
      assertNumStateRows(2),
      AddData(inputData, 10),     // Should not emit anything as data less than watermark
      CheckLastBatch(),
      assertNumStateRows(2)
    )
  }

  test("delay in months and years handled correctly") {
    val currentTimeMs = System.currentTimeMillis
    val currentTime = new Date(currentTimeMs)

    val input = MemoryStream[Long]
    val aggWithWatermark = input.toDF()
      .withColumn("eventTime", $"value".cast("timestamp"))
      .withWatermark("eventTime", "2 years 5 months")
      .groupBy(window($"eventTime", "5 seconds") as 'window)
      .agg(count("*") as 'count)
      .select($"window".getField("start").cast("long").as[Long], $"count".as[Long])

    def monthsSinceEpoch(date: Date): Int = { date.getYear * 12 + date.getMonth }

    testStream(aggWithWatermark)(
      AddData(input, currentTimeMs / 1000),
      CheckAnswer(),
      AddData(input, currentTimeMs / 1000),
      CheckAnswer(),
      assertEventStats { e =>
        assert(timestampFormat.parse(e.get("max")).getTime === (currentTimeMs / 1000) * 1000)
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
      .withColumn("eventTime", $"value".cast("timestamp"))
      .withWatermark("eventTime", "10 seconds")
      .groupBy(window($"eventTime", "5 seconds") as 'window)
      .agg(count("*") as 'count)
      .select($"window".getField("start").cast("long").as[Long], $"count".as[Long])

    testStream(df)(
      AddData(inputData, 10, 11, 12, 13, 14, 15),
      CheckLastBatch(),
      AddData(inputData, 25), // Advance watermark to 15 seconds
      StopStream,
      StartStream(),
      CheckLastBatch(),
      AddData(inputData, 25), // Evict items less than previous watermark.
      CheckLastBatch((10, 5)),
      StopStream,
      AssertOnQuery { q => // clear the sink
        q.sink.asInstanceOf[MemorySink].clear()
        true
      },
      StartStream(),
      CheckLastBatch((10, 5)), // Recompute last batch and re-evict timestamp 10
      AddData(inputData, 30), // Advance watermark to 20 seconds
      CheckLastBatch(),
      StopStream,
      StartStream(), // Watermark should still be 15 seconds
      AddData(inputData, 17),
      CheckLastBatch(), // We still do not see next batch
      AddData(inputData, 30), // Advance watermark to 20 seconds
      CheckLastBatch(),
      AddData(inputData, 30), // Evict items less than previous watermark.
      CheckLastBatch((15, 2)) // Ensure we see next window
    )
  }

  test("dropping old data") {
    val inputData = MemoryStream[Int]

    val windowedAggregation = inputData.toDF()
        .withColumn("eventTime", $"value".cast("timestamp"))
        .withWatermark("eventTime", "10 seconds")
        .groupBy(window($"eventTime", "5 seconds") as 'window)
        .agg(count("*") as 'count)
        .select($"window".getField("start").cast("long").as[Long], $"count".as[Long])

    testStream(windowedAggregation)(
      AddData(inputData, 10, 11, 12),
      CheckAnswer(),
      AddData(inputData, 25),     // Advance watermark to 15 seconds
      CheckAnswer(),
      AddData(inputData, 25),     // Evict items less than previous watermark.
      CheckAnswer((10, 3)),
      AddData(inputData, 10),     // 10 is later than 15 second watermark
      CheckAnswer((10, 3)),
      AddData(inputData, 25),
      CheckAnswer((10, 3))        // Should not emit an incorrect partial result.
    )
  }

  test("complete mode") {
    val inputData = MemoryStream[Int]

    val windowedAggregation = inputData.toDF()
        .withColumn("eventTime", $"value".cast("timestamp"))
        .withWatermark("eventTime", "10 seconds")
        .groupBy(window($"eventTime", "5 seconds") as 'window)
        .agg(count("*") as 'count)
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
        .withColumn("eventTime", $"value".cast("timestamp"))
        .withWatermark("eventTime", "10 seconds")
        .groupBy($"eventTime")
        .agg(count("*") as 'count)
        .select($"eventTime".cast("long").as[Long], $"count".as[Long])

    testStream(windowedAggregation)(
      AddData(inputData, 10),
      CheckAnswer(),
      AddData(inputData, 25), // Advance watermark to 15 seconds
      CheckAnswer(),
      AddData(inputData, 25), // Evict items less than previous watermark.
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

  test("the new watermark should override the old one") {
    val df = MemoryStream[(Long, Long)].toDF()
      .withColumn("first", $"_1".cast("timestamp"))
      .withColumn("second", $"_2".cast("timestamp"))
      .withWatermark("first", "1 minute")
      .withWatermark("second", "2 minutes")

    val eventTimeColumns = df.logicalPlan.output
      .filter(_.metadata.contains(EventTimeWatermark.delayKey))
    assert(eventTimeColumns.size === 1)
    assert(eventTimeColumns(0).name === "second")
  }

  private def assertNumStateRows(numTotalRows: Long): AssertOnQuery = AssertOnQuery { q =>
    val progressWithData = q.recentProgress.filter(_.numInputRows > 0).lastOption.get
    assert(progressWithData.stateOperators(0).numRowsTotal === numTotalRows)
    true
  }

  private def assertEventStats(body: ju.Map[String, String] => Unit): AssertOnQuery = {
    AssertOnQuery { q =>
      body(q.recentProgress.filter(_.numInputRows > 0).lastOption.get.eventTime)
      true
    }
  }

  private val timestampFormat = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSS'Z'") // ISO8601
  timestampFormat.setTimeZone(ju.TimeZone.getTimeZone("UTC"))

  private def formatTimestamp(sec: Long): String = {
    timestampFormat.format(new ju.Date(sec * 1000))
  }
}
