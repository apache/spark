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

import org.scalatest.BeforeAndAfter

import org.apache.spark.internal.Logging
import org.apache.spark.sql.{AnalysisException, Row}
import org.apache.spark.sql.execution.streaming._
import org.apache.spark.sql.functions.{count, window}

class WatermarkSuite extends StreamTest with BeforeAndAfter with Logging {

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


  test("watermark metric") {

    val inputData = MemoryStream[Int]

    val windowedAggregation = inputData.toDF()
        .withColumn("eventTime", $"value".cast("timestamp"))
        .withWatermark("eventTime", "10 seconds")
        .groupBy(window($"eventTime", "5 seconds") as 'window)
        .agg(count("*") as 'count)
        .select($"window".getField("start").cast("long").as[Long], $"count".as[Long])

    testStream(windowedAggregation)(
      AddData(inputData, 15),
      CheckAnswer(),
      AssertOnQuery { query =>
        query.lastProgress.currentWatermark === 5000
      },
      AddData(inputData, 15),
      CheckAnswer(),
      AssertOnQuery { query =>
        query.lastProgress.currentWatermark === 5000
      },
      AddData(inputData, 25),
      CheckAnswer(),
      AssertOnQuery { query =>
        query.lastProgress.currentWatermark === 15000
      }
    )
  }

  test("append-mode watermark aggregation") {
    val inputData = MemoryStream[Int]

    val windowedAggregation = inputData.toDF()
      .withColumn("eventTime", $"value".cast("timestamp"))
      .withWatermark("eventTime", "10 seconds")
      .groupBy(window($"eventTime", "5 seconds") as 'window)
      .agg(count("*") as 'count)
      .select($"window".getField("start").cast("long").as[Long], $"count".as[Long])

    testStream(windowedAggregation)(
      AddData(inputData, 10, 11, 12, 13, 14, 15),
      CheckAnswer(),
      AddData(inputData, 25), // Advance watermark to 15 seconds
      CheckAnswer(),
      AddData(inputData, 25), // Evict items less than previous watermark.
      CheckAnswer((10, 5))
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
}
