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

package org.apache.spark.sql.execution.streaming

import java.util.Optional
import java.util.concurrent.TimeUnit

import scala.collection.JavaConverters._

import org.apache.spark.sql.Row
import org.apache.spark.sql.execution.datasources.DataSource
import org.apache.spark.sql.execution.streaming.continuous._
import org.apache.spark.sql.execution.streaming.sources.{RateStreamBatchTask, RateStreamSourceV2, RateStreamV2Reader}
import org.apache.spark.sql.sources.v2.DataSourceV2Options
import org.apache.spark.sql.sources.v2.streaming.ContinuousReadSupport
import org.apache.spark.sql.streaming.StreamTest
import org.apache.spark.util.ManualClock

class RateSourceV2Suite extends StreamTest {
  import testImplicits._

  case class AdvanceRateManualClock(seconds: Long) extends AddData {
    override def addData(query: Option[StreamExecution]): (BaseStreamingSource, Offset) = {
      assert(query.nonEmpty)
      val rateSource = query.get.logicalPlan.collect {
        case StreamingExecutionRelation(source: RateStreamV2Reader, _) => source
      }.head
      rateSource.clock.asInstanceOf[ManualClock].advance(TimeUnit.SECONDS.toMillis(seconds))
      rateSource.setOffsetRange(Optional.empty(), Optional.empty())
      (rateSource, rateSource.getEndOffset())
    }
  }

  test("microbatch in registry") {
    DataSource.lookupDataSource("ratev2", spark.sqlContext.conf).newInstance() match {
      case ds: MicroBatchReadSupport =>
        val reader = ds.createMicroBatchReader(Optional.empty(), "", DataSourceV2Options.empty())
        assert(reader.isInstanceOf[RateStreamV2Reader])
      case _ =>
        throw new IllegalStateException("Could not find v2 read support for rate")
    }
  }

  test("basic microbatch execution") {
    val input = spark.readStream
      .format("rateV2")
      .option("rowsPerSecond", "10")
      .option("useManualClock", "true")
      .load()
    testStream(input, useV2Sink = true)(
      AdvanceRateManualClock(seconds = 1),
      CheckLastBatch((0 until 10).map(v => new java.sql.Timestamp(v * 100L) -> v): _*),
      StopStream,
      StartStream(),
      // Advance 2 seconds because creating a new RateSource will also create a new ManualClock
      AdvanceRateManualClock(seconds = 2),
      CheckLastBatch((10 until 20).map(v => new java.sql.Timestamp(v * 100L) -> v): _*)
    )
  }

  test("microbatch - numPartitions propagated") {
    val reader = new RateStreamV2Reader(
      new DataSourceV2Options(Map("numPartitions" -> "11", "rowsPerSecond" -> "33").asJava))
    reader.setOffsetRange(Optional.empty(), Optional.empty())
    val tasks = reader.createReadTasks()
    assert(tasks.size == 11)
  }

  test("microbatch - set offset") {
    val reader = new RateStreamV2Reader(DataSourceV2Options.empty())
    val startOffset = RateStreamOffset(Map((0, ValueRunTimeMsPair(0, 1000))))
    val endOffset = RateStreamOffset(Map((0, ValueRunTimeMsPair(0, 2000))))
    reader.setOffsetRange(Optional.of(startOffset), Optional.of(endOffset))
    assert(reader.getStartOffset() == startOffset)
    assert(reader.getEndOffset() == endOffset)
  }

  test("microbatch - infer offsets") {
    val reader = new RateStreamV2Reader(
      new DataSourceV2Options(Map("numPartitions" -> "1", "rowsPerSecond" -> "100").asJava))
    reader.clock.waitTillTime(reader.clock.getTimeMillis() + 100)
    reader.setOffsetRange(Optional.empty(), Optional.empty())
    reader.getStartOffset() match {
      case r: RateStreamOffset =>
        assert(r.partitionToValueAndRunTimeMs(0).runTimeMs == reader.creationTimeMs)
      case _ => throw new IllegalStateException("unexpected offset type")
    }
    reader.getEndOffset() match {
      case r: RateStreamOffset =>
        // End offset may be a bit beyond 100 ms/9 rows after creation if the wait lasted
        // longer than 100ms. It should never be early.
        assert(r.partitionToValueAndRunTimeMs(0).value >= 9)
        assert(r.partitionToValueAndRunTimeMs(0).runTimeMs >= reader.creationTimeMs + 100)

      case _ => throw new IllegalStateException("unexpected offset type")
    }
  }

  test("microbatch - predetermined batch size") {
    val reader = new RateStreamV2Reader(
      new DataSourceV2Options(Map("numPartitions" -> "1", "rowsPerSecond" -> "20").asJava))
    val startOffset = RateStreamOffset(Map((0, ValueRunTimeMsPair(0, 1000))))
    val endOffset = RateStreamOffset(Map((0, ValueRunTimeMsPair(20, 2000))))
    reader.setOffsetRange(Optional.of(startOffset), Optional.of(endOffset))
    val tasks = reader.createReadTasks()
    assert(tasks.size == 1)
    assert(tasks.get(0).asInstanceOf[RateStreamBatchTask].vals.size == 20)
  }

  test("microbatch - data read") {
    val reader = new RateStreamV2Reader(
      new DataSourceV2Options(Map("numPartitions" -> "11", "rowsPerSecond" -> "33").asJava))
    val startOffset = RateStreamSourceV2.createInitialOffset(11, reader.creationTimeMs)
    val endOffset = RateStreamOffset(startOffset.partitionToValueAndRunTimeMs.toSeq.map {
      case (part, ValueRunTimeMsPair(currentVal, currentReadTime)) =>
        (part, ValueRunTimeMsPair(currentVal + 33, currentReadTime + 1000))
    }.toMap)

    reader.setOffsetRange(Optional.of(startOffset), Optional.of(endOffset))
    val tasks = reader.createReadTasks()
    assert(tasks.size == 11)

    val readData = tasks.asScala
      .map(_.createDataReader())
      .flatMap { reader =>
        val buf = scala.collection.mutable.ListBuffer[Row]()
        while (reader.next()) buf.append(reader.get())
        buf
      }

    assert(readData.map(_.getLong(1)).sorted == Range(0, 33))
  }

  test("continuous in registry") {
    DataSource.lookupDataSource("rate", spark.sqlContext.conf).newInstance() match {
      case ds: ContinuousReadSupport =>
        val reader = ds.createContinuousReader(Optional.empty(), "", DataSourceV2Options.empty())
        assert(reader.isInstanceOf[ContinuousRateStreamReader])
      case _ =>
        throw new IllegalStateException("Could not find v2 read support for rate")
    }
  }

  test("continuous data") {
    val reader = new ContinuousRateStreamReader(
      new DataSourceV2Options(Map("numPartitions" -> "2", "rowsPerSecond" -> "20").asJava))
    reader.setOffset(Optional.empty())
    val tasks = reader.createReadTasks()
    assert(tasks.size == 2)

    val data = scala.collection.mutable.ListBuffer[Row]()
    tasks.asScala.foreach {
      case t: RateStreamReadTask =>
        val startTimeMs = reader.getStartOffset()
          .asInstanceOf[RateStreamOffset]
          .partitionToValueAndRunTimeMs(t.partitionIndex)
          .runTimeMs
        val r = t.createDataReader().asInstanceOf[RateStreamDataReader]
        for (rowIndex <- 0 to 9) {
          r.next()
          data.append(r.get())
          assert(r.getOffset() ==
            ContinuousRateStreamPartitionOffset(
              t.partitionIndex,
              t.partitionIndex + rowIndex * 2,
              startTimeMs + (rowIndex + 1) * 100))
        }
        assert(System.currentTimeMillis() >= startTimeMs + 1000)

      case _ => throw new IllegalStateException("Unexpected task type")
    }

    assert(data.map(_.getLong(1)).toSeq.sorted == Range(0, 20))
  }
}
