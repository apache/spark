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

import java.sql.Timestamp
import java.time.{Instant, LocalDateTime, ZoneId}

import org.apache.spark.internal.Logging
import org.apache.spark.sql.AnalysisException
import org.apache.spark.sql.catalyst.ExtendedAnalysisException
import org.apache.spark.sql.execution.streaming.{MemoryStream, StreamExecution}
import org.apache.spark.sql.execution.streaming.state.RocksDBStateStoreProvider
import org.apache.spark.sql.functions.window
import org.apache.spark.sql.internal.SQLConf

class ColumnRenamedStatefulProcessor
  extends StatefulProcessor[String, InputEventRow, RenamedInputEventRow]
  with Logging {

  override def init(outputMode: OutputMode): Unit = { }

  override def handleInputRows(key: String, inputRows: Iterator[InputEventRow],
      timerValues: TimerValues): Iterator[RenamedInputEventRow] = {

    new Iterator[RenamedInputEventRow] {
      override def hasNext: Boolean = inputRows.hasNext

      override def next(): RenamedInputEventRow = {
        Option(inputRows.next()).map { r =>
          RenamedInputEventRow(
            r.key, r.eventTime, r.event
          )
        }.orNull
      }
    }

  }

  override def close(): Unit = { }
}

case class InputEventRow(
    key: String,
    eventTime: Timestamp,
    event: String)

case class RenamedInputEventRow(
    key: String,
    renamedEventTime: Timestamp,
    event: String)

case class OutputEventRow(
    key: String,
    count: Int)

case class Window(
    start: Timestamp,
    end: Timestamp)

case class AggEventRow(
    window: Window,
    count: Long)

class TransformWithStateWatermarkSuite extends StreamTest
  with Logging {
  import testImplicits._

  test("watermark is propagated correctly for next stateful operator" +
    " after transformWithState") {
    withSQLConf(SQLConf.STATE_STORE_PROVIDER_CLASS.key ->
      classOf[RocksDBStateStoreProvider].getName) {
      withSQLConf(SQLConf.SHUFFLE_PARTITIONS.key -> "1") {
        val inputData = MemoryStream[InputEventRow]

        val result = inputData.toDS()
          .withWatermark("eventTime", "1 minute")
          .groupByKey(x => x.key)
          .transformWithState[RenamedInputEventRow](
            new ColumnRenamedStatefulProcessor(),
            TimeoutMode.NoTimeouts(),
            "renamedEventTime",
            OutputMode.Append())
          .groupBy(window($"renamedEventTime", "1 minute"))
          .count()
          .as[AggEventRow]

        testStream(result, OutputMode.Append())(
          AddData(inputData, InputEventRow("k1", timestamp("2024-01-01 00:00:00"), "e1")),
          // watermark should be 1 minute behind `2024-01-01 00:00:00`, nothing is
          // emitted as all records have timestamp > epoch
          CheckNewAnswer(),
          Execute("assertWatermarkEquals") { q =>
            assertWatermarkEquals(q, timestamp("2023-12-31 23:59:00"))
          },
          AddData(inputData, InputEventRow("k1", timestamp("2024-02-01 00:00:00"), "e1")),
          // global watermark should now be 1 minute behind  `2024-02-01 00:00:00`.
          CheckNewAnswer(AggEventRow(
            Window(timestamp("2024-01-01 00:00:00"), timestamp("2024-01-01 00:01:00")), 1)
          ),
          Execute("assertWatermarkEquals") { q =>
            assertWatermarkEquals(q, timestamp("2024-01-31 23:59:00"))
          },
          AddData(inputData, InputEventRow("k1", timestamp("2024-02-02 00:00:00"), "e1")),
          CheckNewAnswer(AggEventRow(
            Window(timestamp("2024-02-01 00:00:00"), timestamp("2024-02-01 00:01:00")), 1)
          )
        )
      }
    }
  }

  test("passing eventTime column to transformWithState fails if" +
    " no watermark is defined") {
    withSQLConf(SQLConf.STATE_STORE_PROVIDER_CLASS.key ->
      classOf[RocksDBStateStoreProvider].getName) {
      val inputData = MemoryStream[InputEventRow]

      intercept[AnalysisException] {
        inputData.toDS()
        .groupByKey(x => x.key)
        .transformWithState[RenamedInputEventRow](
          new ColumnRenamedStatefulProcessor(),
          TimeoutMode.NoTimeouts(),
          "renamedEventTime",
          OutputMode.Append())
      }
    }
  }

  test("missing eventTime column to transformWithState fails the query if" +
    " another stateful operator is added") {
    withSQLConf(SQLConf.STATE_STORE_PROVIDER_CLASS.key ->
      classOf[RocksDBStateStoreProvider].getName) {
      val inputData = MemoryStream[InputEventRow]

      val result = inputData.toDS()
        .withWatermark("eventTime", "1 minute")
        .groupByKey(x => x.key)
        .transformWithState[RenamedInputEventRow](
          new ColumnRenamedStatefulProcessor(),
          TimeoutMode.NoTimeouts(),
          OutputMode.Append())
        .groupBy(window($"renamedEventTime", "1 minute"))
        .count()

      intercept[ExtendedAnalysisException] {
        testStream(result, OutputMode.Append())(
          StartStream()
        )
      }
    }
  }

  private def timestamp(str: String): Timestamp = {
    Timestamp.valueOf(str)
  }

  private def assertWatermarkEquals(q: StreamExecution, watermark: Timestamp): Unit = {
    val queryWatermark = getQueryWatermark(q)
    assert(queryWatermark.isDefined)
    assert(queryWatermark.get === watermark)
  }

  private def getQueryWatermark(q: StreamExecution): Option[Timestamp] = {
    import scala.jdk.CollectionConverters._
    logWarning(s"${q.recentProgress.map(_.eventTime).mkString("Array(", ", ", ")")}")
    val eventTimeMap = q.lastProgress.eventTime.asScala
    val queryWatermark = eventTimeMap.get("watermark")

    logWarning(s"batchId=${q.lastProgress.batchId} $queryWatermark")
    queryWatermark.map { v =>
      val instant = Instant.parse(v)
      val local = LocalDateTime.ofInstant(instant, ZoneId.systemDefault())

      Timestamp.valueOf(local)
    }
  }
}
