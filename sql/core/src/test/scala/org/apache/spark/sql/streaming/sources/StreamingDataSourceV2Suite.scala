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

package org.apache.spark.sql.streaming.sources

import java.util.Optional

import org.apache.spark.sql.{AnalysisException, Row}
import org.apache.spark.sql.execution.streaming.{LongOffset, RateStreamOffset}
import org.apache.spark.sql.sources.DataSourceRegister
import org.apache.spark.sql.sources.v2.{DataSourceV2, DataSourceV2Options}
import org.apache.spark.sql.sources.v2.reader.ReadTask
import org.apache.spark.sql.sources.v2.streaming.{ContinuousReadSupport, ContinuousWriteSupport, MicroBatchReadSupport, MicroBatchWriteSupport}
import org.apache.spark.sql.sources.v2.streaming.reader.{ContinuousReader, MicroBatchReader, Offset, PartitionOffset}
import org.apache.spark.sql.sources.v2.streaming.writer.ContinuousWriter
import org.apache.spark.sql.sources.v2.writer.DataSourceV2Writer
import org.apache.spark.sql.streaming.{OutputMode, StreamTest, Trigger}
import org.apache.spark.sql.types.StructType
import org.apache.spark.util.Utils

object FakeReader extends MicroBatchReader with ContinuousReader {
  def setOffsetRange(start: Optional[Offset], end: Optional[Offset]): Unit = {}
  def getStartOffset: Offset = RateStreamOffset(Map())
  def getEndOffset: Offset = RateStreamOffset(Map())
  def deserializeOffset(json: String): Offset = RateStreamOffset(Map())
  def commit(end: Offset): Unit = {}
  def readSchema(): StructType = StructType(Seq())
  def createReadTasks(): java.util.ArrayList[ReadTask[Row]] = new java.util.ArrayList()
  def stop(): Unit = {}
  def mergeOffsets(offsets: Array[PartitionOffset]): Offset = RateStreamOffset(Map())
  def setOffset(start: Optional[Offset]): Unit = {}
}

class FakeStreamingMicroBatchOnly extends DataSourceRegister
    with DataSourceV2 with MicroBatchReadSupport with MicroBatchWriteSupport {
  override def createMicroBatchReader(
      schema: Optional[StructType],
      checkpointLocation: String,
      options: DataSourceV2Options): MicroBatchReader = FakeReader

  def createMicroBatchWriter(
      queryId: String,
      epochId: Long,
      schema: StructType,
      mode: OutputMode,
      options: DataSourceV2Options): Optional[DataSourceV2Writer] = {
    throw new IllegalStateException("fake sink - cannot actually write")
  }

  override def shortName(): String = "fake-microbatch-only"
}

class FakeStreamingContinuousOnly extends DataSourceRegister
    with DataSourceV2 with ContinuousReadSupport with ContinuousWriteSupport {
  override def createContinuousReader(
      schema: Optional[StructType],
      checkpointLocation: String,
      options: DataSourceV2Options): ContinuousReader = FakeReader

  def createContinuousWriter(
      queryId: String,
      schema: StructType,
      mode: OutputMode,
      options: DataSourceV2Options): Optional[ContinuousWriter] = {
    throw new IllegalStateException("fake sink - cannot actually write")
  }

  override def shortName(): String = "fake-continuous-only"
}

class FakeStreamingBothModes extends DataSourceRegister
    with DataSourceV2 with MicroBatchReadSupport with ContinuousReadSupport
    with MicroBatchWriteSupport with ContinuousWriteSupport {
  override def createMicroBatchReader(
      schema: Optional[StructType],
      checkpointLocation: String,
      options: DataSourceV2Options): MicroBatchReader = FakeReader

  def createMicroBatchWriter(
      queryId: String,
      epochId: Long,
      schema: StructType,
      mode: OutputMode,
      options: DataSourceV2Options): Optional[DataSourceV2Writer] = {
    throw new IllegalStateException("fake sink - cannot actually write")
  }

  override def createContinuousReader(
      schema: Optional[StructType],
      checkpointLocation: String,
      options: DataSourceV2Options): ContinuousReader = FakeReader

  def createContinuousWriter(
      queryId: String,
      schema: StructType,
      mode: OutputMode,
      options: DataSourceV2Options): Optional[ContinuousWriter] = {
    throw new IllegalStateException("fake sink - cannot actually write")
  }

  override def shortName(): String = "fake-both-modes"
}

class FakeStreamingNeitherMode extends DataSourceRegister with DataSourceV2 {
  override def shortName(): String = "fake-neither-mode"
}

class StreamingDataSourceV2Suite extends StreamTest {

  private def df = spark.readStream.format("rate").load()

  override def beforeAll(): Unit = {
    super.beforeAll()
    val fakeCheckpoint = Utils.createTempDir()
    spark.conf.set("spark.sql.streaming.checkpointLocation", fakeCheckpoint.getCanonicalPath)
  }

  testQuietly("create microbatch with only microbatch support") {
    val query = df.writeStream.format("fake-microbatch-only").start()
    query.stop()
  }

  testQuietly("create microbatch with both support") {
    val query = df.writeStream.format("fake-both-modes").start()
    query.stop()
  }

  testQuietly("create continuous with only continuous support") {
    val query = df.writeStream
      .format("fake-continuous-only")
      .trigger(Trigger.Continuous(100))
      .start()
    query.stop()
  }

  testQuietly("create continuous with both support") {
    val query = df.writeStream
      .format("fake-both-modes")
      .trigger(Trigger.Continuous(100))
      .start()
    query.stop()
  }

  test("microbatch with only continuous support") {
    val ex = intercept[UnsupportedOperationException] {
      df.writeStream.format("fake-continuous-only").start()
    }

    assert(ex.getMessage.contains(
      "Data source fake-continuous-only does not support streamed writing"))
  }

  test("microbatch with no support") {
    val ex = intercept[UnsupportedOperationException] {
      df.writeStream.format("fake-neither-mode").start()
    }

    assert(ex.getMessage.contains(
      "Data source fake-neither-mode does not support streamed writing"))
  }

  test("continuous with only microbatch support") {
    val ex = intercept[AnalysisException] {
      df.writeStream
        .format("fake-microbatch-only")
        .trigger(Trigger.Continuous(100))
        .start()
    }

    assert(ex.getMessage.contains(
      "Data source fake-microbatch-only does not support continuous writing"))
  }

  test("continuous with no support") {
    val ex = intercept[AnalysisException] {
      df.writeStream
        .format("fake-neither-mode")
        .trigger(Trigger.Continuous(100))
        .start()
    }

    assert(ex.getMessage.contains(
      "Data source fake-neither-mode does not support continuous writing"))
  }
}
