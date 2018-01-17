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
import org.apache.spark.sql.execution.datasources.DataSource
import org.apache.spark.sql.execution.streaming.{LongOffset, RateStreamOffset}
import org.apache.spark.sql.execution.streaming.continuous.ContinuousTrigger
import org.apache.spark.sql.sources.DataSourceRegister
import org.apache.spark.sql.sources.v2.{DataSourceV2, DataSourceV2Options}
import org.apache.spark.sql.sources.v2.reader.ReadTask
import org.apache.spark.sql.sources.v2.streaming.{ContinuousReadSupport, ContinuousWriteSupport, MicroBatchReadSupport, MicroBatchWriteSupport}
import org.apache.spark.sql.sources.v2.streaming.reader.{ContinuousReader, MicroBatchReader, Offset, PartitionOffset}
import org.apache.spark.sql.sources.v2.streaming.writer.ContinuousWriter
import org.apache.spark.sql.sources.v2.writer.DataSourceV2Writer
import org.apache.spark.sql.streaming.{OutputMode, StreamingQueryException, StreamTest, Trigger}
import org.apache.spark.sql.types.StructType
import org.apache.spark.util.Utils

case class FakeReader() extends MicroBatchReader with ContinuousReader {
  def setOffsetRange(start: Optional[Offset], end: Optional[Offset]): Unit = {}
  def getStartOffset: Offset = RateStreamOffset(Map())
  def getEndOffset: Offset = RateStreamOffset(Map())
  def deserializeOffset(json: String): Offset = RateStreamOffset(Map())
  def commit(end: Offset): Unit = {}
  def readSchema(): StructType = StructType(Seq())
  def stop(): Unit = {}
  def mergeOffsets(offsets: Array[PartitionOffset]): Offset = RateStreamOffset(Map())
  def setOffset(start: Optional[Offset]): Unit = {}

  def createReadTasks(): java.util.ArrayList[ReadTask[Row]] = {
    throw new IllegalStateException("fake source - cannot actually read")
  }
}

trait FakeMicroBatchReadSupport extends MicroBatchReadSupport {
  override def createMicroBatchReader(
      schema: Optional[StructType],
      checkpointLocation: String,
      options: DataSourceV2Options): MicroBatchReader = FakeReader()
}

trait FakeContinuousReadSupport extends ContinuousReadSupport {
  override def createContinuousReader(
      schema: Optional[StructType],
      checkpointLocation: String,
      options: DataSourceV2Options): ContinuousReader = FakeReader()
}

trait FakeMicroBatchWriteSupport extends MicroBatchWriteSupport {
  def createMicroBatchWriter(
      queryId: String,
      epochId: Long,
      schema: StructType,
      mode: OutputMode,
      options: DataSourceV2Options): Optional[DataSourceV2Writer] = {
    throw new IllegalStateException("fake sink - cannot actually write")
  }
}

trait FakeContinuousWriteSupport extends ContinuousWriteSupport {
  def createContinuousWriter(
      queryId: String,
      schema: StructType,
      mode: OutputMode,
      options: DataSourceV2Options): Optional[ContinuousWriter] = {
    throw new IllegalStateException("fake sink - cannot actually write")
  }
}

class FakeReadMicroBatchOnly extends DataSourceRegister with FakeMicroBatchReadSupport {
  override def shortName(): String = "fake-read-microbatch-only"
}

class FakeReadContinuousOnly extends DataSourceRegister with FakeContinuousReadSupport {
  override def shortName(): String = "fake-read-continuous-only"
}

class FakeReadBothModes extends DataSourceRegister
    with FakeMicroBatchReadSupport with FakeContinuousReadSupport {
  override def shortName(): String = "fake-read-microbatch-continuous"
}

class FakeReadNeitherMode extends DataSourceRegister {
  override def shortName(): String = "fake-read-neither-mode"
}

class FakeWriteMicroBatchOnly extends DataSourceRegister with FakeMicroBatchWriteSupport {
  override def shortName(): String = "fake-write-microbatch-only"
}

class FakeWriteContinuousOnly extends DataSourceRegister with FakeContinuousWriteSupport {
  override def shortName(): String = "fake-write-continuous-only"
}

class FakeWriteBothModes extends DataSourceRegister
    with FakeMicroBatchWriteSupport with FakeContinuousWriteSupport {
  override def shortName(): String = "fake-write-microbatch-continuous"
}

class FakeWriteNeitherMode extends DataSourceRegister {
  override def shortName(): String = "fake-write-neither-mode"
}

class StreamingDataSourceV2Suite extends StreamTest {

  override def beforeAll(): Unit = {
    super.beforeAll()
    val fakeCheckpoint = Utils.createTempDir()
    spark.conf.set("spark.sql.streaming.checkpointLocation", fakeCheckpoint.getCanonicalPath)
  }

  val readFormats = Seq(
    "fake-read-microbatch-only",
    "fake-read-continuous-only",
    "fake-read-microbatch-continuous",
    "fake-read-neither-mode")
  val writeFormats = Seq(
    "fake-write-microbatch-only",
    "fake-write-continuous-only",
    "fake-write-microbatch-continuous",
    "fake-write-neither-mode")
  val triggers = Seq(
    Trigger.Once(),
    Trigger.ProcessingTime(1000),
    Trigger.Continuous(1000))

  private def testPositiveCase(readFormat: String, writeFormat: String, trigger: Trigger) = {
    val query = spark.readStream
      .format(readFormat)
      .load()
      .writeStream
      .format(writeFormat)
      .trigger(trigger)
      .start()
    query.stop()
  }

  private def testUnsupportedOperationCase(
      readFormat: String,
      writeFormat: String,
      trigger: Trigger,
      errorMsg: String) = {
    val ex = intercept[UnsupportedOperationException] {
      testPositiveCase(readFormat, writeFormat, trigger)
    }
    assert(ex.getMessage.contains(errorMsg))
  }

  private def testLogicalPlanCase(
      readFormat: String,
      writeFormat: String,
      trigger: Trigger,
      errorMsg: String) = {
    val ex = intercept[StreamingQueryException] {
      spark.readStream
        .format(readFormat)
        .load()
        .writeStream
        .format(writeFormat)
        .trigger(trigger)
        .start()
        .processAllAvailable()
    }
    assert(ex.cause != null)
    assert(ex.cause.getMessage.contains(errorMsg))
  }

  // Get a list of (read, write, trigger) tuples for test cases.
  val cases = readFormats.flatMap { read =>
    writeFormats.flatMap { write =>
      triggers.map(t => (write, t))
    }.map {
      case (write, t) => (read, write, t)
    }
  }

  for ((read, write, trigger) <- cases) {
    testQuietly(s"stream with read format $read, write format $write, trigger $trigger") {
      val readSource = DataSource.lookupDataSource(read, spark.sqlContext.conf).newInstance()
      val writeSource = DataSource.lookupDataSource(write, spark.sqlContext.conf).newInstance()
      (readSource, writeSource, trigger) match {
        // Valid microbatch queries.
        case (_: MicroBatchReadSupport, _: MicroBatchWriteSupport, t)
          if !t.isInstanceOf[ContinuousTrigger] =>
          testPositiveCase(read, write, trigger)

        // Valid continuous queries.
        case (_: ContinuousReadSupport, _: ContinuousWriteSupport, _: ContinuousTrigger) =>
          testPositiveCase(read, write, trigger)

        // Invalid - can't read at all
        case (r, _, _)
            if !r.isInstanceOf[MicroBatchReadSupport]
              && !r.isInstanceOf[ContinuousReadSupport] =>
          testUnsupportedOperationCase(read, write, trigger,
            s"Data source $read does not support streamed reading")

        // Invalid - trigger is continuous but writer is not
        case (_, w, _: ContinuousTrigger) if !w.isInstanceOf[ContinuousWriteSupport] =>
          testUnsupportedOperationCase(read, write, trigger,
            s"Data source $write does not support continuous writing")

        // Invalid - can't write at all
        case (_, w, _)
            if !w.isInstanceOf[MicroBatchWriteSupport]
              && !w.isInstanceOf[ContinuousWriteSupport] =>
          testUnsupportedOperationCase(read, write, trigger,
            s"Data source $write does not support streamed writing")

        // Invalid - trigger and writer are continuous but reader is not
        case (r, _: ContinuousWriteSupport, _: ContinuousTrigger)
            if !r.isInstanceOf[ContinuousReadSupport] =>
          testLogicalPlanCase(read, write, trigger,
            s"Data source $read does not support continuous processing")

        // Invalid - trigger is microbatch but writer is not
        case (_, w, t)
            if !w.isInstanceOf[MicroBatchWriteSupport] && !t.isInstanceOf[ContinuousTrigger] =>
          testUnsupportedOperationCase(read, write, trigger,
            s"Data source $write does not support streamed writing")

        // Invalid - trigger and writer are microbatch but reader is not
        case (r, _, t)
           if !r.isInstanceOf[MicroBatchReadSupport] && !t.isInstanceOf[ContinuousTrigger] =>
          testLogicalPlanCase(read, write, trigger,
            s"Data source $read does not support microbatch processing")
      }
    }
  }
}
