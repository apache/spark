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

import java.util

import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.connector.catalog.{SupportsRead, Table, TableCapability, TableProvider}
import org.apache.spark.sql.connector.expressions.Transform
import org.apache.spark.sql.connector.read.{InputPartition, PartitionReader, PartitionReaderFactory, Scan, ScanBuilder}
import org.apache.spark.sql.connector.read.streaming.{MicroBatchStream, Offset, PartitionOffset, SupportsRealTimeMode, SupportsRealTimeRead}
import org.apache.spark.sql.connector.read.streaming.SupportsRealTimeRead.RecordStatus
import org.apache.spark.sql.execution.streaming.sources.ContinuousMemorySink
import org.apache.spark.sql.sources.DataSourceRegister
import org.apache.spark.sql.types.{IntegerType, StringType, StructType}
import org.apache.spark.sql.util.CaseInsensitiveStringMap
import org.apache.spark.unsafe.types.UTF8String

// scalastyle:off
/**
 * ============================================================================================
 * BACKWARD-COMPATIBILITY GUARD -- DO NOT MODIFY THE SOURCE DEFINITIONS BELOW.
 * ============================================================================================
 *
 * The classes in this file define a self-contained Real-Time Mode (RTM) streaming source that is
 * written EXCLUSIVELY against the public, `@Evolving` connector APIs an external connector author
 * has access to:
 *
 *   - `org.apache.spark.sql.sources.DataSourceRegister`
 *   - `org.apache.spark.sql.connector.catalog.{TableProvider, Table, SupportsRead,
 *       TableCapability}`
 *   - `org.apache.spark.sql.connector.read.{ScanBuilder, Scan, InputPartition, PartitionReader,
 *       PartitionReaderFactory}`
 *   - `org.apache.spark.sql.connector.read.streaming.{MicroBatchStream, Offset, PartitionOffset,
 *       SupportsRealTimeMode, SupportsRealTimeRead}`
 *
 * It deliberately does NOT use any `private[spark]` / internal helper (no `LowLatencyMemoryStream`,
 * `LongOffset`, `SimpleTableProvider`, RPC endpoints, or the engine-internal low latency clock).
 * Its purpose is to pin the source-level backward compatibility of the RTM connector SPI: if a
 * future change to `SupportsRealTimeMode` or `SupportsRealTimeRead` breaks external implementors
 * the way SPARK-55699 did (it replaced `nextWithTimeout(Long)` rather than adding an overload, see
 * SPARK-58386), this file will FAIL TO COMPILE, catching the incompatibility at build time.
 *
 * `SupportsRealTimeRead` offers two `nextWithTimeout` overloads and a source overrides exactly one.
 * Both are covered here so the guard tracks either entry point:
 *   - `CompatOneArgPartitionReader` overrides only `nextWithTimeout(Long)` -- the method a
 *     third-party source is expected to implement. The engine invokes the two-arg overload, so this
 *     reader is exercised through the interface's default delegation.
 *   - `CompatTwoArgPartitionReader` overrides `nextWithTimeout(Long, Long)` directly -- the
 *     overload the engine invokes.
 * Keep both.
 *
 * When you add a genuinely new REQUIRED method to one of these interfaces, prefer a `default`
 * method so this frozen source keeps compiling. If a required change is truly unavoidable, updating
 * this file is a strong signal that external connectors will also break -- treat it accordingly.
 * ============================================================================================
 */
// scalastyle:on

/** Deterministic, never-changing dataset the guard source serves. */
private object CompatRealTimeData {
  val schema: StructType =
    new StructType().add("value", IntegerType).add("name", StringType)

  // A fixed, finite dataset. Frozen on purpose -- do not change.
  val records: Array[(Int, String)] = Array((1, "a"), (2, "b"), (3, "c"))
}

/** A public-API `Offset`: the number of records consumed so far. */
private case class CompatOffset(consumed: Int) extends Offset {
  override def json(): String = consumed.toString
}

/** A public-API per-partition `PartitionOffset`. */
private case class CompatPartitionOffset(partitionId: Int, offset: Int) extends PartitionOffset

/** A serializable `InputPartition` carrying its slice of the frozen dataset. */
private case class CompatInputPartition(
    partitionId: Int,
    startOffset: Int,
    rows: Array[(Int, String)])
    extends InputPartition

/**
 * Shared reader logic for both `nextWithTimeout` entry points. Concrete subclasses only pick which
 * overload of [[SupportsRealTimeRead#nextWithTimeout]] to override; both route to [[pollNext]].
 */
private abstract class CompatRealTimePartitionReaderBase(partition: CompatInputPartition)
    extends SupportsRealTimeRead[InternalRow] {

  private var pos = 0
  private var currentRow: InternalRow = _

  private def toRow(i: Int): InternalRow = {
    val (v, n) = partition.rows(i)
    InternalRow(v, UTF8String.fromString(n))
  }

  /** Return the next record, or wait until the timeout elapses and report no record. */
  protected final def pollNext(timeoutMs: java.lang.Long): RecordStatus = {
    if (pos < partition.rows.length) {
      val (value, _) = partition.rows(pos)
      currentRow = toRow(pos)
      pos += 1
      // Report the record along with a deterministic synthetic arrival time, exercising the
      // arrival-time branch of RecordStatus.
      return RecordStatus.newStatusWithArrivalTimeMs(value.toLong)
    }
    // Exhausted this batch's data: keep waiting until the caller's timeout elapses, then report
    // no record -- the same wait-until-timeout behavior a real source has. Measured against the
    // wall clock, as a third-party source without the engine's reference clock would do.
    val startNs = System.nanoTime()
    var elapsedMs = 0L
    while (elapsedMs < timeoutMs) {
      Thread.sleep(10L)
      elapsedMs = (System.nanoTime() - startNs) / 1000000L
    }
    RecordStatus.newStatusWithoutArrivalTime(false)
  }

  override def getOffset: PartitionOffset =
    CompatPartitionOffset(partition.partitionId, partition.startOffset + pos)

  override def next(): Boolean = {
    if (pos < partition.rows.length) {
      currentRow = toRow(pos)
      pos += 1
      true
    } else {
      false
    }
  }

  override def get(): InternalRow = currentRow

  override def close(): Unit = {}
}

/**
 * Reader that overrides ONLY the single-argument `nextWithTimeout(Long)` -- the method a
 * third-party source is expected to implement. Reached through the two-arg default delegation.
 */
private class CompatOneArgPartitionReader(partition: CompatInputPartition)
    extends CompatRealTimePartitionReaderBase(partition) {
  override def nextWithTimeout(timeoutMs: java.lang.Long): RecordStatus = pollNext(timeoutMs)
}

/** Reader that overrides the two-argument `nextWithTimeout(Long, Long)` the engine invokes. */
private class CompatTwoArgPartitionReader(partition: CompatInputPartition)
    extends CompatRealTimePartitionReaderBase(partition) {
  override def nextWithTimeout(
      startTimeMs: java.lang.Long, timeoutMs: java.lang.Long): RecordStatus = pollNext(timeoutMs)
}

/** A public-API `PartitionReaderFactory`, parameterized by which reader overload to use. */
private class CompatRealTimeReaderFactory(twoArg: Boolean) extends PartitionReaderFactory {
  override def createReader(partition: InputPartition): PartitionReader[InternalRow] = {
    val p = partition.asInstanceOf[CompatInputPartition]
    if (twoArg) new CompatTwoArgPartitionReader(p) else new CompatOneArgPartitionReader(p)
  }
}

/** The RTM stream: a public `MicroBatchStream` that also implements `SupportsRealTimeMode`. */
private class CompatRealTimeStream(twoArg: Boolean)
    extends MicroBatchStream with SupportsRealTimeMode {
  override def initialOffset(): Offset = CompatOffset(0)
  override def deserializeOffset(json: String): Offset = CompatOffset(json.toInt)
  override def commit(end: Offset): Unit = {}
  override def stop(): Unit = {}

  override def latestOffset(): Offset = CompatOffset(CompatRealTimeData.records.length)
  override def planInputPartitions(start: Offset, end: Offset): Array[InputPartition] = {
    val from = start.asInstanceOf[CompatOffset].consumed
    val to = end.asInstanceOf[CompatOffset].consumed
    Array(CompatInputPartition(0, from, CompatRealTimeData.records.slice(from, to)))
  }
  override def createReaderFactory(): PartitionReaderFactory =
    new CompatRealTimeReaderFactory(twoArg)

  override def planInputPartitions(start: Offset): Array[InputPartition] = {
    val from = start.asInstanceOf[CompatOffset].consumed
    Array(CompatInputPartition(0, from, CompatRealTimeData.records.drop(from)))
  }
  override def mergeOffsets(offsets: Array[PartitionOffset]): Offset = {
    val maxOffset = offsets.map(_.asInstanceOf[CompatPartitionOffset].offset).max
    CompatOffset(maxOffset)
  }
}

/** Scan + ScanBuilder wired to the RTM stream, using only public APIs. */
private class CompatRealTimeScan(twoArg: Boolean) extends ScanBuilder with Scan {
  override def build(): Scan = this
  override def readSchema(): StructType = CompatRealTimeData.schema
  override def toMicroBatchStream(checkpointLocation: String): MicroBatchStream =
    new CompatRealTimeStream(twoArg)
}

/**
 * The top-level source, registered as a `TableProvider` + `DataSourceRegister`. Loaded by fully
 * qualified class name via `spark.readStream.format(...)`, so it doesn't need a `META-INF/services`
 * registration entry. The `twoArg` option selects which `nextWithTimeout` overload the reader
 * implements.
 */
class CompatRealTimeSourceProvider extends TableProvider with DataSourceRegister {
  override def shortName(): String = "compat-realtime-source"

  override def inferSchema(options: CaseInsensitiveStringMap): StructType =
    CompatRealTimeData.schema

  override def getTable(
      schema: StructType,
      partitioning: Array[Transform],
      properties: util.Map[String, String]): Table = new CompatRealTimeTable
}

private class CompatRealTimeTable extends Table with SupportsRead {
  override def name(): String = "compat-realtime-source"
  override def schema(): StructType = CompatRealTimeData.schema
  override def capabilities(): util.Set[TableCapability] =
    util.EnumSet.of(TableCapability.MICRO_BATCH_READ)
  override def newScanBuilder(options: CaseInsensitiveStringMap): ScanBuilder =
    new CompatRealTimeScan(options.getBoolean("twoArg", false))
}

/**
 * SPARK-58386: a compile-time and runtime backward-compatibility guard for the public Real-Time
 * Mode connector SPI (`SupportsRealTimeMode` / `SupportsRealTimeRead`). The frozen source above
 * must keep compiling against these interfaces, and these tests run it end-to-end through a real
 * RTM streaming query to prove an external-style source is still driven correctly, for both
 * `nextWithTimeout` entry points.
 */
class StreamingRealTimeModeSourceCompatSuite extends StreamRealTimeModeManualClockSuiteBase {
  import testImplicits._

  private def runSourceEndToEnd(twoArg: Boolean): Unit = {
    val df = spark.readStream
      .format(classOf[CompatRealTimeSourceProvider].getName)
      .option("twoArg", twoArg)
      .load()
      .selectExpr("concat(cast(value as string), '-', name) as output")

    testStream(df, OutputMode.Update, Map.empty, new ContinuousMemorySink())(
      StartStream(),
      CheckAnswerWithTimeout(10000, "1-a", "2-b", "3-c"),
      StopStream
    )
  }

  test("RTM source implementing single-arg nextWithTimeout reads end-to-end") {
    runSourceEndToEnd(twoArg = false)
  }

  test("RTM source implementing two-arg nextWithTimeout reads end-to-end") {
    runSourceEndToEnd(twoArg = true)
  }
}
