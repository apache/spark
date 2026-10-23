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

package org.apache.spark.sql.connector

import java.util
import java.util.OptionalLong
import java.util.concurrent.atomic.{AtomicInteger, AtomicLong}

import scala.jdk.CollectionConverters._

import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.GenericInternalRow
import org.apache.spark.sql.connector.catalog.{SupportsRead, Table, TableCapability, TableProvider}
import org.apache.spark.sql.connector.expressions.{FieldReference, NamedReference, SortOrder, Transform}
import org.apache.spark.sql.connector.expressions.filter.{StorageFilter, StorageFilterNotAppliedException}
import org.apache.spark.sql.connector.read._
import org.apache.spark.sql.execution.vectorized.OnHeapColumnVector
import org.apache.spark.sql.types.{BooleanType, LongType, StructType}
import org.apache.spark.sql.util.CaseInsensitiveStringMap
import org.apache.spark.sql.vectorized.{ColumnarBatch, ColumnVector}

/**
 * A read-only v2 source over a fixed table of `(k, v)` longs that implements
 * [[SupportsStorageFilters]]. Its reader gets the filter in the task and drops the rows the filter
 * rejects. Its options:
 *  - `columnar`: reads in batches, through `StorageFilter.select`, rather than row by row;
 *  - `giveUp`: reads without the filter, as a reader does for a unit it cannot use the filter on.
 *    Here the unit is the one partition;
 *  - `markAllChecked`: reads without the filter but marks every row checked, which breaks the
 *    contract on purpose;
 *  - `declared`: the comma-separated storage-filter columns, `k` by default, a dot separating a
 *    nested field;
 *  - `valueName`: the name of the value column, `v` by default;
 *  - `push`: the comma-separated pushed operations it takes whole without applying them, of
 *    `limit`, `offset`, `topN` and `sample`, for planning tests only.
 *
 * A filter that throws `StorageFilterNotAppliedException` is given up for the rest of the
 * partition too. The counters below record what the readers got, evaluated and returned.
 */
class StorageFilterTestSource extends TableProvider {

  override def inferSchema(options: CaseInsensitiveStringMap): StructType =
    new StructType().add("k", LongType).add(options.getOrDefault("valueName", "v"), LongType)

  override def getTable(
      schema: StructType,
      partitioning: Array[Transform],
      properties: util.Map[String, String]): Table = new StorageFilterTestTable(schema)
}

object StorageFilterTestSource {

  /** Rows the source serves, `(k, k * 10)` for k in 0 until numRows. */
  val numRows: Int = 200

  /** Rows a batch holds at most in a columnar read. */
  val batchSize: Int = 64

  /** How many readers were created with a storage-filter request. */
  val filteredReaders = new AtomicInteger(0)

  /** How many of those requests asked for the checked column. */
  val checkedColumnRequests = new AtomicInteger(0)

  /** Rows the reader evaluated the storage filter against. */
  val rowsEvaluated = new AtomicLong(0)

  /** Rows the reader returned. */
  val rowsReturned = new AtomicLong(0)

  def reset(): Unit = {
    filteredReaders.set(0)
    checkedColumnRequests.set(0)
    rowsEvaluated.set(0)
    rowsReturned.set(0)
  }
}

class StorageFilterTestTable(tableSchema: StructType) extends Table with SupportsRead {

  override def name(): String = "storage_filter_test"

  override def schema(): StructType = tableSchema

  override def capabilities(): util.Set[TableCapability] =
    Set(TableCapability.BATCH_READ).asJava

  override def newScanBuilder(options: CaseInsensitiveStringMap): ScanBuilder = {
    val factory = StorageFilterTestReaderFactory(
      columnar = options.getBoolean("columnar", false),
      giveUp = options.getBoolean("giveUp", false),
      marksAllChecked = options.getBoolean("markAllChecked", false))
    val declared = options.getOrDefault("declared", "k").split(",").filter(_.nonEmpty).toSeq
    val scan = () => new StorageFilterTestScan(tableSchema, factory, declared)
    val pushes = options.getOrDefault("push", "").split(",").filter(_.nonEmpty).toSet
    if (pushes.nonEmpty) new StorageFilterTestPushingScanBuilder(scan, pushes) else () => scan()
  }
}

/**
 * Takes the pushed operations named in `pushes` whole. Its scan does not apply them, so it serves
 * planning tests only.
 */
class StorageFilterTestPushingScanBuilder(scan: () => Scan, pushes: Set[String])
  extends SupportsPushDownLimit
  with SupportsPushDownOffset
  with SupportsPushDownTopN
  with SupportsPushDownTableSample {

  override def build(): Scan = scan()
  override def pushLimit(limit: Int): Boolean = pushes("limit")
  override def pushOffset(offset: Int): Boolean = pushes("offset")
  override def pushTopN(orders: Array[SortOrder], limit: Int): Boolean = pushes("topN")
  override def pushTableSample(
      lowerBound: Double,
      upperBound: Double,
      withReplacement: Boolean,
      seed: Long,
      sampleMethod: SampleMethod): Boolean = pushes("sample")
  override def isPartiallyPushed: Boolean = false
}

class StorageFilterTestScan(
    tableSchema: StructType,
    factory: StorageFilterTestReaderFactory,
    declared: Seq[String])
  extends Scan with Batch with SupportsReportStatistics with SupportsStorageFilters {

  override def readSchema(): StructType = tableSchema

  override def toBatch: Batch = this

  // Over the suite's application-side scan size threshold, so InjectRuntimeFilter considers this
  // side worth filtering.
  override def estimateStatistics(): Statistics = new Statistics {
    override def sizeInBytes(): OptionalLong = OptionalLong.of(10L * 1024 * 1024)
    override def numRows(): OptionalLong = OptionalLong.of(StorageFilterTestSource.numRows.toLong)
  }

  override def storageFilterAttributes(): Array[NamedReference] =
    declared.map(name => FieldReference(name.split('.').toSeq)).toArray[NamedReference]

  override def planInputPartitions(): Array[InputPartition] =
    Array(StorageFilterTestPartition)

  override def createReaderFactory(): PartitionReaderFactory = factory
}

case object StorageFilterTestPartition extends InputPartition

case class StorageFilterTestReaderFactory(
    columnar: Boolean,
    giveUp: Boolean,
    marksAllChecked: Boolean)
  extends PartitionReaderFactory {

  override def supportColumnarReads(partition: InputPartition): Boolean = columnar

  // The filter the reader applies, none when it gives the filter up or only marks the rows.
  private def filterOf(request: StorageFilterRequest): Option[StorageFilter] = {
    StorageFilterTestSource.filteredReaders.incrementAndGet()
    if (request.appendsCheckedColumn()) {
      StorageFilterTestSource.checkedColumnRequests.incrementAndGet()
    }
    if (giveUp || marksAllChecked) None else Some(request.filter())
  }

  override def createReader(partition: InputPartition): PartitionReader[InternalRow] =
    new StorageFilterTestReader(None, appendsChecked = false)

  override def createReader(
      partition: InputPartition,
      request: StorageFilterRequest): PartitionReader[InternalRow] =
    new StorageFilterTestReader(filterOf(request), request.appendsCheckedColumn(), marksAllChecked)

  override def createColumnarReader(partition: InputPartition): PartitionReader[ColumnarBatch] =
    new StorageFilterTestColumnarReader(None, appendsChecked = false)

  override def createColumnarReader(
      partition: InputPartition,
      request: StorageFilterRequest): PartitionReader[ColumnarBatch] =
    new StorageFilterTestColumnarReader(
      filterOf(request), request.appendsCheckedColumn(), marksAllChecked)
}

/**
 * Serves `(k, k * 10)` row by row and drops any row `filter` rejects, with the checked column
 * last when `appendsChecked`. The filter's row layout is its `references()`, only `k` here.
 * `filter` is cleared when it cannot be applied. `marksAllChecked` marks every row checked.
 */
class StorageFilterTestReader(
    private var filter: Option[StorageFilter],
    appendsChecked: Boolean,
    marksAllChecked: Boolean = false)
  extends PartitionReader[InternalRow] {

  private var current: Long = -1L
  private var row: InternalRow = _

  override def next(): Boolean = {
    var found = false
    while (!found && current + 1 < StorageFilterTestSource.numRows) {
      current += 1
      val key = current
      val passes = filter.forall { f =>
        StorageFilterTestSource.rowsEvaluated.incrementAndGet()
        try f.eval(new GenericInternalRow(Array[Any](key))) catch {
          case _: StorageFilterNotAppliedException =>
            filter = None
            true
        }
      }
      if (passes) {
        val values = Array[Any](key, key * 10)
        val checked = filter.isDefined || marksAllChecked
        row = new GenericInternalRow(if (appendsChecked) values :+ checked else values)
        StorageFilterTestSource.rowsReturned.incrementAndGet()
        found = true
      }
    }
    found
  }

  override def get(): InternalRow = row

  override def close(): Unit = {}
}

/**
 * Serves `(k, k * 10)` in batches of up to `batchSize` rows. It keeps the rows `filter.select`
 * keeps over a batch of their keys, with the checked column last when `appendsChecked`. `filter`
 * is cleared when it cannot be applied. `marksAllChecked` marks every row checked.
 */
class StorageFilterTestColumnarReader(
    private var filter: Option[StorageFilter],
    appendsChecked: Boolean,
    marksAllChecked: Boolean = false)
  extends PartitionReader[ColumnarBatch] {

  private val batchSize = StorageFilterTestSource.batchSize
  private val keys = new OnHeapColumnVector(batchSize, LongType)
  private val keyBatch = new ColumnarBatch(Array[ColumnVector](keys))
  private val selected = new Array[Int](batchSize)
  private val values = Seq(
    new OnHeapColumnVector(batchSize, LongType), new OnHeapColumnVector(batchSize, LongType))
  private val checked = Option.when(appendsChecked) {
    new OnHeapColumnVector(batchSize, BooleanType)
  }
  private val batch = new ColumnarBatch((values ++ checked).toArray[ColumnVector])
  private var start = 0

  override def next(): Boolean = {
    var count = 0
    while (count == 0 && start < StorageFilterTestSource.numRows) {
      val n = math.min(batchSize, StorageFilterTestSource.numRows - start)
      (0 until n).foreach(i => keys.putLong(i, start + i))
      keyBatch.setNumRows(n)
      count = filter.flatMap { f =>
        StorageFilterTestSource.rowsEvaluated.addAndGet(n)
        try Some(f.select(keyBatch, selected)) catch {
          case _: StorageFilterNotAppliedException =>
            filter = None
            None
        }
      }.getOrElse {
        (0 until n).foreach(i => selected(i) = i)
        n
      }
      (0 until count).foreach { i =>
        val key = start + selected(i)
        values.head.putLong(i, key)
        values(1).putLong(i, key * 10L)
      }
      checked.foreach(_.putBooleans(0, count, filter.isDefined || marksAllChecked))
      start += n
    }
    batch.setNumRows(count)
    StorageFilterTestSource.rowsReturned.addAndGet(count)
    count > 0
  }

  override def get(): ColumnarBatch = batch

  override def close(): Unit = {
    batch.close()
    keyBatch.close()
  }
}
