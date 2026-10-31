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

import scala.collection.mutable
import scala.jdk.CollectionConverters._

import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.{Expression, GenericInternalRow}
import org.apache.spark.sql.connector.catalog.{MetadataColumn, SupportsMetadataColumns, SupportsRead, Table, TableCapability, TableProvider}
import org.apache.spark.sql.connector.expressions.{Expressions, FieldReference, NamedReference, SortOrder, Transform}
import org.apache.spark.sql.connector.expressions.filter.Predicate
import org.apache.spark.sql.connector.read._
import org.apache.spark.sql.execution.vectorized.OnHeapColumnVector
import org.apache.spark.sql.internal.connector.SupportsRuntimeCatalystFiltering
import org.apache.spark.sql.types.{BooleanType, DataType, LongType, StructField, StructType, VariantType}
import org.apache.spark.sql.util.CaseInsensitiveStringMap
import org.apache.spark.sql.vectorized.{ColumnarBatch, ColumnVector}
import org.apache.spark.util.Utils

/**
 * A read-only v2 source over a fixed table of longs that implements [[SupportsStorageFilters]]:
 * the key `k`, the value `v` holding `k * 10`, and with `wide` also `w` holding `k * 100`. Its
 * reader gets the filter in the task and drops the rows the filter rejects. It reads the columns
 * Spark requires, and builds the filter's row from the filter's `references()`. Its options:
 *  - `columnar`: reads in batches, through `StorageFilter.select`, rather than row by row;
 *  - `giveUp`: reads without the filter, as a reader does for a unit it cannot use the filter on.
 *    Here the unit is the partition;
 *  - `markAllChecked`: reads without the filter but marks every row checked, which breaks the
 *    contract on purpose;
 *  - `dropChecked`: `true` never appends the checked column, and `later` appends it to the first
 *    row or batch only. Either breaks the contract on purpose;
 *  - `checkedLayout`: `front` returns the checked column ahead of the scan's columns, in a columnar
 *    read only. `extra` returns one more column after it. Either breaks the contract on purpose;
 *  - `declared`: the comma-separated storage-filter columns, `k` by default, a dot separating a
 *    nested field;
 *  - `valueName`: the name of the value column, `v` by default;
 *  - `wide`: adds the column `w`;
 *  - `variant`: adds the variant column `j`, which the scan reads only through pushed extractions
 *    of a long. Each one holds `k * 10`;
 *  - `numPartitions`: splits the keys into that many input partitions, 1 by default;
 *  - `metadataColumn`: adds the metadata column `_pos`, which holds the key;
 *  - `runtimeFiltering`: `v2` makes the scan a `SupportsRuntimeV2Filtering` one over every column,
 *    with iterative pushdown on a table partitioned by `k`. `catalyst` makes it a
 *    `SupportsRuntimeCatalystFiltering` one that evaluates the filters on `k` fully. Neither
 *    applies the filters it takes, so they serve planning tests only;
 *  - `push`: the comma-separated pushed operations it takes whole without applying them, of
 *    `limit`, `offset`, `topN` and `sample`, for planning tests only.
 *
 * A filter that throws `StorageFilterNotAppliedException` is given up for the rest of the
 * partition too. The counters below record what the readers got, evaluated and returned.
 */
class StorageFilterTestSource extends TableProvider {

  override def inferSchema(options: CaseInsensitiveStringMap): StructType = {
    val schema = new StructType().add("k", LongType).add(options.getOrDefault("valueName", "v"),
      LongType)
    val wide = if (options.getBoolean("wide", false)) schema.add("w", LongType) else schema
    if (options.getBoolean("variant", false)) wide.add("j", VariantType) else wide
  }

  override def getTable(
      schema: StructType,
      partitioning: Array[Transform],
      properties: util.Map[String, String]): Table = {
    val options = new CaseInsensitiveStringMap(properties)
    if (options.getBoolean("metadataColumn", false)) {
      new StorageFilterTestTable(schema, options) with SupportsMetadataColumns {
        override def metadataColumns(): Array[MetadataColumn] = Array(new MetadataColumn {
          override def name(): String = StorageFilterTestSource.posName
          override def dataType(): DataType = LongType
        })
      }
    } else {
      new StorageFilterTestTable(schema, options)
    }
  }
}

object StorageFilterTestSource {

  /** Rows the source serves, `k` from 0 until numRows. */
  val numRows: Int = 200

  /** Rows a batch holds at most in a columnar read. */
  val batchSize: Int = 64

  /** The name of the metadata column the `metadataColumn` option adds. */
  val posName: String = "_pos"

  /** How many readers were created with a storage-filter request. */
  val filteredReaders = new AtomicInteger(0)

  /** How many of those requests asked for the checked column. */
  val checkedColumnRequests = new AtomicInteger(0)

  /** Rows the reader evaluated the storage filter against. */
  val rowsEvaluated = new AtomicLong(0)

  /** Rows the reader returned. */
  val rowsReturned = new AtomicLong(0)

  /** The serialized size of the first filter a reader got, before it evaluated the filter. */
  val filterBytes = new AtomicLong(0)

  /** The references of the filters the readers got, and the identities of those filters. */
  val filterReferences = mutable.Set.empty[Seq[String]]
  val filterIdentities = mutable.Set.empty[Int]

  /** How many times a runtime-filtering scan was handed filters. */
  val runtimeFilterCalls = new AtomicInteger(0)

  def reset(): Unit = {
    filteredReaders.set(0)
    checkedColumnRequests.set(0)
    rowsEvaluated.set(0)
    rowsReturned.set(0)
    filterBytes.set(0)
    filterReferences.synchronized(filterReferences.clear())
    filterIdentities.synchronized(filterIdentities.clear())
    runtimeFilterCalls.set(0)
  }

  /** The value of column `name` in the row of `key`. Any column but `k`, `w` and `_pos` is `v`. */
  def valueOf(name: String, key: Long): Long = name match {
    case "k" | `posName` => key
    case "w" => key * 100
    case _ => key * 10
  }

  /** The value of `field` in the row of `key`, a struct for the extractions of a variant. */
  def valueOf(field: StructField, key: Long): Any = field.dataType match {
    case struct: StructType => InternalRow.fromSeq(struct.map(_ => valueOf(field.name, key)))
    case _ => valueOf(field.name, key)
  }

  /** The columns of the row the filter evaluates, in that order. */
  def columnsOf(filter: StorageFilter): Seq[String] = filter.references.map(_.fieldNames.head).toSeq

  // Records the filter a reader got, before the reader evaluates it. Only the first reader
  // serializes it, since a later reader of the same task may get it evaluated, see `StorageFilter`.
  private[connector] def record(filter: StorageFilter): Unit = {
    if (filterBytes.get() == 0) filterBytes.compareAndSet(0, Utils.serialize(filter).length)
    filterReferences.synchronized(filterReferences += columnsOf(filter))
    filterIdentities.synchronized(filterIdentities += System.identityHashCode(filter))
  }
}

class StorageFilterTestTable(tableSchema: StructType, tableOptions: CaseInsensitiveStringMap)
  extends Table with SupportsRead {

  override def name(): String = "storage_filter_test"

  override def schema(): StructType = tableSchema

  override def partitioning(): Array[Transform] =
    if (tableOptions.getOrDefault("runtimeFiltering", "") == "v2") {
      Array(Expressions.identity("k"))
    } else {
      Array.empty
    }

  override def capabilities(): util.Set[TableCapability] =
    Set(TableCapability.BATCH_READ).asJava

  override def newScanBuilder(options: CaseInsensitiveStringMap): ScanBuilder =
    new StorageFilterTestScanBuilder(tableSchema, options)
}

/**
 * Prunes the columns Spark does not read, and takes the pushed operations named in `push` whole.
 * Its scan does not apply them, so they serve planning tests only. It takes every variant
 * extraction, and reads each into a struct field of the variant's column named by its position.
 */
class StorageFilterTestScanBuilder(tableSchema: StructType, options: CaseInsensitiveStringMap)
  extends SupportsPushDownRequiredColumns
  with SupportsPushDownLimit
  with SupportsPushDownOffset
  with SupportsPushDownTopN
  with SupportsPushDownTableSample
  with SupportsPushDownVariantExtractions {

  private val pushes = Utils.stringToSeq(options.getOrDefault("push", "")).toSet
  private var required = tableSchema
  private var extractions = Array.empty[VariantExtraction]

  override def pruneColumns(requiredSchema: StructType): Unit = required = requiredSchema

  override def pushVariantExtractions(pushed: Array[VariantExtraction]): Array[Boolean] = {
    extractions = pushed
    pushed.map(_ => true)
  }

  override def build(): Scan = {
    val byColumn = extractions.groupBy(_.columnName().head)
    val schema = StructType(required.map { field =>
      byColumn.get(field.name).map { columnExtractions =>
        field.copy(dataType = StructType(columnExtractions.zipWithIndex.map { case (e, i) =>
          StructField(i.toString, e.expectedDataType(), nullable = true, e.metadata())
        }))
      }.getOrElse(field)
    })
    val factory = StorageFilterTestReaderFactory(
      schema,
      columnar = options.getBoolean("columnar", false),
      giveUp = options.getBoolean("giveUp", false),
      marksAllChecked = options.getBoolean("markAllChecked", false),
      dropChecked = options.getOrDefault("dropChecked", ""),
      checkedLayout = options.getOrDefault("checkedLayout", ""))
    val declared = Utils.stringToSeq(options.getOrDefault("declared", "k"))
    val numPartitions = options.getInt("numPartitions", 1)
    options.getOrDefault("runtimeFiltering", "") match {
      case "v2" => new StorageFilterTestScan(schema, factory, declared, numPartitions)
          with SupportsRuntimeV2Filtering {
        override def filterAttributes(): Array[NamedReference] =
          readSchema().fieldNames.map(FieldReference.column).toArray[NamedReference]
        override def filter(predicates: Array[Predicate]): Unit =
          StorageFilterTestSource.runtimeFilterCalls.incrementAndGet()
        override def supportsIterativePushdown(): Boolean = true
      }
      case "catalyst" => new StorageFilterTestScan(schema, factory, declared, numPartitions)
          with SupportsRuntimeCatalystFiltering {
        override def filterAttributes(): Array[NamedReference] = Array(FieldReference.column("k"))
        override def fullyPushedFilterAttributes(): Array[NamedReference] = filterAttributes()
        override def filter(expressions: Array[Expression]): Unit =
          StorageFilterTestSource.runtimeFilterCalls.incrementAndGet()
      }
      case _ => new StorageFilterTestScan(schema, factory, declared, numPartitions)
    }
  }

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
    schema: StructType,
    factory: StorageFilterTestReaderFactory,
    declared: Seq[String],
    numPartitions: Int)
  extends Scan with Batch with SupportsReportStatistics with SupportsStorageFilters {

  override def readSchema(): StructType = schema

  override def toBatch: Batch = this

  // Over the suite's application-side scan size threshold, so InjectRuntimeFilter considers this
  // side worth filtering.
  override def estimateStatistics(): Statistics = new Statistics {
    override def sizeInBytes(): OptionalLong = OptionalLong.of(10L * 1024 * 1024)
    override def numRows(): OptionalLong = OptionalLong.of(StorageFilterTestSource.numRows.toLong)
  }

  override def storageFilterAttributes(): Array[NamedReference] =
    declared.map(name => FieldReference(name.split('.').toSeq)).toArray[NamedReference]

  override def planInputPartitions(): Array[InputPartition] = {
    val n = StorageFilterTestSource.numRows
    (0 until numPartitions).map { i =>
      RangeInputPartition(i * n / numPartitions, (i + 1) * n / numPartitions)
    }.toArray
  }

  override def createReaderFactory(): PartitionReaderFactory = factory
}

case class StorageFilterTestReaderFactory(
    schema: StructType,
    columnar: Boolean,
    giveUp: Boolean,
    marksAllChecked: Boolean,
    dropChecked: String,
    checkedLayout: String)
  extends PartitionReaderFactory {

  override def supportColumnarReads(partition: InputPartition): Boolean = columnar

  // The filter the reader applies, none when it gives the filter up or only marks the rows.
  private def filterOf(request: StorageFilterRequest): Option[StorageFilter] = {
    StorageFilterTestSource.filteredReaders.incrementAndGet()
    StorageFilterTestSource.record(request.filter())
    if (request.appendsCheckedColumn()) {
      StorageFilterTestSource.checkedColumnRequests.incrementAndGet()
    }
    if (giveUp || marksAllChecked) None else Some(request.filter())
  }

  override def createReader(partition: InputPartition): PartitionReader[InternalRow] =
    new StorageFilterTestReader(keys(partition), schema, None, appendsChecked = false)

  override def createReader(
      partition: InputPartition,
      request: StorageFilterRequest): PartitionReader[InternalRow] =
    new StorageFilterTestReader(keys(partition), schema, filterOf(request),
      request.appendsCheckedColumn() && dropChecked != "true", marksAllChecked,
      dropsCheckedLater = dropChecked == "later", checkedLayout)

  override def createColumnarReader(partition: InputPartition): PartitionReader[ColumnarBatch] =
    new StorageFilterTestColumnarReader(keys(partition), schema, None, appendsChecked = false)

  override def createColumnarReader(
      partition: InputPartition,
      request: StorageFilterRequest): PartitionReader[ColumnarBatch] =
    new StorageFilterTestColumnarReader(keys(partition), schema, filterOf(request),
      request.appendsCheckedColumn() && dropChecked != "true", marksAllChecked,
      dropsCheckedLater = dropChecked == "later", checkedLayout)

  // The keys from `start` until `end`.
  private def keys(partition: InputPartition): RangeInputPartition =
    partition.asInstanceOf[RangeInputPartition]
}

/**
 * Serves the rows of `partition` row by row, holding the columns of `schema`, and drops any row
 * `filter` rejects, with the checked column last when `appendsChecked`. The filter's row holds
 * the values of its `references()`. `filter` is cleared when it cannot be applied.
 * `marksAllChecked` marks every row checked. `dropsCheckedLater` appends the checked column to
 * the first row only. `checkedLayout` `extra` returns one more column after the checked column.
 */
class StorageFilterTestReader(
    partition: RangeInputPartition,
    schema: StructType,
    private var filter: Option[StorageFilter],
    appendsChecked: Boolean,
    marksAllChecked: Boolean = false,
    dropsCheckedLater: Boolean = false,
    checkedLayout: String = "")
  extends PartitionReader[InternalRow] {

  require(checkedLayout != "front", "the `front` layout is for columnar reads only")

  private val referenceNames = filter.map(StorageFilterTestSource.columnsOf).getOrElse(Nil)
  private var current: Long = partition.start - 1L
  private var row: InternalRow = _
  private var returnedAny = false

  override def next(): Boolean = {
    var found = false
    while (!found && current + 1 < partition.end) {
      current += 1
      val key = current
      val passes = filter.forall { f =>
        StorageFilterTestSource.rowsEvaluated.incrementAndGet()
        val keyRow = new GenericInternalRow(
          referenceNames.map(StorageFilterTestSource.valueOf(_, key)).toArray[Any])
        try f.eval(keyRow) catch {
          case _: StorageFilterNotAppliedException =>
            filter = None
            true
        }
      }
      if (passes) {
        val values = schema.fields.map(StorageFilterTestSource.valueOf(_, key))
        val checked = filter.isDefined || marksAllChecked
        val withChecked = appendsChecked && !(dropsCheckedLater && returnedAny)
        val extra = if (withChecked && checkedLayout == "extra") Seq(0L) else Nil
        row = new GenericInternalRow(
          (if (withChecked) values :+ checked else values) ++ extra)
        returnedAny = true
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
 * Serves the rows of `partition` in batches of up to `batchSize` rows, holding the columns of
 * `schema`. It keeps the rows `filter.select` keeps over a batch of the filter's `references()`,
 * with the checked column last when `appendsChecked`. `filter` is cleared when it cannot be
 * applied. `marksAllChecked` marks every row checked. `dropsCheckedLater` appends the checked
 * column to the first batch only. `checkedLayout` places the checked column as the source's option
 * says.
 */
class StorageFilterTestColumnarReader(
    partition: RangeInputPartition,
    schema: StructType,
    private var filter: Option[StorageFilter],
    appendsChecked: Boolean,
    marksAllChecked: Boolean = false,
    dropsCheckedLater: Boolean = false,
    checkedLayout: String = "")
  extends PartitionReader[ColumnarBatch] {

  private val batchSize = StorageFilterTestSource.batchSize
  private val referenceNames = filter.map(StorageFilterTestSource.columnsOf).getOrElse(Nil)
  private val keys = referenceNames.map(_ => new OnHeapColumnVector(batchSize, LongType))
  private val keyBatch = new ColumnarBatch(keys.toArray[ColumnVector])
  private val selected = new Array[Int](batchSize)
  private val values = schema.fields.toSeq.map(f => new OnHeapColumnVector(batchSize, f.dataType))
  private val checked = Option.when(appendsChecked) {
    new OnHeapColumnVector(batchSize, BooleanType)
  }
  private val extra = Option.when(appendsChecked && checkedLayout == "extra") {
    new OnHeapColumnVector(batchSize, LongType)
  }
  private val batch = new ColumnarBatch(checkedLayout match {
    case "front" => (checked ++ values).toArray[ColumnVector]
    case _ => (values ++ checked ++ extra).toArray[ColumnVector]
  })
  // The same vectors without the checked column, which `batch` closes.
  private val narrowBatch = new ColumnarBatch(values.toArray[ColumnVector])
  private var current = batch
  private var returnedAny = false
  private var start = partition.start

  override def next(): Boolean = {
    var count = 0
    while (count == 0 && start < partition.end) {
      val n = math.min(batchSize, partition.end - start)
      referenceNames.zip(keys).foreach { case (name, vector) =>
        (0 until n).foreach { i =>
          vector.putLong(i, StorageFilterTestSource.valueOf(name, start + i))
        }
      }
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
        schema.fields.zip(values).foreach { case (field, vector) =>
          val value = StorageFilterTestSource.valueOf(field.name, key)
          field.dataType match {
            case struct: StructType =>
              struct.indices.foreach(j => vector.getChild(j).putLong(i, value))
            case _ => vector.putLong(i, value)
          }
        }
      }
      checked.foreach(_.putBooleans(0, count, filter.isDefined || marksAllChecked))
      start += n
    }
    if (dropsCheckedLater && returnedAny) current = narrowBatch
    current.setNumRows(count)
    returnedAny = returnedAny || count > 0
    StorageFilterTestSource.rowsReturned.addAndGet(count)
    count > 0
  }

  override def get(): ColumnarBatch = current

  override def close(): Unit = {
    batch.close()
    keyBatch.close()
  }
}
