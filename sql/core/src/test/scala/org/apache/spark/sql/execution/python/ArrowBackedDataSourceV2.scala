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
package org.apache.spark.sql.execution.python

import java.nio.charset.StandardCharsets.UTF_8
import java.util
import java.util.concurrent.atomic.AtomicInteger

import org.apache.arrow.memory.BufferAllocator
import org.apache.arrow.vector._

import org.apache.spark.TaskContext
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.connector.catalog.{SupportsRead, Table, TableCapability, TableProvider}
import org.apache.spark.sql.connector.expressions.Transform
import org.apache.spark.sql.connector.read._
import org.apache.spark.sql.execution.vectorized.ReusedArrowColumnVector
import org.apache.spark.sql.types._
import org.apache.spark.sql.util.ArrowUtils
import org.apache.spark.sql.util.CaseInsensitiveStringMap
import org.apache.spark.sql.vectorized.{ArrowColumnVector, ColumnarArray, ColumnarBatch, ColumnarMap, ColumnVector}
import org.apache.spark.unsafe.types.UTF8String

/**
 * A test-only DataSource V2 implementation that produces Arrow-backed
 * [[ColumnarBatch]]es (with [[ArrowColumnVector]] columns).
 *
 * Used for testing and benchmarking the columnar Arrow Python UDF path
 * that bypasses ColumnarToRow conversion.
 *
 * Schema: (id INT, name STRING, value DOUBLE)
 * Data: configurable number of rows (default 10000), split across partitions.
 *
 * Usage:
 * {{{
 *   spark.read
 *     .format("org.apache.spark.sql.execution.python.ArrowBackedDataSourceV2")
 *     .option("numRows", "10000")
 *     .option("numPartitions", "2")
 *     .load()
 * }}}
 */
class ArrowBackedDataSourceV2 extends TableProvider {

  override def inferSchema(options: CaseInsensitiveStringMap): StructType = {
    ArrowBackedDataSourceV2.SCHEMA
  }

  override def getTable(
      schema: StructType,
      partitioning: Array[Transform],
      properties: util.Map[String, String]): Table = {
    val options = new CaseInsensitiveStringMap(properties)
    new ArrowBackedTable(options)
  }
}

object ArrowBackedDataSourceV2 {
  val SCHEMA: StructType = new StructType()
    .add("id", IntegerType, nullable = false)
    .add("name", StringType, nullable = true)
    .add("value", DoubleType, nullable = true)
    .add("data", StringType, nullable = true)

  val DEFAULT_NUM_ROWS: Int = 10000
  val DEFAULT_NUM_PARTITIONS: Int = 2
  val BATCH_SIZE: Int = 1024
  // The number of batches without rows that a partition reader returns with `emptyBatches`.
  val NUM_EMPTY_BATCHES: Int = 3

  // The batches that ArrowBackedPartitionReaders returned, and the ones among them that their
  // consumer released with closeIfFreeable.
  val returnedBatches = new AtomicInteger
  val releasedBatches = new AtomicInteger
  // The most batches of one reader returned and not released yet at a time.
  val maxUnreleasedBatches = new AtomicInteger

  def resetBatchCounters(): Unit = {
    returnedBatches.set(0)
    releasedBatches.set(0)
    maxUnreleasedBatches.set(0)
  }
}

/** Table that supports batch read with columnar output. */
private class ArrowBackedTable(options: CaseInsensitiveStringMap)
    extends Table with SupportsRead {

  override def name(): String = "ArrowBackedTestTable"

  override def schema(): StructType = ArrowBackedDataSourceV2.SCHEMA

  override def capabilities(): util.Set[TableCapability] = {
    util.EnumSet.of(TableCapability.BATCH_READ)
  }

  override def newScanBuilder(options: CaseInsensitiveStringMap): ScanBuilder = {
    new ArrowBackedScanBuilder(this.options)
  }
}

/**
 * Scan builder / Scan / Batch implementation. If the option `pruneColumns` is true, the scan reads
 * only the columns that the query requires.
 */
private class ArrowBackedScanBuilder(options: CaseInsensitiveStringMap)
    extends ScanBuilder with SupportsPushDownRequiredColumns with Scan with Batch {

  private val numRows =
    options.getInt("numRows", ArrowBackedDataSourceV2.DEFAULT_NUM_ROWS)
  private val numPartitions =
    options.getInt("numPartitions", ArrowBackedDataSourceV2.DEFAULT_NUM_PARTITIONS)

  override def build(): Scan = this

  override def toBatch: Batch = this

  private val supportsColumnPruning = options.getBoolean("pruneColumns", false)
  private var requiredSchema = ArrowBackedDataSourceV2.SCHEMA

  override def pruneColumns(requiredSchema: StructType): Unit = {
    if (supportsColumnPruning) this.requiredSchema = requiredSchema
  }

  override def readSchema(): StructType = requiredSchema

  override def planInputPartitions(): Array[InputPartition] = {
    val rowsPerPartition = numRows / numPartitions
    (0 until numPartitions).map { i =>
      val start = i * rowsPerPartition
      val end = if (i == numPartitions - 1) numRows else start + rowsPerPartition
      ArrowPartition(start, end): InputPartition
    }.toArray
  }

  private val columnar =
    options.getBoolean("columnar", true)
  private val reuseVectors =
    options.getBoolean("reuseVectors", false)
  private val batchSize =
    options.getInt("batchSize", ArrowBackedDataSourceV2.BATCH_SIZE)
  private val arrowIdColumn =
    options.getBoolean("arrowIdColumn", true)
  private val emptyBatches =
    options.getBoolean("emptyBatches", false)

  override def createReaderFactory(): PartitionReaderFactory = {
    new ArrowBackedReaderFactory(
      columnar, reuseVectors, batchSize, arrowIdColumn, emptyBatches,
      requiredSchema.fieldNames.map(ArrowBackedDataSourceV2.SCHEMA.fieldIndex))
  }
}

private case class ArrowPartition(start: Int, end: Int)
    extends InputPartition

/**
 * Factory that produces Arrow-backed readers.
 *
 * @param columnar if true, returns columnar ColumnarBatch output
 *   (supportsColumnarReads = true). If false, returns row-based
 *   InternalRow output by iterating the ColumnarBatch row by row.
 *   This allows benchmarking the same data source with and without
 *   the columnar optimization.
 * @param arrowIdColumn if false, the `id` column of the batches is not an
 *   [[ArrowColumnVector]], so that a batch is not entirely Arrow-backed.
 * @param emptyBatches if true, the reader with fresh vectors also returns
 *   [[ArrowBackedDataSourceV2.NUM_EMPTY_BATCHES]] batches without rows, first, in the middle and
 *   last.
 * @param columnIndices the indices of the columns of the schema to read
 */
private class ArrowBackedReaderFactory(
    columnar: Boolean,
    reuseVectors: Boolean,
    batchSize: Int,
    arrowIdColumn: Boolean,
    emptyBatches: Boolean,
    columnIndices: Array[Int])
  extends PartitionReaderFactory {

  override def supportColumnarReads(
      partition: InputPartition): Boolean = columnar

  override def createReader(
      partition: InputPartition): PartitionReader[InternalRow] = {
    val ArrowPartition(start, end) = partition
    val inner = new ArrowBackedPartitionReader(
      start, end, batchSize, columnIndices, arrowIdColumn)
    // Wrap columnar reader as row reader.
    new PartitionReader[InternalRow] {
      private var rowIter: java.util.Iterator[InternalRow] = _
      override def next(): Boolean = {
        if (rowIter != null && rowIter.hasNext) return true
        if (!inner.next()) return false
        rowIter = inner.get().rowIterator()
        rowIter.hasNext
      }
      override def get(): InternalRow = rowIter.next()
      override def close(): Unit = inner.close()
    }
  }

  override def createColumnarReader(
      partition: InputPartition): PartitionReader[ColumnarBatch] = {
    val ArrowPartition(start, end) = partition
    if (reuseVectors) {
      new ReusingArrowPartitionReader(start, end, batchSize, columnIndices)
    } else {
      new ArrowBackedPartitionReader(
        start, end, batchSize, columnIndices, arrowIdColumn, emptyBatches)
    }
  }
}

/**
 * Reads the same rows as [[ArrowBackedPartitionReader]], with short values in `data`, but refills
 * the same vectors for every batch and closes them once the reader is closed, as columnar readers
 * may: a batch is only valid until the next one. Its columns are [[ReusedArrowColumnVector]]s,
 * whose closeIfFreeable is a no-op.
 */
private class ReusingArrowPartitionReader(
    start: Int, end: Int, batchSize: Int, columnIndices: Array[Int])
    extends PartitionReader[ColumnarBatch] {
  private var current = start
  private val allocator = ArrowUtils.rootAllocator.newChildAllocator(
    s"reusing-arrow-test-reader-$start", 0, Long.MaxValue)
  private val root = VectorSchemaRoot.create(
    ArrowUtils.toArrowSchema(ArrowBackedDataSourceV2.SCHEMA, null,
      errorOnDuplicatedFieldNames = true, largeVarTypes = false),
    allocator)
  private val batch = new ColumnarBatch(columnIndices.map { i =>
    new ReusedArrowColumnVector(root.getFieldVectors.get(i)): ColumnVector
  })

  override def next(): Boolean = {
    if (current >= end) return false
    val count = math.min(batchSize, end - current)
    val id = root.getVector("id").asInstanceOf[IntVector]
    val name = root.getVector("name").asInstanceOf[VarCharVector]
    val value = root.getVector("value").asInstanceOf[Float8Vector]
    val data = root.getVector("data").asInstanceOf[VarCharVector]
    // Overwrites the values of the previous batch in place.
    root.getFieldVectors.forEach(_.reset())
    (0 until count).foreach { i =>
      val row = current + i
      id.setSafe(i, row)
      name.setSafe(i, s"row_$row".getBytes(UTF_8))
      value.setSafe(i, row * 0.1)
      data.setSafe(i, s"data_$row".getBytes(UTF_8))
    }
    root.setRowCount(count)
    batch.setNumRows(count)
    current += count
    true
  }

  override def get(): ColumnarBatch = batch

  override def close(): Unit = {
    root.close()
    allocator.close()
  }
}

/**
 * Partition reader that produces Arrow-backed [[ColumnarBatch]]es.
 *
 * Each batch contains up to `batchSize` rows with:
 * - id (INT): sequential integer starting from partition start
 * - name (STRING): "row_<id>"
 * - value (DOUBLE): id * 0.1
 * - data (STRING): 100 random characters
 *
 * Every batch has vectors of its own, which its consumer releases with closeIfFreeable. The ones
 * it does not release are closed when the task completes. Their release is counted in
 * [[ArrowBackedDataSourceV2]].
 *
 * If `arrowIdColumn` is false, the `id` column is a [[ColumnVector]] over its Arrow vector that is
 * not an [[ArrowColumnVector]]. If `emptyBatches` is true, the reader also returns
 * [[ArrowBackedDataSourceV2.NUM_EMPTY_BATCHES]] batches without rows: before the first batch,
 * after half of the batches and after the last one. Some of them are adjacent if there are fewer
 * than two batches. The batches have the columns at `columnIndices`, the first of which counts the
 * releases; the others are closed.
 */
private class ArrowBackedPartitionReader(
    start: Int,
    end: Int,
    batchSize: Int,
    columnIndices: Array[Int],
    arrowIdColumn: Boolean = true,
    emptyBatches: Boolean = false)
  extends PartitionReader[ColumnarBatch] {

  private var current = start
  // The positions before which the reader returns a batch without rows, in order. A position can
  // occur more than once, so that the number of the batches does not depend on the parameters.
  private val emptyBatchPositions = if (emptyBatches) {
    scala.collection.mutable.Queue(start, start + (end - start) / batchSize / 2 * batchSize, end)
  } else {
    scala.collection.mutable.Queue.empty[Int]
  }
  private var currentBatch: ColumnarBatch = _

  // Single allocator for the entire partition reader lifetime.
  // The vectors of the returned batches are closed when the task
  // completes, so we must NOT close the allocator until then.
  private val allocator: BufferAllocator =
    ArrowUtils.rootAllocator.newChildAllocator(
      s"arrow-test-reader-$start", 0, Long.MaxValue)

  // Track all batches so we can close them when the task completes.
  private val allBatches =
    new java.util.ArrayList[ColumnarBatch]()
  private var numReleased = 0

  // A consumer that stops early does not release the batches, so they are closed when the task
  // completes. A pipelined Python writer that reads them is stopped by a pre-completion listener,
  // which runs before this one.
  TaskContext.get().addTaskCompletionListener[Unit] { _ =>
    allBatches.synchronized {
      allBatches.forEach(_.close())
      allBatches.clear()
    }
    allocator.close()
  }

  private def countRelease(): Unit = allBatches.synchronized {
    numReleased += 1
    ArrowBackedDataSourceV2.releasedBatches.incrementAndGet()
  }

  /** The first column of a batch, which counts the releases of the batch by its consumer. */
  private class ReleaseCountingArrowColumnVector(vector: ValueVector)
      extends ArrowColumnVector(vector) {
    override def closeIfFreeable(): Unit = {
      countRelease()
      super.closeIfFreeable()
    }
  }

  /** Like [[ReleaseCountingArrowColumnVector]], but not an [[ArrowColumnVector]]. */
  private class ReleaseCountingColumnVector(vector: ArrowColumnVector)
      extends ColumnVector(vector.dataType()) {
    override def closeIfFreeable(): Unit = {
      countRelease()
      close()
    }
    override def close(): Unit = vector.close()
    override def hasNull(): Boolean = vector.hasNull()
    override def numNulls(): Int = vector.numNulls()
    override def isNullAt(rowId: Int): Boolean = vector.isNullAt(rowId)
    override def getBoolean(rowId: Int): Boolean = vector.getBoolean(rowId)
    override def getByte(rowId: Int): Byte = vector.getByte(rowId)
    override def getShort(rowId: Int): Short = vector.getShort(rowId)
    override def getInt(rowId: Int): Int = vector.getInt(rowId)
    override def getLong(rowId: Int): Long = vector.getLong(rowId)
    override def getFloat(rowId: Int): Float = vector.getFloat(rowId)
    override def getDouble(rowId: Int): Double = vector.getDouble(rowId)
    override def getArray(rowId: Int): ColumnarArray = vector.getArray(rowId)
    override def getMap(ordinal: Int): ColumnarMap = vector.getMap(ordinal)
    override def getDecimal(rowId: Int, precision: Int, scale: Int): Decimal =
      vector.getDecimal(rowId, precision, scale)
    override def getUTF8String(rowId: Int): UTF8String = vector.getUTF8String(rowId)
    override def getBinary(rowId: Int): Array[Byte] = vector.getBinary(rowId)
    override def getChild(ordinal: Int): ColumnVector = vector.getChild(ordinal)
  }

  override def next(): Boolean = {
    val count = if (emptyBatchPositions.headOption.contains(current)) {
      emptyBatchPositions.dequeue()
      0
    } else if (current >= end) {
      return false
    } else {
      math.min(batchSize, end - current)
    }

    // Create Arrow vectors
    val idVector = createIntVector(allocator, "id", count)
    val nameVector = createStringVector(allocator, "name", count)
    val valueVector = createDoubleVector(allocator, "value", count)
    val dataVector = createLongStringVector(
      allocator, "data", count)

    // Wrap in ArrowColumnVectors
    val vectors = Array[ValueVector](idVector, nameVector, valueVector, dataVector)
    vectors.indices.filterNot(columnIndices.contains).foreach(vectors(_).close())
    val columns: Array[ColumnVector] = columnIndices.zipWithIndex.map {
      case (0, pos) if !arrowIdColumn =>
        val vector = new ArrowColumnVector(idVector)
        if (pos == 0) new ReleaseCountingColumnVector(vector) else vector
      case (i, 0) => new ReleaseCountingArrowColumnVector(vectors(i))
      case (i, _) => new ArrowColumnVector(vectors(i))
    }

    currentBatch = new ColumnarBatch(columns, count)
    allBatches.synchronized {
      allBatches.add(currentBatch)
      ArrowBackedDataSourceV2.returnedBatches.incrementAndGet()
      ArrowBackedDataSourceV2.maxUnreleasedBatches.accumulateAndGet(
        allBatches.size - numReleased, (a, b) => math.max(a, b))
    }
    current += count
    true
  }

  override def get(): ColumnarBatch = currentBatch

  // The returned batches are closed when the task completes.
  override def close(): Unit = {
    currentBatch = null
  }

  private def createIntVector(
      alloc: BufferAllocator, name: String, count: Int): IntVector = {
    val field = ArrowUtils.toArrowField(name, IntegerType, nullable = false, null)
    val vector = field.createVector(alloc).asInstanceOf[IntVector]
    vector.allocateNew(count)
    (0 until count).foreach { i =>
      vector.setSafe(i, current + i)
    }
    vector.setValueCount(count)
    vector
  }

  private def createStringVector(
      alloc: BufferAllocator, name: String, count: Int): VarCharVector = {
    val field = ArrowUtils.toArrowField(name, StringType, nullable = true, null)
    val vector = field.createVector(alloc).asInstanceOf[VarCharVector]
    vector.allocateNew()
    (0 until count).foreach { i =>
      val bytes = s"row_${current + i}".getBytes("UTF-8")
      vector.setSafe(i, bytes, 0, bytes.length)
    }
    vector.setValueCount(count)
    vector
  }

  private def createDoubleVector(
      alloc: BufferAllocator, name: String, count: Int): Float8Vector = {
    val field = ArrowUtils.toArrowField(name, DoubleType, nullable = true, null)
    val vector = field.createVector(alloc).asInstanceOf[Float8Vector]
    vector.allocateNew(count)
    (0 until count).foreach { i =>
      vector.setSafe(i, (current + i) * 0.1)
    }
    vector.setValueCount(count)
    vector
  }

  // Random long strings (~200 chars each) to stress ArrowWriter's
  // per-element VarChar serialization overhead.
  private val rng = new java.util.Random(42)
  private def randomString(len: Int): String = {
    val sb = new java.lang.StringBuilder(len)
    var i = 0
    while (i < len) {
      sb.append(('a' + rng.nextInt(26)).toChar)
      i += 1
    }
    sb.toString
  }

  private def createLongStringVector(
      alloc: BufferAllocator, name: String,
      count: Int): VarCharVector = {
    val field = ArrowUtils.toArrowField(
      name, StringType, nullable = true, null)
    val vector = field.createVector(alloc)
      .asInstanceOf[VarCharVector]
    vector.allocateNew()
    (0 until count).foreach { i =>
      val bytes = randomString(100).getBytes("UTF-8")
      vector.setSafe(i, bytes, 0, bytes.length)
    }
    vector.setValueCount(count)
    vector
  }
}
