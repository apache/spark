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

import java.util

import org.apache.arrow.memory.BufferAllocator
import org.apache.arrow.vector._

import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.connector.catalog.{SupportsRead, Table, TableCapability, TableProvider}
import org.apache.spark.sql.connector.expressions.Transform
import org.apache.spark.sql.connector.read._
import org.apache.spark.sql.types._
import org.apache.spark.sql.util.ArrowUtils
import org.apache.spark.sql.util.CaseInsensitiveStringMap
import org.apache.spark.sql.vectorized.{ArrowColumnVector, ColumnarBatch, ColumnVector}

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

/** Scan builder / Scan / Batch implementation. */
private class ArrowBackedScanBuilder(options: CaseInsensitiveStringMap)
    extends ScanBuilder with Scan with Batch {

  private val numRows =
    options.getInt("numRows", ArrowBackedDataSourceV2.DEFAULT_NUM_ROWS)
  private val numPartitions =
    options.getInt("numPartitions", ArrowBackedDataSourceV2.DEFAULT_NUM_PARTITIONS)

  override def build(): Scan = this

  override def toBatch: Batch = this

  override def readSchema(): StructType = ArrowBackedDataSourceV2.SCHEMA

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

  override def createReaderFactory(): PartitionReaderFactory = {
    new ArrowBackedReaderFactory(columnar)
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
 */
private class ArrowBackedReaderFactory(columnar: Boolean)
    extends PartitionReaderFactory {

  override def supportColumnarReads(
      partition: InputPartition): Boolean = columnar

  override def createReader(
      partition: InputPartition): PartitionReader[InternalRow] = {
    val ArrowPartition(start, end) = partition
    val inner = new ArrowBackedPartitionReader(start, end)
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
    new ArrowBackedPartitionReader(start, end)
  }
}

/**
 * Partition reader that produces Arrow-backed [[ColumnarBatch]]es.
 *
 * Each batch contains up to [[ArrowBackedDataSourceV2.BATCH_SIZE]] rows with:
 * - id (INT): sequential integer starting from partition start
 * - name (STRING): "row_<id>"
 * - value (DOUBLE): id * 0.1
 */
private class ArrowBackedPartitionReader(start: Int, end: Int)
    extends PartitionReader[ColumnarBatch] {

  private val batchSize = ArrowBackedDataSourceV2.BATCH_SIZE
  private var current = start
  private var currentBatch: ColumnarBatch = _

  // Single allocator for the entire partition reader lifetime.
  // Arrow vectors from earlier batches may still be referenced by
  // downstream operators (e.g., pass-through column queue), so we
  // must NOT close the allocator until the reader itself is closed.
  private val allocator: BufferAllocator =
    ArrowUtils.rootAllocator.newChildAllocator(
      s"arrow-test-reader-$start", 0, Long.MaxValue)

  // Track all batches so we can close them in close().
  private val allBatches =
    new java.util.ArrayList[ColumnarBatch]()

  override def next(): Boolean = {
    if (current >= end) return false

    val count = math.min(batchSize, end - current)

    // Create Arrow vectors
    val idVector = createIntVector(allocator, "id", count)
    val nameVector = createStringVector(allocator, "name", count)
    val valueVector = createDoubleVector(allocator, "value", count)
    val dataVector = createLongStringVector(
      allocator, "data", count)

    // Wrap in ArrowColumnVector
    val columns: Array[ColumnVector] = Array(
      new ArrowColumnVector(idVector),
      new ArrowColumnVector(nameVector),
      new ArrowColumnVector(valueVector),
      new ArrowColumnVector(dataVector))

    currentBatch = new ColumnarBatch(columns, count)
    allBatches.add(currentBatch)
    current += count
    true
  }

  override def get(): ColumnarBatch = currentBatch

  override def close(): Unit = {
    allBatches.forEach(_.close())
    allBatches.clear()
    currentBatch = null
    allocator.close()
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
