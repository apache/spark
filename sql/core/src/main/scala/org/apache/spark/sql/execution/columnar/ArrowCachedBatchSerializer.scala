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

package org.apache.spark.sql.execution.columnar

import java.io.{ByteArrayInputStream, ByteArrayOutputStream}
import java.nio.channels.Channels

import scala.jdk.CollectionConverters._

import org.apache.arrow.compression.{Lz4CompressionCodec, ZstdCompressionCodec}
import org.apache.arrow.vector.{VectorLoader, VectorSchemaRoot, VectorUnloader}
import org.apache.arrow.vector.compression.{CompressionCodec, NoCompressionCodec}
import org.apache.arrow.vector.ipc.{ReadChannel, WriteChannel}
import org.apache.arrow.vector.ipc.message.{ArrowRecordBatch, MessageSerializer}

import org.apache.spark.{SparkException, TaskContext}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.Attribute
import org.apache.spark.sql.catalyst.expressions.codegen.UnsafeRowWriter
import org.apache.spark.sql.catalyst.types.DataTypeUtils
import org.apache.spark.sql.columnar.{CachedBatch, SimpleMetricsCachedBatchSerializer}
import org.apache.spark.sql.execution.arrow.ArrowWriter
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.types._
import org.apache.spark.sql.util.ArrowUtils
import org.apache.spark.sql.vectorized.{ArrowColumnVector, ColumnarBatch, ColumnVector}
import org.apache.spark.storage.StorageLevel
import org.apache.spark.unsafe.types.UTF8String
import org.apache.spark.util.Utils

/**
 * A [[CachedBatchSerializer]] that uses Apache Arrow as the cache format.
 *
 * This serializer:
 *  - Supports both row-based (InternalRow) and columnar (ColumnarBatch) input
 *  - Stores data in Arrow IPC streaming format with optional compression (zstd/lz4)
 *  - Enables zero-copy columnar reads when output is ColumnarBatch
 *  - Uses off-heap memory via Arrow allocators
 *  - Collects per-column statistics for partition pruning
 *  - Provides efficient interoperability with Arrow ecosystem
 *
 * Configuration options:
 *  - spark.sql.cache.serializer: Set to this class name to enable
 *  - spark.sql.execution.arrow.maxRecordsPerBatch: Max rows per cached batch
 *  - spark.sql.execution.arrow.compression.codec: Compression (none/zstd/lz4)
 *  - spark.sql.inMemoryColumnarStorage.enableVectorizedReader: Enable columnar output
 */
class ArrowCachedBatchSerializer extends SimpleMetricsCachedBatchSerializer {

  override def supportsColumnarInput(schema: Seq[Attribute]): Boolean = {
    // Check if all data types in the schema are supported by Arrow
    schema.forall(attr => ArrowUtils.isSupportedByArrow(attr.dataType))
  }

  override def convertInternalRowToCachedBatch(
      input: RDD[InternalRow],
      schema: Seq[Attribute],
      storageLevel: StorageLevel,
      conf: SQLConf): RDD[CachedBatch] = {
    // Capture config values on driver before RDD transformation
    val sparkSchema = DataTypeUtils.fromAttributes(schema)
    val maxRecordsPerBatch = conf.arrowMaxRecordsPerBatch
    val timeZoneId = conf.sessionLocalTimeZone
    val compressionCodecName = conf.arrowCompressionCodec
    val compressionLevel = conf.arrowZstdCompressionLevel

    input.mapPartitionsInternal { rowIterator =>
      new InternalRowToArrowCachedBatchIterator(
        rowIterator,
        schema,
        sparkSchema,
        maxRecordsPerBatch,
        timeZoneId,
        compressionCodecName,
        compressionLevel)
    }
  }

  override def convertColumnarBatchToCachedBatch(
      input: RDD[ColumnarBatch],
      schema: Seq[Attribute],
      storageLevel: StorageLevel,
      conf: SQLConf): RDD[CachedBatch] = {
    // Capture config values on driver before RDD transformation
    val sparkSchema = DataTypeUtils.fromAttributes(schema)
    val timeZoneId = conf.sessionLocalTimeZone
    val compressionCodecName = conf.arrowCompressionCodec
    val compressionLevel = conf.arrowZstdCompressionLevel

    input.mapPartitionsInternal { batchIterator =>
      new ColumnarBatchToArrowCachedBatchIterator(
        batchIterator,
        schema,
        sparkSchema,
        timeZoneId,
        compressionCodecName,
        compressionLevel)
    }
  }

  override def supportsColumnarOutput(schema: StructType): Boolean = {
    // Always support columnar output with Arrow
    true
  }

  override def vectorTypes(attributes: Seq[Attribute], conf: SQLConf): Option[Seq[String]] = {
    Option(Seq.fill(attributes.length)(classOf[ArrowColumnVector].getName))
  }

  override def convertCachedBatchToColumnarBatch(
      input: RDD[CachedBatch],
      cacheAttributes: Seq[Attribute],
      selectedAttributes: Seq[Attribute],
      conf: SQLConf): RDD[ColumnarBatch] = {
    val cacheSchema = DataTypeUtils.fromAttributes(cacheAttributes)
    val selectedSchema = DataTypeUtils.fromAttributes(selectedAttributes)
    val columnIndices =
      selectedAttributes.map(a => cacheAttributes.map(o => o.exprId).indexOf(a.exprId)).toArray
    // Capture config on driver
    val timeZoneId = conf.sessionLocalTimeZone

    input.mapPartitionsInternal { batchIterator =>
      new ArrowCachedBatchToColumnarBatchIterator(
        batchIterator,
        cacheSchema,
        selectedSchema,
        columnIndices,
        timeZoneId)
    }
  }

  override def convertCachedBatchToInternalRow(
      input: RDD[CachedBatch],
      cacheAttributes: Seq[Attribute],
      selectedAttributes: Seq[Attribute],
      conf: SQLConf): RDD[InternalRow] = {
    val cacheSchema = DataTypeUtils.fromAttributes(cacheAttributes)
    val selectedSchema = DataTypeUtils.fromAttributes(selectedAttributes)
    val timeZoneId = conf.sessionLocalTimeZone

    // Calculate column indices for projection
    val selectedIndices = selectedAttributes.map { attr =>
      cacheAttributes.indexWhere(_.exprId == attr.exprId)
    }.toArray

    // Check if all selected types can use the fast path.
    // Types not handled by ArrowColumnReader must use the fallback path.
    val needsFallback = selectedSchema.fields.exists { f =>
      f.dataType match {
        case _: ArrayType | _: StructType | _: MapType => true
        case CalendarIntervalType | VariantType | NullType => true
        case _: UserDefinedType[_] => true
        case _ => false
      }
    }

    if (needsFallback) {
      // Fall back to columnar-to-row conversion via ColumnarBatch for complex types.
      // Use UnsafeProjection to convert ColumnarBatchRow to UnsafeRow.
      convertCachedBatchToColumnarBatch(input, cacheAttributes, selectedAttributes, conf)
        .mapPartitionsInternal { batchIter =>
          val toUnsafe = org.apache.spark.sql.catalyst.expressions.UnsafeProjection.create(
            selectedSchema)
          batchIter.flatMap { batch =>
            val numRows = batch.numRows()
            new Iterator[InternalRow] {
              private var rowIdx = 0
              override def hasNext: Boolean = rowIdx < numRows
              override def next(): InternalRow = {
                val row = batch.getRow(rowIdx)
                rowIdx += 1
                toUnsafe(row)
              }
            }
          }
        }
    } else {
      input.mapPartitionsInternal { batchIterator =>
        new ArrowCachedBatchToInternalRowIterator(
          batchIterator,
          cacheSchema,
          selectedSchema,
          selectedIndices,
          timeZoneId)
      }
    }
  }
}

/**
 * Companion object with shared utility methods for Arrow cache serialization.
 */
private object ArrowCachedBatchSerializer {

  // scalastyle:off caselocale
  def createCompressionCodec(
      codecName: String,
      compressionLevel: Int): CompressionCodec = {
    codecName.toLowerCase match {
      case "none" => NoCompressionCodec.INSTANCE
      case "zstd" =>
        val factory = CompressionCodec.Factory.INSTANCE
        val codecType = new ZstdCompressionCodec(compressionLevel).getCodecType()
        factory.createCodec(codecType)
      case "lz4" =>
        val factory = CompressionCodec.Factory.INSTANCE
        val codecType = new Lz4CompressionCodec().getCodecType()
        factory.createCodec(codecType)
      case other =>
        throw SparkException.internalError(
          s"Unsupported Arrow compression codec: $other. Supported values: none, zstd, lz4")
    }
  }
  // scalastyle:on caselocale

  def serializeBatch(batch: ArrowRecordBatch): Array[Byte] = {
    val out = new ByteArrayOutputStream()
    val writeChannel = new WriteChannel(Channels.newChannel(out))
    MessageSerializer.serialize(writeChannel, batch)
    out.toByteArray
  }

  def createColumnStats(dataType: DataType): ColumnStats = {
    dataType match {
      case BooleanType => new BooleanColumnStats
      case ByteType => new ByteColumnStats
      case ShortType => new ShortColumnStats
      case IntegerType => new IntColumnStats
      case DateType => new IntColumnStats  // Date is stored as Int
      case LongType => new LongColumnStats
      case TimestampType => new LongColumnStats  // Timestamp is stored as Long
      case TimestampNTZType => new LongColumnStats  // TimestampNTZ is stored as Long
      case FloatType => new FloatColumnStats
      case DoubleType => new DoubleColumnStats
      case st: StringType => new StringColumnStats(st)
      case BinaryType => new BinaryColumnStats
      case dt: DecimalType => new DecimalColumnStats(dt)
      case CalendarIntervalType => new IntervalColumnStats
      case VariantType => new VariantColumnStats
      case _ => new ObjectColumnStats(dataType)
    }
  }

  def buildStatisticsFromCollectors(
      collectors: Array[ColumnStats],
      schema: Seq[Attribute]): InternalRow = {
    val stats = collectors.flatMap { collector =>
      val collected = collector.collectedStatistics
      // ColumnStats returns: [lowerBound, upperBound, nullCount, count, sizeInBytes]
      Seq(collected(0), collected(1), collected(2), collected(3), collected(4))
    }
    InternalRow.fromSeq(stats.toSeq)
  }

  def collectStatistics(
      root: VectorSchemaRoot,
      schema: Seq[Attribute]): InternalRow = {
    val rowCount = root.getRowCount
    val vectors = root.getFieldVectors.asScala.toSeq

    // Collect stats for each column: lowerBound, upperBound, nullCount, rowCount, sizeInBytes
    val stats = schema.zip(vectors).flatMap { case (attr, vector) =>
      val nullCount = (0 until rowCount).count(i => vector.isNull(i))
      val sizeInBytes = vector.getBufferSize.toLong

      val (lower, upper) = attr.dataType match {
        case BooleanType => calculateMinMaxBoolean(vector, rowCount)
        case ByteType => calculateMinMaxByte(vector, rowCount)
        case ShortType => calculateMinMaxShort(vector, rowCount)
        case IntegerType => calculateMinMaxInt(vector, rowCount)
        case DateType => calculateMinMaxDate(vector, rowCount)
        case LongType => calculateMinMaxLong(vector, rowCount)
        case TimestampType => calculateMinMaxTimestamp(vector, rowCount)
        case TimestampNTZType => calculateMinMaxTimestampNTZ(vector, rowCount)
        case FloatType => calculateMinMaxFloat(vector, rowCount)
        case DoubleType => calculateMinMaxDouble(vector, rowCount)
        case st: StringType => calculateMinMaxString(vector, rowCount, st.collationId)
        case _: DecimalType => calculateMinMaxDecimal(vector, rowCount, attr.dataType)
        case _ => (null, null) // Skip for binary, complex, and other unsupported types
      }

      Seq(lower, upper, nullCount, rowCount, sizeInBytes)
    }

    new org.apache.spark.sql.catalyst.expressions.GenericInternalRow(stats.toArray)
  }

  def calculateMinMaxBoolean(
      vector: org.apache.arrow.vector.FieldVector,
      rowCount: Int): (Any, Any) = {
    var min = true
    var max = false
    var hasValue = false

    (0 until rowCount).foreach { i =>
      if (!vector.isNull(i)) {
        val value = vector.asInstanceOf[org.apache.arrow.vector.BitVector].get(i) != 0
        if (!hasValue) {
          min = value
          max = value
          hasValue = true
        } else {
          if (value < min) min = value
          if (value > max) max = value
        }
      }
    }

    if (hasValue) (min, max) else (null, null)
  }

  def calculateMinMaxByte(
      vector: org.apache.arrow.vector.FieldVector,
      rowCount: Int): (Any, Any) = {
    var min = Byte.MaxValue
    var max = Byte.MinValue
    var hasValue = false

    (0 until rowCount).foreach { i =>
      if (!vector.isNull(i)) {
        val value = vector.asInstanceOf[org.apache.arrow.vector.TinyIntVector].get(i)
        if (!hasValue) {
          min = value
          max = value
          hasValue = true
        } else {
          if (value < min) min = value
          if (value > max) max = value
        }
      }
    }

    if (hasValue) (min, max) else (null, null)
  }

  def calculateMinMaxShort(
      vector: org.apache.arrow.vector.FieldVector,
      rowCount: Int): (Any, Any) = {
    var min = Short.MaxValue
    var max = Short.MinValue
    var hasValue = false

    (0 until rowCount).foreach { i =>
      if (!vector.isNull(i)) {
        val value = vector.asInstanceOf[org.apache.arrow.vector.SmallIntVector].get(i)
        if (!hasValue) {
          min = value
          max = value
          hasValue = true
        } else {
          if (value < min) min = value
          if (value > max) max = value
        }
      }
    }

    if (hasValue) (min, max) else (null, null)
  }

  def calculateMinMaxInt(
      vector: org.apache.arrow.vector.FieldVector,
      rowCount: Int): (Any, Any) = {
    var min = Int.MaxValue
    var max = Int.MinValue
    var hasValue = false

    (0 until rowCount).foreach { i =>
      if (!vector.isNull(i)) {
        val value = vector.asInstanceOf[org.apache.arrow.vector.IntVector].get(i)
        if (!hasValue) {
          min = value
          max = value
          hasValue = true
        } else {
          if (value < min) min = value
          if (value > max) max = value
        }
      }
    }

    if (hasValue) (min, max) else (null, null)
  }

  def calculateMinMaxDate(
      vector: org.apache.arrow.vector.FieldVector,
      rowCount: Int): (Any, Any) = {
    var min = Int.MaxValue
    var max = Int.MinValue
    var hasValue = false

    (0 until rowCount).foreach { i =>
      if (!vector.isNull(i)) {
        val value = vector.asInstanceOf[org.apache.arrow.vector.DateDayVector].get(i)
        if (!hasValue) {
          min = value
          max = value
          hasValue = true
        } else {
          if (value < min) min = value
          if (value > max) max = value
        }
      }
    }

    if (hasValue) (min, max) else (null, null)
  }

  def calculateMinMaxLong(
      vector: org.apache.arrow.vector.FieldVector,
      rowCount: Int): (Any, Any) = {
    var min = Long.MaxValue
    var max = Long.MinValue
    var hasValue = false

    (0 until rowCount).foreach { i =>
      if (!vector.isNull(i)) {
        val value = vector.asInstanceOf[org.apache.arrow.vector.BigIntVector].get(i)
        if (!hasValue) {
          min = value
          max = value
          hasValue = true
        } else {
          if (value < min) min = value
          if (value > max) max = value
        }
      }
    }

    if (hasValue) (min, max) else (null, null)
  }

  def calculateMinMaxTimestamp(
      vector: org.apache.arrow.vector.FieldVector,
      rowCount: Int): (Any, Any) = {
    var min = Long.MaxValue
    var max = Long.MinValue
    var hasValue = false

    (0 until rowCount).foreach { i =>
      if (!vector.isNull(i)) {
        val value =
          vector.asInstanceOf[org.apache.arrow.vector.TimeStampMicroTZVector].get(i)
        if (!hasValue) {
          min = value
          max = value
          hasValue = true
        } else {
          if (value < min) min = value
          if (value > max) max = value
        }
      }
    }

    if (hasValue) (min, max) else (null, null)
  }

  def calculateMinMaxTimestampNTZ(
      vector: org.apache.arrow.vector.FieldVector,
      rowCount: Int): (Any, Any) = {
    var min = Long.MaxValue
    var max = Long.MinValue
    var hasValue = false

    (0 until rowCount).foreach { i =>
      if (!vector.isNull(i)) {
        val value =
          vector.asInstanceOf[org.apache.arrow.vector.TimeStampMicroVector].get(i)
        if (!hasValue) {
          min = value
          max = value
          hasValue = true
        } else {
          if (value < min) min = value
          if (value > max) max = value
        }
      }
    }

    if (hasValue) (min, max) else (null, null)
  }

  def calculateMinMaxFloat(
      vector: org.apache.arrow.vector.FieldVector,
      rowCount: Int): (Any, Any) = {
    var min = Float.MaxValue
    var max = Float.MinValue
    var hasValue = false

    (0 until rowCount).foreach { i =>
      if (!vector.isNull(i)) {
        val value = vector.asInstanceOf[org.apache.arrow.vector.Float4Vector].get(i)
        // Skip NaN: IEEE 754 comparisons with NaN are always false, so NaN never
        // updates min/max in the row-based path (FloatColumnStats.gatherValueStats).
        if (!value.isNaN) {
          if (!hasValue) {
            min = value
            max = value
            hasValue = true
          } else {
            if (value < min) min = value
            if (value > max) max = value
          }
        }
      }
    }

    if (hasValue) (min, max) else (null, null)
  }

  def calculateMinMaxDouble(
      vector: org.apache.arrow.vector.FieldVector,
      rowCount: Int): (Any, Any) = {
    var min = Double.MaxValue
    var max = Double.MinValue
    var hasValue = false

    (0 until rowCount).foreach { i =>
      if (!vector.isNull(i)) {
        val value = vector.asInstanceOf[org.apache.arrow.vector.Float8Vector].get(i)
        // Skip NaN to match DoubleColumnStats.gatherValueStats.
        if (!value.isNaN) {
          if (!hasValue) {
            min = value
            max = value
            hasValue = true
          } else {
            if (value < min) min = value
            if (value > max) max = value
          }
        }
      }
    }

    if (hasValue) (min, max) else (null, null)
  }

  def calculateMinMaxString(
      vector: org.apache.arrow.vector.FieldVector,
      rowCount: Int,
      collationId: Int = StringType.collationId): (Any, Any) = {
    var min: org.apache.spark.unsafe.types.UTF8String = null
    var max: org.apache.spark.unsafe.types.UTF8String = null
    var hasValue = false

    (0 until rowCount).foreach { i =>
      if (!vector.isNull(i)) {
        val bytes = vector.asInstanceOf[org.apache.arrow.vector.VarCharVector].get(i)
        val value = org.apache.spark.unsafe.types.UTF8String.fromBytes(bytes)
        if (!hasValue) {
          min = value.clone()
          max = value.clone()
          hasValue = true
        } else {
          if (value.semanticCompare(min, collationId) < 0) min = value.clone()
          if (value.semanticCompare(max, collationId) > 0) max = value.clone()
        }
      }
    }

    if (hasValue) (min, max) else (null, null)
  }

  def calculateMinMaxDecimal(
      vector: org.apache.arrow.vector.FieldVector,
      rowCount: Int,
      dataType: org.apache.spark.sql.types.DataType): (Any, Any) = {
    val decimalType = dataType.asInstanceOf[DecimalType]
    var min: org.apache.spark.sql.types.Decimal = null
    var max: org.apache.spark.sql.types.Decimal = null
    var hasValue = false

    (0 until rowCount).foreach { i =>
      if (!vector.isNull(i)) {
        val bigDecimal = vector.asInstanceOf[
          org.apache.arrow.vector.DecimalVector].getObject(i)
        val value = org.apache.spark.sql.types.Decimal(
          bigDecimal, decimalType.precision, decimalType.scale)

        if (!hasValue) {
          min = value
          max = value
          hasValue = true
        } else {
          if (value.compareTo(min) < 0) min = value
          if (value.compareTo(max) > 0) max = value
        }
      }
    }

    if (hasValue) (min, max) else (null, null)
  }
}

/**
 * Iterator that converts InternalRow to ArrowCachedBatch.
 */
private class InternalRowToArrowCachedBatchIterator(
    rowIter: Iterator[InternalRow],
    schema: Seq[Attribute],
    sparkSchema: StructType,
    maxRecordsPerBatch: Long,
    timeZoneId: String,
    compressionCodecName: String,
    compressionLevel: Int) extends Iterator[ArrowCachedBatch] {

  private val compressionCodec = ArrowCachedBatchSerializer.createCompressionCodec(
    compressionCodecName,
    compressionLevel)

  private val allocator = ArrowUtils.rootAllocator.newChildAllocator(
    s"InternalRowToArrowCachedBatchIterator-${TaskContext.get().taskAttemptId()}",
    0,
    Long.MaxValue)

  private val arrowSchema = ArrowUtils.toArrowSchema(sparkSchema, timeZoneId, false, false)
  private val root = VectorSchemaRoot.create(arrowSchema, allocator)
  private val arrowWriter = ArrowWriter.create(root)
  private val unloader = new VectorUnloader(root, true, compressionCodec, true)

  // Create statistics collectors for each column
  private val statsCollectors: Array[ColumnStats] = schema.map { attr =>
    ArrowCachedBatchSerializer.createColumnStats(attr.dataType)
  }.toArray

  // Register cleanup
  Option(TaskContext.get()).foreach { tc =>
    tc.addTaskCompletionListener[Unit] { _ =>
      close()
    }
  }

  override def hasNext: Boolean = rowIter.hasNext || {
    close()
    false
  }

  override def next(): ArrowCachedBatch = {
    var rowCount = 0

    // Reset statistics collectors for new batch
    statsCollectors.foreach { stats =>
      // Create new instance to reset state
      val index = statsCollectors.indexOf(stats)
      statsCollectors(index) = ArrowCachedBatchSerializer.createColumnStats(schema(index).dataType)
    }

    Utils.tryWithSafeFinally {
      // Write rows to Arrow vectors and collect statistics incrementally
      while (rowIter.hasNext && rowCount < maxRecordsPerBatch) {
        val row = rowIter.next()
        arrowWriter.write(row)

        // Collect statistics for this row
        var i = 0
        while (i < statsCollectors.length) {
          statsCollectors(i).gatherStats(row, i)
          i += 1
        }

        rowCount += 1
      }
      arrowWriter.finish()

      // Get the Arrow RecordBatch with compression
      val recordBatch = unloader.getRecordBatch()

      Utils.tryWithSafeFinally {
        // Serialize to Arrow IPC format
        val arrowData = ArrowCachedBatchSerializer.serializeBatch(recordBatch)

        // Build statistics InternalRow from collected stats
        val stats = ArrowCachedBatchSerializer.buildStatisticsFromCollectors(
          statsCollectors, schema)

        ArrowCachedBatch(rowCount, arrowData, stats)
      } {
        recordBatch.close()
      }
    } {
      arrowWriter.reset()
    }
  }

  private def close(): Unit = {
    root.close()
    allocator.close()
  }
}

/**
 * Iterator that converts ColumnarBatch to ArrowCachedBatch.
 */
private class ColumnarBatchToArrowCachedBatchIterator(
    batchIter: Iterator[ColumnarBatch],
    schema: Seq[Attribute],
    sparkSchema: StructType,
    timeZoneId: String,
    compressionCodecName: String,
    compressionLevel: Int) extends Iterator[ArrowCachedBatch] {

  private val compressionCodec = ArrowCachedBatchSerializer.createCompressionCodec(
    compressionCodecName,
    compressionLevel)

  private val allocator = ArrowUtils.rootAllocator.newChildAllocator(
    s"ColumnarBatchToArrowCachedBatchIterator-${TaskContext.get().taskAttemptId()}",
    0,
    Long.MaxValue)

  private val arrowSchema = ArrowUtils.toArrowSchema(sparkSchema, timeZoneId, false, false)

  // Register cleanup
  Option(TaskContext.get()).foreach { tc =>
    tc.addTaskCompletionListener[Unit] { _ =>
      allocator.close()
    }
  }

  override def hasNext: Boolean = batchIter.hasNext

  override def next(): ArrowCachedBatch = {
    val batch = batchIter.next()
    val rowCount = batch.numRows()

    // Check if batch is already Arrow-based for zero-copy path
    val vectors = (0 until batch.numCols()).map(batch.column)
    if (vectors.forall(_.isInstanceOf[ArrowColumnVector])) {
      // Fast path: zero-copy extraction of Arrow RecordBatch
      convertArrowBatchZeroCopy(batch, rowCount, schema, vectors)
    } else {
      // Slow path: convert to Arrow via rows
      convertToArrowBatch(batch, rowCount, schema)
    }
  }

  private def convertArrowBatchZeroCopy(
      batch: ColumnarBatch,
      rowCount: Int,
      schema: Seq[Attribute],
      vectors: Seq[ColumnVector]): ArrowCachedBatch = {
    // Zero-copy path: extract Arrow vectors directly from ArrowColumnVector
    val arrowVectors = vectors.map(
      _.asInstanceOf[ArrowColumnVector].getValueVector.asInstanceOf[
        org.apache.arrow.vector.FieldVector])

    // Create a VectorSchemaRoot from the existing vectors
    val root = new VectorSchemaRoot(arrowSchema, arrowVectors.asJava, rowCount)

    Utils.tryWithSafeFinally {
      // Use VectorUnloader to create compressed RecordBatch
      val unloader = new VectorUnloader(root, true, compressionCodec, true)
      val recordBatch = unloader.getRecordBatch()

      Utils.tryWithSafeFinally {
        val arrowData = ArrowCachedBatchSerializer.serializeBatch(recordBatch)
        val stats = ArrowCachedBatchSerializer.collectStatistics(root, schema)
        ArrowCachedBatch(rowCount, arrowData, stats)
      } {
        recordBatch.close()
      }
    } {
      // Note: We don't close the root here because we don't own the vectors
      // They are owned by the input ColumnarBatch
    }
  }

  private def convertToArrowBatch(
      batch: ColumnarBatch,
      rowCount: Int,
      schema: Seq[Attribute]): ArrowCachedBatch = {
    // Convert columnar batch to rows, then to Arrow
    val root = VectorSchemaRoot.create(arrowSchema, allocator)
    val arrowWriter = ArrowWriter.create(root)
    val unloader = new VectorUnloader(root, true, compressionCodec, true)

    // Collect statistics inline during row iteration, same as InternalRowToArrow path
    val statsCollectors: Array[ColumnStats] = schema.map { attr =>
      ArrowCachedBatchSerializer.createColumnStats(attr.dataType)
    }.toArray

    Utils.tryWithSafeFinally {
      val rowIterator = batch.rowIterator().asScala
      while (rowIterator.hasNext) {
        val row = rowIterator.next()
        arrowWriter.write(row)

        // Collect statistics for this row inline
        var i = 0
        while (i < statsCollectors.length) {
          statsCollectors(i).gatherStats(row, i)
          i += 1
        }
      }
      arrowWriter.finish()

      val recordBatch = unloader.getRecordBatch()
      Utils.tryWithSafeFinally {
        val arrowData = ArrowCachedBatchSerializer.serializeBatch(recordBatch)
        val stats = ArrowCachedBatchSerializer.buildStatisticsFromCollectors(
          statsCollectors, schema)
        ArrowCachedBatch(rowCount, arrowData, stats)
      } {
        recordBatch.close()
      }
    } {
      arrowWriter.reset()
      root.close()
    }
  }
}

/**
 * Iterator that converts ArrowCachedBatch to ColumnarBatch.
 */
private class ArrowCachedBatchToColumnarBatchIterator(
    batchIter: Iterator[CachedBatch],
    cacheSchema: StructType,
    selectedSchema: StructType,
    columnIndices: Array[Int],
    timeZoneId: String) extends Iterator[ColumnarBatch] {

  private val allocator = ArrowUtils.rootAllocator.newChildAllocator(
    s"ArrowCachedBatchToColumnarBatchIterator-${TaskContext.get().taskAttemptId()}",
    0,
    Long.MaxValue)

  // Track only the previous root to close it when next batch is produced
  private var previousRoot: VectorSchemaRoot = null

  // Register cleanup - close remaining root and allocator when task completes
  Option(TaskContext.get()).foreach { tc =>
    tc.addTaskCompletionListener[Unit] { _ =>
      if (previousRoot != null) {
        previousRoot.close()
        previousRoot = null
      }
      allocator.close()
    }
  }

  override def hasNext: Boolean = batchIter.hasNext

  override def next(): ColumnarBatch = {
    // Close the previous root since it's been consumed
    if (previousRoot != null) {
      previousRoot.close()
      previousRoot = null
    }

    val cachedBatch = batchIter.next().asInstanceOf[ArrowCachedBatch]

    // Deserialize Arrow IPC data
    val arrowData = cachedBatch.arrowData
    val in = new ByteArrayInputStream(arrowData)
    val readChannel = new ReadChannel(Channels.newChannel(in))

    // Deserialize the RecordBatch
    val recordBatch = MessageSerializer.deserializeRecordBatch(readChannel, allocator)

    Utils.tryWithSafeFinally {
      // Create root and load batch
      val arrowSchema = ArrowUtils.toArrowSchema(cacheSchema, timeZoneId, false, false)
      val root = VectorSchemaRoot.create(arrowSchema, allocator)

      // Track this root as the current/previous root
      previousRoot = root

      val loader = new VectorLoader(root)
      loader.load(recordBatch)

      // Wrap vectors in ArrowColumnVector and project to selected columns
      val allColumns = root.getFieldVectors.asScala.map { vector =>
        new ArrowColumnVector(vector)
      }.toArray[ColumnVector]

      val selectedColumns = columnIndices.map(allColumns(_))

      new ColumnarBatch(selectedColumns, cachedBatch.numRows)
    } {
      recordBatch.close()
    }
  }
}

/**
 * A typed column reader that reads from an Arrow FieldVector and writes directly
 * to an UnsafeRowWriter, avoiding per-row pattern matching overhead.
 */
private abstract class ArrowColumnReader {
  def vector: org.apache.arrow.vector.FieldVector
  def read(rowIndex: Int, ordinal: Int, writer: UnsafeRowWriter): Unit
  def setVector(v: org.apache.arrow.vector.FieldVector): Unit
}

private object ArrowColumnReader {
  import org.apache.arrow.vector._

  def create(dataType: DataType): ArrowColumnReader = dataType match {
    case BooleanType => new ArrowColumnReader {
      private var _vector: BitVector = _
      def vector: FieldVector = _vector
      def setVector(v: FieldVector): Unit = _vector = v.asInstanceOf[BitVector]
      def read(rowIndex: Int, ordinal: Int, writer: UnsafeRowWriter): Unit =
        writer.write(ordinal, _vector.get(rowIndex) != 0)
    }
    case ByteType => new ArrowColumnReader {
      private var _vector: TinyIntVector = _
      def vector: FieldVector = _vector
      def setVector(v: FieldVector): Unit = _vector = v.asInstanceOf[TinyIntVector]
      def read(rowIndex: Int, ordinal: Int, writer: UnsafeRowWriter): Unit =
        writer.write(ordinal, _vector.get(rowIndex))
    }
    case ShortType => new ArrowColumnReader {
      private var _vector: SmallIntVector = _
      def vector: FieldVector = _vector
      def setVector(v: FieldVector): Unit = _vector = v.asInstanceOf[SmallIntVector]
      def read(rowIndex: Int, ordinal: Int, writer: UnsafeRowWriter): Unit =
        writer.write(ordinal, _vector.get(rowIndex))
    }
    case IntegerType | DateType | _: YearMonthIntervalType => new ArrowColumnReader {
      private var _vector: FieldVector = _
      def vector: FieldVector = _vector
      def setVector(v: FieldVector): Unit = _vector = v
      def read(rowIndex: Int, ordinal: Int, writer: UnsafeRowWriter): Unit = {
        val value = _vector match {
          case iv: IntVector => iv.get(rowIndex)
          case dv: DateDayVector => dv.get(rowIndex)
          case iv: org.apache.arrow.vector.IntervalYearVector => iv.get(rowIndex)
        }
        writer.write(ordinal, value)
      }
    }
    case LongType | TimestampType | TimestampNTZType | _: DayTimeIntervalType =>
      new ArrowColumnReader {
      private var _vector: FieldVector = _
      def vector: FieldVector = _vector
      def setVector(v: FieldVector): Unit = _vector = v
      def read(rowIndex: Int, ordinal: Int, writer: UnsafeRowWriter): Unit = {
        val value = _vector match {
          case bv: BigIntVector => bv.get(rowIndex)
          case tv: TimeStampMicroTZVector => tv.get(rowIndex)
          case tv: TimeStampMicroVector => tv.get(rowIndex)
          case dv: org.apache.arrow.vector.DurationVector =>
            org.apache.arrow.vector.DurationVector.get(dv.getDataBuffer, rowIndex)
        }
        writer.write(ordinal, value)
      }
    }
    case FloatType => new ArrowColumnReader {
      private var _vector: Float4Vector = _
      def vector: FieldVector = _vector
      def setVector(v: FieldVector): Unit = _vector = v.asInstanceOf[Float4Vector]
      def read(rowIndex: Int, ordinal: Int, writer: UnsafeRowWriter): Unit =
        writer.write(ordinal, _vector.get(rowIndex))
    }
    case DoubleType => new ArrowColumnReader {
      private var _vector: Float8Vector = _
      def vector: FieldVector = _vector
      def setVector(v: FieldVector): Unit = _vector = v.asInstanceOf[Float8Vector]
      def read(rowIndex: Int, ordinal: Int, writer: UnsafeRowWriter): Unit =
        writer.write(ordinal, _vector.get(rowIndex))
    }
    case _: StringType => new ArrowColumnReader {
      private var _vector: VarCharVector = _
      def vector: FieldVector = _vector
      def setVector(v: FieldVector): Unit = _vector = v.asInstanceOf[VarCharVector]
      def read(rowIndex: Int, ordinal: Int, writer: UnsafeRowWriter): Unit = {
        val bytes = _vector.get(rowIndex)
        writer.write(ordinal, UTF8String.fromBytes(bytes))
      }
    }
    case BinaryType => new ArrowColumnReader {
      private var _vector: VarBinaryVector = _
      def vector: FieldVector = _vector
      def setVector(v: FieldVector): Unit = _vector = v.asInstanceOf[VarBinaryVector]
      def read(rowIndex: Int, ordinal: Int, writer: UnsafeRowWriter): Unit =
        writer.write(ordinal, _vector.get(rowIndex))
    }
    case dt: DecimalType if dt.precision <= Decimal.MAX_LONG_DIGITS =>
      // Fast path for compact decimals (precision <= 18):
      // Read the unscaled long directly from the Arrow buffer, zero allocation.
      // Arrow stores Decimal as 128-bit little-endian integer in 16 bytes.
      // For compact decimals, the value fits in the lower 8 bytes.
      new ArrowColumnReader {
        private var _vector: DecimalVector = _
        private var _dataBuffer: org.apache.arrow.memory.ArrowBuf = _
        private val typeWidth = DecimalVector.TYPE_WIDTH // 16 bytes
        def vector: FieldVector = _vector
        def setVector(v: FieldVector): Unit = {
          _vector = v.asInstanceOf[DecimalVector]
          _dataBuffer = _vector.getDataBuffer
        }
        def read(rowIndex: Int, ordinal: Int, writer: UnsafeRowWriter): Unit = {
          val startIndex = rowIndex.toLong * typeWidth
          val unscaledLong = _dataBuffer.getLong(startIndex)
          writer.write(ordinal, unscaledLong)
        }
      }
    case dt: DecimalType => new ArrowColumnReader {
      // Slow path for wide decimals (precision > 18): must go through BigDecimal
      private var _vector: DecimalVector = _
      private val precision = dt.precision
      private val scale = dt.scale
      def vector: FieldVector = _vector
      def setVector(v: FieldVector): Unit = _vector = v.asInstanceOf[DecimalVector]
      def read(rowIndex: Int, ordinal: Int, writer: UnsafeRowWriter): Unit = {
        val decimal = Decimal(_vector.getObject(rowIndex), precision, scale)
        writer.write(ordinal, decimal, precision, scale)
      }
    }
    case _ =>
      throw new UnsupportedOperationException(
        s"Complex type $dataType is handled by the fallback path")
  }
}

/**
 * Fast-path iterator that converts ArrowCachedBatch to InternalRow.
 * Uses pre-built typed column readers to avoid per-row pattern matching,
 * and writes directly to UnsafeRowWriter to avoid intermediate SpecificInternalRow.
 * Only used for schemas without complex types (Array/Struct/Map).
 */
private class ArrowCachedBatchToInternalRowIterator(
    batchIter: Iterator[CachedBatch],
    cacheSchema: StructType,
    selectedSchema: StructType,
    columnIndices: Array[Int],
    timeZoneId: String) extends Iterator[InternalRow] {

  private val allocator = ArrowUtils.rootAllocator.newChildAllocator(
    s"ArrowCachedBatchToInternalRowIterator-${TaskContext.get().taskAttemptId()}",
    0,
    Long.MaxValue)

  private var currentRoot: VectorSchemaRoot = null
  private var currentRowIndex: Int = 0
  private var currentRowCount: Int = 0

  private val numFields = selectedSchema.length

  // Pre-build typed readers per column at init time -- no per-row pattern match
  private val columnReaders: Array[ArrowColumnReader] =
    selectedSchema.fields.map(f => ArrowColumnReader.create(f.dataType))

  // Write directly to UnsafeRow -- no intermediate SpecificInternalRow + UnsafeProjection
  private val rowWriter = new UnsafeRowWriter(numFields)

  // Register cleanup
  Option(TaskContext.get()).foreach { tc =>
    tc.addTaskCompletionListener[Unit] { _ =>
      if (currentRoot != null) {
        currentRoot.close()
        currentRoot = null
      }
      allocator.close()
    }
  }

  override def hasNext: Boolean = {
    if (currentRowIndex < currentRowCount) {
      true
    } else if (batchIter.hasNext) {
      loadNextBatch()
      currentRowIndex < currentRowCount
    } else {
      if (currentRoot != null) {
        currentRoot.close()
        currentRoot = null
      }
      false
    }
  }

  override def next(): InternalRow = {
    if (!hasNext) {
      throw new NoSuchElementException("No more rows")
    }

    rowWriter.reset()
    rowWriter.zeroOutNullBytes()

    val rowIdx = currentRowIndex
    var i = 0
    while (i < numFields) {
      val reader = columnReaders(i)
      if (reader.vector.isNull(rowIdx)) {
        rowWriter.setNullAt(i)
      } else {
        reader.read(rowIdx, i, rowWriter)
      }
      i += 1
    }

    currentRowIndex += 1
    rowWriter.getRow()
  }

  private def loadNextBatch(): Unit = {
    if (currentRoot != null) {
      currentRoot.close()
      currentRoot = null
    }

    val cachedBatch = batchIter.next().asInstanceOf[ArrowCachedBatch]

    val arrowData = cachedBatch.arrowData
    val in = new ByteArrayInputStream(arrowData)
    val readChannel = new ReadChannel(Channels.newChannel(in))

    val recordBatch = MessageSerializer.deserializeRecordBatch(readChannel, allocator)

    try {
      val arrowSchema = ArrowUtils.toArrowSchema(cacheSchema, timeZoneId, false, false)
      val root = VectorSchemaRoot.create(arrowSchema, allocator)
      currentRoot = root

      val loader = new VectorLoader(root)
      loader.load(recordBatch)

      // Update pre-built readers with new vectors
      var i = 0
      while (i < numFields) {
        columnReaders(i).setVector(root.getVector(columnIndices(i)))
        i += 1
      }

      currentRowIndex = 0
      currentRowCount = cachedBatch.numRows
    } finally {
      recordBatch.close()
    }
  }
}
