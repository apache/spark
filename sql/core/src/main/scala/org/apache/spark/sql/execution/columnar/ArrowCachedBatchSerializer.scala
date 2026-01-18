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
import org.apache.spark.sql.catalyst.types.DataTypeUtils
import org.apache.spark.sql.columnar.{CachedBatch, SimpleMetricsCachedBatchSerializer}
import org.apache.spark.sql.execution.arrow.ArrowWriter
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.types._
import org.apache.spark.sql.util.ArrowUtils
import org.apache.spark.sql.vectorized.{ArrowColumnVector, ColumnarBatch, ColumnVector}
import org.apache.spark.storage.StorageLevel
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
    // Convert to columnar batch first, then iterate rows
    val columnarBatchRDD = convertCachedBatchToColumnarBatch(
      input, cacheAttributes, selectedAttributes, conf)

    val selectedSchema = DataTypeUtils.fromAttributes(selectedAttributes)
    columnarBatchRDD.mapPartitionsInternal { batchIterator =>
      val toUnsafe =
        org.apache.spark.sql.catalyst.expressions.UnsafeProjection.create(selectedSchema)
      batchIterator.flatMap { batch =>
        batch.rowIterator().asScala.map(toUnsafe)
      }
    }
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

  private val compressionCodec = createCompressionCodec(
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

    Utils.tryWithSafeFinally {
      // Write rows to Arrow vectors
      while (rowIter.hasNext && rowCount < maxRecordsPerBatch) {
        val row = rowIter.next()
        arrowWriter.write(row)
        rowCount += 1
      }
      arrowWriter.finish()

      // Get the Arrow RecordBatch with compression
      val recordBatch = unloader.getRecordBatch()

      Utils.tryWithSafeFinally {
        // Serialize to Arrow IPC format
        val arrowData = serializeBatch(recordBatch)

        // Collect statistics
        val stats = collectStatistics(root, schema)

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

  private def serializeBatch(batch: ArrowRecordBatch): Array[Byte] = {
    val out = new ByteArrayOutputStream()
    val writeChannel = new WriteChannel(Channels.newChannel(out))
    MessageSerializer.serialize(writeChannel, batch)
    out.toByteArray
  }

  private def collectStatistics(
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
        case StringType => calculateMinMaxString(vector, rowCount)
        case _: DecimalType => calculateMinMaxDecimal(vector, rowCount, attr.dataType)
        case _ => (null, null) // Skip for binary and complex types
      }

      Seq(lower, upper, nullCount, rowCount, sizeInBytes)
    }

    new org.apache.spark.sql.catalyst.expressions.GenericInternalRow(stats.toArray)
  }

  private def calculateMinMaxBoolean(
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

  private def calculateMinMaxByte(
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

  private def calculateMinMaxShort(
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

  private def calculateMinMaxInt(
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

  private def calculateMinMaxDate(
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

  private def calculateMinMaxLong(
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

  private def calculateMinMaxTimestamp(
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

  private def calculateMinMaxTimestampNTZ(
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

  private def calculateMinMaxFloat(
      vector: org.apache.arrow.vector.FieldVector,
      rowCount: Int): (Any, Any) = {
    var min = Float.MaxValue
    var max = Float.MinValue
    var hasValue = false

    (0 until rowCount).foreach { i =>
      if (!vector.isNull(i)) {
        val value = vector.asInstanceOf[org.apache.arrow.vector.Float4Vector].get(i)
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

  private def calculateMinMaxDouble(
      vector: org.apache.arrow.vector.FieldVector,
      rowCount: Int): (Any, Any) = {
    var min = Double.MaxValue
    var max = Double.MinValue
    var hasValue = false

    (0 until rowCount).foreach { i =>
      if (!vector.isNull(i)) {
        val value = vector.asInstanceOf[org.apache.arrow.vector.Float8Vector].get(i)
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

  private def calculateMinMaxString(
      vector: org.apache.arrow.vector.FieldVector,
      rowCount: Int): (Any, Any) = {
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
          if (value.binaryCompare(min) < 0) min = value.clone()
          if (value.binaryCompare(max) > 0) max = value.clone()
        }
      }
    }

    if (hasValue) (min, max) else (null, null)
  }

  private def calculateMinMaxDecimal(
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

  // scalastyle:off caselocale
  private def createCompressionCodec(
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

  private val compressionCodec = createCompressionCodec(
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
        val arrowData = serializeBatch(recordBatch)
        val stats = collectStatistics(root, schema)
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

    Utils.tryWithSafeFinally {
      val rowIterator = batch.rowIterator().asScala
      while (rowIterator.hasNext) {
        arrowWriter.write(rowIterator.next())
      }
      arrowWriter.finish()

      val recordBatch = unloader.getRecordBatch()
      Utils.tryWithSafeFinally {
        val arrowData = serializeBatch(recordBatch)
        val stats = collectStatistics(root, schema)
        ArrowCachedBatch(rowCount, arrowData, stats)
      } {
        recordBatch.close()
      }
    } {
      arrowWriter.reset()
      root.close()
    }
  }

  private def serializeBatch(batch: ArrowRecordBatch): Array[Byte] = {
    val out = new ByteArrayOutputStream()
    val writeChannel = new WriteChannel(Channels.newChannel(out))
    MessageSerializer.serialize(writeChannel, batch)
    out.toByteArray
  }

  private def collectStatistics(
      root: VectorSchemaRoot,
      schema: Seq[Attribute]): InternalRow = {
    // Reuse the collectStatistics from InternalRowToArrowCachedBatchIterator
    // by calling the same logic
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
        case StringType => calculateMinMaxString(vector, rowCount)
        case _: DecimalType => calculateMinMaxDecimal(vector, rowCount, attr.dataType)
        case _ => (null, null) // Skip for binary and complex types
      }

      Seq(lower, upper, nullCount, rowCount, sizeInBytes)
    }

    new org.apache.spark.sql.catalyst.expressions.GenericInternalRow(stats.toArray)
  }

  private def calculateMinMaxBoolean(
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

  private def calculateMinMaxByte(
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

  private def calculateMinMaxShort(
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

  private def calculateMinMaxInt(
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

  private def calculateMinMaxDate(
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

  private def calculateMinMaxLong(
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

  private def calculateMinMaxTimestamp(
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

  private def calculateMinMaxTimestampNTZ(
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

  private def calculateMinMaxFloat(
      vector: org.apache.arrow.vector.FieldVector,
      rowCount: Int): (Any, Any) = {
    var min = Float.MaxValue
    var max = Float.MinValue
    var hasValue = false

    (0 until rowCount).foreach { i =>
      if (!vector.isNull(i)) {
        val value = vector.asInstanceOf[org.apache.arrow.vector.Float4Vector].get(i)
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

  private def calculateMinMaxDouble(
      vector: org.apache.arrow.vector.FieldVector,
      rowCount: Int): (Any, Any) = {
    var min = Double.MaxValue
    var max = Double.MinValue
    var hasValue = false

    (0 until rowCount).foreach { i =>
      if (!vector.isNull(i)) {
        val value = vector.asInstanceOf[org.apache.arrow.vector.Float8Vector].get(i)
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

  private def calculateMinMaxString(
      vector: org.apache.arrow.vector.FieldVector,
      rowCount: Int): (Any, Any) = {
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
          if (value.binaryCompare(min) < 0) min = value.clone()
          if (value.binaryCompare(max) > 0) max = value.clone()
        }
      }
    }

    if (hasValue) (min, max) else (null, null)
  }

  private def calculateMinMaxDecimal(
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

  // scalastyle:off caselocale
  private def createCompressionCodec(
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

  // Track roots to close them when task completes
  private val roots = new java.util.ArrayList[VectorSchemaRoot]()

  // Register cleanup - close all roots and allocator when task completes
  Option(TaskContext.get()).foreach { tc =>
    tc.addTaskCompletionListener[Unit] { _ =>
      import scala.jdk.CollectionConverters._
      roots.asScala.foreach(_.close())
      roots.clear()
      allocator.close()
    }
  }

  override def hasNext: Boolean = batchIter.hasNext

  override def next(): ColumnarBatch = {
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

      // Track this root for cleanup at task completion
      roots.add(root)

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
