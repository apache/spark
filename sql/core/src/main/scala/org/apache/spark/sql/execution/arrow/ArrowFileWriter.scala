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

package org.apache.spark.sql.execution.arrow

import java.io.{ByteArrayInputStream, ByteArrayOutputStream, FileInputStream, FileOutputStream, OutputStream}
import java.nio.channels.{Channels, ReadableByteChannel}

import scala.collection.mutable.ArrayBuffer
import scala.jdk.CollectionConverters._

import org.apache.arrow.compression.{Lz4CompressionCodec, ZstdCompressionCodec}
import org.apache.arrow.flatbuf.MessageHeader
import org.apache.arrow.memory.BufferAllocator
import org.apache.arrow.vector._
import org.apache.arrow.vector.compression.{CompressionCodec, NoCompressionCodec}
import org.apache.arrow.vector.ipc.{ArrowFileWriter, ArrowStreamReader, ArrowStreamWriter, ReadChannel, WriteChannel}
import org.apache.arrow.vector.ipc.message.{ArrowRecordBatch, IpcOption, MessageSerializer}

import org.apache.spark.SparkException
import org.apache.spark.TaskContext
import org.apache.spark.internal.Logging
import org.apache.spark.network.util.JavaUtils
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.{UnsafeProjection, UnsafeRow}
import org.apache.spark.sql.catalyst.plans.logical.LocalRelation
import org.apache.spark.sql.catalyst.types.DataTypeUtils.toAttributes
import org.apache.spark.sql.classic.{DataFrame, Dataset, SparkSession}
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.types._
import org.apache.spark.sql.util.ArrowUtils
import org.apache.spark.sql.vectorized.{ArrowColumnVector, ColumnarBatch, ColumnVector}
import org.apache.spark.util.{ByteBufferOutputStream, SizeEstimator, Utils}
import org.apache.spark.util.ArrayImplicits._

private[spark] class ArrowBatchFileWriter(
  schema: StructType,
  out: FileOutputStream,
  maxRecordsPerBatch: Long,
  timeZoneId: String,
  errorOnDuplicatedFieldNames: Boolean,
  largeVarTypes: Boolean,
  context: TaskContext) extends AutoCloseable {

  protected val arrowSchema =
    ArrowUtils.toArrowSchema(schema, timeZoneId, errorOnDuplicatedFieldNames, largeVarTypes)
  private val allocator =
    ArrowUtils.rootAllocator.newChildAllocator(
      s"to${this.getClass.getSimpleName}", 0, Long.MaxValue)

  protected val root = VectorSchemaRoot.create(arrowSchema, allocator)
  val writer = new ArrowFileWriter(root, null, out.getChannel)

  // Create compression codec based on config
  private val compressionCodecName = SQLConf.get.arrowCompressionCodec
  private val codec = compressionCodecName match {
    case "none" => NoCompressionCodec.INSTANCE
    case "zstd" =>
      val compressionLevel = SQLConf.get.arrowZstdCompressionLevel
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
  protected val unloader = new VectorUnloader(root, true, codec, true)
  protected val arrowWriter = ArrowWriter.create(root)

  Option(context).foreach {_.addTaskCompletionListener[Unit] { _ =>
    close()
  }}

  def writeRows(rowIter: Iterator[InternalRow]): Unit = {
    writer.start()
    while (rowIter.hasNext) {
      Utils.tryWithSafeFinally {
        var rowCount = 0L
        while (rowIter.hasNext && (maxRecordsPerBatch <= 0 || rowCount < maxRecordsPerBatch)) {
          val row = rowIter.next()
          arrowWriter.write(row)
          rowCount += 1
        }
        arrowWriter.finish()
        writer.writeBatch()
      } {
        arrowWriter.reset()
        root.clear()
      }
    }
    writer.end()
  }

  override def close(): Unit = {
    root.close()
    allocator.close()
  }
}
