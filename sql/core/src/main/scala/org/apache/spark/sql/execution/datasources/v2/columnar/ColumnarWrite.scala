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
package org.apache.spark.sql.execution.datasources.v2.columnar

import scala.jdk.CollectionConverters._

import org.apache.arrow.vector.VectorSchemaRoot

import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.connector.metric.CustomTaskMetric
import org.apache.spark.sql.connector.write._
import org.apache.spark.sql.connector.write.streaming.{StreamingDataWriterFactory, StreamingWrite}
import org.apache.spark.sql.execution.arrow.ArrowWriter
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.util.ArrowUtils
import org.apache.spark.sql.vectorized.{ArrowColumnVector, ColumnarBatch, ColumnVector}
import org.apache.spark.util.Utils

class ColumnarWriteBuilder(
    shortName: String,
    info: LogicalWriteInfo,
    createDataSource: () => ColumnarDataSource)
  extends WriteBuilder with SupportsTruncate {

  private var overwrite = false

  override def truncate(): WriteBuilder = {
    overwrite = true
    this
  }

  override def build(): Write = new ColumnarWrite(shortName, info, overwrite, createDataSource)
}

class ColumnarWrite(
    shortName: String,
    info: LogicalWriteInfo,
    overwrite: Boolean,
    createDataSource: () => ColumnarDataSource)
  extends Write {

  override def description(): String = shortName

  override def toBatch: BatchWrite = {
    new ColumnarBatchWrite(createDataSource().writer(info.schema(), overwrite), info.schema())
  }

  override def toStreaming: StreamingWrite = {
    new ColumnarStreamingWrite(
      createDataSource().streamWriter(info.schema(), overwrite), info.schema())
  }
}

class ColumnarBatchWrite(writer: ColumnarWriter, schema: StructType) extends BatchWrite {

  override def createBatchWriterFactory(info: PhysicalWriteInfo): DataWriterFactory = {
    new ColumnarDataWriterFactory(writer, ArrowBatchingOptions(schema))
  }

  override def commit(messages: Array[WriterCommitMessage]): Unit = writer.commit(messages)

  override def abort(messages: Array[WriterCommitMessage]): Unit = writer.abort(messages)
}

class ColumnarStreamingWrite(writer: ColumnarStreamWriter, schema: StructType)
  extends StreamingWrite {

  override def createStreamingWriterFactory(
      info: PhysicalWriteInfo): StreamingDataWriterFactory = {
    new ColumnarStreamingDataWriterFactory(writer, ArrowBatchingOptions(schema))
  }

  override def commit(epochId: Long, messages: Array[WriterCommitMessage]): Unit = {
    writer.commit(epochId, messages)
  }

  override def abort(epochId: Long, messages: Array[WriterCommitMessage]): Unit = {
    writer.abort(epochId, messages)
  }
}

class ColumnarDataWriterFactory(writer: ColumnarWriter, options: ArrowBatchingOptions)
  extends DataWriterFactory {
  override def createWriter(partitionId: Int, taskId: Long): DataWriter[InternalRow] = {
    new ArrowBatchingDataWriter(writer.createWriter(partitionId, taskId), options)
  }
}

class ColumnarStreamingDataWriterFactory(
    writer: ColumnarStreamWriter,
    options: ArrowBatchingOptions)
  extends StreamingDataWriterFactory {
  override def createWriter(
      partitionId: Int,
      taskId: Long,
      epochId: Long): DataWriter[InternalRow] = {
    new ArrowBatchingDataWriter(writer.createWriter(partitionId, taskId, epochId), options)
  }
}

/**
 * How [[ArrowBatchingDataWriter]] converts rows to Arrow, captured on the driver from the
 * session configuration.
 */
case class ArrowBatchingOptions(schema: StructType, timeZoneId: String, maxRecordsPerBatch: Int)

object ArrowBatchingOptions {
  def apply(schema: StructType): ArrowBatchingOptions = {
    val conf = SQLConf.get
    val maxRecords = conf.arrowMaxRecordsPerBatch
    ArrowBatchingOptions(
      schema, conf.sessionLocalTimeZone, if (maxRecords > 0) maxRecords else Int.MaxValue)
  }
}

/**
 * Converts the rows of a write task to Arrow-backed [[ColumnarBatch]]es for the [[DataWriter]]
 * of a [[ColumnarDataSource]]. A new Arrow vector schema root is created for each batch, so a
 * native writer that took ownership of the data of a batch can keep it after the call.
 */
class ArrowBatchingDataWriter(delegate: DataWriter[ColumnarBatch], options: ArrowBatchingOptions)
  extends DataWriter[InternalRow] {

  private val arrowSchema = ArrowUtils.toArrowSchema(
    options.schema, options.timeZoneId, errorOnDuplicatedFieldNames = true, largeVarTypes = false)
  private val allocator =
    ArrowUtils.rootAllocator.newChildAllocator("columnar data source writer", 0, Long.MaxValue)
  private var root: VectorSchemaRoot = _
  private var arrowWriter: ArrowWriter = _
  private var numRows = 0

  override def write(record: InternalRow): Unit = {
    if (root == null) {
      root = VectorSchemaRoot.create(arrowSchema, allocator)
      arrowWriter = ArrowWriter.create(root)
    }
    arrowWriter.write(record)
    numRows += 1
    if (numRows >= options.maxRecordsPerBatch) {
      flush()
    }
  }

  private def flush(): Unit = {
    if (root != null) {
      arrowWriter.finish()
      val columns = root.getFieldVectors.asScala.map(new ArrowColumnVector(_): ColumnVector)
      try {
        delegate.write(new ColumnarBatch(columns.toArray, root.getRowCount))
      } finally {
        releaseBatch()
      }
    }
  }

  private def releaseBatch(): Unit = {
    if (root != null) {
      root.close()
      root = null
      arrowWriter = null
      numRows = 0
    }
  }

  override def commit(): WriterCommitMessage = {
    flush()
    delegate.commit()
  }

  override def abort(): Unit = {
    releaseBatch()
    delegate.abort()
  }

  override def close(): Unit = {
    try {
      delegate.close()
    } finally {
      releaseBatch()
      Utils.tryLogNonFatalError(allocator.close())
    }
  }

  override def currentMetricsValues(): Array[CustomTaskMetric] = delegate.currentMetricsValues()
}
