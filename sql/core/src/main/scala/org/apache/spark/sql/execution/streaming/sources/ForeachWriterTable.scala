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

package org.apache.spark.sql.execution.streaming.sources

import java.util

import scala.collection.JavaConverters._

import org.apache.spark.sql.{ForeachWriter, SparkSession}
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.encoders.ExpressionEncoder
import org.apache.spark.sql.catalyst.expressions.UnsafeRow
import org.apache.spark.sql.execution.python.PythonForeachWriter
import org.apache.spark.sql.sources.v2.{SupportsWrite, Table, TableCapability}
import org.apache.spark.sql.sources.v2.writer.{DataWriter, SupportsTruncate, WriteBuilder, WriterCommitMessage}
import org.apache.spark.sql.sources.v2.writer.streaming.{StreamingDataWriterFactory, StreamingWrite}
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.util.CaseInsensitiveStringMap

/**
 * A write-only table for forwarding data into the specified [[ForeachWriter]].
 *
 * @param writer The [[ForeachWriter]] to process all data.
 * @param converter An object to convert internal rows to target type T. Either it can be
 *                  a [[ExpressionEncoder]] or a direct converter function.
 * @tparam T The expected type of the sink.
 */
case class ForeachWriterTable[T](
    writer: ForeachWriter[T],
    converter: Either[ExpressionEncoder[T], InternalRow => T])
  extends Table with SupportsWrite {

  override def name(): String = "ForeachSink"

  override def schema(): StructType = StructType(Nil)

  override def capabilities(): util.Set[TableCapability] = {
    Set(TableCapability.STREAMING_WRITE).asJava
  }

  override def newWriteBuilder(options: CaseInsensitiveStringMap): WriteBuilder = {
    new WriteBuilder with SupportsTruncate {
      private var inputSchema: StructType = _

      override def withInputDataSchema(schema: StructType): WriteBuilder = {
        this.inputSchema = schema
        this
      }

      // Do nothing for truncate. Foreach sink is special that it just forwards all the records to
      // ForeachWriter.
      override def truncate(): WriteBuilder = this

      override def buildForStreaming(): StreamingWrite = {
        new StreamingWrite {
          override def commit(epochId: Long, messages: Array[WriterCommitMessage]): Unit = {}
          override def abort(epochId: Long, messages: Array[WriterCommitMessage]): Unit = {}

          override def createStreamingWriterFactory(): StreamingDataWriterFactory = {
            val rowConverter: InternalRow => T = converter match {
              case Left(enc) =>
                val boundEnc = enc.resolveAndBind(
                  inputSchema.toAttributes,
                  SparkSession.getActiveSession.get.sessionState.analyzer)
                boundEnc.fromRow
              case Right(func) =>
                func
            }
            ForeachWriterFactory(writer, rowConverter)
          }
        }
      }
    }
  }
}

object ForeachWriterTable {
  def apply[T](
      writer: ForeachWriter[T],
      encoder: ExpressionEncoder[T]): ForeachWriterTable[_] = {
    writer match {
      case pythonWriter: PythonForeachWriter =>
        new ForeachWriterTable[UnsafeRow](
          pythonWriter, Right((x: InternalRow) => x.asInstanceOf[UnsafeRow]))
      case _ =>
        new ForeachWriterTable[T](writer, Left(encoder))
    }
  }
}

case class ForeachWriterFactory[T](
    writer: ForeachWriter[T],
    rowConverter: InternalRow => T)
  extends StreamingDataWriterFactory {
  override def createWriter(
      partitionId: Int,
      taskId: Long,
      epochId: Long): ForeachDataWriter[T] = {
    new ForeachDataWriter(writer, rowConverter, partitionId, epochId)
  }
}

/**
 * A [[DataWriter]] which writes data in this partition to a [[ForeachWriter]].
 *
 * @param writer The [[ForeachWriter]] to process all data.
 * @param rowConverter A function which can convert [[InternalRow]] to the required type [[T]]
 * @param partitionId
 * @param epochId
 * @tparam T The type expected by the writer.
 */
class ForeachDataWriter[T](
    writer: ForeachWriter[T],
    rowConverter: InternalRow => T,
    partitionId: Int,
    epochId: Long)
  extends DataWriter[InternalRow] {

  // If open returns false, we should skip writing rows.
  private val opened = writer.open(partitionId, epochId)

  override def write(record: InternalRow): Unit = {
    if (!opened) return

    try {
      writer.process(rowConverter(record))
    } catch {
      case t: Throwable =>
        writer.close(t)
        throw t
    }
  }

  override def commit(): WriterCommitMessage = {
    writer.close(null)
    ForeachWriterCommitMessage
  }

  override def abort(): Unit = {}
}

/**
 * An empty [[WriterCommitMessage]]. [[ForeachWriter]] implementations have no global coordination.
 */
case object ForeachWriterCommitMessage extends WriterCommitMessage
