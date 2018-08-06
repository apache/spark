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

import org.apache.spark.sql.{ForeachWriter, SparkSession}
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.encoders.ExpressionEncoder
import org.apache.spark.sql.catalyst.expressions.UnsafeRow
import org.apache.spark.sql.execution.python.PythonForeachWriter
import org.apache.spark.sql.sources.v2.{DataSourceOptions, StreamWriteSupport}
import org.apache.spark.sql.sources.v2.writer.{DataWriter, DataWriterFactory, WriterCommitMessage}
import org.apache.spark.sql.sources.v2.writer.streaming.StreamWriter
import org.apache.spark.sql.streaming.OutputMode
import org.apache.spark.sql.types.StructType

/**
 * A [[org.apache.spark.sql.sources.v2.DataSourceV2]] for forwarding data into the specified
 * [[ForeachWriter]].
 *
 * @param writer The [[ForeachWriter]] to process all data.
 * @param converter An object to convert internal rows to target type T. Either it can be
 *                  a [[ExpressionEncoder]] or a direct converter function.
 * @tparam T The expected type of the sink.
 */
case class ForeachWriterProvider[T](
    writer: ForeachWriter[T],
    converter: Either[ExpressionEncoder[T], InternalRow => T]) extends StreamWriteSupport {

  override def createStreamWriter(
      queryId: String,
      schema: StructType,
      mode: OutputMode,
      options: DataSourceOptions): StreamWriter = {
    new StreamWriter {
      override def commit(epochId: Long, messages: Array[WriterCommitMessage]): Unit = {}
      override def abort(epochId: Long, messages: Array[WriterCommitMessage]): Unit = {}

      override def createWriterFactory(): DataWriterFactory[InternalRow] = {
        val rowConverter: InternalRow => T = converter match {
          case Left(enc) =>
            val boundEnc = enc.resolveAndBind(
              schema.toAttributes,
              SparkSession.getActiveSession.get.sessionState.analyzer)
            boundEnc.fromRow
          case Right(func) =>
            func
        }
        ForeachWriterFactory(writer, rowConverter)
      }

      override def toString: String = "ForeachSink"
    }
  }
}

object ForeachWriterProvider {
  def apply[T](
      writer: ForeachWriter[T],
      encoder: ExpressionEncoder[T]): ForeachWriterProvider[_] = {
    writer match {
      case pythonWriter: PythonForeachWriter =>
        new ForeachWriterProvider[UnsafeRow](
          pythonWriter, Right((x: InternalRow) => x.asInstanceOf[UnsafeRow]))
      case _ =>
        new ForeachWriterProvider[T](writer, Left(encoder))
    }
  }
}

case class ForeachWriterFactory[T](
    writer: ForeachWriter[T],
    rowConverter: InternalRow => T)
  extends DataWriterFactory[InternalRow] {
  override def createDataWriter(
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
