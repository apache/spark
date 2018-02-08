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

package org.apache.spark.sql.execution.streaming

import java.io.{ByteArrayInputStream, ByteArrayOutputStream, ObjectInputStream, ObjectOutputStream}

import org.apache.spark.TaskContext
import org.apache.spark.sql._
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.encoders.{encoderFor, ExpressionEncoder}
import org.apache.spark.sql.execution.streaming.continuous.ContinuousExecution
import org.apache.spark.sql.sources.v2.DataSourceOptions
import org.apache.spark.sql.sources.v2.writer._
import org.apache.spark.sql.sources.v2.writer.streaming.StreamWriter
import org.apache.spark.sql.streaming.OutputMode
import org.apache.spark.sql.types.StructType


case class ForeachWriterProvider[T: Encoder](writer: ForeachWriter[T]) extends StreamWriteSupport {
  override def createStreamWriter(
      queryId: String,
      schema: StructType,
      mode: OutputMode,
      options: DataSourceOptions): StreamWriter = {
    val encoder = encoderFor[T].resolveAndBind(
      schema.toAttributes,
      SparkSession.getActiveSession.get.sessionState.analyzer)
    ForeachInternalWriter(writer, encoder)
  }
}

case class ForeachInternalWriter[T: Encoder](
    writer: ForeachWriter[T], encoder: ExpressionEncoder[T])
    extends StreamWriter with SupportsWriteInternalRow {
  override def commit(epochId: Long, messages: Array[WriterCommitMessage]): Unit = {}
  override def abort(epochId: Long, messages: Array[WriterCommitMessage]): Unit = {}

  override def createInternalRowWriterFactory(): DataWriterFactory[InternalRow] = {
    ForeachWriterFactory(writer, encoder)
  }
}

case class ForeachWriterFactory[T: Encoder](writer: ForeachWriter[T], encoder: ExpressionEncoder[T])
    extends DataWriterFactory[InternalRow] {
  override def createDataWriter(partitionId: Int, attemptNumber: Int): ForeachDataWriter[T] = {
    new ForeachDataWriter(writer, encoder, partitionId)
  }
}

class ForeachDataWriter[T : Encoder](
    private var writer: ForeachWriter[T], encoder: ExpressionEncoder[T], partitionId: Int)
    extends DataWriter[InternalRow] {
  private val initialEpochId: Long = {
    // Start with the microbatch ID. If it's not there, we're in continuous execution,
    // so get the start epoch.
    // This ID will be incremented as commits happen.
    TaskContext.get().getLocalProperty(MicroBatchExecution.BATCH_ID_KEY) match {
      case null => TaskContext.get().getLocalProperty(ContinuousExecution.START_EPOCH_KEY).toLong
      case batch => batch.toLong
    }
  }
  private var currentEpochId = initialEpochId

  // The lifecycle of the ForeachWriter is incompatible with the lifecycle of DataSourceV2 writers.
  // Unfortunately, we cannot migrate ForeachWriter, as its implementations live in user code. So
  // we need a small state machine to shim between them.
  //  * CLOSED means close() has been called.
  //  * OPENED
  private object WriterState extends Enumeration {
    type WriterState = Value
    val CLOSED, OPENED, OPENED_SKIP_PROCESSING = Value
  }
  import WriterState._

  private var state = CLOSED

  private def openAndSetState(epochId: Long) = {
    // Create a new writer by roundtripping through the serialization for compatibility.
    // In the old API, a writer instantiation would never get reused.
    val byteStream = new ByteArrayOutputStream()
    val objectStream = new ObjectOutputStream(byteStream)
    objectStream.writeObject(writer)
    writer = new ObjectInputStream(new ByteArrayInputStream(byteStream.toByteArray)).readObject()
      .asInstanceOf[ForeachWriter[T]]

    writer.open(partitionId, epochId) match {
      case true => state = OPENED
      case false => state = OPENED_SKIP_PROCESSING
    }
  }

  openAndSetState(initialEpochId)

  override def write(record: InternalRow): Unit = {
    try {
      state match {
        case OPENED => writer.process(encoder.fromRow(record))
        case OPENED_SKIP_PROCESSING => ()
        case CLOSED =>
          // First record of a new epoch, so we need to open a new writer for it.
          currentEpochId += 1
          openAndSetState(currentEpochId)
          writer.process(encoder.fromRow(record))
      }
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

case object ForeachWriterCommitMessage extends WriterCommitMessage
