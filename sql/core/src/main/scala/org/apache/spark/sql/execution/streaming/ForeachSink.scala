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
import org.apache.spark.sql.sources.v2.{DataSourceOptions, StreamWriteSupport}
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
    new StreamWriter with SupportsWriteInternalRow {
      override def commit(epochId: Long, messages: Array[WriterCommitMessage]): Unit = {}
      override def abort(epochId: Long, messages: Array[WriterCommitMessage]): Unit = {}

      override def createInternalRowWriterFactory(): DataWriterFactory[InternalRow] = {
        val byteStream = new ByteArrayOutputStream()
        val objectStream = new ObjectOutputStream(byteStream)
        objectStream.writeObject(writer)
        ForeachWriterFactory(byteStream.toByteArray, encoder)
      }
    }
  }
}

case class ForeachWriterFactory[T: Encoder](
    serializedWriter: Array[Byte],
    encoder: ExpressionEncoder[T])
    extends DataWriterFactory[InternalRow] {
  override def createDataWriter(partitionId: Int, attemptNumber: Int): ForeachDataWriter[T] = {
    new ForeachDataWriter(serializedWriter, encoder, partitionId)
  }
}

/**
 * A [[DataWriter]] for the foreach sink.
 *
 * Note that [[ForeachWriter]] has the following lifecycle, and (as was true in the V1 sink API)
 * assumes that it's never reused:
 *  * [create writer]
 *  * open(partitionId, batchId)
 *  * if open() returned true: write, write, write, ...
 *  * close()
 * while DataSourceV2 writers have a slightly different lifecycle and will be reused for multiple
 * epochs in the continuous processing engine:
 *  * [create writer]
 *  * write, write, write, ...
 *  * commit()
 *
 * The bulk of the implementation here is a shim between these two models.
 *
 * @param serializedWriter a serialized version of the user-provided [[ForeachWriter]]
 * @param encoder encoder from [[Row]] to the type param [[T]]
 * @param partitionId the ID of the partition this data writer is responsible for
 *
 * @tparam T the type of data to be handled by the writer
 */
class ForeachDataWriter[T : Encoder](
    serializedWriter: Array[Byte],
    encoder: ExpressionEncoder[T],
    partitionId: Int)
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

  // A small state machine representing the lifecycle of the underlying ForeachWriter.
  //  * CLOSED means close() has been called.
  //  * OPENED means open() was called and returned true.
  //  * OPENED_SKIP_PROCESSING means open() was called and returned false.
  private object WriterState extends Enumeration {
    type WriterState = Value
    val CLOSED, OPENED, OPENED_SKIP_PROCESSING = Value
  }
  import WriterState._

  private var writer: ForeachWriter[T] = _
  private var state: WriterState = _
  private var currentEpochId = initialEpochId

  private def openAndSetState(epochId: Long) = {
    writer = new ObjectInputStream(new ByteArrayInputStream(serializedWriter)).readObject()
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
    // Close if the writer got opened for this epoch.
    state match {
      case CLOSED => ()
      case _ => writer.close(null)
    }
    state = CLOSED
    currentEpochId += 1
    ForeachWriterCommitMessage
  }

  override def abort(): Unit = {}
}

/**
 * An empty [[WriterCommitMessage]]. [[ForeachWriter]] implementations have no global coordination.
 */
case object ForeachWriterCommitMessage extends WriterCommitMessage
