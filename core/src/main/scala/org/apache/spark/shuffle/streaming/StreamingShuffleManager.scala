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

package org.apache.spark.shuffle.streaming

import org.apache.spark.{ShuffleDependency, SparkException, SparkRuntimeException, TaskContext}
import org.apache.spark.internal.Logging
import org.apache.spark.network.shuffle.streaming.{DataMessage, StreamingShuffleMessage, StreamingShuffleMessageType, TerminationControlMessage}
import org.apache.spark.shuffle._

class StreamingShuffleHandle[K, V, C](shuffleId: Int, dependency: ShuffleDependency[K, V, C])
  extends BaseShuffleHandle[K, V, C](shuffleId, dependency)

object StreamingShuffleManager extends Logging {
  // Exposed for testing
  private[spark] val QUERY_ID_PROPERTY_KEY = "sql.streaming.queryId"
  // Since above is not applicable for batch query, we use below id to track error for batch
  // query with streaming shuffle
  private val QUERY_EXECUTION_ID_PROPERTY_KEY = "spark.sql.execution.id"

  def getQueryId(context: TaskContext): String = {
    Option(context.getLocalProperty(QUERY_ID_PROPERTY_KEY))
      .orElse(Option(context.getLocalProperty(QUERY_EXECUTION_ID_PROPERTY_KEY)))
      .getOrElse(throw SparkException.internalError(
        "Streaming shuffle requires the query id or SQL execution id local property to be set"))
  }

  /* Called from the reader side to get the writerId associated with a message */
  def getWriterId(message: StreamingShuffleMessage): Int = {
    message.messageType() match {
      case StreamingShuffleMessageType.DATA_MESSAGE_UNSAFE_ROW =>
        message.asInstanceOf[DataMessage].shuffleWriterId
      case StreamingShuffleMessageType.TERMINATION_CONTROL_MESSAGE =>
        message.asInstanceOf[TerminationControlMessage].shuffleWriterId
      case _ =>
        // Should not reach here
        throw streamingShuffleUnexpectedMessageType(message.messageType());
    }
  }

  def streamingShuffleIncorrectSequenceNumber(
      messageType: StreamingShuffleMessageType,
      writerId: Int,
      readerId: Int,
      expSeqNum: Long,
      actSeqNum: Long): RuntimeException = {
    new SparkRuntimeException(
      errorClass = "STREAMING_SHUFFLE_INCORRECT_SEQUENCE_NUMBER",
      messageParameters = Map(
        "messageType" -> messageType.toString,
        "writerId" -> writerId.toString,
        "readerId" -> readerId.toString,
        "expSeqNum" -> expSeqNum.toString,
        "actSeqNum" -> actSeqNum.toString))
  }

  def streamingShuffleUnexpectedMessageType(
      messageType: StreamingShuffleMessageType): RuntimeException = {
    new SparkRuntimeException(
      errorClass = "STREAMING_SHUFFLE_UNEXPECTED_MESSAGE_TYPE",
      messageParameters = Map("messageType" -> messageType.toString))
  }
}

private[spark] class StreamingShuffleManager extends PipelinedShuffle with Logging {

  logInfo(log"Using StreamingShuffleManager")

  override def registerShuffle[K, V, C](
      shuffleId: Int,
      dependency: ShuffleDependency[K, V, C]): ShuffleHandle = {
    new StreamingShuffleHandle(shuffleId, dependency)
  }

  override def getWriter[K, V](
      handle: ShuffleHandle,
      mapId: Long,
      context: TaskContext,
      metrics: ShuffleWriteMetricsReporter): ShuffleWriter[K, V] = {
    val streamingShuffleHandle = handle.asInstanceOf[StreamingShuffleHandle[K, V, _]]
    new StreamingShuffleWriter[K, V](streamingShuffleHandle, mapId, context)
  }

  /**
   * For the streaming shuffle, the startMapIndex, endMapIndex, startPartition, and endPartition
   * arguments are not relevant.
   */
  override def getReader[K, C](
      handle: ShuffleHandle,
      startMapIndex: Int,
      endMapIndex: Int,
      startPartition: Int,
      endPartition: Int,
      context: TaskContext,
      metrics: ShuffleReadMetricsReporter): ShuffleReader[K, C] = {
    val streamingShuffleHandle = handle.asInstanceOf[StreamingShuffleHandle[K, _, C]]
    new StreamingShuffleReader[K, C](streamingShuffleHandle, context)
  }

  override def unregisterShuffle(shuffleId: Int): Boolean = {
    // No manager-side state to release here: the driver's StreamingShuffleOutputTracker is
    // unregistered in BlockManagerStorageEndpoint's RemoveShuffle handler, and per-task writer
    // and reader resources are released via task completion listeners.
    true
  }

  // No shuffleBlockResolver: a StreamingShuffleManager is a PipelinedShuffle, serving its output
  // out-of-band rather than as block-manager-addressed blocks. Block-by-id resolution never routes
  // here (see BlockingShuffle).

  override def stop(): Unit = {}
}
