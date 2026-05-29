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

private[spark] class StreamingShuffleManager extends ShuffleManager with Logging {

  logInfo(log"Using StreamingShuffleManager")

  /**
   * Register a shuffle with the manager and obtain a handle for it to pass to tasks.
   */
  override def registerShuffle[K, V, C](
      shuffleId: Int,
      dependency: ShuffleDependency[K, V, C]): ShuffleHandle = {
    new StreamingShuffleHandle(shuffleId, dependency)
  }

  /** Get a writer for a given partition. Called on executors by map tasks. */
  override def getWriter[K, V](
      handle: ShuffleHandle,
      mapId: Long,
      context: TaskContext,
      metrics: ShuffleWriteMetricsReporter): ShuffleWriter[K, V] = {
    // Implementation is added in a follow-up commit that introduces StreamingShuffleWriter.
    throw new UnsupportedOperationException(
      "StreamingShuffleManager.getWriter is not yet implemented")
  }

  /**
   * Get a reader for a range of reduce partitions (startPartition to endPartition-1, inclusive)
   * to read from a range of map outputs(startMapIndex to endMapIndex-1, inclusive). If
   * endMapIndex=Int.MaxValue, the actual endMapIndex will be changed to the length of total map
   * outputs of the shuffle in `getMapSizesByExecutorId`.
   *
   * Called on executors by reduce tasks.
   *
   * For the streaming shuffle arguments startMapIndex, endMapIndex, startPartition,
   * and endPartition are not relevant
   */
  override def getReader[K, C](
      handle: ShuffleHandle,
      startMapIndex: Int,
      endMapIndex: Int,
      startPartition: Int,
      endPartition: Int,
      context: TaskContext,
      metrics: ShuffleReadMetricsReporter): ShuffleReader[K, C] = {
    // Implementation is added in a follow-up commit that introduces StreamingShuffleReader.
    throw new UnsupportedOperationException(
      "StreamingShuffleManager.getReader is not yet implemented")
  }

  /**
   * Remove a shuffle's metadata from the ShuffleManager.
   * @return
   *   true if the metadata removed successfully, otherwise false.
   */
  override def unregisterShuffle(shuffleId: Int): Boolean = {
    // No manager-side state to release here: the driver's StreamingShuffleOutputTracker is
    // unregistered in BlockManagerStorageEndpoint's RemoveShuffle handler, and per-task writer
    // and reader resources are released via task completion listeners.
    true
  }

  /**
   * Return a resolver capable of retrieving shuffle block data based on block coordinates.
   */
  override def shuffleBlockResolver: ShuffleBlockResolver = {
    // don't need to support this for the streaming shuffle implementation
    // since block manager is not used
    throw new UnsupportedOperationException()
  }

  /** Shut down this ShuffleManager. */
  override def stop(): Unit = {}
}
