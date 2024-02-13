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

package org.apache.spark.sql.connect.execution

import java.util.UUID

import scala.collection.mutable

import com.google.protobuf.Message
import io.grpc.stub.StreamObserver

import org.apache.spark.{SparkEnv, SparkSQLException}
import org.apache.spark.connect.proto
import org.apache.spark.internal.Logging
import org.apache.spark.sql.connect.config.Connect.CONNECT_EXECUTE_REATTACHABLE_OBSERVER_RETRY_BUFFER_SIZE
import org.apache.spark.sql.connect.service.ExecuteHolder

/**
 * This StreamObserver is running on the execution thread. Execution pushes responses to it, it
 * caches them. ExecuteResponseGRPCSender is the consumer of the responses ExecuteResponseObserver
 * "produces". It waits on the responseLock. New produced responses notify the responseLock.
 * @see
 *   getResponse.
 *
 * ExecuteResponseObserver controls how responses stay cached after being returned to consumer,
 * @see
 *   removeCachedResponses.
 *
 * A single ExecuteResponseGRPCSender can be attached to the ExecuteResponseObserver. Attaching a
 * new one will notify an existing one that it was detached.
 * @see
 *   attachConsumer
 */
private[connect] class ExecuteResponseObserver[T <: Message](val executeHolder: ExecuteHolder)
    extends StreamObserver[T]
    with Logging {

  /**
   * Cached responses produced by the execution. Map from response index -> response. Response
   * indexes are numbered consecutively starting from 1.
   */
  private val responses: mutable.Map[Long, CachedStreamResponse[T]] =
    new mutable.HashMap[Long, CachedStreamResponse[T]]()

  private val responseIndexToId: mutable.Map[Long, String] = new mutable.HashMap[Long, String]()

  private val responseIdToIndex: mutable.Map[String, Long] = new mutable.HashMap[String, Long]()

  /** Cached error of the execution, if an error was thrown. */
  private var error: Option[Throwable] = None

  /**
   * If execution stream is finished (completed or with error), the index of the final response.
   */
  private var finalProducedIndex: Option[Long] = None // index of final response before completed.

  /** The index of the last response produced by execution. */
  private var lastProducedIndex: Long = 0 // first response will have index 1

  // For testing
  private[connect] var releasedUntilIndex: Long = 0

  /**
   * Highest response index that was consumed. Keeps track of it to decide which responses needs
   * to be cached, and to assert that all responses are consumed.
   *
   * Visible for testing.
   */
  private[connect] var highestConsumedIndex: Long = 0

  /**
   * Lock used for synchronization between responseObserver and grpcResponseSenders. *
   * grpcResponseSenders wait on it for a new response to be available. * grpcResponseSenders also
   * notify it to wake up when interrupted * responseObserver notifies it when new responses are
   * available.
   */
  private[connect] val responseLock = new Object()

  // Statistics about cached responses.
  private val cachedSizeUntilHighestConsumed = CachedSize()
  private val cachedSizeUntilLastProduced = CachedSize()
  private val autoRemovedSize = CachedSize()
  private val totalSize = CachedSize()

  /**
   * Total size of response to be held buffered after giving out with getResponse. 0 for none, any
   * value greater than 0 will buffer the response from getResponse.
   */
  private val retryBufferSize = if (executeHolder.reattachable) {
    SparkEnv.get.conf.get(CONNECT_EXECUTE_REATTACHABLE_OBSERVER_RETRY_BUFFER_SIZE)
  } else {
    0
  }

  def tryOnNext(r: T): Boolean = responseLock.synchronized {
    if (finalProducedIndex.nonEmpty) {
      return false
    }
    lastProducedIndex += 1
    val processedResponse = setCommonResponseFields(r)
    val responseId = getResponseId(processedResponse)
    val response = CachedStreamResponse[T](processedResponse, responseId, lastProducedIndex)

    responses += ((lastProducedIndex, response))
    responseIndexToId += ((lastProducedIndex, responseId))
    responseIdToIndex += ((responseId, lastProducedIndex))

    cachedSizeUntilLastProduced.add(response)
    totalSize.add(response)

    logDebug(
      s"Execution opId=${executeHolder.operationId} produced response " +
        s"responseId=${responseId} idx=$lastProducedIndex")
    responseLock.notifyAll()
    true
  }

  def onNext(r: T): Unit = {
    if (!tryOnNext(r)) {
      throw new IllegalStateException("Stream onNext can't be called after stream completed")
    }
  }

  def onError(t: Throwable): Unit = responseLock.synchronized {
    if (finalProducedIndex.nonEmpty) {
      throw new IllegalStateException("Stream onError can't be called after stream completed")
    }
    error = Some(t)
    finalProducedIndex = Some(lastProducedIndex) // no responses to be send after error.
    logDebug(
      s"Execution opId=${executeHolder.operationId} produced error. " +
        s"Last stream index is $lastProducedIndex.")
    responseLock.notifyAll()
  }

  def onCompleted(): Unit = responseLock.synchronized {
    if (finalProducedIndex.nonEmpty) {
      throw new IllegalStateException("Stream onCompleted can't be called after stream completed")
    }
    finalProducedIndex = Some(lastProducedIndex)
    logDebug(
      s"Execution opId=${executeHolder.operationId} completed stream. " +
        s"Last stream index is $lastProducedIndex.")
    responseLock.notifyAll()
  }

  /**
   * Get response with a given index in the stream, if set. Note: Upon returning the response,
   * this response observer assumes that the response is consumed, and the response and previous
   * response can be uncached, keeping retryBufferSize of responses for the case of retries.
   */
  def consumeResponse(index: Long): Option[CachedStreamResponse[T]] = responseLock.synchronized {
    // we index stream responses from 1, getting a lower index would be invalid.
    assert(index >= 1)
    // it would be invalid if consumer would skip a response
    assert(index <= highestConsumedIndex + 1)
    val ret = responses.get(index)
    if (ret.isDefined) {
      if (index > highestConsumedIndex) {
        highestConsumedIndex = index
        cachedSizeUntilHighestConsumed.add(ret.get)
      }
      // When the response is consumed, figure what previous responses can be uncached.
      // (We keep at least one response before the one we send to consumer now)
      removeCachedResponses(index - 1)
      logDebug(
        s"CONSUME opId=${executeHolder.operationId} responseId=${ret.get.responseId} " +
          s"idx=$index. size=${ret.get.serializedByteSize} " +
          s"cachedUntilConsumed=$cachedSizeUntilHighestConsumed " +
          s"cachedUntilProduced=$cachedSizeUntilLastProduced")
    } else if (index <= highestConsumedIndex) {
      // If index is <= highestConsumedIndex and not available, it was already removed from cache.
      // This may happen if ReattachExecute is too late and the cached response was evicted.
      val responseId = responseIndexToId.getOrElse(index, "<UNKNOWN>")
      throw new SparkSQLException(
        errorClass = "INVALID_CURSOR.POSITION_NOT_AVAILABLE",
        messageParameters = Map("index" -> index.toString, "responseId" -> responseId))
    } else if (getLastResponseIndex().exists(index > _)) {
      // If index > lastIndex, it's out of bounds. This is an internal error.
      throw new IllegalStateException(
        s"Cursor position $index is beyond last index ${getLastResponseIndex()}.")
    }
    ret
  }

  /** Get the stream error if there is one, otherwise None. */
  def getError(): Option[Throwable] = responseLock.synchronized {
    error
  }

  /** If the stream is finished, the index of the last response, otherwise None. */
  def getLastResponseIndex(): Option[Long] = responseLock.synchronized {
    finalProducedIndex
  }

  /** Get the index in the stream for given response id. */
  def getResponseIndexById(responseId: String): Long = responseLock.synchronized {
    responseIdToIndex.getOrElse(
      responseId,
      throw new SparkSQLException(
        errorClass = "INVALID_CURSOR.POSITION_NOT_FOUND",
        messageParameters = Map("responseId" -> responseId)))
  }

  /** Remove cached responses up to and including response with given id. */
  def removeResponsesUntilId(responseId: String): Unit = responseLock.synchronized {
    val index = getResponseIndexById(responseId)
    removeResponsesUntilIndex(index)
    logDebug(
      s"RELEASE opId=${executeHolder.operationId} until " +
        s"responseId=$responseId " +
        s"idx=$index. " +
        s"cachedUntilConsumed=$cachedSizeUntilHighestConsumed " +
        s"cachedUntilProduced=$cachedSizeUntilLastProduced")
  }

  /** Remove all cached responses */
  def removeAll(): Unit = responseLock.synchronized {
    removeResponsesUntilIndex(lastProducedIndex)
    logInfo(
      s"Release all for opId=${executeHolder.operationId}. Execution stats: " +
        s"total=${totalSize} " +
        s"autoRemoved=${autoRemovedSize} " +
        s"cachedUntilConsumed=$cachedSizeUntilHighestConsumed " +
        s"cachedUntilProduced=$cachedSizeUntilLastProduced " +
        s"maxCachedUntilConsumed=${cachedSizeUntilHighestConsumed.max} " +
        s"maxCachedUntilProduced=${cachedSizeUntilLastProduced.max}")
  }

  /** Returns if the stream is finished. */
  def completed(): Boolean = responseLock.synchronized {
    finalProducedIndex.isDefined
  }

  /**
   * Remove cached responses after response with lastReturnedIndex is returned from getResponse.
   * Remove according to caching policy:
   *   - if retryBufferSize is 0 (or query is not reattachable), remove all responses up to and
   *     including lastSentIndex.
   *   - otherwise keep responses backwards from lastSentIndex until their total size exceeds
   *     retryBufferSize
   */
  private def removeCachedResponses(lastSentIndex: Long) = {
    var i = lastSentIndex
    var totalResponsesSize = 0L
    while (i >= 1 && responses.get(i).isDefined && totalResponsesSize < retryBufferSize) {
      totalResponsesSize += responses.get(i).get.serializedByteSize
      i -= 1
    }
    if (responses.get(i).isDefined) {
      logDebug(
        s"AUTORELEASE opId=${executeHolder.operationId} until idx=$i. " +
          s"cachedUntilConsumed=$cachedSizeUntilHighestConsumed " +
          s"cachedUntilProduced=$cachedSizeUntilLastProduced")
      removeResponsesUntilIndex(i, true)
    } else {
      logDebug(
        s"NO AUTORELEASE opId=${executeHolder.operationId}. " +
          s"cachedUntilConsumed=$cachedSizeUntilHighestConsumed " +
          s"cachedUntilProduced=$cachedSizeUntilLastProduced")
    }
  }

  /**
   * Remove cached responses until given index. Iterating backwards, once an index is encountered
   * that has been removed, all earlier indexes would also be removed.
   */
  private def removeResponsesUntilIndex(index: Long, autoRemoved: Boolean = false) = {
    var i = index
    while (i >= 1 && responses.get(i).isDefined) {
      val r = responses.get(i).get
      cachedSizeUntilHighestConsumed.remove(r)
      cachedSizeUntilLastProduced.remove(r)
      if (autoRemoved) autoRemovedSize.add(r)
      responses.remove(i)
      i -= 1
    }
    releasedUntilIndex = index
  }

  /**
   * Populate response fields that are common and should be set in every response.
   */
  private def setCommonResponseFields(response: T): T = {
    response match {
      case executePlanResponse: proto.ExecutePlanResponse =>
        executePlanResponse
          .toBuilder()
          .setSessionId(executeHolder.sessionHolder.sessionId)
          .setServerSideSessionId(executeHolder.sessionHolder.serverSessionId)
          .setOperationId(executeHolder.operationId)
          .setResponseId(UUID.randomUUID.toString)
          .build()
          .asInstanceOf[T]
    }
  }

  /**
   * Get the response id from the response proto message.
   */
  private def getResponseId(response: T): String = {
    response match {
      case executePlanResponse: proto.ExecutePlanResponse =>
        executePlanResponse.getResponseId
    }
  }

  /**
   * Helper for counting statistics about cached responses.
   */
  private case class CachedSize(var bytes: Long = 0L, var num: Long = 0L) {
    var maxBytes: Long = 0L
    var maxNum: Long = 0L

    def add(t: CachedStreamResponse[T]): Unit = {
      bytes += t.serializedByteSize
      if (bytes > maxBytes) maxBytes = bytes
      num += 1
      if (num > maxNum) maxNum = num
    }

    def remove(t: CachedStreamResponse[T]): Unit = {
      bytes -= t.serializedByteSize
      num -= 1
    }

    def max: CachedSize = CachedSize(maxBytes, maxNum)
  }
}
