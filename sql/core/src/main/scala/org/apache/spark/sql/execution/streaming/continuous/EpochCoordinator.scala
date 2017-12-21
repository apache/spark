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

package org.apache.spark.sql.execution.streaming.continuous

import java.util.concurrent.atomic.AtomicLong

import scala.collection.mutable

import org.apache.spark.SparkEnv
import org.apache.spark.internal.Logging
import org.apache.spark.rpc.{RpcCallContext, RpcEndpointRef, RpcEnv, ThreadSafeRpcEndpoint}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.execution.streaming.StreamingQueryWrapper
import org.apache.spark.sql.sources.v2.reader.{ContinuousReader, PartitionOffset}
import org.apache.spark.sql.sources.v2.writer.{ContinuousWriter, WriterCommitMessage}
import org.apache.spark.util.RpcUtils

private[continuous] sealed trait EpochCoordinatorMessage extends Serializable

// Driver epoch trigger message
/**
 * Atomically increment the current epoch and get the new value.
 */
private[sql] case object IncrementAndGetEpoch extends EpochCoordinatorMessage

// Init messages
/**
 * Set the reader and writer partition counts. Tasks may not be started until the coordinator
 * has acknowledged these messages.
 */
private[sql] case class SetReaderPartitions(numPartitions: Int) extends EpochCoordinatorMessage
case class SetWriterPartitions(numPartitions: Int) extends EpochCoordinatorMessage

// Partition task messages
/**
 * Get the current epoch.
 */
private[sql] case object GetCurrentEpoch extends EpochCoordinatorMessage
/**
 * Commit a partition at the specified epoch with the given message.
 */
private[sql] case class CommitPartitionEpoch(
    partitionId: Int,
    epoch: Long,
    message: WriterCommitMessage) extends EpochCoordinatorMessage
/**
 * Report that a partition is ending the specified epoch at the specified offset.
 */
private[sql] case class ReportPartitionOffset(
    partitionId: Int,
    epoch: Long,
    offset: PartitionOffset) extends EpochCoordinatorMessage


/** Helper object used to create reference to [[EpochCoordinator]]. */
private[sql] object EpochCoordinatorRef extends Logging {
  private def endpointName(runId: String) = s"EpochCoordinator-$runId"

  /**
   * Create a reference to a new [[EpochCoordinator]].
   */
  def create(
      writer: ContinuousWriter,
      reader: ContinuousReader,
      query: ContinuousExecution,
      startEpoch: Long,
      session: SparkSession,
      env: SparkEnv): RpcEndpointRef = synchronized {
    val coordinator = new EpochCoordinator(
      writer, reader, query, startEpoch, query.id.toString(), session, env.rpcEnv)
    val ref = env.rpcEnv.setupEndpoint(endpointName(query.runId.toString()), coordinator)
    logInfo("Registered EpochCoordinator endpoint")
    ref
  }

  def get(runId: String, env: SparkEnv): RpcEndpointRef = synchronized {
    val rpcEndpointRef = RpcUtils.makeDriverRef(endpointName(runId), env.conf, env.rpcEnv)
    logDebug("Retrieved existing EpochCoordinator endpoint")
    rpcEndpointRef
  }
}

/**
 * Handles three major epoch coordination tasks for continuous processing:
 *
 * * Maintains a local epoch counter (the "driver epoch"), incremented by IncrementAndGetEpoch
 *   and pollable from executors by GetCurrentEpoch. Note that this epoch is *not* immediately
 *   reflected anywhere in ContinuousExecution.
 * * Collates ReportPartitionOffset messages, and forwards to ContinuousExecution when all
 *   readers have ended a given epoch.
 * * Collates CommitPartitionEpoch messages, and forwards to ContinuousExecution when all readers
 *   have both committed and reported an end offset for a given epoch.
 */
private[continuous] class EpochCoordinator(
    writer: ContinuousWriter,
    reader: ContinuousReader,
    query: ContinuousExecution,
    startEpoch: Long,
    queryId: String,
    session: SparkSession,
    override val rpcEnv: RpcEnv)
  extends ThreadSafeRpcEndpoint with Logging {

  private var numReaderPartitions: Int = _
  private var numWriterPartitions: Int = _

  private var currentDriverEpoch = startEpoch

  // (epoch, partition) -> message
  private val partitionCommits =
    mutable.Map[(Long, Int), WriterCommitMessage]()
  // (epoch, partition) -> offset
  private val partitionOffsets =
    mutable.Map[(Long, Int), PartitionOffset]()

  private def resolveCommitsAtEpoch(epoch: Long) = {
    val thisEpochCommits =
      partitionCommits.collect { case ((e, _), msg) if e == epoch => msg }
    val nextEpochOffsets =
      partitionOffsets.collect { case ((e, _), o) if e == epoch => o }

    if (thisEpochCommits.size == numWriterPartitions &&
      nextEpochOffsets.size == numReaderPartitions) {
      logDebug(s"Epoch $epoch has received commits from all partitions. Committing globally.")
      val query = session.streams.get(queryId).asInstanceOf[StreamingQueryWrapper]
        .streamingQuery.asInstanceOf[ContinuousExecution]
      // Sequencing is important here. We must commit to the writer before recording the commit
      // in the query, or we will end up dropping the commit if we restart in the middle.
      writer.commit(epoch, thisEpochCommits.toArray)
      query.commit(epoch)

      // Cleanup state from before this epoch, now that we know all partitions are forever past it.
      for (k <- partitionCommits.keys.filter { case (e, _) => e < epoch }) {
        partitionCommits.remove(k)
      }
      for (k <- partitionOffsets.keys.filter { case (e, _) => e < epoch }) {
        partitionCommits.remove(k)
      }
    }
  }

  override def receive: PartialFunction[Any, Unit] = {
    case CommitPartitionEpoch(partitionId, epoch, message) =>
      logDebug(s"Got commit from partition $partitionId at epoch $epoch: $message")
      if (!partitionCommits.isDefinedAt((epoch, partitionId))) {
        partitionCommits.put((epoch, partitionId), message)
        resolveCommitsAtEpoch(epoch)
      }

    case ReportPartitionOffset(partitionId, epoch, offset) =>
      val query = session.streams.get(queryId).asInstanceOf[StreamingQueryWrapper]
        .streamingQuery.asInstanceOf[ContinuousExecution]
      partitionOffsets.put((epoch, partitionId), offset)
      val thisEpochOffsets =
        partitionOffsets.collect { case ((e, _), o) if e == epoch => o }
      if (thisEpochOffsets.size == numReaderPartitions) {
        logDebug(s"Epoch $epoch has offsets reported from all partitions: $thisEpochOffsets")
        query.addOffset(epoch, reader, thisEpochOffsets.toSeq)
        resolveCommitsAtEpoch(epoch)
      }
  }

  override def receiveAndReply(context: RpcCallContext): PartialFunction[Any, Unit] = {
    case GetCurrentEpoch =>
      val result = currentDriverEpoch
      logDebug(s"Epoch $result")
      context.reply(result)

    case IncrementAndGetEpoch =>
      currentDriverEpoch += 1
      context.reply(currentDriverEpoch)

    case SetReaderPartitions(numPartitions) =>
      numReaderPartitions = numPartitions
      context.reply(())

    case SetWriterPartitions(numPartitions) =>
      numWriterPartitions = numPartitions
      context.reply(())
  }
}
