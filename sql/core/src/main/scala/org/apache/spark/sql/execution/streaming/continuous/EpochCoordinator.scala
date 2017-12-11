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

case class CommitPartitionEpoch(
    partitionId: Int,
    epoch: Long,
    message: WriterCommitMessage)

case class GetCurrentEpoch()

case class ReportPartitionOffset(
    partitionId: Int,
    epoch: Long,
    offset: PartitionOffset)

case class SetReaderPartitions(numPartitions: Int)
case class SetWriterPartitions(numPartitions: Int)

// Should be used only by ContinuousExecution during epoch advancement.
case class IncrementAndGetEpoch()


/** Helper object used to create reference to [[EpochCoordinator]]. */
object EpochCoordinatorRef extends Logging {

  private val endpointNamePrefix = "EpochCoordinator-"

  private def endpointName(queryId: String) = s"EpochCoordinator-$queryId"

  /**
   * Create a reference to a new [[EpochCoordinator]].
   */
  def create(
      writer: ContinuousWriter,
      reader: ContinuousReader,
      startEpoch: Long,
      queryId: String,
      session: SparkSession,
      env: SparkEnv): RpcEndpointRef = synchronized {
    val coordinator = new EpochCoordinator(writer, reader, startEpoch, queryId, session, env.rpcEnv)
    val ref = env.rpcEnv.setupEndpoint(endpointName(queryId), coordinator)
    logInfo("Registered EpochCoordinator endpoint")
    ref
  }

  def get(queryId: String, env: SparkEnv): RpcEndpointRef = synchronized {
    val rpcEndpointRef = RpcUtils.makeDriverRef(endpointName(queryId), env.conf, env.rpcEnv)
    logDebug("Retrieved existing EpochCoordinator endpoint")
    rpcEndpointRef
  }
}

class EpochCoordinator(
    writer: ContinuousWriter,
    reader: ContinuousReader,
    startEpoch: Long,
    queryId: String,
    session: SparkSession,
    override val rpcEnv: RpcEnv)
  extends ThreadSafeRpcEndpoint with Logging {

  private var numReaderPartitions: Int = _
  private var numWriterPartitions: Int = _

  // Should only be mutated by this coordinator's subthread.
  private var currentDriverEpoch = startEpoch

  // (epoch, partition) -> message
  // This is small enough that we don't worry too much about optimizing the shape of the structure.
  private val partitionCommits =
  mutable.Map[(Long, Int), WriterCommitMessage]()

  private val partitionOffsets =
    mutable.Map[(Long, Int), PartitionOffset]()

  private def resolveCommitsAtEpoch(epoch: Long) = {
    val thisEpochCommits =
      partitionCommits.collect { case ((e, _), msg) if e == epoch => msg }
    val nextEpochOffsets =
      partitionOffsets.collect { case ((e, _), o) if e == epoch + 1 => o }

    if (thisEpochCommits.size == numWriterPartitions &&
      nextEpochOffsets.size == numReaderPartitions) {
      logDebug(s"Epoch $epoch has received commits from all partitions. Committing globally.")
      val query = session.streams.get(queryId).asInstanceOf[StreamingQueryWrapper]
        .streamingQuery.asInstanceOf[ContinuousExecution]
      // Sequencing is important - writer commits to epoch are required to be replayable
      writer.commit(epoch, thisEpochCommits.toArray)
      query.commit(epoch)
      // TODO: cleanup unnecessary state
    }
  }

  override def receive: PartialFunction[Any, Unit] = {
    case CommitPartitionEpoch(partitionId, epoch, message) =>
      logDebug(s"Got commit from partition $partitionId at epoch $epoch: $message")
      if (!partitionCommits.isDefinedAt((epoch, partitionId))) {
        partitionCommits.put((epoch, partitionId), message)
        resolveCommitsAtEpoch(epoch)
      }

    case ReportPartitionOffset(partitionId, epoch, offset) if offset != null =>
      val query = session.streams.get(queryId).asInstanceOf[StreamingQueryWrapper]
        .streamingQuery.asInstanceOf[ContinuousExecution]
      partitionOffsets.put((epoch, partitionId), offset)
      val thisEpochOffsets =
        partitionOffsets.collect { case ((e, _), o) if e == epoch => o }
      if (thisEpochOffsets.size == numReaderPartitions) {
        logDebug(s"Epoch $epoch has offsets reported from all partitions: $thisEpochOffsets")
        query.addOffset(epoch, reader, thisEpochOffsets.toSeq)
        resolveCommitsAtEpoch(epoch - 1)
      }

    // We can get null offsets reported if the epoch advances before the executor
    // has read any data. Ignore those, since they don't affect where we'd want to restart.
    case ReportPartitionOffset(_, _, offset) if offset == null =>
  }

  override def receiveAndReply(context: RpcCallContext): PartialFunction[Any, Unit] = {
    case GetCurrentEpoch() =>
      val result = currentDriverEpoch
      logDebug(s"Epoch $result")
      context.reply(result)

    case IncrementAndGetEpoch() =>
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
