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

package org.apache.spark.storage

import java.util.UUID

import org.apache.spark.SparkException
import org.apache.spark.annotation.{DeveloperApi, Since}
import org.apache.spark.errors.SparkCoreErrors
import org.apache.spark.network.shuffle.RemoteBlockPushResolver
import org.apache.spark.storage.LogBlockType.LogBlockType

/**
 * :: DeveloperApi ::
 * Identifies a particular Block of data, usually associated with a single file.
 * A Block can be uniquely identified by its filename, but each type of Block has a different
 * set of keys which produce its unique name.
 *
 * If your BlockId should be serializable, be sure to add it to the BlockId.apply() method.
 */
@DeveloperApi
sealed abstract class BlockId {
  /** A globally unique identifier for this Block. Can be used for ser/de. */
  def name: String

  // convenience methods
  def asRDDId: Option[RDDBlockId] = if (isRDD) Some(asInstanceOf[RDDBlockId]) else None
  def isRDD: Boolean = isInstanceOf[RDDBlockId]
  def isShuffle: Boolean = {
    (isInstanceOf[ShuffleBlockId] || isInstanceOf[ShuffleBlockBatchId] ||
     isInstanceOf[ShuffleDataBlockId] || isInstanceOf[ShuffleIndexBlockId])
  }
  def isShuffleChunk: Boolean = isInstanceOf[ShuffleBlockChunkId]
  def isBroadcast: Boolean = isInstanceOf[BroadcastBlockId]

  override def toString: String = name
}

@DeveloperApi
case class RDDBlockId(rddId: Int, splitIndex: Int) extends BlockId {
  override def name: String = "rdd_" + rddId + "_" + splitIndex
}

// Format of the shuffle block ids (including data and index) should be kept in sync with
// org.apache.spark.network.shuffle.ExternalShuffleBlockResolver#getBlockData().
@DeveloperApi
case class ShuffleBlockId(shuffleId: Int, mapId: Long, reduceId: Int) extends BlockId {
  override def name: String = "shuffle_" + shuffleId + "_" + mapId + "_" + reduceId
}

// The batch id of continuous shuffle blocks of same mapId in range [startReduceId, endReduceId).
@DeveloperApi
case class ShuffleBlockBatchId(
    shuffleId: Int,
    mapId: Long,
    startReduceId: Int,
    endReduceId: Int) extends BlockId {
  override def name: String = {
    "shuffle_" + shuffleId + "_" + mapId + "_" + startReduceId + "_" + endReduceId
  }
}

@Since("3.2.0")
@DeveloperApi
case class ShuffleBlockChunkId(
    shuffleId: Int,
    shuffleMergeId: Int,
    reduceId: Int,
    chunkId: Int) extends BlockId {
  override def name: String =
    "shuffleChunk_" + shuffleId  + "_" + shuffleMergeId + "_" + reduceId + "_" + chunkId
}

@DeveloperApi
case class ShuffleDataBlockId(shuffleId: Int, mapId: Long, reduceId: Int) extends BlockId {
  override def name: String = "shuffle_" + shuffleId + "_" + mapId + "_" + reduceId + ".data"
}

@DeveloperApi
case class ShuffleIndexBlockId(shuffleId: Int, mapId: Long, reduceId: Int) extends BlockId {
  override def name: String = "shuffle_" + shuffleId + "_" + mapId + "_" + reduceId + ".index"
}

@Since("3.2.0")
@DeveloperApi
case class ShuffleChecksumBlockId(shuffleId: Int, mapId: Long, reduceId: Int) extends BlockId {
  override def name: String = "shuffle_" + shuffleId + "_" + mapId + "_" + reduceId + ".checksum"
}

@Since("3.2.0")
@DeveloperApi
case class ShufflePushBlockId(
    shuffleId: Int,
    shuffleMergeId: Int,
    mapIndex: Int,
    reduceId: Int) extends BlockId {
  override def name: String = "shufflePush_" + shuffleId + "_" +
    shuffleMergeId + "_" + mapIndex + "_" + reduceId + ""
}

@Since("3.2.0")
@DeveloperApi
case class ShuffleMergedBlockId(
    shuffleId: Int,
    shuffleMergeId: Int,
    reduceId: Int) extends BlockId {
  override def name: String = "shuffleMerged_" + shuffleId + "_" +
    shuffleMergeId + "_" + reduceId
}

@Since("3.2.0")
@DeveloperApi
case class ShuffleMergedDataBlockId(
    appId: String,
    shuffleId: Int,
    shuffleMergeId: Int,
    reduceId: Int) extends BlockId {
  override def name: String = RemoteBlockPushResolver.MERGED_SHUFFLE_FILE_NAME_PREFIX + "_" +
    appId + "_" + shuffleId + "_" + shuffleMergeId + "_" + reduceId + ".data"
}

@Since("3.2.0")
@DeveloperApi
case class ShuffleMergedIndexBlockId(
    appId: String,
    shuffleId: Int,
    shuffleMergeId: Int,
    reduceId: Int) extends BlockId {
  override def name: String = RemoteBlockPushResolver.MERGED_SHUFFLE_FILE_NAME_PREFIX + "_" +
    appId + "_" + shuffleId + "_" + shuffleMergeId + "_" + reduceId + ".index"
}

@Since("3.2.0")
@DeveloperApi
case class ShuffleMergedMetaBlockId(
    appId: String,
    shuffleId: Int,
    shuffleMergeId: Int,
    reduceId: Int) extends BlockId {
  override def name: String = RemoteBlockPushResolver.MERGED_SHUFFLE_FILE_NAME_PREFIX + "_" +
    appId + "_" + shuffleId + "_" + shuffleMergeId + "_" + reduceId + ".meta"
}

@DeveloperApi
case class BroadcastBlockId(broadcastId: Long, field: String = "") extends BlockId {
  override def name: String = "broadcast_" + broadcastId + (if (field == "") "" else "_" + field)
}

@DeveloperApi
case class TaskResultBlockId(taskId: Long) extends BlockId {
  override def name: String = "taskresult_" + taskId
}

@DeveloperApi
case class StreamBlockId(streamId: Int, uniqueId: Long) extends BlockId {
  override def name: String = "input-" + streamId + "-" + uniqueId
}

@DeveloperApi
case class PythonStreamBlockId(streamId: Int, uniqueId: Long) extends BlockId {
  override def name: String = "python-stream-" + streamId + "-" + uniqueId
}

object LogBlockType extends Enumeration {
  type LogBlockType = Value
  val TEST = Value
  val PYTHON_WORKER = Value
}

/**
 * Identifies a block of log data.
 *
 * @param lastLogTime the timestamp of the last log entry in this block, used for filtering
 *                    and log management.
 * @param executorId the ID of the executor that produced this log block.
 */
abstract sealed class LogBlockId extends BlockId {
  def lastLogTime: Long
  def executorId: String
  def logBlockType: LogBlockType
}

object LogBlockId {
  def empty(logBlockType: LogBlockType): LogBlockId = {
    logBlockType match {
      case LogBlockType.TEST => TestLogBlockId(0L, "")
      case LogBlockType.PYTHON_WORKER => PythonWorkerLogBlockId(0L, "", "", "")
      case _ => throw new SparkException(s"Unsupported log block type: $logBlockType")
    }
  }
}

// Used for test purpose only.
case class TestLogBlockId(lastLogTime: Long, executorId: String)
  extends LogBlockId {
  override def name: String =
    "test_log_" + lastLogTime + "_" + executorId

  override def logBlockType: LogBlockType = LogBlockType.TEST
}

/**
 * Identifies a block of Python worker log data.
 *
 * @param lastLogTime the timestamp of the last log entry in this block, used for filtering
 *                    and log management.
 * @param executorId the ID of the executor that produced this log block.
 * @param sessionId the session ID to isolate the logs.
 * @param workerId the worker ID to distinguish the Python worker process.
 */
@DeveloperApi
case class PythonWorkerLogBlockId(
    lastLogTime: Long,
    executorId: String,
    sessionId: String,
    workerId: String)
  extends LogBlockId {
  override def name: String = {
    s"python_worker_log_${lastLogTime}_${executorId}_${sessionId}_$workerId"
  }

  override def logBlockType: LogBlockType = LogBlockType.PYTHON_WORKER
}

/** Id associated with temporary local data managed as blocks. Not serializable. */
private[spark] case class TempLocalBlockId(id: UUID) extends BlockId {
  override def name: String = "temp_local_" + id
}

/** Id associated with temporary shuffle data managed as blocks. Not serializable. */
private[spark] case class TempShuffleBlockId(id: UUID) extends BlockId {
  override def name: String = "temp_shuffle_" + id
}

// Intended only for testing purposes
private[spark] case class TestBlockId(id: String) extends BlockId {
  override def name: String = "test_" + id
}

@DeveloperApi
class UnrecognizedBlockId(name: String)
    extends SparkException(s"Failed to parse $name into a block ID")

@DeveloperApi
case class CacheId(sessionUUID: String, hash: String) extends BlockId {
  override def name: String = s"cache_${sessionUUID}_$hash"
}

@DeveloperApi
object BlockId {
  val RDD = "rdd_([0-9]+)_([0-9]+)".r
  val SHUFFLE = "shuffle_([0-9]+)_([0-9]+)_([0-9]+)".r
  val SHUFFLE_BATCH = "shuffle_([0-9]+)_([0-9]+)_([0-9]+)_([0-9]+)".r
  val SHUFFLE_DATA = "shuffle_([0-9]+)_([0-9]+)_([0-9]+).data".r
  val SHUFFLE_INDEX = "shuffle_([0-9]+)_([0-9]+)_([0-9]+).index".r
  val SHUFFLE_PUSH = "shufflePush_([0-9]+)_([0-9]+)_([0-9]+)_([0-9]+)".r
  val SHUFFLE_MERGED = "shuffleMerged_([0-9]+)_([0-9]+)_([0-9]+)".r
  val SHUFFLE_MERGED_DATA =
    "shuffleMerged_([_A-Za-z0-9]*)_([0-9]+)_([0-9]+)_([0-9]+).data".r
  val SHUFFLE_MERGED_INDEX =
    "shuffleMerged_([_A-Za-z0-9]*)_([0-9]+)_([0-9]+)_([0-9]+).index".r
  val SHUFFLE_MERGED_META =
    "shuffleMerged_([_A-Za-z0-9]*)_([0-9]+)_([0-9]+)_([0-9]+).meta".r
  val SHUFFLE_CHUNK = "shuffleChunk_([0-9]+)_([0-9]+)_([0-9]+)_([0-9]+)".r
  val BROADCAST = "broadcast_([0-9]+)([_A-Za-z0-9]*)".r
  val TASKRESULT = "taskresult_([0-9]+)".r
  val STREAM = "input-([0-9]+)-([0-9]+)".r
  val PYTHON_STREAM = "python-stream-([0-9]+)-([0-9]+)".r
  val TEMP_LOCAL = "temp_local_([-A-Fa-f0-9]+)".r
  val TEMP_SHUFFLE = "temp_shuffle_([-A-Fa-f0-9]+)".r
  val TEST = "test_(.*)".r
  val TEST_LOG_BLOCK = "test_log_([0-9]+)_(.*)".r
  val PYTHON_WORKER_LOG_BLOCK = "python_worker_log_([0-9]+)_([^_]*)_([^_]*)_([^_]*)".r

  def apply(name: String): BlockId = name match {
    case RDD(rddId, splitIndex) =>
      RDDBlockId(rddId.toInt, splitIndex.toInt)
    case SHUFFLE(shuffleId, mapId, reduceId) =>
      ShuffleBlockId(shuffleId.toInt, mapId.toLong, reduceId.toInt)
    case SHUFFLE_BATCH(shuffleId, mapId, startReduceId, endReduceId) =>
      ShuffleBlockBatchId(shuffleId.toInt, mapId.toLong, startReduceId.toInt, endReduceId.toInt)
    case SHUFFLE_DATA(shuffleId, mapId, reduceId) =>
      ShuffleDataBlockId(shuffleId.toInt, mapId.toLong, reduceId.toInt)
    case SHUFFLE_INDEX(shuffleId, mapId, reduceId) =>
      ShuffleIndexBlockId(shuffleId.toInt, mapId.toLong, reduceId.toInt)
    case SHUFFLE_PUSH(shuffleId, shuffleMergeId, mapIndex, reduceId) =>
      ShufflePushBlockId(shuffleId.toInt, shuffleMergeId.toInt, mapIndex.toInt,
        reduceId.toInt)
    case SHUFFLE_MERGED(shuffleId, shuffleMergeId, reduceId) =>
      ShuffleMergedBlockId(shuffleId.toInt, shuffleMergeId.toInt, reduceId.toInt)
    case SHUFFLE_MERGED_DATA(appId, shuffleId, shuffleMergeId, reduceId) =>
      ShuffleMergedDataBlockId(appId, shuffleId.toInt, shuffleMergeId.toInt, reduceId.toInt)
    case SHUFFLE_MERGED_INDEX(appId, shuffleId, shuffleMergeId, reduceId) =>
      ShuffleMergedIndexBlockId(appId, shuffleId.toInt, shuffleMergeId.toInt,
        reduceId.toInt)
    case SHUFFLE_MERGED_META(appId, shuffleId, shuffleMergeId, reduceId) =>
      ShuffleMergedMetaBlockId(appId, shuffleId.toInt, shuffleMergeId.toInt,
        reduceId.toInt)
    case SHUFFLE_CHUNK(shuffleId, shuffleMergeId, reduceId, chunkId) =>
      ShuffleBlockChunkId(shuffleId.toInt, shuffleMergeId.toInt, reduceId.toInt,
        chunkId.toInt)
    case BROADCAST(broadcastId, field) =>
      BroadcastBlockId(broadcastId.toLong, field.stripPrefix("_"))
    case TASKRESULT(taskId) =>
      TaskResultBlockId(taskId.toLong)
    case STREAM(streamId, uniqueId) =>
      StreamBlockId(streamId.toInt, uniqueId.toLong)
    case PYTHON_STREAM(streamId, uniqueId) =>
      PythonStreamBlockId(streamId.toInt, uniqueId.toLong)
    case TEMP_LOCAL(uuid) =>
      TempLocalBlockId(UUID.fromString(uuid))
    case TEMP_SHUFFLE(uuid) =>
      TempShuffleBlockId(UUID.fromString(uuid))
    case TEST_LOG_BLOCK(lastLogTime, executorId) =>
      TestLogBlockId(lastLogTime.toLong, executorId)
    case PYTHON_WORKER_LOG_BLOCK(lastLogTime, executorId, sessionId, workerId) =>
      PythonWorkerLogBlockId(lastLogTime.toLong, executorId, sessionId, workerId)
    case TEST(value) =>
      TestBlockId(value)
    case _ => throw SparkCoreErrors.unrecognizedBlockIdError(name)
  }
}
