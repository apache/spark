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
import org.apache.spark.annotation.DeveloperApi

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
  def isShuffle: Boolean = isInstanceOf[ShuffleBlockId]
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
case class ShuffleBlockId(shuffleId: Int, mapId: Int, reduceId: Int) extends BlockId {
  override def name: String = "shuffle_" + shuffleId + "_" + mapId + "_" + reduceId
}

@DeveloperApi
case class ShuffleDataBlockId(shuffleId: Int, mapId: Int, reduceId: Int) extends BlockId {
  override def name: String = "shuffle_" + shuffleId + "_" + mapId + "_" + reduceId + ".data"
}

@DeveloperApi
case class ShuffleIndexBlockId(shuffleId: Int, mapId: Int, reduceId: Int) extends BlockId {
  override def name: String = "shuffle_" + shuffleId + "_" + mapId + "_" + reduceId + ".index"
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
object BlockId {
  val RDD = "rdd_([0-9]+)_([0-9]+)".r
  val SHUFFLE = "shuffle_([0-9]+)_([0-9]+)_([0-9]+)".r
  val SHUFFLE_DATA = "shuffle_([0-9]+)_([0-9]+)_([0-9]+).data".r
  val SHUFFLE_INDEX = "shuffle_([0-9]+)_([0-9]+)_([0-9]+).index".r
  val BROADCAST = "broadcast_([0-9]+)([_A-Za-z0-9]*)".r
  val TASKRESULT = "taskresult_([0-9]+)".r
  val STREAM = "input-([0-9]+)-([0-9]+)".r
  val TEMP_LOCAL = "temp_local_([-A-Fa-f0-9]+)".r
  val TEMP_SHUFFLE = "temp_shuffle_([-A-Fa-f0-9]+)".r
  val TEST = "test_(.*)".r

  def apply(name: String): BlockId = name match {
    case RDD(rddId, splitIndex) =>
      RDDBlockId(rddId.toInt, splitIndex.toInt)
    case SHUFFLE(shuffleId, mapId, reduceId) =>
      ShuffleBlockId(shuffleId.toInt, mapId.toInt, reduceId.toInt)
    case SHUFFLE_DATA(shuffleId, mapId, reduceId) =>
      ShuffleDataBlockId(shuffleId.toInt, mapId.toInt, reduceId.toInt)
    case SHUFFLE_INDEX(shuffleId, mapId, reduceId) =>
      ShuffleIndexBlockId(shuffleId.toInt, mapId.toInt, reduceId.toInt)
    case BROADCAST(broadcastId, field) =>
      BroadcastBlockId(broadcastId.toLong, field.stripPrefix("_"))
    case TASKRESULT(taskId) =>
      TaskResultBlockId(taskId.toLong)
    case STREAM(streamId, uniqueId) =>
      StreamBlockId(streamId.toInt, uniqueId.toLong)
    case TEMP_LOCAL(uuid) =>
      TempLocalBlockId(UUID.fromString(uuid))
    case TEMP_SHUFFLE(uuid) =>
      TempShuffleBlockId(UUID.fromString(uuid))
    case TEST(value) =>
      TestBlockId(value)
    case _ =>
      throw new UnrecognizedBlockId(name)
  }
}
