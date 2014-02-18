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

import org.apache.spark.scheduler.JsonSerializable
import org.apache.spark.util.Utils

import net.liftweb.json.JsonDSL._
import net.liftweb.json.JsonAST.JValue
import net.liftweb.json.DefaultFormats

/**
 * Identifies a particular Block of data, usually associated with a single file.
 * A Block can be uniquely identified by its filename, but each type of Block has a different
 * set of keys which produce its unique name.
 *
 * If your BlockId should be serializable, be sure to add it to the BlockId.apply() method.
 */
private[spark] sealed abstract class BlockId extends JsonSerializable {
  /** A globally unique identifier for this Block. Can be used for ser/de. */
  def name: String

  // convenience methods
  def asRDDId = if (isRDD) Some(asInstanceOf[RDDBlockId]) else None
  def isRDD = isInstanceOf[RDDBlockId]
  def isShuffle = isInstanceOf[ShuffleBlockId]
  def isBroadcast = isInstanceOf[BroadcastBlockId] || isInstanceOf[BroadcastHelperBlockId]

  override def toString = name
  override def hashCode = name.hashCode
  override def equals(other: Any): Boolean = other match {
    case o: BlockId => getClass == o.getClass && name.equals(o.name)
    case _ => false
  }

  override def toJson = "Type" -> Utils.getFormattedClassName(this)
}

private[spark] case class RDDBlockId(rddId: Int, splitIndex: Int) extends BlockId {
  def name = "rdd_" + rddId + "_" + splitIndex

  override def toJson = {
    super.toJson ~
    ("RDD ID" -> rddId) ~
    ("Split Index" -> splitIndex)
  }
}

private[spark]
case class ShuffleBlockId(shuffleId: Int, mapId: Int, reduceId: Int) extends BlockId {
  def name = "shuffle_" + shuffleId + "_" + mapId + "_" + reduceId

  override def toJson = {
    super.toJson ~
    ("Shuffle ID" -> shuffleId) ~
    ("Map ID" -> mapId) ~
    ("Reduce ID" -> reduceId)
  }
}

private[spark] case class BroadcastBlockId(broadcastId: Long) extends BlockId {
  def name = "broadcast_" + broadcastId

  override def toJson = {
    super.toJson ~
    ("Broadcast ID" -> broadcastId)
  }
}

private[spark]
case class BroadcastHelperBlockId(broadcastId: BroadcastBlockId, hType: String) extends BlockId {
  def name = broadcastId.name + "_" + hType

  override def toJson = {
    super.toJson ~
    ("Broadcast Block ID" -> broadcastId.toJson) ~
    ("Helper Type" -> hType)
  }
}

private[spark] case class TaskResultBlockId(taskId: Long) extends BlockId {
  def name = "taskresult_" + taskId

  override def toJson = {
    super.toJson ~
    ("Task ID" -> taskId)
  }
}

private[spark] case class StreamBlockId(streamId: Int, uniqueId: Long) extends BlockId {
  def name = "input-" + streamId + "-" + uniqueId

  override def toJson = {
    super.toJson ~
    ("Stream ID" -> streamId) ~
    ("Unique ID" -> uniqueId)
  }
}

/** Id associated with temporary data managed as blocks. Not serializable. */
private[spark] case class TempBlockId(id: UUID) extends BlockId {
  def name = "temp_" + id

  override def toJson = {
    val UUIDJson = Utils.UUIDToJson(id)
    super.toJson ~
    ("Temp ID" -> UUIDJson)
  }
}

// Intended only for testing purposes
private[spark] case class TestBlockId(id: String) extends BlockId {
  def name = "test_" + id

  override def toJson = {
    super.toJson ~
    ("Test ID" -> id)
  }
}

private[spark] object BlockId {
  val RDD = "rdd_([0-9]+)_([0-9]+)".r
  val SHUFFLE = "shuffle_([0-9]+)_([0-9]+)_([0-9]+)".r
  val BROADCAST = "broadcast_([0-9]+)".r
  val BROADCAST_HELPER = "broadcast_([0-9]+)_([A-Za-z0-9]+)".r
  val TASKRESULT = "taskresult_([0-9]+)".r
  val STREAM = "input-([0-9]+)-([0-9]+)".r
  val TEST = "test_(.*)".r

  /** Converts a BlockId "name" String back into a BlockId. */
  def apply(id: String) = id match {
    case RDD(rddId, splitIndex) =>
      RDDBlockId(rddId.toInt, splitIndex.toInt)
    case SHUFFLE(shuffleId, mapId, reduceId) =>
      ShuffleBlockId(shuffleId.toInt, mapId.toInt, reduceId.toInt)
    case BROADCAST(broadcastId) =>
      BroadcastBlockId(broadcastId.toLong)
    case BROADCAST_HELPER(broadcastId, hType) =>
      BroadcastHelperBlockId(BroadcastBlockId(broadcastId.toLong), hType)
    case TASKRESULT(taskId) =>
      TaskResultBlockId(taskId.toLong)
    case STREAM(streamId, uniqueId) =>
      StreamBlockId(streamId.toInt, uniqueId.toLong)
    case TEST(value) =>
      TestBlockId(value)
    case _ =>
      throw new IllegalStateException("Unrecognized BlockId: " + id)
  }

  def fromJson(json: JValue): BlockId = {
    implicit val format = DefaultFormats
    val rddBlockId = Utils.getFormattedClassName(RDDBlockId)
    val shuffleBlockId = Utils.getFormattedClassName(ShuffleBlockId)
    val broadcastBlockId = Utils.getFormattedClassName(BroadcastBlockId)
    val broadcastHelperBlockId = Utils.getFormattedClassName(BroadcastHelperBlockId)
    val taskResultBlockId = Utils.getFormattedClassName(TaskResultBlockId)
    val streamBlockId = Utils.getFormattedClassName(StreamBlockId)
    val tempBlockId = Utils.getFormattedClassName(TempBlockId)
    val testBlockId = Utils.getFormattedClassName(TestBlockId)

    (json \ "Type").extract[String] match {
      case `rddBlockId` => rddBlockIdFromJson(json)
      case `shuffleBlockId` => shuffleBlockIdFromJson(json)
      case `broadcastBlockId` => broadcastBlockIdFromJson(json)
      case `broadcastHelperBlockId` => broadcastHelperBlockIdFromJson(json)
      case `taskResultBlockId` => taskResultBlockIdFromJson(json)
      case `streamBlockId` => streamBlockIdFromJson(json)
      case `tempBlockId` => tempBlockIdFromJson(json)
      case `testBlockId` => testBlockIdFromJson(json)
    }
  }

  private def rddBlockIdFromJson(json: JValue) = {
    implicit val format = DefaultFormats
    new RDDBlockId(
      (json \ "RDD ID").extract[Int],
      (json \ "Split Index").extract[Int])
  }

  private def shuffleBlockIdFromJson(json: JValue) = {
    implicit val format = DefaultFormats
    new ShuffleBlockId(
      (json \ "Shuffle ID").extract[Int],
      (json \ "Map ID").extract[Int],
      (json \ "Reduce ID").extract[Int])
  }

  private def broadcastBlockIdFromJson(json: JValue) = {
    implicit val format = DefaultFormats
    new BroadcastBlockId((json \ "Broadcast ID").extract[Long])
  }

  private def broadcastHelperBlockIdFromJson(json: JValue) = {
    implicit val format = DefaultFormats
    new BroadcastHelperBlockId(
      broadcastBlockIdFromJson(json \ "Broadcast Block ID"),
      (json \ "Helper Type").extract[String])
  }

  private def taskResultBlockIdFromJson(json: JValue) = {
    implicit val format = DefaultFormats
    new TaskResultBlockId((json \ "Task ID").extract[Long])
  }

  private def streamBlockIdFromJson(json: JValue) = {
    implicit val format = DefaultFormats
    new StreamBlockId(
      (json \ "Stream ID").extract[Int],
      (json \ "Unique ID").extract[Long])
  }

  private def tempBlockIdFromJson(json: JValue) = {
    new TempBlockId(Utils.UUIDFromJson(json \ "Temp ID"))
  }

  private def testBlockIdFromJson(json: JValue) = {
    implicit val format = DefaultFormats
    new TestBlockId((json \ "Test ID").extract[String])
  }
}
