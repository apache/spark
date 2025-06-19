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

package org.apache.spark.streaming.util

import java.io._

import scala.reflect.ClassTag

import com.esotericsoftware.kryo.{Kryo, KryoSerializable}
import com.esotericsoftware.kryo.io.{Input, Output}

import org.apache.spark.SparkConf
import org.apache.spark.serializer.{KryoInputObjectInputBridge, KryoOutputObjectOutputBridge}
import org.apache.spark.streaming.StreamingConf.SESSION_BY_KEY_DELTA_CHAIN_THRESHOLD
import org.apache.spark.streaming.util.OpenHashMapBasedStateMap._
import org.apache.spark.util.collection.OpenHashMap

/** Internal interface for defining the map that keeps track of sessions. */
private[streaming] abstract class StateMap[K, S] extends Serializable {

  /** Get the state for a key if it exists */
  def get(key: K): Option[S]

  /** Get all the keys and states whose updated time is older than the given threshold time */
  def getByTime(threshUpdatedTime: Long): Iterator[(K, S, Long)]

  /** Get all the keys and states in this map. */
  def getAll(): Iterator[(K, S, Long)]

  /** Add or update state */
  def put(key: K, state: S, updatedTime: Long): Unit

  /** Remove a key */
  def remove(key: K): Unit

  /**
   * Shallow copy `this` map to create a new state map.
   * Updates to the new map should not mutate `this` map.
   */
  def copy(): StateMap[K, S]

  def toDebugString(): String = toString()
}

/** Companion object for [[StateMap]], with utility methods */
private[streaming] object StateMap {
  def empty[K, S]: StateMap[K, S] = new EmptyStateMap[K, S]

  def create[K: ClassTag, S: ClassTag](conf: SparkConf): StateMap[K, S] = {
    val deltaChainThreshold = conf.get(SESSION_BY_KEY_DELTA_CHAIN_THRESHOLD)
    new OpenHashMapBasedStateMap[K, S](deltaChainThreshold)
  }
}

/** Implementation of StateMap interface representing an empty map */
private[streaming] class EmptyStateMap[K, S] extends StateMap[K, S] {
  override def put(key: K, session: S, updateTime: Long): Unit = {
    throw new UnsupportedOperationException("put() should not be called on an EmptyStateMap")
  }
  override def get(key: K): Option[S] = None
  override def getByTime(threshUpdatedTime: Long): Iterator[(K, S, Long)] = Iterator.empty
  override def getAll(): Iterator[(K, S, Long)] = Iterator.empty
  override def copy(): StateMap[K, S] = this
  override def remove(key: K): Unit = { }
  override def toDebugString(): String = ""
}

/** Implementation of StateMap based on Spark's [[org.apache.spark.util.collection.OpenHashMap]] */
private[streaming] class OpenHashMapBasedStateMap[K, S](
    @transient @volatile var parentStateMap: StateMap[K, S],
    private var initialCapacity: Int = DEFAULT_INITIAL_CAPACITY,
    private var deltaChainThreshold: Int = DELTA_CHAIN_LENGTH_THRESHOLD
  )(implicit private var keyClassTag: ClassTag[K], private var stateClassTag: ClassTag[S])
  extends StateMap[K, S] with KryoSerializable { self =>

  def this(initialCapacity: Int, deltaChainThreshold: Int)
      (implicit keyClassTag: ClassTag[K], stateClassTag: ClassTag[S]) = this(
    new EmptyStateMap[K, S],
    initialCapacity = initialCapacity,
    deltaChainThreshold = deltaChainThreshold)

  def this(deltaChainThreshold: Int)
      (implicit keyClassTag: ClassTag[K], stateClassTag: ClassTag[S]) = this(
    initialCapacity = DEFAULT_INITIAL_CAPACITY, deltaChainThreshold = deltaChainThreshold)

  def this()(implicit keyClassTag: ClassTag[K], stateClassTag: ClassTag[S]) = {
    this(DELTA_CHAIN_LENGTH_THRESHOLD)
  }

  require(initialCapacity >= 1, "Invalid initial capacity")
  require(deltaChainThreshold >= 1, "Invalid delta chain threshold")

  @transient @volatile private var deltaMap = new OpenHashMap[K, StateInfo[S]](initialCapacity)

  /** Get the session data if it exists */
  override def get(key: K): Option[S] = {
    val stateInfo = deltaMap(key)
    if (stateInfo != null) {
      if (!stateInfo.deleted) {
        Some(stateInfo.data)
      } else {
        None
      }
    } else {
      parentStateMap.get(key)
    }
  }

  /** Get all the keys and states whose updated time is older than the give threshold time */
  override def getByTime(threshUpdatedTime: Long): Iterator[(K, S, Long)] = {
    val oldStates = parentStateMap.getByTime(threshUpdatedTime).filter { case (key, value, _) =>
      !deltaMap.contains(key)
    }

    val updatedStates = deltaMap.iterator.filter { case (_, stateInfo) =>
      !stateInfo.deleted && stateInfo.updateTime < threshUpdatedTime
    }.map { case (key, stateInfo) =>
      (key, stateInfo.data, stateInfo.updateTime)
    }
    oldStates ++ updatedStates
  }

  /** Get all the keys and states in this map. */
  override def getAll(): Iterator[(K, S, Long)] = {

    val oldStates = parentStateMap.getAll().filter { case (key, _, _) =>
      !deltaMap.contains(key)
    }

    val updatedStates = deltaMap.iterator.filter { ! _._2.deleted }.map { case (key, stateInfo) =>
      (key, stateInfo.data, stateInfo.updateTime)
    }
    oldStates ++ updatedStates
  }

  /** Add or update state */
  override def put(key: K, state: S, updateTime: Long): Unit = {
    val stateInfo = deltaMap(key)
    if (stateInfo != null) {
      stateInfo.update(state, updateTime)
    } else {
      deltaMap.update(key, new StateInfo(state, updateTime))
    }
  }

  /** Remove a state */
  override def remove(key: K): Unit = {
    val stateInfo = deltaMap(key)
    if (stateInfo != null) {
      stateInfo.markDeleted()
    } else {
      val newInfo = new StateInfo[S](deleted = true)
      deltaMap.update(key, newInfo)
    }
  }

  /**
   * Shallow copy the map to create a new session store. Updates to the new map
   * should not mutate `this` map.
   */
  override def copy(): StateMap[K, S] = {
    new OpenHashMapBasedStateMap[K, S](this, deltaChainThreshold = deltaChainThreshold)
  }

  /** Whether the delta chain length is long enough that it should be compacted */
  def shouldCompact: Boolean = {
    deltaChainLength >= deltaChainThreshold
  }

  /** Length of the delta chains of this map */
  def deltaChainLength: Int = parentStateMap match {
    case map: OpenHashMapBasedStateMap[_, _] => map.deltaChainLength + 1
    case _ => 0
  }

  /**
   * Approximate number of keys in the map. This is an overestimation that is mainly used to
   * reserve capacity in a new map at delta compaction time.
   */
  def approxSize: Int = deltaMap.size + {
    parentStateMap match {
      case s: OpenHashMapBasedStateMap[_, _] => s.approxSize
      case _ => 0
    }
  }

  /** Get all the data of this map as string formatted as a tree based on the delta depth */
  override def toDebugString(): String = {
    val tabs = if (deltaChainLength > 0) {
      ("    " * (deltaChainLength - 1)) + "+--- "
    } else ""
    parentStateMap.toDebugString() + "\n" + deltaMap.iterator.mkString(tabs, "\n" + tabs, "")
  }

  override def toString(): String = {
    s"[${System.identityHashCode(this)}, ${System.identityHashCode(parentStateMap)}]"
  }

  /**
   * Serialize the map data. Besides serialization, this method actually compact the deltas
   * (if needed) in a single pass over all the data in the map.
   */
  private def writeObjectInternal(outputStream: ObjectOutput): Unit = {
    // Write the data in the delta of this state map
    outputStream.writeInt(deltaMap.size)
    val deltaMapIterator = deltaMap.iterator
    var deltaMapCount = 0
    while (deltaMapIterator.hasNext) {
      deltaMapCount += 1
      val (key, stateInfo) = deltaMapIterator.next()
      outputStream.writeObject(key)
      outputStream.writeObject(stateInfo)
    }
    assert(deltaMapCount == deltaMap.size)

    // Write the data in the parent state map while copying the data into a new parent map for
    // compaction (if needed)
    val doCompaction = shouldCompact
    val newParentSessionStore = if (doCompaction) {
      val initCapacity = if (approxSize > 0) approxSize else 64
      new OpenHashMapBasedStateMap[K, S](initialCapacity = initCapacity, deltaChainThreshold)
    } else { null }

    val iterOfActiveSessions = parentStateMap.getAll()

    var parentSessionCount = 0

    // First write the approximate size of the data to be written, so that readObject can
    // allocate appropriately sized OpenHashMap.
    outputStream.writeInt(approxSize)

    while (iterOfActiveSessions.hasNext) {
      parentSessionCount += 1

      val (key, state, updateTime) = iterOfActiveSessions.next()
      outputStream.writeObject(key)
      outputStream.writeObject(state)
      outputStream.writeLong(updateTime)

      if (doCompaction) {
        newParentSessionStore.deltaMap.update(
          key, StateInfo(state, updateTime, deleted = false))
      }
    }

    // Write the final limit marking object with the correct count of records written.
    val limiterObj = new LimitMarker(parentSessionCount)
    outputStream.writeObject(limiterObj)
    if (doCompaction) {
      parentStateMap = newParentSessionStore
    }
  }

  /** Deserialize the map data. */
  private def readObjectInternal(inputStream: ObjectInput): Unit = {
    // Read the data of the delta
    val deltaMapSize = inputStream.readInt()
    deltaMap = if (deltaMapSize != 0) {
        new OpenHashMap[K, StateInfo[S]](deltaMapSize)
      } else {
        new OpenHashMap[K, StateInfo[S]](initialCapacity)
      }
    var deltaMapCount = 0
    while (deltaMapCount < deltaMapSize) {
      val key = inputStream.readObject().asInstanceOf[K]
      val sessionInfo = inputStream.readObject().asInstanceOf[StateInfo[S]]
      deltaMap.update(key, sessionInfo)
      deltaMapCount += 1
    }


    // Read the data of the parent map. Keep reading records, until the limiter is reached
    // First read the approximate number of records to expect and allocate properly size
    // OpenHashMap
    val parentStateMapSizeHint = inputStream.readInt()
    val newStateMapInitialCapacity = math.max(parentStateMapSizeHint, DEFAULT_INITIAL_CAPACITY)
    val newParentSessionStore = new OpenHashMapBasedStateMap[K, S](
      initialCapacity = newStateMapInitialCapacity, deltaChainThreshold)

    // Read the records until the limit marking object has been reached
    var parentSessionLoopDone = false
    while (!parentSessionLoopDone) {
      val obj = inputStream.readObject()
      obj match {
        case marker: LimitMarker =>
          parentSessionLoopDone = true
          val expectedCount = marker.num
          assert(expectedCount == newParentSessionStore.deltaMap.size)
        case _ =>
          val key = obj.asInstanceOf[K]
          val state = inputStream.readObject().asInstanceOf[S]
          val updateTime = inputStream.readLong()
          newParentSessionStore.deltaMap.update(
            key, StateInfo(state, updateTime, deleted = false))
      }
    }
    parentStateMap = newParentSessionStore
  }

  private def writeObject(outputStream: ObjectOutputStream): Unit = {
    // Write all the non-transient fields, especially class tags, etc.
    outputStream.defaultWriteObject()
    writeObjectInternal(outputStream)
  }

  private def readObject(inputStream: ObjectInputStream): Unit = {
    // Read the non-transient fields, especially class tags, etc.
    inputStream.defaultReadObject()
    readObjectInternal(inputStream)
  }

  override def write(kryo: Kryo, output: Output): Unit = {
    output.writeInt(initialCapacity)
    output.writeInt(deltaChainThreshold)
    kryo.writeClassAndObject(output, keyClassTag)
    kryo.writeClassAndObject(output, stateClassTag)
    writeObjectInternal(new KryoOutputObjectOutputBridge(kryo, output))
  }

  override def read(kryo: Kryo, input: Input): Unit = {
    initialCapacity = input.readInt()
    deltaChainThreshold = input.readInt()
    keyClassTag = kryo.readClassAndObject(input).asInstanceOf[ClassTag[K]]
    stateClassTag = kryo.readClassAndObject(input).asInstanceOf[ClassTag[S]]
    readObjectInternal(new KryoInputObjectInputBridge(kryo, input))
  }
}

/**
 * Companion object of [[OpenHashMapBasedStateMap]] having associated helper
 * classes and methods
 */
private[streaming] object OpenHashMapBasedStateMap {

  /** Internal class to represent the state information */
  case class StateInfo[S](
      var data: S = null.asInstanceOf[S],
      var updateTime: Long = -1,
      var deleted: Boolean = false) {

    def markDeleted(): Unit = {
      deleted = true
    }

    def update(newData: S, newUpdateTime: Long): Unit = {
      data = newData
      updateTime = newUpdateTime
      deleted = false
    }
  }

  /**
   * Internal class to represent a marker that demarcates the end of all state data in the
   * serialized bytes.
   */
  class LimitMarker(val num: Int) extends Serializable

  val DELTA_CHAIN_LENGTH_THRESHOLD = 20

  val DEFAULT_INITIAL_CAPACITY = 64
}
