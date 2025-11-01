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
package org.apache.spark.sql.execution.streaming.operators.stateful.transformwithstate.testing

import java.time.Clock
import java.time.Instant
import java.util.UUID

import scala.collection.mutable
import scala.reflect.ClassTag

import org.apache.spark.sql.Encoder
import org.apache.spark.sql.catalyst.encoders.ExpressionEncoder
import org.apache.spark.sql.execution.streaming.operators.stateful.transformwithstate.statefulprocessor.ImplicitGroupingKeyTracker
import org.apache.spark.sql.execution.streaming.operators.stateful.transformwithstate.statefulprocessor.QueryInfoImpl
import org.apache.spark.sql.execution.streaming.operators.stateful.transformwithstate.statefulprocessor.StatefulProcessorHandleImplBase
import org.apache.spark.sql.streaming.ListState
import org.apache.spark.sql.streaming.MapState
import org.apache.spark.sql.streaming.QueryInfo
import org.apache.spark.sql.streaming.TimeMode
import org.apache.spark.sql.streaming.TTLConfig
import org.apache.spark.sql.streaming.ValueState

/** Helper to track expired keys. */
class TtlTracker(val clock: Clock, ttl: TTLConfig) {
  require(!ttl.ttlDuration.isNegative())
  private val keyToLastUpdatedTime = mutable.Map[Any, Instant]()

  def isKeyExpired(): Boolean = {
    if (ttl.ttlDuration.isZero()) {
      return false
    }
    val key = ImplicitGroupingKeyTracker.getImplicitKeyOption.get
    if (!keyToLastUpdatedTime.contains(key)) {
      return false
    }
    val expiration: Instant = keyToLastUpdatedTime.get(key).get.plus(ttl.ttlDuration)
    return expiration.isBefore(clock.instant())
  }

  def onKeyUpdated(): Unit = {
    val key = ImplicitGroupingKeyTracker.getImplicitKeyOption.get
    keyToLastUpdatedTime.put(key, clock.instant())
  }
}

class InMemoryValueState[T](clock: Clock, ttl: TTLConfig) extends ValueState[T] {
  private val keyToStateValue = mutable.Map[Any, T]()
  private val ttlTracker = new TtlTracker(clock, ttl)

  private def getValue: Option[T] = {
    if (ttlTracker.isKeyExpired()) {
      return None
    }
    keyToStateValue.get(ImplicitGroupingKeyTracker.getImplicitKeyOption.get)
  }

  override def exists(): Boolean = {
    getValue.isDefined
  }

  override def get(): T = getValue.getOrElse(null.asInstanceOf[T])

  override def update(newState: T): Unit = {
    ttlTracker.onKeyUpdated()
    keyToStateValue.put(ImplicitGroupingKeyTracker.getImplicitKeyOption.get, newState)
  }

  override def clear(): Unit = {
    keyToStateValue.remove(ImplicitGroupingKeyTracker.getImplicitKeyOption.get)
  }
}

class InMemoryListState[T](clock: Clock, ttl: TTLConfig) extends ListState[T] {
  private val keyToStateValue = mutable.Map[Any, mutable.ArrayBuffer[T]]()
  private val ttlTracker = new TtlTracker(clock, ttl)

  private def getList: Option[mutable.ArrayBuffer[T]] = {
    if (ttlTracker.isKeyExpired()) {
      return None
    }
    keyToStateValue.get(ImplicitGroupingKeyTracker.getImplicitKeyOption.get)
  }

  override def exists(): Boolean = getList.isDefined

  override def get(): Iterator[T] = {
    getList.orElse(Some(mutable.ArrayBuffer.empty[T])).get.iterator
  }

  override def put(newState: Array[T]): Unit = {
    ttlTracker.onKeyUpdated()
    keyToStateValue.put(
      ImplicitGroupingKeyTracker.getImplicitKeyOption.get,
      mutable.ArrayBuffer.empty[T] ++ newState
    )
  }

  override def appendValue(newState: T): Unit = {
    ttlTracker.onKeyUpdated()
    if (!exists()) {
      keyToStateValue.put(
        ImplicitGroupingKeyTracker.getImplicitKeyOption.get,
        mutable.ArrayBuffer.empty[T]
      )
    }
    keyToStateValue(ImplicitGroupingKeyTracker.getImplicitKeyOption.get) += newState
  }

  override def appendList(newState: Array[T]): Unit = {
    ttlTracker.onKeyUpdated()
    if (!exists()) {
      keyToStateValue.put(
        ImplicitGroupingKeyTracker.getImplicitKeyOption.get,
        mutable.ArrayBuffer.empty[T]
      )
    }
    keyToStateValue(ImplicitGroupingKeyTracker.getImplicitKeyOption.get) ++= newState
  }

  override def clear(): Unit = {
    keyToStateValue.remove(ImplicitGroupingKeyTracker.getImplicitKeyOption.get)
  }
}

class InMemoryMapState[K, V](clock: Clock, ttl: TTLConfig) extends MapState[K, V] {
  private val keyToStateValue = mutable.Map[Any, mutable.HashMap[K, V]]()
  private val ttlTracker = new TtlTracker(clock, ttl)

  private def getMap: Option[mutable.HashMap[K, V]] = {
    if (ttlTracker.isKeyExpired()) {
      return None
    }
    keyToStateValue.get(ImplicitGroupingKeyTracker.getImplicitKeyOption.get)
  }

  override def exists(): Boolean = getMap.isDefined

  override def getValue(key: K): V = {
    getMap
      .orElse(Some(mutable.HashMap.empty[K, V]))
      .get
      .getOrElse(key, null.asInstanceOf[V])
  }

  override def containsKey(key: K): Boolean = {
    getMap
      .orElse(Some(mutable.HashMap.empty[K, V]))
      .get
      .contains(key)
  }

  override def updateValue(key: K, value: V): Unit = {
    ttlTracker.onKeyUpdated()
    if (!exists()) {
      keyToStateValue.put(
        ImplicitGroupingKeyTracker.getImplicitKeyOption.get,
        mutable.HashMap.empty[K, V]
      )
    }

    keyToStateValue(ImplicitGroupingKeyTracker.getImplicitKeyOption.get) += (key -> value)
  }

  override def iterator(): Iterator[(K, V)] = {
    getMap
      .orElse(Some(mutable.HashMap.empty[K, V]))
      .get
      .iterator
  }

  override def keys(): Iterator[K] = {
    getMap
      .orElse(Some(mutable.HashMap.empty[K, V]))
      .get
      .keys
      .iterator
  }

  override def values(): Iterator[V] = {
    getMap
      .orElse(Some(mutable.HashMap.empty[K, V]))
      .get
      .values
      .iterator
  }

  override def removeKey(key: K): Unit = {
    getMap
      .orElse(Some(mutable.HashMap.empty[K, V]))
      .get
      .remove(key)
  }

  override def clear(): Unit = {
    keyToStateValue.remove(ImplicitGroupingKeyTracker.getImplicitKeyOption.get)
  }
}

class InMemoryTimers {
  private val keyToTimers = mutable.Map[Any, mutable.TreeSet[Long]]()

  def registerTimer(expiryTimestampMs: Long): Unit = {
    val groupingKey = ImplicitGroupingKeyTracker.getImplicitKeyOption.get
    if (!keyToTimers.contains(groupingKey)) {
      keyToTimers.put(groupingKey, mutable.TreeSet[Long]())
    }
    keyToTimers(groupingKey).add(expiryTimestampMs)
  }

  def deleteTimer(expiryTimestampMs: Long): Unit = {
    val groupingKey = ImplicitGroupingKeyTracker.getImplicitKeyOption.get
    if (keyToTimers.contains(groupingKey)) {
      keyToTimers(groupingKey).remove(expiryTimestampMs)
      if (keyToTimers(groupingKey).isEmpty) {
        keyToTimers.remove(groupingKey)
      }
    }
  }

  def listTimers(): Iterator[Long] = {
    val groupingKey = ImplicitGroupingKeyTracker.getImplicitKeyOption.get
    keyToTimers.get(groupingKey) match {
      case Some(timers) => timers.iterator
      case None => Iterator.empty
    }
  }

  def getAllKeysWithTimers(): Iterator[Any] = {
    keyToTimers.keys.iterator
  }
}

class InMemoryStatefulProcessorHandleImpl(
    timeMode: TimeMode,
    keyExprEnc: ExpressionEncoder[Any],
    clock: Clock = Clock.systemUTC())
    extends StatefulProcessorHandleImplBase(timeMode, keyExprEnc) {

  private val states = mutable.Map[String, Any]()

  override def getValueState[T](
      stateName: String,
      valEncoder: Encoder[T],
      ttlConfig: TTLConfig
  ): ValueState[T] = {
    require(!states.contains(stateName), s"State $stateName already defined.")
    states
      .getOrElseUpdate(stateName, new InMemoryValueState[T](clock, ttlConfig))
      .asInstanceOf[InMemoryValueState[T]]
  }

  override def getValueState[T: Encoder](stateName: String, ttlConfig: TTLConfig): ValueState[T] = {
    getValueState(stateName, implicitly[Encoder[T]], ttlConfig)
  }

  override def getListState[T](
      stateName: String,
      valEncoder: Encoder[T],
      ttlConfig: TTLConfig
  ): ListState[T] = {
    require(!states.contains(stateName), s"State $stateName already defined.")
    states
      .getOrElseUpdate(stateName, new InMemoryListState[T](clock, ttlConfig))
      .asInstanceOf[InMemoryListState[T]]
  }

  override def getListState[T: Encoder](stateName: String, ttlConfig: TTLConfig): ListState[T] = {
    getListState(stateName, implicitly[Encoder[T]], ttlConfig)
  }

  override def getMapState[K, V](
      stateName: String,
      userKeyEnc: Encoder[K],
      valEncoder: Encoder[V],
      ttlConfig: TTLConfig
  ): MapState[K, V] = {
    require(!states.contains(stateName), s"State $stateName already defined.")
    states
      .getOrElseUpdate(stateName, new InMemoryMapState[K, V](clock, ttlConfig))
      .asInstanceOf[InMemoryMapState[K, V]]
  }

  override def  getMapState[K: Encoder, V: Encoder](
      stateName: String,
      ttlConfig: TTLConfig
  ): MapState[K, V] = {
    getMapState(stateName, implicitly[Encoder[K]], implicitly[Encoder[V]], ttlConfig)
  }

  override def getQueryInfo(): QueryInfo = {
    new QueryInfoImpl(UUID.randomUUID(), UUID.randomUUID(), 0L)
  }

  private val timers = new InMemoryTimers()

  override def registerTimer(expiryTimestampMs: Long): Unit = {
    require(timeMode != TimeMode.None, "Timers are not supported with TimeMode.None.")
    timers.registerTimer(expiryTimestampMs)
  }

  override def deleteTimer(expiryTimestampMs: Long): Unit = {
    require(timeMode != TimeMode.None, "Timers are not supported with TimeMode.None.")
    timers.deleteTimer(expiryTimestampMs)
  }

  override def listTimers(): Iterator[Long] = {
    require(timeMode != TimeMode.None, "Timers are not supported with TimeMode.None.")
    timers.listTimers()
  }

  override def deleteIfExists(stateName: String): Unit = {
    states.remove(stateName)
  }

  def setValueState[T](stateName: String, value: T): Unit = {
    require(states.contains(stateName), s"State $stateName has not been initialized.")
    states(stateName).asInstanceOf[InMemoryValueState[T]].update(value)
  }

  def peekValueState[T](stateName: String): Option[T] = {
    require(states.contains(stateName), s"State $stateName has not been initialized.")
    val value: T = states(stateName).asInstanceOf[InMemoryValueState[T]].get()
    Option(value)
  }

  def setListState[T](stateName: String, value: List[T])(implicit ct: ClassTag[T]): Unit = {
    require(states.contains(stateName), s"State $stateName has not been initialized.")
    states(stateName).asInstanceOf[InMemoryListState[T]].put(value.toArray)
  }

  def peekListState[T](stateName: String): List[T] = {
    require(states.contains(stateName), s"State $stateName has not been initialized.")
    states(stateName).asInstanceOf[InMemoryListState[T]].get().toList
  }

  def setMapState[MK, MV](stateName: String, value: Map[MK, MV]): Unit = {
    require(states.contains(stateName), s"State $stateName has not been initialized.")
    val mapState = states(stateName).asInstanceOf[InMemoryMapState[MK, MV]]
    mapState.clear()
    value.foreach { case (k, v) => mapState.updateValue(k, v) }
  }

  def peekMapState[MK, MV](stateName: String): Map[MK, MV] = {
    require(states.contains(stateName), s"State $stateName has not been initialized.")
    states(stateName).asInstanceOf[InMemoryMapState[MK, MV]].iterator().toMap
  }

  def getAllKeysWithTimers[K](): Iterator[K] = {
    timers.getAllKeysWithTimers().map(_.asInstanceOf[K])
  }
}
