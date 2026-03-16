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

import java.util.UUID

import scala.collection.mutable
import scala.reflect.ClassTag

import org.apache.spark.sql.Encoder
import org.apache.spark.sql.execution.streaming.operators.stateful.transformwithstate.statefulprocessor.ImplicitGroupingKeyTracker
import org.apache.spark.sql.execution.streaming.operators.stateful.transformwithstate.statefulprocessor.QueryInfoImpl
import org.apache.spark.sql.streaming.{ListState, MapState, QueryInfo, StatefulProcessorHandle, TimeMode, TTLConfig, ValueState}
import org.apache.spark.util.Clock

/** In-memory implementation of ValueState. */
class InMemoryValueState[T](clock: Clock, ttl: TTLConfig) extends ValueState[T] {
  private val keyToStateValue = mutable.Map[Any, T]()

  private def getValue: Option[T] = {
    keyToStateValue.get(ImplicitGroupingKeyTracker.getImplicitKeyOption.get)
  }
  override def exists(): Boolean = getValue.isDefined

  override def get(): T = getValue.getOrElse(null.asInstanceOf[T])

  override def update(newState: T): Unit =
    keyToStateValue.put(ImplicitGroupingKeyTracker.getImplicitKeyOption.get, newState)

  override def clear(): Unit =
    keyToStateValue.remove(ImplicitGroupingKeyTracker.getImplicitKeyOption.get)
}

/** In-memory implementation of ListState. */
class InMemoryListState[T](clock: Clock, ttl: TTLConfig) extends ListState[T] {
  private val keyToStateValue = mutable.Map[Any, mutable.ArrayBuffer[T]]()

  private def getList: Option[mutable.ArrayBuffer[T]] = {
    keyToStateValue.get(ImplicitGroupingKeyTracker.getImplicitKeyOption.get)
  }

  override def exists(): Boolean = getList.isDefined

  override def get(): Iterator[T] = {
    getList.getOrElse(mutable.ArrayBuffer.empty[T]).iterator
  }

  override def put(newState: Array[T]): Unit = {
    keyToStateValue.put(
      ImplicitGroupingKeyTracker.getImplicitKeyOption.get,
      mutable.ArrayBuffer.empty[T] ++ newState
    )
  }

  override def appendValue(newState: T): Unit = {
    if (!exists()) {
      keyToStateValue.put(
        ImplicitGroupingKeyTracker.getImplicitKeyOption.get,
        mutable.ArrayBuffer.empty[T]
      )
    }
    keyToStateValue(ImplicitGroupingKeyTracker.getImplicitKeyOption.get) += newState
  }

  override def appendList(newState: Array[T]): Unit = {
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

/** In-memory implementation of MapState. */
class InMemoryMapState[K, V](clock: Clock, ttl: TTLConfig) extends MapState[K, V] {
  private val keyToStateValue = mutable.Map[Any, mutable.HashMap[K, V]]()

  private def getMap: Option[mutable.HashMap[K, V]] =
    keyToStateValue.get(ImplicitGroupingKeyTracker.getImplicitKeyOption.get)

  override def exists(): Boolean = getMap.isDefined

  override def getValue(key: K): V = {
    getMap.flatMap(_.get(key)).getOrElse(null.asInstanceOf[V])
  }

  override def containsKey(key: K): Boolean = {
    getMap.exists(_.contains(key))
  }

  override def updateValue(key: K, value: V): Unit = {
    if (!exists()) {
      keyToStateValue.put(
        ImplicitGroupingKeyTracker.getImplicitKeyOption.get,
        mutable.HashMap.empty[K, V]
      )
    }

    keyToStateValue(ImplicitGroupingKeyTracker.getImplicitKeyOption.get) += (key -> value)
  }

  override def iterator(): Iterator[(K, V)] = {
    getMap.map(_.iterator).getOrElse(Iterator.empty)
  }

  override def keys(): Iterator[K] = {
    getMap.map(_.keys.iterator).getOrElse(Iterator.empty)
  }

  override def values(): Iterator[V] = {
    getMap.map(_.values.iterator).getOrElse(Iterator.empty)
  }

  override def removeKey(key: K): Unit = {
    getMap.foreach(_.remove(key))
  }

  override def clear(): Unit = {
    keyToStateValue.remove(ImplicitGroupingKeyTracker.getImplicitKeyOption.get)
  }
}

/** In-memory timers. */
class InMemoryTimers {
  // Maps expiration times to keys.
  // If time t is mapped to key k, there is a timer associated with key k expiring at time t.
  private val timeToKeys = mutable.Map[Long, mutable.HashSet[Any]]()

  def registerTimer(expiryTimestampMs: Long): Unit = {
    val groupingKey = ImplicitGroupingKeyTracker.getImplicitKeyOption.get
    if (!timeToKeys.contains(expiryTimestampMs)) {
      timeToKeys.put(expiryTimestampMs, mutable.HashSet[Any]())
    }
    timeToKeys(expiryTimestampMs).add(groupingKey)
  }

  def deleteTimer(expiryTimestampMs: Long): Unit = {
    val groupingKey = ImplicitGroupingKeyTracker.getImplicitKeyOption.get
    if (timeToKeys.contains(expiryTimestampMs)) {
      timeToKeys(expiryTimestampMs).remove(groupingKey)
      if (timeToKeys(expiryTimestampMs).isEmpty) {
        timeToKeys.remove(expiryTimestampMs)
      }
    }
  }

  def listTimers(): Iterator[Long] = {
    val groupingKey = ImplicitGroupingKeyTracker.getImplicitKeyOption.get
    timeToKeys.iterator.filter(_._2.contains(groupingKey)).map(_._1)
  }

  // Lists pairs (expiryTimestampMs, key) for all timers such that expiryTimestampMs<=currentTimeMs.
  // Result is ordered by timestamp.
  def listExpiredTimers[K](currentTimeMs: Long): List[(Long, K)] = {
    timeToKeys.iterator
      .filter(_._1 <= currentTimeMs)
      .flatMap(entry => entry._2.map(key => (entry._1, key.asInstanceOf[K])))
      .toList
      .sortBy(_._1)
  }
}

/**
 * In-memory implementation of StatefulProcessorHandle for testing purposes.
 *
 * == Internal Implementation ==
 *
 * '''State Storage:''' All state is stored in Scala mutable collections. A central
 * `states: Map[String, Any]` maps state names to their corresponding in-memory state instances
 * (InMemoryValueState, InMemoryListState, or InMemoryMapState). Each state instance internally
 * uses a `Map[Any, T]` where the key is the implicit grouping key obtained from
 * [[ImplicitGroupingKeyTracker]].
 *
 * '''Grouping Key Tracking:''' Operations on state are scoped to the current grouping key,
 * which is retrieved via `ImplicitGroupingKeyTracker.getImplicitKeyOption`. This mirrors the
 * production implementation where state is partitioned by key.
 *
 * '''Timers:''' Managed by [[InMemoryTimers]], which maintains a `Map[Long, Set[Any]]` mapping
 * expiration timestamps to sets of grouping keys. Expired timers can be queried via
 * `listExpiredTimers()`.
 *
 * '''Direct State Access:''' Unlike the production handle, this implementation exposes
 * `peekXxxState` and `updateXxxState` methods for test assertions and setup, allowing
 * direct manipulation of state without going through the processor logic.
 */
class InMemoryStatefulProcessorHandle(timeMode: TimeMode, clock: Clock)
  extends StatefulProcessorHandle {

  val timers = new InMemoryTimers()
  private val states = mutable.Map[String, Any]()
  private val queryInfo = new QueryInfoImpl(UUID.randomUUID(), UUID.randomUUID(), 0L)
  private var currentWatermarkMs: Long = 0L

  /** Updates the current watermark. Used by TwsTester to sync watermark state. */
  def setWatermark(watermarkMs: Long): Unit = {
    currentWatermarkMs = watermarkMs
  }

  override def getValueState[T](
      stateName: String,
      valEncoder: Encoder[T],
      ttlConfig: TTLConfig): ValueState[T] = {
    states
      .getOrElseUpdate(stateName, new InMemoryValueState[T](clock, ttlConfig))
      .asInstanceOf[InMemoryValueState[T]]
  }

  override def getValueState[T: Encoder](stateName: String, ttlConfig: TTLConfig): ValueState[T] =
    getValueState(stateName, implicitly[Encoder[T]], ttlConfig)

  override def getListState[T](
      stateName: String,
      valEncoder: Encoder[T],
      ttlConfig: TTLConfig): ListState[T] = {
    states
      .getOrElseUpdate(stateName, new InMemoryListState[T](clock, ttlConfig))
      .asInstanceOf[InMemoryListState[T]]
  }

  override def getListState[T: Encoder](stateName: String, ttlConfig: TTLConfig): ListState[T] =
    getListState(stateName, implicitly[Encoder[T]], ttlConfig)

  override def getMapState[K, V](
      stateName: String,
      userKeyEnc: Encoder[K],
      valEncoder: Encoder[V],
      ttlConfig: TTLConfig): MapState[K, V] = {
    states
      .getOrElseUpdate(stateName, new InMemoryMapState[K, V](clock, ttlConfig))
      .asInstanceOf[InMemoryMapState[K, V]]
  }

  override def getMapState[K: Encoder, V: Encoder](
      stateName: String,
      ttlConfig: TTLConfig): MapState[K, V] =
    getMapState(stateName, implicitly[Encoder[K]], implicitly[Encoder[V]], ttlConfig)

  override def getQueryInfo(): QueryInfo = queryInfo

  override def registerTimer(expiryTimestampMs: Long): Unit = {
    require(timeMode != TimeMode.None, "Timers are not supported with TimeMode.None.")
    if (timeMode == TimeMode.EventTime()) {
      require(
        expiryTimestampMs > currentWatermarkMs,
        s"Cannot register timer with expiry $expiryTimestampMs ms " +
          s"which is not later than the current watermark $currentWatermarkMs ms."
      )
    }
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

  override def deleteIfExists(stateName: String): Unit = states.remove(stateName)

  def updateValueState[T](stateName: String, value: T): Unit = {
    require(states.contains(stateName), s"State $stateName has not been initialized.")
    states(stateName).asInstanceOf[InMemoryValueState[T]].update(value)
  }

  def peekValueState[T](stateName: String): Option[T] = {
    require(states.contains(stateName), s"State $stateName has not been initialized.")
    Option(states(stateName).asInstanceOf[InMemoryValueState[T]].get())
  }

  def updateListState[T](stateName: String, value: List[T])(implicit ct: ClassTag[T]): Unit = {
    require(states.contains(stateName), s"State $stateName has not been initialized.")
    states(stateName).asInstanceOf[InMemoryListState[T]].put(value.toArray)
  }

  def peekListState[T](stateName: String): List[T] = {
    require(states.contains(stateName), s"State $stateName has not been initialized.")
    states(stateName).asInstanceOf[InMemoryListState[T]].get().toList
  }

  def updateMapState[MK, MV](stateName: String, value: Map[MK, MV]): Unit = {
    require(states.contains(stateName), s"State $stateName has not been initialized.")
    val mapState = states(stateName).asInstanceOf[InMemoryMapState[MK, MV]]
    mapState.clear()
    value.foreach { case (k, v) => mapState.updateValue(k, v) }
  }

  def peekMapState[MK, MV](stateName: String): Map[MK, MV] = {
    require(states.contains(stateName), s"State $stateName has not been initialized.")
    states(stateName).asInstanceOf[InMemoryMapState[MK, MV]].iterator().toMap
  }

  def deleteState(stateName: String): Unit = {
    require(states.contains(stateName), s"State $stateName has not been initialized.")
    states(stateName) match {
      case s: InMemoryValueState[_] => s.clear()
      case s: InMemoryListState[_] => s.clear()
      case s: InMemoryMapState[_, _] => s.clear()
    }
  }
}
