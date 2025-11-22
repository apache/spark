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
import org.apache.spark.sql.streaming.{ListState, MapState, QueryInfo, StatefulProcessorHandle, TTLConfig, ValueState}

/** In-memory implementation of ValueState. */
class InMemoryValueState[T] extends ValueState[T] {
  private val keyToStateValue = mutable.Map[Any, T]()

  override def exists(): Boolean =
    keyToStateValue.contains(ImplicitGroupingKeyTracker.getImplicitKeyOption.get)

  override def get(): T =
    keyToStateValue.getOrElse(
      ImplicitGroupingKeyTracker.getImplicitKeyOption.get,
      null.asInstanceOf[T]
    )

  override def update(newState: T): Unit =
    keyToStateValue.put(ImplicitGroupingKeyTracker.getImplicitKeyOption.get, newState)

  override def clear(): Unit =
    keyToStateValue.remove(ImplicitGroupingKeyTracker.getImplicitKeyOption.get)
}

/** In-memory implementation of ListState. */
class InMemoryListState[T] extends ListState[T] {
  private val keyToStateValue = mutable.Map[Any, mutable.ArrayBuffer[T]]()

  override def exists(): Boolean =
    keyToStateValue.contains(ImplicitGroupingKeyTracker.getImplicitKeyOption.get)

  private def getList: mutable.ArrayBuffer[T] = {
    if (!exists()) {
      keyToStateValue.put(
        ImplicitGroupingKeyTracker.getImplicitKeyOption.get,
        mutable.ArrayBuffer.empty[T]
      )
    }
    keyToStateValue.get(ImplicitGroupingKeyTracker.getImplicitKeyOption.get).get
  }

  override def get(): Iterator[T] =
    if (exists()) getList.iterator else Iterator.empty

  override def put(newState: Array[T]): Unit =
    keyToStateValue.put(
      ImplicitGroupingKeyTracker.getImplicitKeyOption.get,
      mutable.ArrayBuffer.empty[T] ++ newState
    )

  override def appendValue(newState: T): Unit = getList += newState

  override def appendList(newState: Array[T]): Unit = getList ++= newState

  override def clear(): Unit =
    keyToStateValue.remove(ImplicitGroupingKeyTracker.getImplicitKeyOption.get)
}

/** In-memory implementation of MapState. */
class InMemoryMapState[K, V] extends MapState[K, V] {
  private val keyToStateValue = mutable.Map[Any, mutable.HashMap[K, V]]()

  override def exists(): Boolean =
    keyToStateValue.contains(ImplicitGroupingKeyTracker.getImplicitKeyOption.get)

  private def getMap: mutable.HashMap[K, V] = {
    if (!exists()) {
      keyToStateValue.put(
        ImplicitGroupingKeyTracker.getImplicitKeyOption.get,
        mutable.HashMap.empty[K, V]
      )
    }
    keyToStateValue(ImplicitGroupingKeyTracker.getImplicitKeyOption.get)
  }

  private def getMapIfExists: Option[mutable.HashMap[K, V]] = {
    keyToStateValue.get(ImplicitGroupingKeyTracker.getImplicitKeyOption.get)
  }

  override def getValue(key: K): V =
    getMapIfExists.flatMap(_.get(key)).getOrElse(null.asInstanceOf[V])

  override def containsKey(key: K): Boolean = getMapIfExists.exists(_.contains(key))

  override def updateValue(key: K, value: V): Unit = getMap.put(key, value)

  override def iterator(): Iterator[(K, V)] =
    getMapIfExists.map(_.iterator).getOrElse(Iterator.empty)

  override def keys(): Iterator[K] = getMapIfExists.map(_.keys.iterator).getOrElse(Iterator.empty)

  override def values(): Iterator[V] =
    getMapIfExists.map(_.values.iterator).getOrElse(Iterator.empty)

  override def removeKey(key: K): Unit = getMapIfExists.foreach(_.remove(key))

  override def clear(): Unit =
    keyToStateValue.remove(ImplicitGroupingKeyTracker.getImplicitKeyOption.get)
}

/**
 * In-memory implementation of StatefulProcessorHandle.
 *
 * Doesn't support timers and TTL. Supports directly accessing state.
 */
class InMemoryStatefulProcessorHandle() extends StatefulProcessorHandle {
  private val states = mutable.Map[String, Any]()

  override def getValueState[T](
      stateName: String,
      valEncoder: Encoder[T],
      ttlConfig: TTLConfig
  ): ValueState[T] = {
    states
      .getOrElseUpdate(stateName, new InMemoryValueState[T]())
      .asInstanceOf[InMemoryValueState[T]]
  }

  override def getValueState[T: Encoder](stateName: String, ttlConfig: TTLConfig): ValueState[T] =
    getValueState(stateName, implicitly[Encoder[T]], ttlConfig)

  override def getListState[T](
      stateName: String,
      valEncoder: Encoder[T],
      ttlConfig: TTLConfig
  ): ListState[T] = {
    states
      .getOrElseUpdate(stateName, new InMemoryListState[T]())
      .asInstanceOf[InMemoryListState[T]]
  }

  override def getListState[T: Encoder](stateName: String, ttlConfig: TTLConfig): ListState[T] =
    getListState(stateName, implicitly[Encoder[T]], ttlConfig)

  override def getMapState[K, V](
      stateName: String,
      userKeyEnc: Encoder[K],
      valEncoder: Encoder[V],
      ttlConfig: TTLConfig
  ): MapState[K, V] = {
    states
      .getOrElseUpdate(stateName, new InMemoryMapState[K, V]())
      .asInstanceOf[InMemoryMapState[K, V]]
  }

  override def getMapState[K: Encoder, V: Encoder](
      stateName: String,
      ttlConfig: TTLConfig): MapState[K, V] =
    getMapState(stateName, implicitly[Encoder[K]], implicitly[Encoder[V]], ttlConfig)

  override def getQueryInfo(): QueryInfo =
    new QueryInfoImpl(UUID.randomUUID(), UUID.randomUUID(), 0L)

  override def registerTimer(expiryTimestampMs: Long): Unit =
    throw new UnsupportedOperationException("Timers are not supported.")

  override def deleteTimer(expiryTimestampMs: Long): Unit =
    throw new UnsupportedOperationException("Timers are not supported.")

  override def listTimers(): Iterator[Long] =
    throw new UnsupportedOperationException("Timers are not supported.")

  override def deleteIfExists(stateName: String): Unit = states.remove(stateName)

  def setValueState[T](stateName: String, value: T): Unit = {
    require(states.contains(stateName), s"State $stateName has not been initialized.")
    states(stateName).asInstanceOf[InMemoryValueState[T]].update(value)
  }

  def peekValueState[T](stateName: String): Option[T] = {
    require(states.contains(stateName), s"State $stateName has not been initialized.")
    Option(states(stateName).asInstanceOf[InMemoryValueState[T]].get())
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
}
