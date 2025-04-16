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
package org.apache.spark.sql.execution.streaming

import org.apache.spark.sql.execution.streaming.StatefulProcessorHandleState.PRE_INIT
import org.apache.spark.sql.execution.streaming.state.StateStoreErrors
import org.apache.spark.sql.streaming.{ListState, MapState, ValueState}

trait DriverSideState {
  protected val stateName: String

  protected def throwInitPhaseError(operation: String): Nothing = {
    throw StateStoreErrors.cannotPerformOperationWithInvalidHandleState(
      s"$stateName.$operation", PRE_INIT.toString)
  }
}

class DriverSideValueState[S](override val stateName: String)
  extends ValueState[S] with DriverSideState {
  override def exists(): Boolean = throwInitPhaseError("exists")
  override def get(): S = throwInitPhaseError("get")
  override def update(newState: S): Unit = throwInitPhaseError("update")
  override def clear(): Unit = throwInitPhaseError("clear")
}

class DriverSideListState[S](override val stateName: String)
  extends ListState[S] with DriverSideState {
  override def exists(): Boolean = throwInitPhaseError("exists")
  override def get(): Iterator[S] = throwInitPhaseError("get")
  override def put(newState: Array[S]): Unit = throwInitPhaseError("put")
  override def appendValue(newState: S): Unit = throwInitPhaseError("appendValue")
  override def appendList(newState: Array[S]): Unit = throwInitPhaseError("appendList")
  override def clear(): Unit = throwInitPhaseError("clear")
}

class DriverSideMapState[K, V](override val stateName: String)
  extends MapState[K, V] with DriverSideState {
  override def exists(): Boolean = throwInitPhaseError("exists")
  override def getValue(key: K): V = throwInitPhaseError("getValue")
  override def containsKey(key: K): Boolean = throwInitPhaseError("containsKey")
  override def updateValue(key: K, value: V): Unit = throwInitPhaseError("updateValue")
  override def iterator(): Iterator[(K, V)] = throwInitPhaseError("iterator")
  override def keys(): Iterator[K] = throwInitPhaseError("keys")
  override def values(): Iterator[V] = throwInitPhaseError("values")
  override def removeKey(key: K): Unit = throwInitPhaseError("removeKey")
  override def clear(): Unit = throwInitPhaseError("clear")
}
