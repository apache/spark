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
package org.apache.spark.sql.streaming.processors

import org.apache.spark.sql.Encoders
import org.apache.spark.sql.streaming.{OutputMode, StatefulProcessorWithInitialState, TimeMode, TimerValues, TTLConfig, ValueState}

/** Test StatefulProcessor implementation that maintains a running count. */
class RunningCountProcessor[T](ttl: TTLConfig = TTLConfig.NONE)
    extends StatefulProcessorWithInitialState[String, T, (String, Long), Long] {

  @transient private var countState: ValueState[Long] = _

  override def init(outputMode: OutputMode, timeMode: TimeMode): Unit = {
    countState = getHandle.getValueState[Long]("count", Encoders.scalaLong, ttl)
  }

  override def handleInitialState(
      key: String,
      initialState: Long,
      timerValues: TimerValues
  ): Unit = {
    countState.update(initialState)
  }

  override def handleInputRows(
      key: String,
      inputRows: Iterator[T],
      timerValues: TimerValues
  ): Iterator[(String, Long)] = {
    val incoming = inputRows.size
    val current = countState.get()
    val updated = current + incoming
    countState.update(updated)
    Iterator.single((key, updated))
  }
}

