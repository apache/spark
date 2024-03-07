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

package org.apache.spark.sql.streaming

import java.io.Serializable

import org.apache.spark.annotation.{Evolving, Experimental}
import org.apache.spark.sql.errors.ExecutionErrors

/**
 * Represents the arbitrary stateful logic that needs to be provided by the user to perform
 * stateful manipulations on keyed streams.
 */
@Experimental
@Evolving
private[sql] trait StatefulProcessor[K, I, O] extends Serializable {

  /**
   * Handle to the stateful processor that provides access to the state store and other
   * stateful processing related APIs.
   */
  private var statefulProcessorHandle: StatefulProcessorHandle = null

  /**
   * Function that will be invoked as the first method that allows for users to
   * initialize all their state variables and perform other init actions before handling data.
   * @param outputMode - output mode for the stateful processor
   */
  def init(outputMode: OutputMode): Unit

  /**
   * Function that will allow users to interact with input data rows along with the grouping key
   * and current timer values and optionally provide output rows.
   * @param key - grouping key
   * @param inputRows - iterator of input rows associated with grouping key
   * @param timerValues - instance of TimerValues that provides access to current processing/event
   *                    time if available
   * @return - Zero or more output rows
   */
  def handleInputRows(
      key: K,
      inputRows: Iterator[I],
      timerValues: TimerValues): Iterator[O]

  /**
   * Function called as the last method that allows for users to perform
   * any cleanup or teardown operations.
   */
  def close (): Unit = {}

  /**
   * Function to set the stateful processor handle that will be used to interact with the state
   * store and other stateful processor related operations.
   *
   * @param handle - instance of StatefulProcessorHandle
   */
  final def setHandle(handle: StatefulProcessorHandle): Unit = {
    statefulProcessorHandle = handle
  }

  /**
   * Function to get the stateful processor handle that will be used to interact with the state
   *
   * @return handle - instance of StatefulProcessorHandle
   */
  final def getHandle: StatefulProcessorHandle = {
    if (statefulProcessorHandle == null) {
      throw ExecutionErrors.stateStoreHandleNotInitialized()
    }
    statefulProcessorHandle
  }
}
