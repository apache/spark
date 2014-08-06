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

package org.apache.spark

import scala.collection.mutable.ArrayBuffer

import org.apache.spark.annotation.DeveloperApi
import org.apache.spark.executor.TaskMetrics

/**
 * :: DeveloperApi ::
 * Contextual information about a task which can be read or mutated during execution.
 */
@DeveloperApi
class TaskContext(
    val stageId: Int,
    val partitionId: Int,
    val attemptId: Long,
    val runningLocally: Boolean = false,
    private[spark] val taskMetrics: TaskMetrics = TaskMetrics.empty)
  extends Serializable {

  @deprecated("use partitionId", "0.8.1")
  def splitId = partitionId

  // List of callback functions to execute when the task completes.
  @transient private val onCompleteCallbacks = new ArrayBuffer[() => Unit]

  // Whether the corresponding task has been killed.
  @volatile var interrupted: Boolean = false

  // Whether the task has completed, before the onCompleteCallbacks are executed.
  @volatile var completed: Boolean = false

  /**
   * Add a callback function to be executed on task completion. An example use
   * is for HadoopRDD to register a callback to close the input stream.
   * Will be called in any situation - success, failure, or cancellation.
   * @param f Callback function.
   */
  def addOnCompleteCallback(f: () => Unit) {
    onCompleteCallbacks += f
  }

  def executeOnCompleteCallbacks() {
    completed = true
    // Process complete callbacks in the reverse order of registration
    onCompleteCallbacks.reverse.foreach { _() }
  }
}
