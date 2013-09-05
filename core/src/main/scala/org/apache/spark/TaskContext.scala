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

import executor.TaskMetrics
import scala.collection.mutable.ArrayBuffer

class TaskContext(
  val stageId: Int,
  val splitId: Int,
  val attemptId: Long,
  val runningLocally: Boolean = false,
  val taskMetrics: TaskMetrics = TaskMetrics.empty()
) extends Serializable {

  @transient val onCompleteCallbacks = new ArrayBuffer[() => Unit]

  // Add a callback function to be executed on task completion. An example use
  // is for HadoopRDD to register a callback to close the input stream.
  def addOnCompleteCallback(f: () => Unit) {
    onCompleteCallbacks += f
  }

  def executeOnCompleteCallbacks() {
    onCompleteCallbacks.foreach{_()}
  }
}
