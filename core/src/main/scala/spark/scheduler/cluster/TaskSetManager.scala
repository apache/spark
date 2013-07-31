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

package spark.scheduler.cluster

import java.nio.ByteBuffer

import spark.TaskState.TaskState
import spark.scheduler.TaskSet

private[spark] trait TaskSetManager extends Schedulable {
  def schedulableQueue = null
  
  def schedulingMode = SchedulingMode.NONE
  
  def taskSet: TaskSet

  def slaveOffer(
      execId: String,
      hostPort: String,
      availableCpus: Double,
      overrideLocality: TaskLocality.TaskLocality = null): Option[TaskDescription]

  def numPendingTasksForHostPort(hostPort: String): Int

  def numRackLocalPendingTasksForHost(hostPort: String): Int

  def numPendingTasksForHost(hostPort: String): Int

  def statusUpdate(tid: Long, state: TaskState, serializedData: ByteBuffer)

  def error(message: String)
}
