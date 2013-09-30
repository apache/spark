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

package org.apache.spark.scheduler

import java.nio.ByteBuffer

import org.apache.spark.TaskState.TaskState

/**
 * Tracks and schedules the tasks within a single TaskSet. This class keeps track of the status of
 * each task and is responsible for retries on failure and locality. The main interfaces to it
 * are resourceOffer, which asks the TaskSet whether it wants to run a task on one node, and
 * statusUpdate, which tells it that one of its tasks changed state (e.g. finished).
 *
 * THREADING: This class is designed to only be called from code with a lock on the TaskScheduler
 * (e.g. its event handlers). It should not be called from other threads.
 */
private[spark] trait TaskSetManager extends Schedulable {
  def schedulableQueue = null
  
  def schedulingMode = SchedulingMode.NONE
  
  def taskSet: TaskSet

  def resourceOffer(
      execId: String,
      host: String,
      availableCpus: Int,
      maxLocality: TaskLocality.TaskLocality)
    : Option[TaskDescription]

  def error(message: String)
}
