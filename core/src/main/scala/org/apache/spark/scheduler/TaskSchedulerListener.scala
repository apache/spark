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

import org.apache.spark.scheduler.cluster.TaskInfo
import scala.collection.mutable.Map

import org.apache.spark.TaskEndReason
import org.apache.spark.executor.TaskMetrics

/**
 * Interface for getting events back from the TaskScheduler.
 */
private[spark] trait TaskSchedulerListener {
  // A task has started.
  def taskStarted(task: Task[_], taskInfo: TaskInfo)

  // A task has finished or failed.
  def taskEnded(task: Task[_], reason: TaskEndReason, result: Any, accumUpdates: Map[Long, Any],
                taskInfo: TaskInfo, taskMetrics: TaskMetrics): Unit

  // A node was added to the cluster.
  def executorGained(execId: String, host: String): Unit

  // A node was lost from the cluster.
  def executorLost(execId: String): Unit

  // The TaskScheduler wants to abort an entire task set.
  def taskSetFailed(taskSet: TaskSet, reason: String): Unit
}
