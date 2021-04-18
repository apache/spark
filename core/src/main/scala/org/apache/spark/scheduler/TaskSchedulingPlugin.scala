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

/**
 * This trait provides a plugin interface for suggesting task scheduling to Spark
 * scheduler.
 */
private[spark] trait TaskSchedulingPlugin {

  /**
   * Ranks the given Spark tasks waiting for scheduling for the given executor
   * offer. That is said, the head of returned task indexes points to mostly preferred
   * task to be scheduled on the given executor. Note that the returned is index offsets
   * instead of indexes. For example, if the given task indexes are [1, 2, 3], and the
   * plugin returns [1, 2, 0], it means the ranked task indexes are actually [2, 3, 1].
   *
   * @param tasks The full list of tasks
   * @param taskIndexes The indexes of tasks eligible for scheduling on the executor/host.
   * @return The index offsets of tasks, ranked by the preference of scheduling.
   */
  def rankTasks(
    execId: String, host: String, tasks: Seq[Task[_]], taskIndexes: Seq[Int]): Seq[Int]

  /**
   * Spark scheduler takes the ranks of tasks returned by `rankTasks`. Once
   * the scheduler decides which task to be actually scheduled, it will call
   * this method to inform the plugin. Note that it is possible that the
   * scheduler does not choose top-1 ranked task. The plugin may decide what
   * action is needed if it is happening.
   */
  def informScheduledTask(message: TaskScheduledResult): Unit = {}

  /**
   * This method is called by Spark scheduler when the scheduler resets a task assignment.
   * For barrier task set, if the barrier tasks are partially launched, Spark scheduler will revert
   * the task assignment for the tasks.
   */
  def revokeAssignTask(message: TaskRevokeForSchedule): Unit = {}
}

private[spark] trait TaskScheduledResult {
  def execId: String
  def host: String
  def scheduledTask: Task[_]
  def scheduledTaskIndex: Int
}

private[spark] case class TaskWaitingForSchedule(
  execId: String, host: String, scheduledTask: Task[_], scheduledTaskIndex: Int)
  extends TaskScheduledResult

private[spark] case class TaskRevokeForSchedule(
  execId: String, host: String, scheduledTask: Task[_], scheduledTaskIndex: Int)
  extends TaskScheduledResult
