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

import scala.collection.mutable.HashMap

/**
 * Small helper for tracking failed tasks for blacklisting purposes.  Info on all failures on one
 * executor, within one task set.
 */
private[scheduler] class ExecutorFailuresInTaskSet(val node: String) {
  /**
   * Mapping from index of the tasks in the taskset, to the number of times it has failed on this
   * executor.
   */
  val taskToFailureCount = HashMap[Int, Int]()

  def updateWithFailure(taskIndex: Int): Unit = {
    val prevFailureCount = taskToFailureCount.getOrElse(taskIndex, 0)
    taskToFailureCount(taskIndex) = prevFailureCount + 1
  }

  def numUniqueTasksWithFailures: Int = taskToFailureCount.size

  /**
   * Return the number of times this executor has failed on the given task index.
   */
  def getNumTaskFailures(index: Int): Int = {
    taskToFailureCount.getOrElse(index, 0)
  }

  override def toString(): String = {
    s"numUniqueTasksWithFailures = $numUniqueTasksWithFailures; " +
      s"tasksToFailureCount = $taskToFailureCount"
  }
}
