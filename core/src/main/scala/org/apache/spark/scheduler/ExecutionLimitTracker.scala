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

import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.atomic.AtomicInteger

import org.apache.spark.annotation.DeveloperApi

@DeveloperApi
object ExecutionLimitTracker {

  // executionId -> running tasks size
  val RUNNING_TASKS = new ConcurrentHashMap[Long, AtomicInteger]()

  val SQL_EXECUTION_ID_KEY = "spark.sql.execution.id"

  val EXECUTION_CORES_LIMIT_NUMBER = "spark.sql.execution.coresLimitNumber"

  def shouldLimit(taskSet: TaskSet): Boolean = {
    if (!hasLegalConfig(taskSet)) {
      return false
    }
    val executionId = taskSet.properties
      .getProperty(SQL_EXECUTION_ID_KEY).toLong
    val runningTasks = RUNNING_TASKS.getOrDefault(executionId, new AtomicInteger(0)).get()
    val restrictNumber = taskSet.properties
      .getProperty(EXECUTION_CORES_LIMIT_NUMBER).toInt
    runningTasks >= restrictNumber
  }

  def increaseRunningTasksIfNeed(taskSet: TaskSet): Unit = {
    if (!hasLegalConfig(taskSet)) {
      return
    }
    val executionId = taskSet.properties
      .getProperty(SQL_EXECUTION_ID_KEY).toLong
    if (!RUNNING_TASKS.containsKey(executionId)) {
      RUNNING_TASKS.put(executionId, new AtomicInteger(0))
    }
    RUNNING_TASKS.get(executionId).incrementAndGet()
  }

  def decreaseRunningTasksIfNeed(taskSet: TaskSet): Unit = {
    if (!hasLegalConfig(taskSet)) {
      return
    }
    val executionId = taskSet.properties
      .getProperty(SQL_EXECUTION_ID_KEY).toLong
    if (!RUNNING_TASKS.containsKey(executionId)) {
      return
    }
    RUNNING_TASKS.get(executionId).decrementAndGet()
  }

  def cleanRunningTasks(executionId: Long): Unit = {
    RUNNING_TASKS.remove(executionId)
  }

  private def hasLegalConfig(taskSet: TaskSet): Boolean = {
    if (taskSet.properties == null) {
      return false
    }
    var legalRestrictNumber = false
    if (taskSet.properties.containsKey(EXECUTION_CORES_LIMIT_NUMBER) &&
      taskSet.properties.getProperty(EXECUTION_CORES_LIMIT_NUMBER).toInt > 0) {
      legalRestrictNumber = true
    }
    taskSet.properties.containsKey(SQL_EXECUTION_ID_KEY) && legalRestrictNumber
  }
}
