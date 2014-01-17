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

import scala.collection.mutable.ArrayBuffer
import scala.collection.mutable.HashMap

import org.apache.spark.Logging
import org.apache.spark.scheduler.SchedulingMode.SchedulingMode

/**
 * An Schedulable entity that represent collection of Pools or TaskSetManagers
 */

private[spark] class Pool(
    val poolName: String,
    val schedulingMode: SchedulingMode,
    initMinShare: Int,
    initWeight: Int)
  extends Schedulable
  with Logging {

  var schedulableQueue = new ArrayBuffer[Schedulable]
  var schedulableNameToSchedulable = new HashMap[String, Schedulable]

  var weight = initWeight
  var minShare = initMinShare
  var runningTasks = 0

  var priority = 0

  // A pool's stage id is used to break the tie in scheduling.
  var stageId = -1

  var name = poolName
  var parent: Pool = null

  var taskSetSchedulingAlgorithm: SchedulingAlgorithm = {
    schedulingMode match {
      case SchedulingMode.FAIR =>
        new FairSchedulingAlgorithm()
      case SchedulingMode.FIFO =>
        new FIFOSchedulingAlgorithm()
    }
  }

  override def addSchedulable(schedulable: Schedulable) {
    schedulableQueue += schedulable
    schedulableNameToSchedulable(schedulable.name) = schedulable
    schedulable.parent= this
  }

  override def removeSchedulable(schedulable: Schedulable) {
    schedulableQueue -= schedulable
    schedulableNameToSchedulable -= schedulable.name
  }

  override def getSchedulableByName(schedulableName: String): Schedulable = {
    if (schedulableNameToSchedulable.contains(schedulableName)) {
      return schedulableNameToSchedulable(schedulableName)
    }
    for (schedulable <- schedulableQueue) {
      val sched = schedulable.getSchedulableByName(schedulableName)
      if (sched != null) {
        return sched
      }
    }
    null
  }

  override def executorLost(executorId: String, host: String) {
    schedulableQueue.foreach(_.executorLost(executorId, host))
  }

  override def checkSpeculatableTasks(): Boolean = {
    var shouldRevive = false
    for (schedulable <- schedulableQueue) {
      shouldRevive |= schedulable.checkSpeculatableTasks()
    }
    shouldRevive
  }

  override def getSortedTaskSetQueue(): ArrayBuffer[TaskSetManager] = {
    var sortedTaskSetQueue = new ArrayBuffer[TaskSetManager]
    val sortedSchedulableQueue = schedulableQueue.sortWith(taskSetSchedulingAlgorithm.comparator)
    for (schedulable <- sortedSchedulableQueue) {
      sortedTaskSetQueue ++= schedulable.getSortedTaskSetQueue()
    }
    sortedTaskSetQueue
  }

  def increaseRunningTasks(taskNum: Int) {
    runningTasks += taskNum
    if (parent != null) {
      parent.increaseRunningTasks(taskNum)
    }
  }

  def decreaseRunningTasks(taskNum: Int) {
    runningTasks -= taskNum
    if (parent != null) {
      parent.decreaseRunningTasks(taskNum)
    }
  }
}
