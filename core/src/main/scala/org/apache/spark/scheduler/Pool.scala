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

import java.util
import java.util.concurrent.{ConcurrentHashMap, ConcurrentLinkedQueue}

import scala.collection.JavaConverters._
import scala.collection.mutable.ArrayBuffer

import org.apache.spark.internal.Logging
import org.apache.spark.scheduler.SchedulingMode.SchedulingMode

/**
 * A Schedulable entity that represents collection of Pools or TaskSetManagers
 */
private[spark] class Pool(
    val poolName: String,
    val schedulingMode: SchedulingMode,
    initMinShare: Int,
    initWeight: Int)
  extends Schedulable with Logging {

  val schedulableQueue = new ConcurrentLinkedQueue[Schedulable]
  val schedulableNameToSchedulable = new ConcurrentHashMap[String, Schedulable]
  val weight = initWeight
  val minShare = initMinShare
  var runningTasks = 0
  val priority = 0

  // A pool's stage id is used to break the tie in scheduling.
  var stageId = -1
  val name = poolName
  var parent: Pool = null

  private val taskSetSchedulingAlgorithm: SchedulingAlgorithm = {
    schedulingMode match {
      case SchedulingMode.FAIR =>
        new FairSchedulingAlgorithm()
      case SchedulingMode.FIFO =>
        new FIFOSchedulingAlgorithm()
      case _ =>
        val msg = s"Unsupported scheduling mode: $schedulingMode. Use FAIR or FIFO instead."
        throw new IllegalArgumentException(msg)
    }
  }

  override def addSchedulable(schedulable: Schedulable): Unit = {
    require(schedulable != null)
    schedulableQueue.add(schedulable)
    schedulableNameToSchedulable.put(schedulable.name, schedulable)
    schedulable.parent = this
  }

  override def removeSchedulable(schedulable: Schedulable): Unit = {
    schedulableQueue.remove(schedulable)
    schedulableNameToSchedulable.remove(schedulable.name)
  }

  override def getSchedulableByName(schedulableName: String): Schedulable = {
    if (schedulableNameToSchedulable.containsKey(schedulableName)) {
      return schedulableNameToSchedulable.get(schedulableName)
    }
    for (schedulable <- schedulableQueue.asScala) {
      val sched = schedulable.getSchedulableByName(schedulableName)
      if (sched != null) {
        return sched
      }
    }
    null
  }

  override def executorLost(executorId: String, host: String, reason: ExecutorLossReason): Unit = {
    schedulableQueue.asScala.foreach(_.executorLost(executorId, host, reason))
  }

  override def checkSpeculatableTasks(minTimeToSpeculation: Int): Boolean = {
    var shouldRevive = false
    for (schedulable <- schedulableQueue.asScala) {
      shouldRevive |= schedulable.checkSpeculatableTasks(minTimeToSpeculation)
    }
    shouldRevive
  }

  override def getSortedTaskSetQueue: ArrayBuffer[TaskSetManager] = {
    val sortedTaskSetQueue = new ArrayBuffer[TaskSetManager]
    val sortedSchedulableQueue =
      schedulableQueue.asScala.toSeq.sortWith(taskSetSchedulingAlgorithm.comparator)
    for (schedulable <- sortedSchedulableQueue) {
      sortedTaskSetQueue ++= schedulable.getSortedTaskSetQueue
    }
    sortedTaskSetQueue
  }

  def increaseRunningTasks(taskNum: Int): Unit = {
    runningTasks += taskNum
    if (parent != null) {
      parent.increaseRunningTasks(taskNum)
    }
  }

  def decreaseRunningTasks(taskNum: Int): Unit = {
    runningTasks -= taskNum
    if (parent != null) {
      parent.decreaseRunningTasks(taskNum)
    }
  }

  // Update the number of slots considered available for each TaskSetManager whose ancestor
  // in the tree is this pool
  // For FAIR scheduling, slots are distributed among pools based on weights and minshare.
  //   If a pool requires fewer slots than are available to it, the leftover slots are redistributed
  //   to the remaining pools using the remaining pools' weights.
  // For FIFO scheduling, the schedulable queue is iterated over in FIFO order,
  //   giving each schedulable the remaining slots,
  //   up to the number of remaining tasks for that schedulable.
  override def updateAvailableSlots(numSlots: Float): Unit = {
    schedulingMode match {
      case SchedulingMode.FAIR =>
        val queueCopy = new util.LinkedList[Schedulable](schedulableQueue)
        var shouldRedistribute = true
        var totalWeights = schedulableQueue.asScala.map(_.weight).sum
        var totalSlots = numSlots
        while (totalSlots > 0 && shouldRedistribute) {
          shouldRedistribute = false
          var nextWeights = totalWeights
          var nextSlots = totalSlots
          val iterator = queueCopy.iterator()
          while (iterator.hasNext) {
            val schedulable = iterator.next()
            val numTasksRemaining = schedulable.getSortedTaskSetQueue
              .map(tsm => tsm.tasks.length - tsm.tasksSuccessful).sum
            val allocatedSlots = Math.max(
              totalSlots * schedulable.weight / totalWeights,
              schedulable.minShare)
            if (numTasksRemaining < allocatedSlots) {
              schedulable.updateAvailableSlots(numTasksRemaining)
              nextWeights -= schedulable.weight
              nextSlots -= numTasksRemaining
              shouldRedistribute = true
              iterator.remove()
            }
          }
          totalWeights = nextWeights
          totalSlots = nextSlots
        }

        // All schedulables remaining have more or equal remaining tasks than their share of slots,
        // so no need to recalculate remaining tasks. Just give them their total share of slots.
        queueCopy.forEach(schedulable => {
          val allocatedSlots = Math.max(
            totalSlots * schedulable.weight / totalWeights,
            schedulable.minShare)
          schedulable.updateAvailableSlots(allocatedSlots)
        })
      case SchedulingMode.FIFO =>
        val sortedSchedulableQueue =
          schedulableQueue.asScala.toSeq.sortWith(taskSetSchedulingAlgorithm.comparator)
        var remainingSlots = numSlots
        for (schedulable <- sortedSchedulableQueue) {
          if (remainingSlots == 0) {
            schedulable.updateAvailableSlots(0)
          } else {
            val numTasksRemaining = schedulable.getSortedTaskSetQueue
              .map(tsm => tsm.tasks.length - tsm.tasksSuccessful).sum
            val toAssign = Math.min(numTasksRemaining, remainingSlots)
            schedulable.updateAvailableSlots(toAssign)
            remainingSlots -= toAssign
          }
        }
      case _ =>
        val msg = s"Unsupported scheduling mode: $schedulingMode. Use FAIR or FIFO instead."
        throw new IllegalArgumentException(msg)
    }
  }
}
