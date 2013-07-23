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

package spark.scheduler.local

import java.io.File
import java.util.concurrent.atomic.AtomicInteger
import java.nio.ByteBuffer
import scala.collection.mutable.ArrayBuffer
import scala.collection.mutable.HashMap
import scala.collection.mutable.HashSet

import spark._
import spark.TaskState.TaskState
import spark.scheduler._
import spark.scheduler.cluster._

private[spark] class LocalTaskSetManager(sched: LocalScheduler, val taskSet: TaskSet) extends TaskSetManager with Logging {
  var parent: Schedulable = null
  var weight: Int = 1
  var minShare: Int = 0
  var runningTasks: Int = 0
  var priority: Int = taskSet.priority
  var stageId: Int = taskSet.stageId
  var name: String = "TaskSet_"+taskSet.stageId.toString


  var failCount = new Array[Int](taskSet.tasks.size)
  val taskInfos = new HashMap[Long, TaskInfo]
  val numTasks = taskSet.tasks.size
  var numFinished = 0
  val env = SparkEnv.get
  val ser = env.closureSerializer.newInstance()
  val copiesRunning = new Array[Int](numTasks)
  val finished = new Array[Boolean](numTasks)
  val numFailures = new Array[Int](numTasks)
  val MAX_TASK_FAILURES = sched.maxFailures

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

  def addSchedulable(schedulable: Schedulable): Unit = {
    //nothing
  }

  def removeSchedulable(schedulable: Schedulable): Unit = {
    //nothing
  }

  def getSchedulableByName(name: String): Schedulable = {
    return null
  }

  def executorLost(executorId: String, host: String): Unit = {
    //nothing
  }

  def checkSpeculatableTasks(): Boolean = {
    return true
  }

  def getSortedTaskSetQueue(): ArrayBuffer[TaskSetManager] = {
    var sortedTaskSetQueue = new ArrayBuffer[TaskSetManager]
    sortedTaskSetQueue += this
    return sortedTaskSetQueue
  }

  def hasPendingTasks(): Boolean = {
    return true
  }

  def findTask(): Option[Int] = {
    for (i <- 0 to numTasks-1) {
      if (copiesRunning(i) == 0 && !finished(i)) {
        return Some(i)
      }
    }
    return None
  }

  def slaveOffer(execId: String, hostPort: String, availableCpus: Double, overrideLocality: TaskLocality.TaskLocality = null): Option[TaskDescription] = {
    SparkEnv.set(sched.env)
    logDebug("availableCpus:%d,numFinished:%d,numTasks:%d".format(availableCpus.toInt, numFinished, numTasks))
    if (availableCpus > 0 && numFinished < numTasks) {
      findTask() match {
        case Some(index) =>
          val taskId = sched.attemptId.getAndIncrement()
          val task = taskSet.tasks(index)
          val info = new TaskInfo(taskId, index, System.currentTimeMillis(), "local", "local:1", TaskLocality.NODE_LOCAL)
          taskInfos(taskId) = info
          val bytes = Task.serializeWithDependencies(task, sched.sc.addedFiles, sched.sc.addedJars, ser)
          logInfo("Size of task " + taskId + " is " + bytes.limit + " bytes")
          val taskName = "task %s:%d".format(taskSet.id, index)
          copiesRunning(index) += 1
          increaseRunningTasks(1)
          return Some(new TaskDescription(taskId, null, taskName, bytes))
        case None => {}
      }
    }
    return None
  }

  def numPendingTasksForHostPort(hostPort: String): Int = {
    return 0
  }

  def numRackLocalPendingTasksForHost(hostPort :String): Int = {
    return 0
  }

  def numPendingTasksForHost(hostPort: String): Int = {
    return 0
  }

  def statusUpdate(tid: Long, state: TaskState, serializedData: ByteBuffer) {
    SparkEnv.set(env)
    state match {
      case TaskState.FINISHED =>
        taskEnded(tid, state, serializedData)
      case TaskState.FAILED =>
        taskFailed(tid, state, serializedData)
      case _ => {}
    }
  }

  def taskEnded(tid: Long, state: TaskState, serializedData: ByteBuffer) {
    val info = taskInfos(tid)
    val index = info.index
    val task = taskSet.tasks(index)
    info.markSuccessful()
    val result = ser.deserialize[TaskResult[_]](serializedData, getClass.getClassLoader)
    result.metrics.resultSize = serializedData.limit()
    sched.listener.taskEnded(task, Success, result.value, result.accumUpdates, info, result.metrics)
    numFinished += 1
    decreaseRunningTasks(1)
    finished(index) = true
    if (numFinished == numTasks) {
      sched.taskSetFinished(this)
    }
  }

  def taskFailed(tid: Long, state: TaskState, serializedData: ByteBuffer) {
    val info = taskInfos(tid)
    val index = info.index
    val task = taskSet.tasks(index)
    info.markFailed()
    decreaseRunningTasks(1)
    val reason: ExceptionFailure = ser.deserialize[ExceptionFailure](serializedData, getClass.getClassLoader)
    sched.listener.taskEnded(task, reason, null, null, info, reason.metrics.getOrElse(null))
    if (!finished(index)) {
      copiesRunning(index) -= 1
      numFailures(index) += 1
      val locs = reason.stackTrace.map(loc => "\tat %s".format(loc.toString))
      logInfo("Loss was due to %s\n%s\n%s".format(reason.className, reason.description, locs.mkString("\n")))
      if (numFailures(index) > MAX_TASK_FAILURES) {
        val errorMessage = "Task %s:%d failed more than %d times; aborting job %s".format(taskSet.id, index, 4, reason.description)
        decreaseRunningTasks(runningTasks)
        sched.listener.taskSetFailed(taskSet, errorMessage)
        // need to delete failed Taskset from schedule queue
        sched.taskSetFinished(this)
      }
    }
  }

  def error(message: String) {
  }
}
