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

package org.apache.spark.scheduler.local

import java.nio.ByteBuffer
import java.util.concurrent.atomic.AtomicInteger

import scala.collection.mutable.{ArrayBuffer, HashMap, HashSet}

import akka.actor._

import org.apache.spark._
import org.apache.spark.TaskState.TaskState
import org.apache.spark.executor.{Executor, ExecutorBackend}
import org.apache.spark.scheduler._
import org.apache.spark.scheduler.SchedulingMode.SchedulingMode


/**
 * A FIFO or Fair TaskScheduler implementation that runs tasks locally in a thread pool. Optionally
 * the scheduler also allows each task to fail up to maxFailures times, which is useful for
 * testing fault recovery.
 */

private[local]
case class LocalReviveOffers()

private[local]
case class LocalStatusUpdate(taskId: Long, state: TaskState, serializedData: ByteBuffer)

private[local]
case class KillTask(taskId: Long)

private[spark]
class LocalActor(localScheduler: LocalScheduler, private var freeCores: Int)
  extends Actor with Logging {

  val executor = new Executor("localhost", "localhost", Seq.empty, isLocal = true)

  def receive = {
    case LocalReviveOffers =>
      launchTask(localScheduler.resourceOffer(freeCores))

    case LocalStatusUpdate(taskId, state, serializeData) =>
      if (TaskState.isFinished(state)) {
        freeCores += 1
        launchTask(localScheduler.resourceOffer(freeCores))
      }

    case KillTask(taskId) =>
      executor.killTask(taskId)
  }

  private def launchTask(tasks: Seq[TaskDescription]) {
    for (task <- tasks) {
      freeCores -= 1
      executor.launchTask(localScheduler, task.taskId, task.serializedTask)
    }
  }
}

private[spark] class LocalScheduler(val threads: Int, val maxFailures: Int, val sc: SparkContext)
  extends TaskScheduler
  with ExecutorBackend
  with Logging {

  val env = SparkEnv.get
  val attemptId = new AtomicInteger
  var dagScheduler: DAGScheduler = null

  // Application dependencies (added through SparkContext) that we've fetched so far on this node.
  // Each map holds the master's timestamp for the version of that file or JAR we got.
  val currentFiles: HashMap[String, Long] = new HashMap[String, Long]()
  val currentJars: HashMap[String, Long] = new HashMap[String, Long]()

  var schedulableBuilder: SchedulableBuilder = null
  var rootPool: Pool = null
  val schedulingMode: SchedulingMode = SchedulingMode.withName(
    System.getProperty("spark.scheduler.mode", "FIFO"))
  val activeTaskSets = new HashMap[String, LocalTaskSetManager]
  val taskIdToTaskSetId = new HashMap[Long, String]
  val taskSetTaskIds = new HashMap[String, HashSet[Long]]

  var localActor: ActorRef = null

  override def start() {
    // temporarily set rootPool name to empty
    rootPool = new Pool("", schedulingMode, 0, 0)
    schedulableBuilder = {
      schedulingMode match {
        case SchedulingMode.FIFO =>
          new FIFOSchedulableBuilder(rootPool)
        case SchedulingMode.FAIR =>
          new FairSchedulableBuilder(rootPool)
      }
    }
    schedulableBuilder.buildPools()

    localActor = env.actorSystem.actorOf(Props(new LocalActor(this, threads)), "Test")
  }

  override def setDAGScheduler(dagScheduler: DAGScheduler) {
    this.dagScheduler = dagScheduler
  }

  override def submitTasks(taskSet: TaskSet) {
    synchronized {
      val manager = new LocalTaskSetManager(this, taskSet)
      schedulableBuilder.addTaskSetManager(manager, manager.taskSet.properties)
      activeTaskSets(taskSet.id) = manager
      taskSetTaskIds(taskSet.id) = new HashSet[Long]()
      localActor ! LocalReviveOffers
    }
  }

  override def cancelTasks(stageId: Int): Unit = synchronized {
    logInfo("Cancelling stage " + stageId)
    logInfo("Cancelling stage " + activeTaskSets.map(_._2.stageId))
    activeTaskSets.find(_._2.stageId == stageId).foreach { case (_, tsm) =>
      // There are two possible cases here:
      // 1. The task set manager has been created and some tasks have been scheduled.
      //    In this case, send a kill signal to the executors to kill the task and then abort
      //    the stage.
      // 2. The task set manager has been created but no tasks has been scheduled. In this case,
      //    simply abort the stage.
      val taskIds = taskSetTaskIds(tsm.taskSet.id)
      if (taskIds.size > 0) {
        taskIds.foreach { tid =>
          localActor ! KillTask(tid)
        }
      }
      logInfo("Stage %d was cancelled".format(stageId))
      taskSetFinished(tsm)
    }
  }

  def resourceOffer(freeCores: Int): Seq[TaskDescription] = {
    synchronized {
      var freeCpuCores = freeCores
      val tasks = new ArrayBuffer[TaskDescription](freeCores)
      val sortedTaskSetQueue = rootPool.getSortedTaskSetQueue()
      for (manager <- sortedTaskSetQueue) {
        logDebug("parentName:%s,name:%s,runningTasks:%s".format(
          manager.parent.name, manager.name, manager.runningTasks))
      }

      var launchTask = false
      for (manager <- sortedTaskSetQueue) {
        do {
          launchTask = false
          manager.resourceOffer(null, null, freeCpuCores, null) match {
            case Some(task) =>
              tasks += task
              taskIdToTaskSetId(task.taskId) = manager.taskSet.id
              taskSetTaskIds(manager.taskSet.id) += task.taskId
              freeCpuCores -= 1
              launchTask = true
            case None => {}
          }
        } while(launchTask)
      }
      return tasks
    }
  }

  def taskSetFinished(manager: TaskSetManager) {
    synchronized {
      activeTaskSets -= manager.taskSet.id
      manager.parent.removeSchedulable(manager)
      logInfo("Remove TaskSet %s from pool %s".format(manager.taskSet.id, manager.parent.name))
      taskIdToTaskSetId --= taskSetTaskIds(manager.taskSet.id)
      taskSetTaskIds -= manager.taskSet.id
    }
  }

  override def statusUpdate(taskId: Long, state: TaskState, serializedData: ByteBuffer) {
    if (TaskState.isFinished(state)) {
      synchronized {
        taskIdToTaskSetId.get(taskId) match {
          case Some(taskSetId) =>
            val taskSetManager = activeTaskSets.get(taskSetId)
            taskSetManager.foreach { tsm =>
              taskSetTaskIds(taskSetId) -= taskId

              state match {
                case TaskState.FINISHED =>
                  tsm.taskEnded(taskId, state, serializedData)
                case TaskState.FAILED =>
                  tsm.taskFailed(taskId, state, serializedData)
                case TaskState.KILLED =>
                  tsm.error("Task %d was killed".format(taskId))
                case _ => {}
              }
            }
          case None =>
            logInfo("Ignoring update from TID " + taskId + " because its task set is gone")
        }
      }
      localActor ! LocalStatusUpdate(taskId, state, serializedData)
    }
  }

  override def stop() {
  }

  override def defaultParallelism() = threads
}
