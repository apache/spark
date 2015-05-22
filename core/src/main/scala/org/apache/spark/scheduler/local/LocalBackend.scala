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

import akka.actor.{Actor, ActorRef, Props}

import org.apache.spark.{Logging, SparkContext, SparkEnv, TaskState}
import org.apache.spark.TaskState.TaskState
import org.apache.spark.executor.{Executor, ExecutorBackend}
import org.apache.spark.scheduler.{SchedulerBackend, TaskSchedulerImpl, WorkerOffer}
import org.apache.spark.util.ActorLogReceive

private case class ReviveOffers()

private case class StatusUpdate(taskId: Long, state: TaskState, serializedData: ByteBuffer)

private case class KillTask(taskId: Long, interruptThread: Boolean)

private case class StopExecutor()

/**
 * Calls to LocalBackend are all serialized through LocalActor. Using an actor makes the calls on
 * LocalBackend asynchronous, which is necessary to prevent deadlock between LocalBackend
 * and the TaskSchedulerImpl.
 */
private[spark] class LocalActor(
    scheduler: TaskSchedulerImpl,
    executorBackend: LocalBackend,
    private val totalCores: Int)
  extends Actor with ActorLogReceive with Logging {

  private var freeCores = totalCores

  private val localExecutorId = SparkContext.DRIVER_IDENTIFIER
  private val localExecutorHostname = "localhost"

  private val executor = new Executor(
    localExecutorId, localExecutorHostname, SparkEnv.get, isLocal = true)

  override def receiveWithLogging = {
    case ReviveOffers =>
      reviveOffers()

    case StatusUpdate(taskId, state, serializedData) =>
      scheduler.statusUpdate(taskId, state, serializedData)
      if (TaskState.isFinished(state)) {
        freeCores += scheduler.CPUS_PER_TASK
        reviveOffers()
      }

    case KillTask(taskId, interruptThread) =>
      executor.killTask(taskId, interruptThread)

    case StopExecutor =>
      executor.stop()
  }

  def reviveOffers() {
    val offers = Seq(new WorkerOffer(localExecutorId, localExecutorHostname, freeCores))
    for (task <- scheduler.resourceOffers(offers).flatten) {
      freeCores -= scheduler.CPUS_PER_TASK
      executor.launchTask(executorBackend, taskId = task.taskId, attemptNumber = task.attemptNumber,
        task.name, task.serializedTask)
    }
  }
}

/**
 * LocalBackend is used when running a local version of Spark where the executor, backend, and
 * master all run in the same JVM. It sits behind a TaskSchedulerImpl and handles launching tasks
 * on a single Executor (created by the LocalBackend) running locally.
 */
private[spark] class LocalBackend(scheduler: TaskSchedulerImpl, val totalCores: Int)
  extends SchedulerBackend with ExecutorBackend {

  private val appId = "local-" + System.currentTimeMillis
  var localActor: ActorRef = null

  override def start() {
    localActor = SparkEnv.get.actorSystem.actorOf(
      Props(new LocalActor(scheduler, this, totalCores)),
      "LocalBackendActor")
  }

  override def stop() {
    localActor ! StopExecutor
  }

  override def reviveOffers() {
    localActor ! ReviveOffers
  }

  override def defaultParallelism() =
    scheduler.conf.getInt("spark.default.parallelism", totalCores)

  override def killTask(taskId: Long, executorId: String, interruptThread: Boolean) {
    localActor ! KillTask(taskId, interruptThread)
  }

  override def statusUpdate(taskId: Long, state: TaskState, serializedData: ByteBuffer) {
    localActor ! StatusUpdate(taskId, state, serializedData)
  }

  override def applicationId(): String = appId

}
