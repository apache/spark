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

import org.apache.spark.{SparkContext, SparkEnv, TaskState}
import org.apache.spark.TaskState.TaskState
import org.apache.spark.executor.{Executor, ExecutorBackend}
import org.apache.spark.scheduler.{SchedulerBackend, ClusterScheduler, WorkerOffer}

/**
 * LocalBackend is used when running a local version of Spark where the executor, backend, and
 * master all run in the same JVM. It sits behind a ClusterScheduler and handles launching tasks
 * on a single Executor (created by the LocalBackend) running locally.
 *
 * THREADING: Because methods can be called both from the Executor and the TaskScheduler, and
 * because the Executor class is not thread safe, all methods are synchronized.
 */
private[spark] class LocalBackend(scheduler: ClusterScheduler, private val totalCores: Int)
  extends SchedulerBackend with ExecutorBackend {

  private var freeCores = totalCores

  private val localExecutorId = "localhost"
  private val localExecutorHostname = "localhost"

  val executor = new Executor(localExecutorId, localExecutorHostname, Seq.empty, isLocal = true)

  override def start() {
  }

  override def stop() {
  }

  override def reviveOffers() = synchronized {
   val offers = Seq(new WorkerOffer(localExecutorId, localExecutorHostname, freeCores))
    for (task <- scheduler.resourceOffers(offers).flatten) {
      freeCores -= 1
      executor.launchTask(this, task.taskId, task.serializedTask)
    }
  }

  override def defaultParallelism() = totalCores

  override def killTask(taskId: Long, executorId: String) = synchronized {
    executor.killTask(taskId)
  }

  override def statusUpdate(taskId: Long, state: TaskState, serializedData: ByteBuffer) = synchronized {
    scheduler.statusUpdate(taskId, state, serializedData)
    if (TaskState.isFinished(state)) {
      freeCores += 1
      reviveOffers()
    }
  }
}
