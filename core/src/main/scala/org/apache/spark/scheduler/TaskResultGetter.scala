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

import java.nio.ByteBuffer
import java.util.concurrent.{LinkedBlockingDeque, ThreadFactory, ThreadPoolExecutor, TimeUnit}

import org.apache.spark._
import org.apache.spark.TaskState.TaskState
import org.apache.spark.serializer.SerializerInstance
import org.apache.spark.util.Utils

/**
 * Runs a thread pool that deserializes and remotely fetches (if necessary) task results.
 */
private[spark] class TaskResultGetter(sparkEnv: SparkEnv, scheduler: TaskSchedulerImpl)
  extends Logging {

  private val THREADS = sparkEnv.conf.getInt("spark.resultGetter.threads", 4)
  private val getTaskResultExecutor = Utils.newDaemonFixedThreadPool(
    THREADS, "Result resolver thread")

  protected val serializer = new ThreadLocal[SerializerInstance] {
    override def initialValue(): SerializerInstance = {
      sparkEnv.closureSerializer.newInstance()
    }
  }

  def enqueueSuccessfulTask(
    taskSetManager: TaskSetManager, tid: Long, serializedData: ByteBuffer) {
    getTaskResultExecutor.execute(new Runnable {
      override def run() {
        try {
          val result = serializer.get().deserialize[TaskResult[_]](serializedData) match {
            case directResult: DirectTaskResult[_] => directResult
            case IndirectTaskResult(blockId) =>
              logDebug("Fetching indirect task result for TID %s".format(tid))
              scheduler.handleTaskGettingResult(taskSetManager, tid)
              val serializedTaskResult = sparkEnv.blockManager.getRemoteBytes(blockId)
              if (!serializedTaskResult.isDefined) {
                /* We won't be able to get the task result if the machine that ran the task failed
                 * between when the task ended and when we tried to fetch the result, or if the
                 * block manager had to flush the result. */
                scheduler.handleFailedTask(
                  taskSetManager, tid, TaskState.FINISHED, Some(TaskResultLost))
                return
              }
              val deserializedResult = serializer.get().deserialize[DirectTaskResult[_]](
                serializedTaskResult.get)
              sparkEnv.blockManager.master.removeBlock(blockId)
              deserializedResult
          }
          result.metrics.resultSize = serializedData.limit()
          scheduler.handleSuccessfulTask(taskSetManager, tid, result)
        } catch {
          case cnf: ClassNotFoundException =>
            val loader = Thread.currentThread.getContextClassLoader
            taskSetManager.abort("ClassNotFound with classloader: " + loader)
          case ex: Throwable =>
            taskSetManager.abort("Exception while deserializing and fetching task: %s".format(ex))
        }
      }
    })
  }

  def enqueueFailedTask(taskSetManager: TaskSetManager, tid: Long, taskState: TaskState,
    serializedData: ByteBuffer) {
    var reason: Option[TaskEndReason] = None
    getTaskResultExecutor.execute(new Runnable {
      override def run() {
        try {
          if (serializedData != null && serializedData.limit() > 0) {
            reason = Some(serializer.get().deserialize[TaskEndReason](
              serializedData, getClass.getClassLoader))
          }
        } catch {
          case cnd: ClassNotFoundException =>
            // Log an error but keep going here -- the task failed, so not catastropic if we can't
            // deserialize the reason.
            val loader = Thread.currentThread.getContextClassLoader
            logError(
              "Could not deserialize TaskEndReason: ClassNotFound with classloader " + loader)
          case ex: Throwable => {}
        }
        scheduler.handleFailedTask(taskSetManager, tid, taskState, reason)
      }
    })
  }

  def stop() {
    getTaskResultExecutor.shutdownNow()
  }
}
