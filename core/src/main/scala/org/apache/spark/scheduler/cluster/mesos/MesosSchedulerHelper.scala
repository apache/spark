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

package org.apache.spark.scheduler.cluster.mesos

import org.apache.mesos.{Scheduler, MesosSchedulerDriver, SchedulerDriver}
import org.apache.mesos.Protos.{TaskState, Resource, FrameworkInfo}
import org.apache.spark.Logging

import java.util.concurrent.CountDownLatch
import java.util.List

import scala.collection.JavaConversions._

private[spark] trait MesosSchedulerHelper extends Logging {
  // Lock used to wait for scheduler to be registered
  final val registerLatch = new CountDownLatch(1)

  // Driver for talking to Mesos
  var driver: SchedulerDriver = null

  def startScheduler(name: String, masterUrl: String, scheduler: Scheduler, fwInfo: FrameworkInfo) {
    synchronized {
      if (driver != null) {
        waitForRegister()
        return
      }

      new Thread(name + " driver") {
        setDaemon(true)

        override def run() {
          driver = new MesosSchedulerDriver(scheduler, fwInfo, masterUrl)
          try {
            val ret = driver.run()
            logInfo("driver.run() returned with code " + ret)
          } catch {
            case e: Exception => logError("driver.run() failed", e)
          }
        }
      }.start()

      waitForRegister()
    }
  }

  private def waitForRegister() {
    registerLatch.await()
  }

  def markRegistered() {
    registerLatch.countDown()
  }

  def getResource(res: List[Resource], name: String): Double = {
    for (r <- res if r.getName == name) {
      return r.getScalar.getValue
    }
    0.0
  }

  /** Check whether a Mesos task state represents a finished task */
  def isFinished(state: TaskState) = {
    state == TaskState.TASK_FINISHED ||
      state == TaskState.TASK_FAILED ||
      state == TaskState.TASK_KILLED ||
      state == TaskState.TASK_LOST ||
      state == TaskState.TASK_ERROR
  }
}
