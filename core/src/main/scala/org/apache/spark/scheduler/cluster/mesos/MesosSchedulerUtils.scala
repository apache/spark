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

import java.util.List
import java.util.concurrent.CountDownLatch

import org.apache.mesos.Protos.{FrameworkInfo, Resource}
import org.apache.mesos.{MesosSchedulerDriver, Scheduler, SchedulerDriver}
import org.apache.spark.Logging

import scala.collection.JavaConversions._

/**
 * Shared trait for implementing a Mesos Scheduler. This holds common state and helper
 * methods and Mesos scheduler will use.
 */
private[mesos] trait MesosSchedulerUtils extends Logging {
  // Lock used to wait for scheduler to be registered
  final val registerLatch = new CountDownLatch(1)

  // Driver for talking to Mesos
  var driver: SchedulerDriver = null

  /**
   * Starts the MesosSchedulerDriver with the provided information. This method returns
   * until the scheduler has registered with Mesos.
   * @param name Name of the scheduler
   * @param masterUrl Mesos master connection URL
   * @param scheduler Scheduler object
   * @param fwInfo FrameworkInfo to pass to the Mesos master
   */
  def startScheduler(
      name: String,
      masterUrl: String,
      scheduler: Scheduler,
      fwInfo: FrameworkInfo): Unit = {
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

  /**
   * Waits for the scheduler to be registered, which the scheduler will signal by calling
   * markRegistered().
   */
  def waitForRegister(): Unit = {
    registerLatch.await()
  }

  /**
   * Signal that the scheduler has registered with Mesos.
   */
  def markRegistered(): Unit = {
    registerLatch.countDown()
  }

  def getResource(res: List[Resource], name: String): Double = {
    for (r <- res if r.getName == name) {
      return r.getScalar.getValue
    }
    0.0
  }
}
