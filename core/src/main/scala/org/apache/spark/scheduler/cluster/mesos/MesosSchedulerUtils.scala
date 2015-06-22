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

import scala.collection.JavaConversions._

import org.apache.mesos.Protos.{FrameworkInfo, Resource, Status}
import org.apache.mesos.{MesosSchedulerDriver, Scheduler}
import org.apache.spark.Logging
import org.apache.spark.util.Utils

/**
 * Shared trait for implementing a Mesos Scheduler. This holds common state and helper
 * methods and Mesos scheduler will use.
 */
private[mesos] trait MesosSchedulerUtils extends Logging {
  // Lock used to wait for scheduler to be registered
  private final val registerLatch = new CountDownLatch(1)

  // Driver for talking to Mesos
  protected var mesosDriver: MesosSchedulerDriver = null

  /**
   * Starts the MesosSchedulerDriver with the provided information. This method returns
   * only after the scheduler has registered with Mesos.
   * @param masterUrl Mesos master connection URL
   * @param scheduler Scheduler object
   * @param fwInfo FrameworkInfo to pass to the Mesos master
   */
  def startScheduler(masterUrl: String, scheduler: Scheduler, fwInfo: FrameworkInfo): Unit = {
    synchronized {
      if (mesosDriver != null) {
        registerLatch.await()
        return
      }

      new Thread(Utils.getFormattedClassName(this) + "-mesos-driver") {
        setDaemon(true)

        override def run() {
          mesosDriver = new MesosSchedulerDriver(scheduler, fwInfo, masterUrl)
          try {
            val ret = mesosDriver.run()
            logInfo("driver.run() returned with code " + ret)
            if (ret.equals(Status.DRIVER_ABORTED)) {
              System.exit(1)
            }
          } catch {
            case e: Exception => {
              logError("driver.run() failed", e)
              System.exit(1)
            }
          }
        }
      }.start()

      registerLatch.await()
    }
  }

  /**
   * Signal that the scheduler has registered with Mesos.
   */
  protected def markRegistered(): Unit = {
    registerLatch.countDown()
  }

  /**
   * Get the amount of resources for the specified type from the resource list
   */
  protected def getResource(res: List[Resource], name: String): Double = {
    for (r <- res if r.getName == name) {
      return r.getScalar.getValue
    }
    0.0
  }
}
