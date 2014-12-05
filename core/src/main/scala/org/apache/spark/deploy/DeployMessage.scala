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

package org.apache.spark.deploy

import scala.collection.immutable.List

import org.apache.spark.deploy.ExecutorState.ExecutorState
import org.apache.spark.deploy.master.{ApplicationInfo, DriverInfo, WorkerInfo}
import org.apache.spark.deploy.master.DriverState.DriverState
import org.apache.spark.deploy.master.RecoveryState.MasterState
import org.apache.spark.deploy.worker.{DriverRunner, ExecutorRunner}
import org.apache.spark.util.Utils

private[deploy] sealed trait DeployMessage extends Serializable

/** Contains messages sent between Scheduler actor nodes. */
private[deploy] object DeployMessages {

  // Worker to Master

  case class RegisterWorker(
      id: String,
      host: String,
      port: Int,
      cores: Int,
      memory: Int,
      webUiPort: Int,
      publicAddress: String)
    extends DeployMessage {
    Utils.checkHost(host, "Required hostname")
    assert (port > 0)
  }

  case class ExecutorStateChanged(
      appId: String,
      execId: Int,
      state: ExecutorState,
      message: Option[String],
      exitStatus: Option[Int])
    extends DeployMessage

  case class DriverStateChanged(
      driverId: String,
      state: DriverState,
      exception: Option[Exception])
    extends DeployMessage

  case class WorkerSchedulerStateResponse(id: String, executors: List[ExecutorDescription],
     driverIds: Seq[String])

  case class Heartbeat(workerId: String) extends DeployMessage

  // Master to Worker

  case class RegisteredWorker(masterUrl: String, masterWebUiUrl: String) extends DeployMessage

  case class RegisterWorkerFailed(message: String) extends DeployMessage

  case class ReconnectWorker(masterUrl: String) extends DeployMessage

  case class KillExecutor(masterUrl: String, appId: String, execId: Int) extends DeployMessage

  case class LaunchExecutor(
      masterUrl: String,
      appId: String,
      execId: Int,
      appDesc: ApplicationDescription,
      cores: Int,
      memory: Int)
    extends DeployMessage

  case class LaunchDriver(driverId: String, driverDesc: DriverDescription) extends DeployMessage

  case class KillDriver(driverId: String) extends DeployMessage

  // Worker internal

  case object WorkDirCleanup      // Sent to Worker actor periodically for cleaning up app folders

  case object ReregisterWithMaster // used when a worker attempts to reconnect to a master

  // AppClient to Master

  case class RegisterApplication(appDescription: ApplicationDescription)
    extends DeployMessage

  case class MasterChangeAcknowledged(appId: String)

  // Master to AppClient

  case class RegisteredApplication(appId: String, masterUrl: String) extends DeployMessage

  // TODO(matei): replace hostPort with host
  case class ExecutorAdded(id: Int, workerId: String, hostPort: String, cores: Int, memory: Int) {
    Utils.checkHostPort(hostPort, "Required hostport")
  }

  case class ExecutorUpdated(id: Int, state: ExecutorState, message: Option[String],
    exitStatus: Option[Int])

  case class ApplicationRemoved(message: String)

  // DriverClient <-> Master

  case class RequestSubmitDriver(driverDescription: DriverDescription) extends DeployMessage

  case class SubmitDriverResponse(success: Boolean, driverId: Option[String], message: String)
    extends DeployMessage

  case class RequestKillDriver(driverId: String) extends DeployMessage

  case class KillDriverResponse(driverId: String, success: Boolean, message: String)
    extends DeployMessage

  case class RequestDriverStatus(driverId: String) extends DeployMessage

  case class DriverStatusResponse(found: Boolean, state: Option[DriverState],
    workerId: Option[String], workerHostPort: Option[String], exception: Option[Exception])

  // Internal message in AppClient

  case object StopAppClient

  // Master to Worker & AppClient

  case class MasterChanged(masterUrl: String, masterWebUiUrl: String)

  // MasterWebUI To Master

  case object RequestMasterState

  // Master to MasterWebUI

  case class MasterStateResponse(host: String, port: Int, workers: Array[WorkerInfo],
    activeApps: Array[ApplicationInfo], completedApps: Array[ApplicationInfo],
    activeDrivers: Array[DriverInfo], completedDrivers: Array[DriverInfo],
    status: MasterState) {

    Utils.checkHost(host, "Required hostname")
    assert (port > 0)

    def uri = "spark://" + host + ":" + port
  }

  //  WorkerWebUI to Worker

  case object RequestWorkerState

  // Worker to WorkerWebUI

  case class WorkerStateResponse(host: String, port: Int, workerId: String,
    executors: List[ExecutorRunner], finishedExecutors: List[ExecutorRunner],
    drivers: List[DriverRunner], finishedDrivers: List[DriverRunner], masterUrl: String,
    cores: Int, memory: Int, coresUsed: Int, memoryUsed: Int, masterWebUiUrl: String) {

    Utils.checkHost(host, "Required hostname")
    assert (port > 0)
  }

  // Liveness checks in various places

  case object SendHeartbeat
}
