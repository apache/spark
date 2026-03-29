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

import org.json4s.JsonAST._
import org.json4s.JsonDSL._

import org.apache.spark.deploy.DeployMessages.{MasterStateResponse, WorkerStateResponse}
import org.apache.spark.deploy.master._
import org.apache.spark.deploy.worker.ExecutorRunner
import org.apache.spark.resource.{ResourceInformation, ResourceRequirement}

private[deploy] object JsonProtocol {

  private def writeResourcesInfo(info: Map[String, ResourceInformation]): JObject = {
    val jsonFields = info.map {
      case (k, v) => JField(k, v.toJson())
    }
    JObject(jsonFields.toList)
  }

  private def writeResourceRequirement(req: ResourceRequirement): JObject = {
    ("name" -> req.resourceName) ~
    ("amount" -> req.amount)
  }

  /**
   * Export the [[WorkerInfo]] to a Json object. A [[WorkerInfo]] consists of the information of a
   * worker.
   *
   * @return a Json object containing the following fields:
   *         `id` a string identifier of the worker
   *         `host` the host that the worker is running on
   *         `port` the port that the worker is bound to
   *         `webuiaddress` the address used in web UI
   *         `cores` total cores of the worker
   *         `coresused` allocated cores of the worker
   *         `coresfree` free cores of the worker
   *         `memory` total memory of the worker
   *         `memoryused` allocated memory of the worker
   *         `memoryfree` free memory of the worker
   *         `resources` total resources of the worker
   *         `resourcesused` allocated resources of the worker
   *         `resourcesfree` free resources of the worker
   *         `state` state of the worker, see [[WorkerState]]
   *         `lastheartbeat` time in milliseconds that the latest heart beat message from the
   *         worker is received
   */
  def writeWorkerInfo(obj: WorkerInfo): JObject = {
    ("id" -> obj.id) ~
    ("host" -> obj.host) ~
    ("port" -> obj.port) ~
    ("webuiaddress" -> obj.webUiAddress) ~
    ("cores" -> obj.cores) ~
    ("coresused" -> obj.coresUsed) ~
    ("coresfree" -> obj.coresFree) ~
    ("memory" -> obj.memory) ~
    ("memoryused" -> obj.memoryUsed) ~
    ("memoryfree" -> obj.memoryFree) ~
    ("resources" -> writeResourcesInfo(obj.resourcesInfo)) ~
    ("resourcesused" -> writeResourcesInfo(obj.resourcesInfoUsed)) ~
    ("resourcesfree" -> writeResourcesInfo(obj.resourcesInfoFree)) ~
    ("state" -> obj.state.toString) ~
    ("lastheartbeat" -> obj.lastHeartbeat)
  }

  /**
   * Export the [[ApplicationInfo]] to a Json object. An [[ApplicationInfo]] consists of the
   * information of an application.
   *
   * @return a Json object containing the following fields:
   *         `id` a string identifier of the application
   *         `starttime` time in milliseconds that the application starts
   *         `name` the description of the application
   *         `cores` total cores granted to the application
   *         `user` name of the user who submitted the application
   *         `memoryperexecutor` minimal memory in MB required to each executor
   *         `resourcesperexecutor` minimal resources required to each executor
   *         `submitdate` time in Date that the application is submitted
   *         `state` state of the application, see [[ApplicationState]]
   *         `duration` time in milliseconds that the application has been running
   * For compatibility also returns the deprecated `memoryperslave` & `resourcesperslave` fields.
   */
  def writeApplicationInfo(obj: ApplicationInfo): JObject = {
    ("id" -> obj.id) ~
    ("starttime" -> obj.startTime) ~
    ("name" -> obj.desc.name) ~
    ("cores" -> obj.coresGranted) ~
    ("user" -> obj.desc.user) ~
    ("memoryperexecutor" -> obj.desc.memoryPerExecutorMB) ~
    ("memoryperslave" -> obj.desc.memoryPerExecutorMB) ~
    ("resourcesperexecutor" -> obj.desc.resourceReqsPerExecutor
      .toList.map(writeResourceRequirement)) ~
    ("resourcesperslave" -> obj.desc.resourceReqsPerExecutor
      .toList.map(writeResourceRequirement)) ~
    ("submitdate" -> obj.submitDate.toString) ~
    ("state" -> obj.state.toString) ~
    ("duration" -> obj.duration)
  }

  /**
   * Export the [[ApplicationDescription]] to a Json object. An [[ApplicationDescription]] consists
   * of the description of an application.
   *
   * @return a Json object containing the following fields:
   *         `name` the description of the application
   *         `cores` max cores that can be allocated to the application, 0 means unlimited
   *         `memoryperexecutor` minimal memory in MB required to each executor
   *         `resourcesperexecutor` minimal resources required to each executor
   *         `user` name of the user who submitted the application
   *         `command` the command string used to submit the application
   * For compatibility also returns the deprecated `memoryperslave` & `resourcesperslave` fields.
   */
  def writeApplicationDescription(obj: ApplicationDescription): JObject = {
    ("name" -> obj.name) ~
    ("cores" -> obj.maxCores.getOrElse(0)) ~
    ("memoryperexecutor" -> obj.memoryPerExecutorMB) ~
    ("resourcesperexecutor" -> obj.resourceReqsPerExecutor.toList.map(writeResourceRequirement)) ~
    ("memoryperslave" -> obj.memoryPerExecutorMB) ~
    ("resourcesperslave" -> obj.resourceReqsPerExecutor.toList.map(writeResourceRequirement)) ~
    ("user" -> obj.user) ~
    ("command" -> obj.command.toString)
  }

  /**
   * Export the [[ExecutorRunner]] to a Json object. An [[ExecutorRunner]] consists of the
   * information of an executor.
   *
   * @return a Json object containing the following fields:
   *         `id` an integer identifier of the executor
   *         `memory` memory in MB allocated to the executor
   *         `resources` resources allocated to the executor
   *         `appid` a string identifier of the application that the executor is working on
   *         `appdesc` a Json object of the [[ApplicationDescription]] of the application that the
   *         executor is working on
   */
  def writeExecutorRunner(obj: ExecutorRunner): JObject = {
    ("id" -> obj.execId) ~
    ("memory" -> obj.memory) ~
    ("resources" -> writeResourcesInfo(obj.resources)) ~
    ("appid" -> obj.appId) ~
    ("appdesc" -> writeApplicationDescription(obj.appDesc))
  }

  /**
   * Export the [[DriverInfo]] to a Json object. A [[DriverInfo]] consists of the information of a
   * driver.
   *
   * @return a Json object containing the following fields:
   *         `id` a string identifier of the driver
   *         `starttime` time in milliseconds that the driver starts
   *         `state` state of the driver, see [[DriverState]]
   *         `cores` cores allocated to the driver
   *         `memory` memory in MB allocated to the driver
   *         `resources` resources allocated to the driver
   *         `submitdate` time in Date that the driver is created
   *         `worker` identifier of the worker that the driver is running on
   *         `mainclass` main class of the command string that started the driver
   */
  def writeDriverInfo(obj: DriverInfo): JObject = {
    ("id" -> obj.id) ~
    ("starttime" -> obj.startTime.toString) ~
    ("state" -> obj.state.toString) ~
    ("cores" -> obj.desc.cores) ~
    ("memory" -> obj.desc.mem) ~
    ("resources" -> writeResourcesInfo(obj.resources)) ~
    ("submitdate" -> obj.submitDate.toString) ~
    ("worker" -> obj.worker.map(_.id).getOrElse("None")) ~
    ("mainclass" -> obj.desc.command.arguments(2))
  }

  /**
   * Export the [[MasterStateResponse]] to a Json object. A [[MasterStateResponse]] consists the
   * information of a master node.
   *
   * @return a Json object containing the following fields if `field` is None:
   *         `url` the url of the master node
   *         `workers` a list of Json objects of [[WorkerInfo]] of the workers allocated to the
   *         master
   *         `aliveworkers` size of alive workers allocated to the master
   *         `cores` total cores available of the master
   *         `coresused` cores used by the master
   *         `memory` total memory available of the master
   *         `memoryused` memory used by the master
   *         `resources` total resources available of the master
   *         `resourcesused` resources used by the master
   *         `activeapps` a list of Json objects of [[ApplicationInfo]] of the active applications
   *         running on the master
   *         `completedapps` a list of Json objects of [[ApplicationInfo]] of the applications
   *         completed in the master
   *         `activedrivers` a list of Json objects of [[DriverInfo]] of the active drivers of the
   *         master
   *         `completeddrivers` a list of Json objects of [[DriverInfo]] of the completed drivers
   *         of the master
   *         `status` status of the master,
   *         see [[org.apache.spark.deploy.master.RecoveryState.MasterState]].
   *         If `field` is not None, the Json object will contain the matched field.
   *         If `field` doesn't match, the Json object `(field -> "")` is returned.
   */
  def writeMasterState(obj: MasterStateResponse, field: Option[String] = None): JObject = {
    val aliveWorkers = obj.workers.filter(_.isAlive())
    field match {
      case None =>
        ("url" -> obj.uri) ~
        ("workers" -> obj.workers.toList.map (writeWorkerInfo) ) ~
        ("aliveworkers" -> aliveWorkers.length) ~
        ("cores" -> aliveWorkers.map (_.cores).sum) ~
        ("coresused" -> aliveWorkers.map (_.coresUsed).sum) ~
        ("memory" -> aliveWorkers.map (_.memory).sum) ~
        ("memoryused" -> aliveWorkers.map (_.memoryUsed).sum) ~
        ("resources" -> aliveWorkers.map (_.resourcesInfo).toList.map (writeResourcesInfo) ) ~
        ("resourcesused" ->
          aliveWorkers.map (_.resourcesInfoUsed).toList.map (writeResourcesInfo) ) ~
        ("activeapps" -> obj.activeApps.toList.map (writeApplicationInfo) ) ~
        ("completedapps" -> obj.completedApps.toList.map (writeApplicationInfo) ) ~
        ("activedrivers" -> obj.activeDrivers.toList.map (writeDriverInfo) ) ~
        ("completeddrivers" -> obj.completedDrivers.toList.map (writeDriverInfo) ) ~
        ("status" -> obj.status.toString)
      case Some(field) =>
        field match {
          case "url" =>
            ("url" -> obj.uri)
          case "workers" =>
            ("workers" -> obj.workers.toList.map (writeWorkerInfo) )
          case "aliveworkers" =>
            ("aliveworkers" -> aliveWorkers.length)
          case "cores" =>
            ("cores" -> aliveWorkers.map (_.cores).sum)
          case "coresused" =>
            ("coresused" -> aliveWorkers.map (_.coresUsed).sum)
          case "memory" =>
            ("memory" -> aliveWorkers.map (_.memory).sum)
          case "memoryused" =>
            ("memoryused" -> aliveWorkers.map (_.memoryUsed).sum)
          case "resources" =>
            ("resources" -> aliveWorkers.map (_.resourcesInfo).toList.map (writeResourcesInfo) )
          case "resourcesused" =>
            ("resourcesused" ->
              aliveWorkers.map (_.resourcesInfoUsed).toList.map (writeResourcesInfo) )
          case "activeapps" =>
            ("activeapps" -> obj.activeApps.toList.map (writeApplicationInfo) )
          case "completedapps" =>
            ("completedapps" -> obj.completedApps.toList.map (writeApplicationInfo) )
          case "activedrivers" =>
            ("activedrivers" -> obj.activeDrivers.toList.map (writeDriverInfo) )
          case "completeddrivers" =>
            ("completeddrivers" -> obj.completedDrivers.toList.map (writeDriverInfo) )
          case "status" =>
            ("status" -> obj.status.toString)
          case field => (field -> "")
        }
    }
  }

  /**
   * Export the [[WorkerStateResponse]] to a Json object. A [[WorkerStateResponse]] consists the
   * information of a worker node.
   *
   * @return a Json object containing the following fields:
   *         `id` a string identifier of the worker node
   *         `masterurl` url of the master node of the worker
   *         `masterwebuiurl` the address used in web UI of the master node of the worker
   *         `cores` total cores of the worker
   *         `coreused` used cores of the worker
   *         `memory` total memory of the worker
   *         `memoryused` used memory of the worker
   *         `resources` total resources of the worker
   *         `resourcesused` used resources of the worker
   *         `executors` a list of Json objects of [[ExecutorRunner]] of the executors running on
   *         the worker
   *         `finishedexecutors` a list of Json objects of [[ExecutorRunner]] of the finished
   *         executors of the worker
   */
  def writeWorkerState(obj: WorkerStateResponse): JObject = {
    ("id" -> obj.workerId) ~
    ("masterurl" -> obj.masterUrl) ~
    ("masterwebuiurl" -> obj.masterWebUiUrl) ~
    ("cores" -> obj.cores) ~
    ("coresused" -> obj.coresUsed) ~
    ("memory" -> obj.memory) ~
    ("memoryused" -> obj.memoryUsed) ~
    ("resources" -> writeResourcesInfo(obj.resources)) ~
    ("resourcesused" -> writeResourcesInfo(obj.resourcesUsed)) ~
    ("executors" -> obj.executors.map(writeExecutorRunner)) ~
    ("finishedexecutors" -> obj.finishedExecutors.map(writeExecutorRunner))
  }

  /**
   * Export the cluster utilization based on the [[MasterStateResponse]] to a Json object.
   */
  def writeClusterUtilization(obj: MasterStateResponse): JObject = {
    val aliveWorkers = obj.workers.filter(_.isAlive())
    val cores = aliveWorkers.map(_.cores).sum
    val coresUsed = aliveWorkers.map(_.coresUsed).sum
    val memory = aliveWorkers.map(_.memory).sum
    val memoryUsed = aliveWorkers.map(_.memoryUsed).sum
    ("waitingDrivers" -> obj.activeDrivers.count(_.state == DriverState.SUBMITTED)) ~
    ("cores" -> cores) ~
    ("coresused" -> coresUsed) ~
    ("coresutilization" -> (if (cores == 0) 100 else 100 * coresUsed / cores)) ~
    ("memory" -> memory) ~
    ("memoryused" -> memoryUsed) ~
    ("memoryutilization" -> (if (memory == 0) 100 else 100 * memoryUsed / memory))
  }
}
