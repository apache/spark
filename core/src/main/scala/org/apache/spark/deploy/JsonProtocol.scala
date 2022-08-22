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

import scala.collection.JavaConverters._

import com.fasterxml.jackson.databind.JsonNode
import com.fasterxml.jackson.databind.node.ArrayNode

import org.apache.spark.deploy.DeployMessages.{MasterStateResponse, WorkerStateResponse}
import org.apache.spark.deploy.master._
import org.apache.spark.deploy.worker.ExecutorRunner
import org.apache.spark.resource.{ResourceInformation, ResourceRequirement}
import org.apache.spark.util.JacksonUtils

// TODO: toList ? or toSeq
private[deploy] object JsonProtocol {

  private def writeResourcesInfo(info: Map[String, ResourceInformation]): JsonNode = {
    val node = JacksonUtils.createObjectNode
    info.foreach { case (k, v) => node.set[JsonNode](k, v.toJson()) }
    node
  }

  private def writeResourceRequirement(req: ResourceRequirement): JsonNode = {
    val node = JacksonUtils.createObjectNode
    node.put("name", req.resourceName)
    node.put("amount", req.amount)
    node
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
  def writeWorkerInfo(obj: WorkerInfo): JsonNode = {
    val node = JacksonUtils.createObjectNode
    node.put("id", obj.id)
    node.put("host", obj.host)
    node.put("port", obj.port)
    node.put("webuiaddress", obj.webUiAddress)
    node.put("cores", obj.cores)
    node.put("coresused", obj.coresUsed)
    node.put("coresfree", obj.coresFree)
    node.put("memory", obj.memory)
    node.put("memoryused", obj.memoryUsed)
    node.put("memoryfree", obj.memoryFree)
    node.set[JsonNode]("resources", writeResourcesInfo(obj.resourcesInfo))
    node.set[JsonNode]("resourcesused", writeResourcesInfo(obj.resourcesInfoUsed))
    node.set[JsonNode]("resourcesfree", writeResourcesInfo(obj.resourcesInfoFree))
    node.put("state", obj.state.toString)
    node.put("lastheartbeat", obj.lastHeartbeat)
    node
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
  def writeApplicationInfo(obj: ApplicationInfo): JsonNode = {
    val node = JacksonUtils.createObjectNode
    node.put("id", obj.id)
    node.put("starttime", obj.startTime)
    node.put("name", obj.desc.name)
    node.put("cores", obj.coresGranted)
    node.put("user", obj.desc.user)
    node.put("memoryperexecutor", obj.desc.memoryPerExecutorMB)
    node.put("memoryperslave", obj.desc.memoryPerExecutorMB)

    val resourceReqsPerExecutor = obj.desc.resourceReqsPerExecutor.toList
      .map(r => writeResourceRequirement(r)).asJava
    val resourcePerExecutorArray =
      JacksonUtils.createArrayNode(resourceReqsPerExecutor.size())
    resourcePerExecutorArray.addAll(resourceReqsPerExecutor)
    node.set[JsonNode]("resourcesperexecutor", resourcePerExecutorArray)
    node.set[JsonNode]("resourcesperslave", resourcePerExecutorArray)

    node.put("submitdate", obj.submitDate.toString)
    node.put("state", obj.state.toString)
    node.put("duration", obj.duration)
    node
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
  def writeApplicationDescription(obj: ApplicationDescription): JsonNode = {
    val node = JacksonUtils.createObjectNode

    node.put("name", obj.name)
    node.put("cores", obj.maxCores.getOrElse(0))
    node.put("memoryperexecutor", obj.memoryPerExecutorMB)

    val resourceReqsPerExecutor = obj.resourceReqsPerExecutor.toList
      .map(r => writeResourceRequirement(r)).asJava
    val resourcePerExecutorArray =
      JacksonUtils.createArrayNode(resourceReqsPerExecutor.size())
    resourcePerExecutorArray.addAll(resourceReqsPerExecutor)
    node.set[JsonNode]("resourcesperexecutor", resourcePerExecutorArray)

    node.put("memoryperslave", obj.memoryPerExecutorMB)

    node.set[JsonNode]("resourcesperslave", resourcePerExecutorArray)

    node.put("user", obj.user)
    node.put("command", obj.command.toString)

    node
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
  def writeExecutorRunner(obj: ExecutorRunner): JsonNode = {
    val node = JacksonUtils.createObjectNode
    node.put("id", obj.execId)
    node.put("memory", obj.memory)
    node.set[JsonNode]("resources", writeResourcesInfo(obj.resources))
    node.put("appid", obj.appId)
    node.set[JsonNode]("appdesc", writeApplicationDescription(obj.appDesc))
    node
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
  def writeDriverInfo(obj: DriverInfo): JsonNode = {
    val node = JacksonUtils.createObjectNode
    node.put("id", obj.id)
    node.put("starttime", obj.startTime.toString)
    node.put("state", obj.state.toString)
    node.put("cores", obj.desc.cores)
    node.put("memory", obj.desc.mem)
    node.set("resources", writeResourcesInfo(obj.resources))
    node.put("submitdate", obj.submitDate.toString)
    node.put("worker", obj.worker.map(_.id).getOrElse("None"))
    node.put("mainclass", obj.desc.command.arguments(2))
    node
  }

  /**
   * Export the [[MasterStateResponse]] to a Json object. A [[MasterStateResponse]] consists the
   * information of a master node.
   *
   * @return a Json object containing the following fields:
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
   *         see [[org.apache.spark.deploy.master.RecoveryState.MasterState]]
   */
  def writeMasterState(obj: MasterStateResponse): JsonNode = {
    val aliveWorkers = obj.workers.filter(_.isAlive())
    val node = JacksonUtils.createObjectNode
    node.put("url", obj.uri)

    val workers =
      obj.workers.toList.map(w => writeWorkerInfo(w)).asJava
    val workersArray = JacksonUtils.createArrayNode(workers.size())
    workersArray.addAll(workers)
    node.set[JsonNode]("workers", workersArray)

    node.put("aliveworkers", aliveWorkers.length)
    node.put("cores", aliveWorkers.map(_.cores).sum)
    node.put("coresused", aliveWorkers.map(_.coresUsed).sum)
    node.put("memory", aliveWorkers.map(_.memory).sum)
    node.put("memoryused", aliveWorkers.map(_.memoryUsed).sum)

    val resources =
      aliveWorkers.map(_.resourcesInfo).toList.map(r => writeResourcesInfo(r)).asJava
    val resourcesArray = JacksonUtils.createArrayNode(resources.size())
    resourcesArray.addAll(resources)
    node.set[JsonNode]("resources", resourcesArray)


    val resourcesUsed =
      aliveWorkers.map(_.resourcesInfoUsed).toList.map(r => writeResourcesInfo(r)).asJava
    val resourcesUsedArray = JacksonUtils.createArrayNode(resourcesUsed.size())
    resourcesUsedArray.addAll(resourcesUsed)
    node.set[JsonNode]("resourcesused", resourcesUsedArray)

    val activeApps = obj.activeApps.toList.map(r => writeApplicationInfo(r)).asJava
    val activeAppsArray = JacksonUtils.createArrayNode(activeApps.size())
    activeAppsArray.addAll(activeApps)
    node.set[JsonNode]("activeapps", activeAppsArray)

    val completedApps = obj.completedApps.toList.map(r => writeApplicationInfo(r)).asJava
    val completedAppsArray = JacksonUtils.createArrayNode(completedApps.size())
    completedAppsArray.addAll(completedApps)
    node.set[JsonNode]("completedapps", completedAppsArray)

    val activeDrivers = obj.activeDrivers.toList.map(r => writeDriverInfo(r)).asJava
    val activeDriversArray = JacksonUtils.createArrayNode(activeDrivers.size())
    activeDriversArray.addAll(activeDrivers)
    node.set[JsonNode]("activedrivers", activeDriversArray)

    val completedDrivers = obj.completedDrivers.toList.map(r => writeDriverInfo(r)).asJava
    val completedDriversArray = JacksonUtils.createArrayNode(completedDrivers.size())
    completedDriversArray.addAll(completedDrivers)
    node.set[JsonNode]("completeddrivers", completedDriversArray)

    node.put("status", obj.status.toString)
    node
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
  def writeWorkerState(obj: WorkerStateResponse): JsonNode = {
    val node = JacksonUtils.createObjectNode
    node.put("id", obj.workerId)
    node.put("masterurl", obj.masterUrl)
    node.put("masterwebuiurl", obj.masterWebUiUrl)
    node.put("cores", obj.cores)
    node.put("coresused", obj.coresUsed)
    node.put("memory", obj.memory)
    node.put("memoryused", obj.memoryUsed)

    node.set[JsonNode]("resources", writeResourcesInfo(obj.resources))
    node.set[JsonNode]("resourcesused", writeResourcesInfo(obj.resourcesUsed))

    val executors = obj.executors.map(r => writeExecutorRunner(r)).asJava
    val executorsArray = JacksonUtils.createArrayNode(executors.size())
    executorsArray.addAll(executors)
    node.set[JsonNode]("executors", executorsArray)

    val finishedExecutorsArray =
      buildArrayNode(obj.finishedExecutors.map(r => writeExecutorRunner(r)))
    node.set[JsonNode]("finishedexecutors", finishedExecutorsArray)

    node
  }

  private def buildArrayNode(data: List[JsonNode]): ArrayNode = {
    val node = JacksonUtils.createArrayNode(data.length)
    node.addAll(data.asJava)
    node
  }
}
