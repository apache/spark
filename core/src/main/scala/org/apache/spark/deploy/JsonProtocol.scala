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

import org.json4s.JsonAST.JObject
import org.json4s.JsonDSL._

import org.apache.spark.deploy.DeployMessages.{MasterStateResponse, WorkerStateResponse}
import org.apache.spark.deploy.master.{ApplicationInfo, DriverInfo, WorkerInfo}
import org.apache.spark.deploy.worker.ExecutorRunner

private[deploy] object JsonProtocol {
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
   ("state" -> obj.state.toString) ~
   ("lastheartbeat" -> obj.lastHeartbeat)
 }

  def writeApplicationInfo(obj: ApplicationInfo): JObject = {
    ("starttime" -> obj.startTime) ~
    ("id" -> obj.id) ~
    ("name" -> obj.desc.name) ~
    ("cores" -> obj.desc.maxCores) ~
    ("user" -> obj.desc.user) ~
    ("memoryperslave" -> obj.desc.memoryPerExecutorMB) ~
    ("submitdate" -> obj.submitDate.toString) ~
    ("state" -> obj.state.toString) ~
    ("duration" -> obj.duration)
  }

  def writeApplicationDescription(obj: ApplicationDescription): JObject = {
    ("name" -> obj.name) ~
    ("cores" -> obj.maxCores) ~
    ("memoryperslave" -> obj.memoryPerExecutorMB) ~
    ("user" -> obj.user) ~
    ("command" -> obj.command.toString)
  }

  def writeExecutorRunner(obj: ExecutorRunner): JObject = {
    ("id" -> obj.execId) ~
    ("memory" -> obj.memory) ~
    ("appid" -> obj.appId) ~
    ("appdesc" -> writeApplicationDescription(obj.appDesc))
  }

  def writeDriverInfo(obj: DriverInfo): JObject = {
    ("id" -> obj.id) ~
    ("starttime" -> obj.startTime.toString) ~
    ("state" -> obj.state.toString) ~
    ("cores" -> obj.desc.cores) ~
    ("memory" -> obj.desc.mem)
  }

  def writeMasterState(obj: MasterStateResponse): JObject = {
    val aliveWorkers = obj.workers.filter(_.isAlive())
    ("url" -> obj.uri) ~
    ("workers" -> obj.workers.toList.map(writeWorkerInfo)) ~
    ("cores" -> aliveWorkers.map(_.cores).sum) ~
    ("coresused" -> aliveWorkers.map(_.coresUsed).sum) ~
    ("memory" -> aliveWorkers.map(_.memory).sum) ~
    ("memoryused" -> aliveWorkers.map(_.memoryUsed).sum) ~
    ("activeapps" -> obj.activeApps.toList.map(writeApplicationInfo)) ~
    ("completedapps" -> obj.completedApps.toList.map(writeApplicationInfo)) ~
    ("activedrivers" -> obj.activeDrivers.toList.map(writeDriverInfo)) ~
    ("status" -> obj.status.toString)
  }

  def writeWorkerState(obj: WorkerStateResponse): JObject = {
    ("id" -> obj.workerId) ~
    ("masterurl" -> obj.masterUrl) ~
    ("masterwebuiurl" -> obj.masterWebUiUrl) ~
    ("cores" -> obj.cores) ~
    ("coresused" -> obj.coresUsed) ~
    ("memory" -> obj.memory) ~
    ("memoryused" -> obj.memoryUsed) ~
    ("executors" -> obj.executors.toList.map(writeExecutorRunner)) ~
    ("finishedexecutors" -> obj.finishedExecutors.toList.map(writeExecutorRunner))
  }
}
