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

import java.io.File
import java.util.Date

import net.liftweb.json.{JsonAST, JsonParser}
import net.liftweb.json.JsonAST.JValue
import org.scalatest.FunSuite

import org.apache.spark.deploy.DeployMessages.{MasterStateResponse, WorkerStateResponse}
import org.apache.spark.deploy.master.{ApplicationInfo, RecoveryState, WorkerInfo}
import org.apache.spark.deploy.worker.ExecutorRunner

class JsonProtocolSuite extends FunSuite {
  test("writeApplicationInfo") {
    val output = JsonProtocol.writeApplicationInfo(createAppInfo())
    assertValidJson(output)
  }

  test("writeWorkerInfo") {
    val output = JsonProtocol.writeWorkerInfo(createWorkerInfo())
    assertValidJson(output)
  }

  test("writeApplicationDescription") {
    val output = JsonProtocol.writeApplicationDescription(createAppDesc())
    assertValidJson(output)
  }

  test("writeExecutorRunner") {
    val output = JsonProtocol.writeExecutorRunner(createExecutorRunner())
    assertValidJson(output)
  }

  test("writeMasterState") {
    val workers = Array[WorkerInfo](createWorkerInfo(), createWorkerInfo())
    val activeApps = Array[ApplicationInfo](createAppInfo())
    val completedApps = Array[ApplicationInfo]()
    val stateResponse = new MasterStateResponse("host", 8080, workers, activeApps, completedApps,
      RecoveryState.ALIVE)
    val output = JsonProtocol.writeMasterState(stateResponse)
    assertValidJson(output)
  }

  test("writeWorkerState") {
    val executors = List[ExecutorRunner]()
    val finishedExecutors = List[ExecutorRunner](createExecutorRunner(), createExecutorRunner())
    val stateResponse = new WorkerStateResponse("host", 8080, "workerId", executors,
      finishedExecutors, "masterUrl", 4, 1234, 4, 1234, "masterWebUiUrl")
    val output = JsonProtocol.writeWorkerState(stateResponse)
    assertValidJson(output)
  }

  def createAppDesc() : ApplicationDescription = {
    val cmd = new Command("mainClass", List("arg1", "arg2"), Map())
    new ApplicationDescription("name", 4, 1234, cmd, "sparkHome", "appUiUrl")
  }
  def createAppInfo() : ApplicationInfo = {
    new ApplicationInfo(3, "id", createAppDesc(), new Date(123456789), null, "appUriStr")
  }
  def createWorkerInfo() : WorkerInfo = {
    new WorkerInfo("id", "host", 8080, 4, 1234, null, 80, "publicAddress")
  }
  def createExecutorRunner() : ExecutorRunner = {
    new ExecutorRunner("appId", 123, createAppDesc(), 4, 1234, null, "workerId", "host",
      new File("sparkHome"), new File("workDir"), ExecutorState.RUNNING)
  }

  def assertValidJson(json: JValue) {
    try {
      JsonParser.parse(JsonAST.compactRender(json))
    } catch {
      case e: JsonParser.ParseException => fail("Invalid Json detected", e)
    }
  }
}
