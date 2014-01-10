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
import org.apache.spark.deploy.master.{ApplicationInfo, DriverInfo, RecoveryState, WorkerInfo}
import org.apache.spark.deploy.worker.{ExecutorRunner, DriverRunner}

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
    val workers = Array(createWorkerInfo(), createWorkerInfo())
    val activeApps = Array(createAppInfo())
    val completedApps = Array[ApplicationInfo]()
    val activeDrivers = Array(createDriverInfo())
    val completedDrivers = Array(createDriverInfo())
    val stateResponse = new MasterStateResponse("host", 8080, workers, activeApps, completedApps,
      activeDrivers, completedDrivers, RecoveryState.ALIVE)
    val output = JsonProtocol.writeMasterState(stateResponse)
    assertValidJson(output)
  }

  test("writeWorkerState") {
    val executors = List[ExecutorRunner]()
    val finishedExecutors = List[ExecutorRunner](createExecutorRunner(), createExecutorRunner())
    val drivers = List(createDriverRunner())
    val finishedDrivers = List(createDriverRunner(), createDriverRunner())
    val stateResponse = new WorkerStateResponse("host", 8080, "workerId", executors,
      finishedExecutors, drivers, finishedDrivers, "masterUrl", 4, 1234, 4, 1234, "masterWebUiUrl")
    val output = JsonProtocol.writeWorkerState(stateResponse)
    assertValidJson(output)
  }

  def createAppDesc(): ApplicationDescription = {
    val cmd = new Command("mainClass", List("arg1", "arg2"), Map())
    new ApplicationDescription("name", Some(4), 1234, cmd, "sparkHome", "appUiUrl")
  }

  def createAppInfo() : ApplicationInfo = {
    new ApplicationInfo(
      3, "id", createAppDesc(), new Date(123456789), null, "appUriStr", Int.MaxValue)
  }

  def createDriverCommand() = new Command(
    "org.apache.spark.FakeClass", Seq("some arg --and-some options -g foo"),
    Map(("K1", "V1"), ("K2", "V2"))
  )

  def createDriverDesc() = new DriverDescription("hdfs://some-dir/some.jar", 100, 3,
    false, createDriverCommand())

  def createDriverInfo(): DriverInfo = new DriverInfo(3, "driver-3", createDriverDesc(), new Date())

  def createWorkerInfo(): WorkerInfo = {
    new WorkerInfo("id", "host", 8080, 4, 1234, null, 80, "publicAddress")
  }
  def createExecutorRunner(): ExecutorRunner = {
    new ExecutorRunner("appId", 123, createAppDesc(), 4, 1234, null, "workerId", "host",
      new File("sparkHome"), new File("workDir"), "akka://worker", ExecutorState.RUNNING)
  }
  def createDriverRunner(): DriverRunner = {
    new DriverRunner("driverId", new File("workDir"), new File("sparkHome"), createDriverDesc(),
      null, "akka://worker")
  }

  def assertValidJson(json: JValue) {
    try {
      JsonParser.parse(JsonAST.compactRender(json))
    } catch {
      case e: JsonParser.ParseException => fail("Invalid Json detected", e)
    }
  }
}
