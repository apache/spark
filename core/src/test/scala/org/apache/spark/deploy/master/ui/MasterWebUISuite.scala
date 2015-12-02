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

package org.apache.spark.deploy.master.ui

import java.util.Date

import scala.io.Source
import scala.language.postfixOps

import org.json4s.jackson.JsonMethods._
import org.json4s.JsonAST.{JNothing, JString, JInt}
import org.mockito.Mockito.{mock, when}
import org.scalatest.BeforeAndAfter

import org.apache.spark.{SparkConf, SecurityManager, SparkFunSuite}
import org.apache.spark.deploy.DeployMessages.MasterStateResponse
import org.apache.spark.deploy.DeployTestUtils._
import org.apache.spark.deploy.master._
import org.apache.spark.rpc.RpcEnv


class MasterWebUISuite extends SparkFunSuite with BeforeAndAfter {

  val masterPage = mock(classOf[MasterPage])
  val master = {
    val conf = new SparkConf
    val securityMgr = new SecurityManager(conf)
    val rpcEnv = RpcEnv.create(Master.SYSTEM_NAME, "localhost", 0, conf, securityMgr)
    val master = new Master(rpcEnv, rpcEnv.address, 0, securityMgr, conf)
    master
  }
  val masterWebUI = new MasterWebUI(master, 0, customMasterPage = Some(masterPage))

  before {
    masterWebUI.bind()
  }

  after {
    masterWebUI.stop()
  }

  test("list applications") {
    val worker = createWorkerInfo()
    val appDesc = createAppDesc()
    // use new start date so it isn't filtered by UI
    val activeApp = new ApplicationInfo(
      new Date().getTime, "id", appDesc, new Date(), null, Int.MaxValue)
    activeApp.addExecutor(worker, 2)

    val workers = Array[WorkerInfo](worker)
    val activeApps = Array(activeApp)
    val completedApps = Array[ApplicationInfo]()
    val activeDrivers = Array[DriverInfo]()
    val completedDrivers = Array[DriverInfo]()
    val stateResponse = new MasterStateResponse(
      "host", 8080, None, workers, activeApps, completedApps,
      activeDrivers, completedDrivers, RecoveryState.ALIVE)

    when(masterPage.getMasterState).thenReturn(stateResponse)

    val resultJson = Source.fromURL(
      s"http://localhost:${masterWebUI.boundPort}/api/v1/applications")
      .mkString
    val parsedJson = parse(resultJson)
    val firstApp = parsedJson(0)

    assert(firstApp \ "id" === JString(activeApp.id))
    assert(firstApp \ "name" === JString(activeApp.desc.name))
    assert(firstApp \ "coresGranted" === JInt(2))
    assert(firstApp \ "maxCores" === JInt(4))
    assert(firstApp \ "memoryPerExecutorMB" === JInt(1234))
    assert(firstApp \ "coresPerExecutor" === JNothing)
  }

}
