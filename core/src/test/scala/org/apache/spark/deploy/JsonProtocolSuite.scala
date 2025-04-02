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

import java.util.Date

import com.fasterxml.jackson.core.JsonParseException
import org.json4s._
import org.json4s.jackson.JsonMethods

import org.apache.spark.{JsonTestUtils, SparkFunSuite}
import org.apache.spark.deploy.DeployMessages.{MasterStateResponse, WorkerStateResponse}
import org.apache.spark.deploy.master.{ApplicationInfo, RecoveryState, WorkerInfo}
import org.apache.spark.deploy.worker.ExecutorRunner

class JsonProtocolSuite extends SparkFunSuite with JsonTestUtils {

  import org.apache.spark.deploy.DeployTestUtils._

  test("writeApplicationInfo") {
    val output = JsonProtocol.writeApplicationInfo(createAppInfo())
    assertValidJson(output)
    assertValidDataInJson(output, JsonMethods.parse(JsonConstants.appInfoJsonStr))
  }

  test("writeWorkerInfo") {
    val output = JsonProtocol.writeWorkerInfo(createWorkerInfo())
    assertValidJson(output)
    assertValidDataInJson(output, JsonMethods.parse(JsonConstants.workerInfoJsonStr))
  }

  test("writeApplicationDescription") {
    val output = JsonProtocol.writeApplicationDescription(createAppDesc())
    assertValidJson(output)
    assertValidDataInJson(output, JsonMethods.parse(JsonConstants.appDescJsonStr))
  }

  test("writeExecutorRunner") {
    val output = JsonProtocol.writeExecutorRunner(createExecutorRunner(123, true))
    assertValidJson(output)
    assertValidDataInJson(output, JsonMethods.parse(JsonConstants.executorRunnerJsonStr))
  }

  test("writeDriverInfo") {
    val output = JsonProtocol.writeDriverInfo(createDriverInfo())
    assertValidJson(output)
    assertValidDataInJson(output, JsonMethods.parse(JsonConstants.driverInfoJsonStr))
  }

  test("writeMasterState") {
    val workers = Array(createWorkerInfo(), createWorkerInfo())
    val activeApps = Array(createAppInfo())
    val completedApps = Array.empty[ApplicationInfo]
    val activeDrivers = Array(createDriverInfo())
    val completedDrivers = Array(createDriverInfo())
    val stateResponse = new MasterStateResponse(
      "host", 8080, None, workers, activeApps, completedApps,
      activeDrivers, completedDrivers, RecoveryState.ALIVE)
    val output = JsonProtocol.writeMasterState(stateResponse)
    assertValidJson(output)
    assertValidDataInJson(output, JsonMethods.parse(JsonConstants.masterStateJsonStr))
  }

  test("SPARK-45474: filtered writeMasterState") {
    val workers = Array(createWorkerInfo(), createWorkerInfo())
    val activeApps = Array(createAppInfo())
    val completedApps = Array.empty[ApplicationInfo]
    val activeDrivers = Array(createDriverInfo())
    val completedDrivers = Array(createDriverInfo())
    val stateResponse = new MasterStateResponse(
      "host", 8080, None, workers, activeApps, completedApps,
      activeDrivers, completedDrivers, RecoveryState.ALIVE)
    val output = JsonProtocol.writeMasterState(stateResponse, Some("activedrivers"))
    assertValidJson(output)

    val expected = """{"activedrivers":[%s]}""".format(JsonConstants.driverInfoJsonStr).stripMargin
    assertValidDataInJson(output, JsonMethods.parse(expected))
  }

  test("writeWorkerState") {
    val executors = List[ExecutorRunner]()
    val finishedExecutors = List[ExecutorRunner](createExecutorRunner(123, true),
      createExecutorRunner(123, true))
    val drivers = List(createDriverRunner("driverId"))
    val finishedDrivers = List(createDriverRunner("driverId"), createDriverRunner("driverId"))
    val stateResponse = new WorkerStateResponse("host", 8080, "workerId", executors,
      finishedExecutors, drivers, finishedDrivers, "masterUrl", 4, 1234, 4, 1234, "masterWebUiUrl")
    val output = JsonProtocol.writeWorkerState(stateResponse)
    assertValidJson(output)
    assertValidDataInJson(output, JsonMethods.parse(JsonConstants.workerStateJsonStr))
  }

  test("SPARK-46883: writeClusterUtilization") {
    val workers = Array(createWorkerInfo(), createWorkerInfo())
    val activeApps = Array(createAppInfo())
    val completedApps = Array.empty[ApplicationInfo]
    val activeDrivers = Array(createDriverInfo())
    val completedDrivers = Array(createDriverInfo())
    val stateResponse = new MasterStateResponse(
      "host", 8080, None, workers, activeApps, completedApps,
      activeDrivers, completedDrivers, RecoveryState.ALIVE)
    val output = JsonProtocol.writeClusterUtilization(stateResponse)
    assertValidJson(output)
    assertValidDataInJson(output, JsonMethods.parse(JsonConstants.clusterUtilizationJsonStr))
  }

  test("SPARK-46883: writeClusterUtilization without workers") {
    val workers = Array.empty[WorkerInfo]
    val activeApps = Array(createAppInfo())
    val completedApps = Array.empty[ApplicationInfo]
    val activeDrivers = Array(createDriverInfo())
    val completedDrivers = Array(createDriverInfo())
    val stateResponse = new MasterStateResponse(
      "host", 8080, None, workers, activeApps, completedApps,
      activeDrivers, completedDrivers, RecoveryState.ALIVE)
    val output = JsonProtocol.writeClusterUtilization(stateResponse)
    assertValidJson(output)
    assertValidDataInJson(output,
      JsonMethods.parse(JsonConstants.clusterUtilizationWithoutWorkersJsonStr))
  }

  def assertValidJson(json: JValue): Unit = {
    try {
      JsonMethods.parse(JsonMethods.compact(json))
    } catch {
      case e: JsonParseException => fail("Invalid Json detected", e)
    }
  }
}

object JsonConstants {
  val currTimeInMillis = System.currentTimeMillis()
  val appInfoStartTime = 3
  val submitDate = new Date(123456789)
  val appInfoJsonStr =
    """
      |{"id":"id","starttime":3,"name":"name",
      |"cores":0,"user":"%s",
      |"memoryperexecutor":1234,
      |"resourcesperexecutor":[{"name":"fpga",
      |"amount":3},{"name":"gpu","amount":3}],
      |"memoryperslave":1234,
      |"resourcesperslave":[{"name":"fpga",
      |"amount":3},{"name":"gpu","amount":3}],
      |"submitdate":"%s",
      |"state":"WAITING","duration":%d}
    """.format(System.getProperty("user.name", "<unknown>"),
        submitDate.toString, currTimeInMillis - appInfoStartTime).stripMargin

  val workerInfoJsonStr =
    """
      |{"id":"id","host":"host","port":8080,
      |"webuiaddress":"http://publicAddress:80",
      |"cores":4,"coresused":0,"coresfree":4,
      |"memory":1234,"memoryused":0,"memoryfree":1234,
      |"resources":{"gpu":{"name":"gpu","addresses":
      |["0","1","2"]},"fpga":{"name":"fpga","addresses"
      |:["3","4","5"]}},"resourcesused":{"gpu":
      |{"name":"gpu","addresses":[]},"fpga":
      |{"name":"fpga","addresses":[]}},"resourcesfree":
      |{"gpu":{"name":"gpu","addresses":["0","1","2"]},
      |"fpga":{"name":"fpga","addresses":["3","4","5"]}},
      |"state":"ALIVE","lastheartbeat":%d}
    """.format(currTimeInMillis).stripMargin

  val appDescJsonStr =
    """
      |{"name":"name","cores":4,"memoryperexecutor":1234,"resourcesperexecutor":[],
      |"memoryperslave":1234,"resourcesperslave":[],
      |"user":"%s","command":"Command(mainClass,List(arg1, arg2),Map(),List(),List(),List())"}
    """.format(System.getProperty("user.name", "<unknown>")).stripMargin

  val executorRunnerJsonStr =
    """
      |{"id":123,"memory":1234,"resources":
      |{"gpu":{"name":"gpu","addresses":["0","1","2"]},
      |"fpga":{"name":"fpga","addresses":["3","4","5"]}},
      |"appid":"appId",
      |"appdesc":%s}
    """.format(appDescJsonStr).stripMargin

  val driverInfoJsonStr =
    """
      |{"id":"driver-3","starttime":"3",
      |"state":"SUBMITTED","cores":3,"memory":100,
      |"resources":{"gpu":{"name":"gpu","addresses":
      |["0","1","2"]},"fpga":{"name":"fpga",
      |"addresses":["3","4","5"]}},
      |"submitdate":"%s","worker":"None",
      |"mainclass":"mainClass"}
    """.format(submitDate.toString).stripMargin

  val masterStateJsonStr =
    """
      |{"url":"spark://host:8080",
      |"workers":[%s,%s],
      |"aliveworkers":2,
      |"cores":8,"coresused":0,"memory":2468,"memoryused":0,
      |"resources":[{"gpu":{"name":"gpu","addresses":
      |["0","1","2"]},"fpga":{"name":"fpga","addresses":
      |["3","4","5"]}},{"gpu":{"name":"gpu","addresses":
      |["0","1","2"]},"fpga":{"name":"fpga","addresses":
      |["3","4","5"]}}],"resourcesused":[{"gpu":{"name":
      |"gpu","addresses":[]},"fpga":{"name":"fpga","addresses":[]}}
      |,{"gpu":{"name":"gpu","addresses":[]},"fpga":
      |{"name":"fpga","addresses":[]}}],
      |"activeapps":[%s],"completedapps":[],
      |"activedrivers":[%s],
      |"completeddrivers":[%s],
      |"status":"ALIVE"}
    """.format(workerInfoJsonStr, workerInfoJsonStr,
        appInfoJsonStr, driverInfoJsonStr, driverInfoJsonStr).stripMargin

  val workerStateJsonStr =
    """
      |{"id":"workerId","masterurl":"masterUrl",
      |"masterwebuiurl":"masterWebUiUrl",
      |"cores":4,"coresused":4,"memory":1234,"memoryused":1234,
      |"resources":{},"resourcesused":{},
      |"executors":[],
      |"finishedexecutors":[%s,%s]}
    """.format(executorRunnerJsonStr, executorRunnerJsonStr).stripMargin

  val clusterUtilizationJsonStr =
    """
      |{"waitingDrivers":1,
      |"cores":8,"coresused":0,"coresutilization":0,
      |"memory":2468,"memoryused":0,"memoryutilization":0}
    """.stripMargin

  val clusterUtilizationWithoutWorkersJsonStr =
    """
      |{"waitingDrivers":1,
      |"cores":0,"coresused":0,"coresutilization":100,
      |"memory":0,"memoryused":0,"memoryutilization":100}
    """.stripMargin
}
