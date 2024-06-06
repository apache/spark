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

package org.apache.spark.deploy.master

import java.util.Date
import java.util.concurrent.ConcurrentLinkedQueue

import scala.concurrent.duration._
import scala.jdk.CollectionConverters._

import org.apache.spark.SparkConf
import org.apache.spark.deploy._
import org.apache.spark.deploy.DeployMessages._
import org.apache.spark.internal.config._
import org.apache.spark.internal.config.Deploy._
import org.apache.spark.resource.ResourceProfile
import org.apache.spark.rpc.{RpcAddress, RpcEndpoint, RpcEnv}

class MasterSuite extends MasterSuiteBase {
  test("basic scheduling - spread out") {
    basicScheduling(spreadOut = true)
  }

  test("basic scheduling - no spread out") {
    basicScheduling(spreadOut = false)
  }

  test("basic scheduling with more memory - spread out") {
    basicSchedulingWithMoreMemory(spreadOut = true)
  }

  test("basic scheduling with more memory - no spread out") {
    basicSchedulingWithMoreMemory(spreadOut = false)
  }

  test("scheduling with max cores - spread out") {
    schedulingWithMaxCores(spreadOut = true)
  }

  test("scheduling with max cores - no spread out") {
    schedulingWithMaxCores(spreadOut = false)
  }

  test("scheduling with cores per executor - spread out") {
    schedulingWithCoresPerExecutor(spreadOut = true)
  }

  test("scheduling with cores per executor - no spread out") {
    schedulingWithCoresPerExecutor(spreadOut = false)
  }

  test("scheduling with cores per executor AND max cores - spread out") {
    schedulingWithCoresPerExecutorAndMaxCores(spreadOut = true)
  }

  test("scheduling with cores per executor AND max cores - no spread out") {
    schedulingWithCoresPerExecutorAndMaxCores(spreadOut = false)
  }

  test("scheduling with executor limit - spread out") {
    schedulingWithExecutorLimit(spreadOut = true)
  }

  test("scheduling with executor limit - no spread out") {
    schedulingWithExecutorLimit(spreadOut = false)
  }

  test("scheduling with executor limit AND max cores - spread out") {
    schedulingWithExecutorLimitAndMaxCores(spreadOut = true)
  }

  test("scheduling with executor limit AND max cores - no spread out") {
    schedulingWithExecutorLimitAndMaxCores(spreadOut = false)
  }

  test("scheduling with executor limit AND cores per executor - spread out") {
    schedulingWithExecutorLimitAndCoresPerExecutor(spreadOut = true)
  }

  test("scheduling with executor limit AND cores per executor - no spread out") {
    schedulingWithExecutorLimitAndCoresPerExecutor(spreadOut = false)
  }

  test("scheduling with executor limit AND cores per executor AND max cores - spread out") {
    schedulingWithEverything(spreadOut = true)
  }

  test("scheduling with executor limit AND cores per executor AND max cores - no spread out") {
    schedulingWithEverything(spreadOut = false)
  }

  test("SPARK-45174: scheduling with max drivers") {
    val master = makeMaster(new SparkConf().set(MAX_DRIVERS, 4))
    master.state = RecoveryState.ALIVE
    master.workers += workerInfo
    val drivers = getDrivers(master)
    val waitingDrivers = master.invokePrivate(_waitingDrivers())

    master.invokePrivate(_schedule())
    assert(drivers.size === 0 && waitingDrivers.size === 0)

    val command = Command("", Seq.empty, Map.empty, Seq.empty, Seq.empty, Seq.empty)
    val desc = DriverDescription("", 1, 1, false, command)
    (1 to 3).foreach { i =>
      val driver = new DriverInfo(0, "driver" + i, desc, new Date())
      waitingDrivers += driver
      drivers.add(driver)
    }
    assert(drivers.size === 3 && waitingDrivers.size === 3)
    master.invokePrivate(_schedule())
    assert(drivers.size === 3 && waitingDrivers.size === 0)

    (4 to 6).foreach { i =>
      val driver = new DriverInfo(0, "driver" + i, desc, new Date())
      waitingDrivers += driver
      drivers.add(driver)
    }
    master.invokePrivate(_schedule())
    assert(drivers.size === 6 && waitingDrivers.size === 2)
  }

  test("SPARK-46800: schedule to spread out drivers") {
    verifyDrivers(true, 1, 1, 1)
  }

  test("SPARK-46800: schedule not to spread out drivers") {
    verifyDrivers(false, 3, 0, 0)
  }

  test("SPARK-13604: Master should ask Worker kill unknown executors and drivers") {
    val master = makeAliveMaster()
    val killedExecutors = new ConcurrentLinkedQueue[(String, Int)]()
    val killedDrivers = new ConcurrentLinkedQueue[String]()
    val fakeWorker = master.rpcEnv.setupEndpoint("worker", new RpcEndpoint {
      override val rpcEnv: RpcEnv = master.rpcEnv

      override def receive: PartialFunction[Any, Unit] = {
        case KillExecutor(_, appId, execId) => killedExecutors.add((appId, execId))
        case KillDriver(driverId) => killedDrivers.add(driverId)
      }
    })

    master.self.send(RegisterWorker(
      "1",
      "localhost",
      9999,
      fakeWorker,
      10,
      128,
      "http://localhost:8080",
      RpcAddress("localhost", 9999)))
    val executors = (0 until 3).map { i =>
      new ExecutorDescription(appId = i.toString, execId = i,
        ResourceProfile.DEFAULT_RESOURCE_PROFILE_ID, 2, 128, ExecutorState.RUNNING)
    }
    master.self.send(WorkerLatestState("1", executors, driverIds = Seq("0", "1", "2")))

    eventually(timeout(10.seconds)) {
      assert(killedExecutors.asScala.toList.sorted === List("0" -> 0, "1" -> 1, "2" -> 2))
      assert(killedDrivers.asScala.toList.sorted === List("0", "1", "2"))
    }
  }

  test("SPARK-45753: Support driver id pattern") {
    val master = makeMaster(new SparkConf().set(DRIVER_ID_PATTERN, "my-driver-%2$05d"))
    val submitDate = new Date()
    assert(master.invokePrivate(_newDriverId(submitDate)) === "my-driver-00000")
    assert(master.invokePrivate(_newDriverId(submitDate)) === "my-driver-00001")
  }

  test("SPARK-45753: Prevent invalid driver id patterns") {
    val m = intercept[IllegalArgumentException] {
      makeMaster(new SparkConf().set(DRIVER_ID_PATTERN, "my driver"))
    }.getMessage
    assert(m.contains("Whitespace is not allowed"))
  }

  test("SPARK-45754: Support app id pattern") {
    val master = makeMaster(new SparkConf().set(APP_ID_PATTERN, "my-app-%2$05d"))
    val submitDate = new Date()
    assert(master.invokePrivate(_newApplicationId(submitDate)) === "my-app-00000")
    assert(master.invokePrivate(_newApplicationId(submitDate)) === "my-app-00001")
  }

  test("SPARK-45754: Prevent invalid app id patterns") {
    val m = intercept[IllegalArgumentException] {
      makeMaster(new SparkConf().set(APP_ID_PATTERN, "my app"))
    }.getMessage
    assert(m.contains("Whitespace is not allowed"))
  }

  test("SPARK-45785: Rotate app num with modulo operation") {
    val conf = new SparkConf().set(APP_ID_PATTERN, "%2$d").set(APP_NUMBER_MODULO, 1000)
    val master = makeMaster(conf)
    val submitDate = new Date()
    (0 to 2000).foreach { i =>
      assert(master.invokePrivate(_newApplicationId(submitDate)) === s"${i % 1000}")
    }
  }

  test("SPARK-45756: Use appName for appId") {
    val conf = new SparkConf()
      .set(MASTER_USE_APP_NAME_AS_APP_ID, true)
    val master = makeMaster(conf)
    val desc = new ApplicationDescription(
        name = " spark - 45756 ",
        maxCores = None,
        command = null,
        appUiUrl = "",
        defaultProfile = DeployTestUtils.defaultResourceProfile,
        eventLogDir = None,
        eventLogCodec = None)
    assert(master.invokePrivate(_createApplication(desc, null)).id === "spark-45756")
  }
}
