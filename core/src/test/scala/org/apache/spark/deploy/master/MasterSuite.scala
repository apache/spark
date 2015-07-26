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

import scala.concurrent.duration._
import scala.io.Source
import scala.language.postfixOps

import org.json4s._
import org.json4s.jackson.JsonMethods._
import org.scalatest.{Matchers, PrivateMethodTester}
import org.scalatest.concurrent.Eventually
import other.supplier.{CustomPersistenceEngine, CustomRecoveryModeFactory}

import org.apache.spark.{SecurityManager, SparkConf, SparkFunSuite}
import org.apache.spark.deploy._
import org.apache.spark.rpc.RpcEnv

class MasterSuite extends SparkFunSuite with Matchers with Eventually with PrivateMethodTester {

  test("can use a custom recovery mode factory") {
    val conf = new SparkConf(loadDefaults = false)
    conf.set("spark.deploy.recoveryMode", "CUSTOM")
    conf.set("spark.deploy.recoveryMode.factory",
      classOf[CustomRecoveryModeFactory].getCanonicalName)

    val instantiationAttempts = CustomRecoveryModeFactory.instantiationAttempts

    val commandToPersist = new Command(
      mainClass = "",
      arguments = Nil,
      environment = Map.empty,
      classPathEntries = Nil,
      libraryPathEntries = Nil,
      javaOpts = Nil
    )

    val appToPersist = new ApplicationInfo(
      startTime = 0,
      id = "test_app",
      desc = new ApplicationDescription(
        name = "",
        maxCores = None,
        memoryPerExecutorMB = 0,
        command = commandToPersist,
        appUiUrl = "",
        eventLogDir = None,
        eventLogCodec = None,
        coresPerExecutor = None),
      submitDate = new Date(),
      driver = null,
      defaultCores = 0
    )

    val driverToPersist = new DriverInfo(
      startTime = 0,
      id = "test_driver",
      desc = new DriverDescription(
        jarUrl = "",
        mem = 0,
        cores = 0,
        supervise = false,
        command = commandToPersist
      ),
      submitDate = new Date()
    )

    val workerToPersist = new WorkerInfo(
      id = "test_worker",
      host = "127.0.0.1",
      port = 10000,
      cores = 0,
      memory = 0,
      endpoint = null,
      webUiPort = 0,
      publicAddress = ""
    )

    val (rpcEnv, uiPort, restPort) =
      Master.startRpcEnvAndEndpoint("127.0.0.1", 7077, 8080, conf)

    try {
      rpcEnv.setupEndpointRef(Master.SYSTEM_NAME, rpcEnv.address, Master.ENDPOINT_NAME)

      CustomPersistenceEngine.lastInstance.isDefined shouldBe true
      val persistenceEngine = CustomPersistenceEngine.lastInstance.get

      persistenceEngine.addApplication(appToPersist)
      persistenceEngine.addDriver(driverToPersist)
      persistenceEngine.addWorker(workerToPersist)

      val (apps, drivers, workers) = persistenceEngine.readPersistedData(rpcEnv)

      apps.map(_.id) should contain(appToPersist.id)
      drivers.map(_.id) should contain(driverToPersist.id)
      workers.map(_.id) should contain(workerToPersist.id)

    } finally {
      rpcEnv.shutdown()
      rpcEnv.awaitTermination()
    }

    CustomRecoveryModeFactory.instantiationAttempts should be > instantiationAttempts
  }

  test("Master & worker web ui available") {
    implicit val formats = org.json4s.DefaultFormats
    val conf = new SparkConf()
    val localCluster = new LocalSparkCluster(2, 2, 512, conf)
    localCluster.start()
    try {
      eventually(timeout(5 seconds), interval(100 milliseconds)) {
        val json = Source.fromURL(s"http://localhost:${localCluster.masterWebUIPort}/json")
          .getLines().mkString("\n")
        val JArray(workers) = (parse(json) \ "workers")
        workers.size should be (2)
        workers.foreach { workerSummaryJson =>
          val JString(workerWebUi) = workerSummaryJson \ "webuiaddress"
          val workerResponse = parse(Source.fromURL(s"${workerWebUi}/json")
            .getLines().mkString("\n"))
          (workerResponse \ "cores").extract[Int] should be (2)
        }
      }
    } finally {
      localCluster.stop()
    }
  }

  test("basic scheduling - spread out") {
    testBasicScheduling(spreadOut = true)
  }

  test("basic scheduling - no spread out") {
    testBasicScheduling(spreadOut = false)
  }

  test("scheduling with max cores - spread out") {
    testSchedulingWithMaxCores(spreadOut = true)
  }

  test("scheduling with max cores - no spread out") {
    testSchedulingWithMaxCores(spreadOut = false)
  }

  test("scheduling with cores per executor - spread out") {
    testSchedulingWithCoresPerExecutor(spreadOut = true)
  }

  test("scheduling with cores per executor - no spread out") {
    testSchedulingWithCoresPerExecutor(spreadOut = false)
  }

  test("scheduling with cores per executor AND max cores - spread out") {
    testSchedulingWithCoresPerExecutorAndMaxCores(spreadOut = true)
  }

  test("scheduling with cores per executor AND max cores - no spread out") {
    testSchedulingWithCoresPerExecutorAndMaxCores(spreadOut = false)
  }

  private def testBasicScheduling(spreadOut: Boolean): Unit = {
    val master = makeMaster()
    val appInfo = makeAppInfo(1024)
    val workerInfo = makeWorkerInfo(4096, 10)
    val workerInfos = Array(workerInfo, workerInfo, workerInfo)
    val scheduledCores = master.invokePrivate(
      _scheduleExecutorsOnWorkers(appInfo, workerInfos, spreadOut))
    assert(scheduledCores.length === 3)
    assert(scheduledCores(0) === 10)
    assert(scheduledCores(1) === 10)
    assert(scheduledCores(2) === 10)
  }

  private def testSchedulingWithMaxCores(spreadOut: Boolean): Unit = {
    val master = makeMaster()
    val appInfo1 = makeAppInfo(1024, maxCores = Some(8))
    val appInfo2 = makeAppInfo(1024, maxCores = Some(16))
    val workerInfo = makeWorkerInfo(4096, 10)
    val workerInfos = Array(workerInfo, workerInfo, workerInfo)
    var scheduledCores = master.invokePrivate(
      _scheduleExecutorsOnWorkers(appInfo1, workerInfos, spreadOut))
    assert(scheduledCores.length === 3)
    // With spreading out, each worker should be assigned a few cores
    if (spreadOut) {
      assert(scheduledCores(0) === 3)
      assert(scheduledCores(1) === 3)
      assert(scheduledCores(2) === 2)
    } else {
      // Without spreading out, the cores should be concentrated on the first worker
      assert(scheduledCores(0) === 8)
      assert(scheduledCores(1) === 0)
      assert(scheduledCores(2) === 0)
    }
    // Now test the same thing with max cores > cores per worker
    scheduledCores = master.invokePrivate(
      _scheduleExecutorsOnWorkers(appInfo2, workerInfos, spreadOut))
    assert(scheduledCores.length === 3)
    if (spreadOut) {
      assert(scheduledCores(0) === 6)
      assert(scheduledCores(1) === 5)
      assert(scheduledCores(2) === 5)
    } else {
      // Without spreading out, the first worker should be fully booked,
      // and the leftover cores should spill over to the second worker only.
      assert(scheduledCores(0) === 10)
      assert(scheduledCores(1) === 6)
      assert(scheduledCores(2) === 0)
    }
  }

  private def testSchedulingWithCoresPerExecutor(spreadOut: Boolean): Unit = {
    val master = makeMaster()
    val appInfo1 = makeAppInfo(1024, coresPerExecutor = Some(2))
    val appInfo2 = makeAppInfo(256, coresPerExecutor = Some(2))
    val appInfo3 = makeAppInfo(256, coresPerExecutor = Some(3))
    val workerInfo = makeWorkerInfo(4096, 10)
    val workerInfos = Array(workerInfo, workerInfo, workerInfo)
    // Each worker should end up with 4 executors with 2 cores each
    // This should be 4 because of the memory restriction on each worker
    var scheduledCores = master.invokePrivate(
      _scheduleExecutorsOnWorkers(appInfo1, workerInfos, spreadOut))
    assert(scheduledCores.length === 3)
    assert(scheduledCores(0) === 8)
    assert(scheduledCores(1) === 8)
    assert(scheduledCores(2) === 8)
    // Now test the same thing without running into the worker memory limit
    // Each worker should now end up with 5 executors with 2 cores each
    scheduledCores = master.invokePrivate(
      _scheduleExecutorsOnWorkers(appInfo2, workerInfos, spreadOut))
    assert(scheduledCores.length === 3)
    assert(scheduledCores(0) === 10)
    assert(scheduledCores(1) === 10)
    assert(scheduledCores(2) === 10)
    // Now test the same thing with a cores per executor that 10 is not divisible by
    scheduledCores = master.invokePrivate(
      _scheduleExecutorsOnWorkers(appInfo3, workerInfos, spreadOut))
    assert(scheduledCores.length === 3)
    assert(scheduledCores(0) === 9)
    assert(scheduledCores(1) === 9)
    assert(scheduledCores(2) === 9)
  }

  // Sorry for the long method name!
  private def testSchedulingWithCoresPerExecutorAndMaxCores(spreadOut: Boolean): Unit = {
    val master = makeMaster()
    val appInfo1 = makeAppInfo(256, coresPerExecutor = Some(2), maxCores = Some(4))
    val appInfo2 = makeAppInfo(256, coresPerExecutor = Some(2), maxCores = Some(20))
    val appInfo3 = makeAppInfo(256, coresPerExecutor = Some(3), maxCores = Some(20))
    val workerInfo = makeWorkerInfo(4096, 10)
    val workerInfos = Array(workerInfo, workerInfo, workerInfo)
    // We should only launch two executors, each with exactly 2 cores
    var scheduledCores = master.invokePrivate(
      _scheduleExecutorsOnWorkers(appInfo1, workerInfos, spreadOut))
    assert(scheduledCores.length === 3)
    if (spreadOut) {
      assert(scheduledCores(0) === 2)
      assert(scheduledCores(1) === 2)
      assert(scheduledCores(2) === 0)
    } else {
      assert(scheduledCores(0) === 4)
      assert(scheduledCores(1) === 0)
      assert(scheduledCores(2) === 0)
    }
    // Test max cores > number of cores per worker
    scheduledCores = master.invokePrivate(
      _scheduleExecutorsOnWorkers(appInfo2, workerInfos, spreadOut))
    assert(scheduledCores.length === 3)
    if (spreadOut) {
      assert(scheduledCores(0) === 8)
      assert(scheduledCores(1) === 6)
      assert(scheduledCores(2) === 6)
    } else {
      assert(scheduledCores(0) === 10)
      assert(scheduledCores(1) === 10)
      assert(scheduledCores(2) === 0)
    }
    // Test max cores > number of cores per worker AND
    // a cores per executor that is 10 is not divisible by
    scheduledCores = master.invokePrivate(
      _scheduleExecutorsOnWorkers(appInfo3, workerInfos, spreadOut))
    assert(scheduledCores.length === 3)
    if (spreadOut) {
      assert(scheduledCores(0) === 6)
      assert(scheduledCores(1) === 6)
      assert(scheduledCores(2) === 6)
    } else {
      assert(scheduledCores(0) === 9)
      assert(scheduledCores(1) === 9)
      assert(scheduledCores(2) === 0)
    }
  }

  // ===============================
  // | Utility methods for testing |
  // ===============================

  private val _scheduleExecutorsOnWorkers = PrivateMethod[Array[Int]]('scheduleExecutorsOnWorkers)

  private def makeMaster(conf: SparkConf = new SparkConf): Master = {
    val securityMgr = new SecurityManager(conf)
    val rpcEnv = RpcEnv.create(Master.SYSTEM_NAME, "localhost", 7077, conf, securityMgr)
    val master = new Master(rpcEnv, rpcEnv.address, 8080, securityMgr, conf)
    master
  }

  private def makeAppInfo(
      memoryPerExecutorMb: Int,
      coresPerExecutor: Option[Int] = None,
      maxCores: Option[Int] = None): ApplicationInfo = {
    val desc = new ApplicationDescription(
      "test", maxCores, memoryPerExecutorMb, null, "", None, None, coresPerExecutor)
    val appId = System.currentTimeMillis.toString
    new ApplicationInfo(0, appId, desc, new Date, null, Int.MaxValue)
  }

  private def makeWorkerInfo(memoryMb: Int, cores: Int): WorkerInfo = {
    val workerId = System.currentTimeMillis.toString
    new WorkerInfo(workerId, "host", 100, cores, memoryMb, null, 101, "address")
  }

}
