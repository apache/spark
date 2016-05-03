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

import scala.collection.JavaConverters._
import scala.concurrent.duration._
import scala.io.Source
import scala.language.postfixOps

import org.json4s._
import org.json4s.jackson.JsonMethods._
import org.scalatest.{BeforeAndAfter, Matchers, PrivateMethodTester}
import org.scalatest.concurrent.Eventually
import other.supplier.{CustomPersistenceEngine, CustomRecoveryModeFactory}

import org.apache.spark.{SecurityManager, SparkConf, SparkFunSuite}
import org.apache.spark.deploy._
import org.apache.spark.deploy.DeployMessages._
import org.apache.spark.rpc.{RpcEndpoint, RpcEnv}

class MasterSuite extends SparkFunSuite
  with Matchers with Eventually with PrivateMethodTester with BeforeAndAfter {

  private var _master: Master = _

  after {
    if (_master != null) {
      _master.rpcEnv.shutdown()
      _master.rpcEnv.awaitTermination()
      _master = null
    }
  }

  test("can use a custom recovery mode factory") {
    val conf = new SparkConf(loadDefaults = false)
    conf.set("spark.deploy.recoveryMode", "CUSTOM")
    conf.set("spark.deploy.recoveryMode.factory",
      classOf[CustomRecoveryModeFactory].getCanonicalName)
    conf.set("spark.master.rest.enabled", "false")

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
      webUiAddress = "http://localhost:80"
    )

    val (rpcEnv, _, _) =
      Master.startRpcEnvAndEndpoint("127.0.0.1", 0, 0, conf)

    try {
      rpcEnv.setupEndpointRef(rpcEnv.address, Master.ENDPOINT_NAME)

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

  test("master/worker web ui available") {
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

  private def basicScheduling(spreadOut: Boolean): Unit = {
    val master = makeMaster()
    val appInfo = makeAppInfo(1024)
    val scheduledCores = scheduleExecutorsOnWorkers(master, appInfo, workerInfos, spreadOut)
    assert(scheduledCores === Array(10, 10, 10))
  }

  private def basicSchedulingWithMoreMemory(spreadOut: Boolean): Unit = {
    val master = makeMaster()
    val appInfo = makeAppInfo(3072)
    val scheduledCores = scheduleExecutorsOnWorkers(master, appInfo, workerInfos, spreadOut)
    assert(scheduledCores === Array(10, 10, 10))
  }

  private def schedulingWithMaxCores(spreadOut: Boolean): Unit = {
    val master = makeMaster()
    val appInfo1 = makeAppInfo(1024, maxCores = Some(8))
    val appInfo2 = makeAppInfo(1024, maxCores = Some(16))
    val scheduledCores1 = scheduleExecutorsOnWorkers(master, appInfo1, workerInfos, spreadOut)
    val scheduledCores2 = scheduleExecutorsOnWorkers(master, appInfo2, workerInfos, spreadOut)
    if (spreadOut) {
      assert(scheduledCores1 === Array(3, 3, 2))
      assert(scheduledCores2 === Array(6, 5, 5))
    } else {
      assert(scheduledCores1 === Array(8, 0, 0))
      assert(scheduledCores2 === Array(10, 6, 0))
    }
  }

  private def schedulingWithCoresPerExecutor(spreadOut: Boolean): Unit = {
    val master = makeMaster()
    val appInfo1 = makeAppInfo(1024, coresPerExecutor = Some(2))
    val appInfo2 = makeAppInfo(256, coresPerExecutor = Some(2))
    val appInfo3 = makeAppInfo(256, coresPerExecutor = Some(3))
    val scheduledCores1 = scheduleExecutorsOnWorkers(master, appInfo1, workerInfos, spreadOut)
    val scheduledCores2 = scheduleExecutorsOnWorkers(master, appInfo2, workerInfos, spreadOut)
    val scheduledCores3 = scheduleExecutorsOnWorkers(master, appInfo3, workerInfos, spreadOut)
    assert(scheduledCores1 === Array(8, 8, 8)) // 4 * 2 because of memory limits
    assert(scheduledCores2 === Array(10, 10, 10)) // 5 * 2
    assert(scheduledCores3 === Array(9, 9, 9)) // 3 * 3
  }

  // Sorry for the long method name!
  private def schedulingWithCoresPerExecutorAndMaxCores(spreadOut: Boolean): Unit = {
    val master = makeMaster()
    val appInfo1 = makeAppInfo(256, coresPerExecutor = Some(2), maxCores = Some(4))
    val appInfo2 = makeAppInfo(256, coresPerExecutor = Some(2), maxCores = Some(20))
    val appInfo3 = makeAppInfo(256, coresPerExecutor = Some(3), maxCores = Some(20))
    val scheduledCores1 = scheduleExecutorsOnWorkers(master, appInfo1, workerInfos, spreadOut)
    val scheduledCores2 = scheduleExecutorsOnWorkers(master, appInfo2, workerInfos, spreadOut)
    val scheduledCores3 = scheduleExecutorsOnWorkers(master, appInfo3, workerInfos, spreadOut)
    if (spreadOut) {
      assert(scheduledCores1 === Array(2, 2, 0))
      assert(scheduledCores2 === Array(8, 6, 6))
      assert(scheduledCores3 === Array(6, 6, 6))
    } else {
      assert(scheduledCores1 === Array(4, 0, 0))
      assert(scheduledCores2 === Array(10, 10, 0))
      assert(scheduledCores3 === Array(9, 9, 0))
    }
  }

  private def schedulingWithExecutorLimit(spreadOut: Boolean): Unit = {
    val master = makeMaster()
    val appInfo = makeAppInfo(256)
    appInfo.executorLimit = 0
    val scheduledCores1 = scheduleExecutorsOnWorkers(master, appInfo, workerInfos, spreadOut)
    appInfo.executorLimit = 2
    val scheduledCores2 = scheduleExecutorsOnWorkers(master, appInfo, workerInfos, spreadOut)
    appInfo.executorLimit = 5
    val scheduledCores3 = scheduleExecutorsOnWorkers(master, appInfo, workerInfos, spreadOut)
    assert(scheduledCores1 === Array(0, 0, 0))
    assert(scheduledCores2 === Array(10, 10, 0))
    assert(scheduledCores3 === Array(10, 10, 10))
  }

  private def schedulingWithExecutorLimitAndMaxCores(spreadOut: Boolean): Unit = {
    val master = makeMaster()
    val appInfo = makeAppInfo(256, maxCores = Some(16))
    appInfo.executorLimit = 0
    val scheduledCores1 = scheduleExecutorsOnWorkers(master, appInfo, workerInfos, spreadOut)
    appInfo.executorLimit = 2
    val scheduledCores2 = scheduleExecutorsOnWorkers(master, appInfo, workerInfos, spreadOut)
    appInfo.executorLimit = 5
    val scheduledCores3 = scheduleExecutorsOnWorkers(master, appInfo, workerInfos, spreadOut)
    assert(scheduledCores1 === Array(0, 0, 0))
    if (spreadOut) {
      assert(scheduledCores2 === Array(8, 8, 0))
      assert(scheduledCores3 === Array(6, 5, 5))
    } else {
      assert(scheduledCores2 === Array(10, 6, 0))
      assert(scheduledCores3 === Array(10, 6, 0))
    }
  }

  private def schedulingWithExecutorLimitAndCoresPerExecutor(spreadOut: Boolean): Unit = {
    val master = makeMaster()
    val appInfo = makeAppInfo(256, coresPerExecutor = Some(4))
    appInfo.executorLimit = 0
    val scheduledCores1 = scheduleExecutorsOnWorkers(master, appInfo, workerInfos, spreadOut)
    appInfo.executorLimit = 2
    val scheduledCores2 = scheduleExecutorsOnWorkers(master, appInfo, workerInfos, spreadOut)
    appInfo.executorLimit = 5
    val scheduledCores3 = scheduleExecutorsOnWorkers(master, appInfo, workerInfos, spreadOut)
    assert(scheduledCores1 === Array(0, 0, 0))
    if (spreadOut) {
      assert(scheduledCores2 === Array(4, 4, 0))
    } else {
      assert(scheduledCores2 === Array(8, 0, 0))
    }
    assert(scheduledCores3 === Array(8, 8, 4))
  }

  // Everything being: executor limit + cores per executor + max cores
  private def schedulingWithEverything(spreadOut: Boolean): Unit = {
    val master = makeMaster()
    val appInfo = makeAppInfo(256, coresPerExecutor = Some(4), maxCores = Some(18))
    appInfo.executorLimit = 0
    val scheduledCores1 = scheduleExecutorsOnWorkers(master, appInfo, workerInfos, spreadOut)
    appInfo.executorLimit = 2
    val scheduledCores2 = scheduleExecutorsOnWorkers(master, appInfo, workerInfos, spreadOut)
    appInfo.executorLimit = 5
    val scheduledCores3 = scheduleExecutorsOnWorkers(master, appInfo, workerInfos, spreadOut)
    assert(scheduledCores1 === Array(0, 0, 0))
    if (spreadOut) {
      assert(scheduledCores2 === Array(4, 4, 0))
      assert(scheduledCores3 === Array(8, 4, 4))
    } else {
      assert(scheduledCores2 === Array(8, 0, 0))
      assert(scheduledCores3 === Array(8, 8, 0))
    }
  }

  // ==========================================
  // | Utility methods and fields for testing |
  // ==========================================

  private val _scheduleExecutorsOnWorkers = PrivateMethod[Array[Int]]('scheduleExecutorsOnWorkers)
  private val workerInfo = makeWorkerInfo(4096, 10)
  private val workerInfos = Array(workerInfo, workerInfo, workerInfo)

  private def makeMaster(conf: SparkConf = new SparkConf): Master = {
    assert(_master === null, "Some Master's RpcEnv is leaked in tests")
    val securityMgr = new SecurityManager(conf)
    val rpcEnv = RpcEnv.create(Master.SYSTEM_NAME, "localhost", 0, conf, securityMgr)
    _master = new Master(rpcEnv, rpcEnv.address, 0, securityMgr, conf)
    _master
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
    new WorkerInfo(workerId, "host", 100, cores, memoryMb, null, "http://localhost:80")
  }

  private def scheduleExecutorsOnWorkers(
      master: Master,
      appInfo: ApplicationInfo,
      workerInfos: Array[WorkerInfo],
      spreadOut: Boolean): Array[Int] = {
    master.invokePrivate(_scheduleExecutorsOnWorkers(appInfo, workerInfos, spreadOut))
  }

  test("SPARK-13604: Master should ask Worker kill unknown executors and drivers") {
    val master = makeMaster()
    master.rpcEnv.setupEndpoint(Master.ENDPOINT_NAME, master)
    eventually(timeout(10.seconds)) {
      val masterState = master.self.askWithRetry[MasterStateResponse](RequestMasterState)
      assert(masterState.status === RecoveryState.ALIVE, "Master is not alive")
    }

    val killedExecutors = new ConcurrentLinkedQueue[(String, Int)]()
    val killedDrivers = new ConcurrentLinkedQueue[String]()
    val fakeWorker = master.rpcEnv.setupEndpoint("worker", new RpcEndpoint {
      override val rpcEnv: RpcEnv = master.rpcEnv

      override def receive: PartialFunction[Any, Unit] = {
        case KillExecutor(_, appId, execId) => killedExecutors.add(appId, execId)
        case KillDriver(driverId) => killedDrivers.add(driverId)
      }
    })

    master.self.ask(
      RegisterWorker("1", "localhost", 9999, fakeWorker, 10, 1024, "http://localhost:8080"))
    val executors = (0 until 3).map { i =>
      new ExecutorDescription(appId = i.toString, execId = i, 2, ExecutorState.RUNNING)
    }
    master.self.send(WorkerLatestState("1", executors, driverIds = Seq("0", "1", "2")))

    eventually(timeout(10.seconds)) {
      assert(killedExecutors.asScala.toList.sorted === List("0" -> 0, "1" -> 1, "2" -> 2))
      assert(killedDrivers.asScala.toList.sorted === List("0", "1", "2"))
    }
  }
}
