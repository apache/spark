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
import java.util.concurrent.{ConcurrentLinkedQueue, CountDownLatch, TimeUnit}
import java.util.concurrent.atomic.AtomicInteger

import scala.collection.JavaConverters._
import scala.collection.mutable
import scala.collection.mutable.{HashMap, HashSet}
import scala.concurrent.duration._
import scala.io.Source
import scala.reflect.ClassTag

import org.json4s._
import org.json4s.jackson.JsonMethods._
import org.mockito.Mockito.{mock, when}
import org.scalatest.{BeforeAndAfter, PrivateMethodTester}
import org.scalatest.concurrent.Eventually
import org.scalatest.matchers.must.Matchers
import org.scalatest.matchers.should.Matchers._
import other.supplier.{CustomPersistenceEngine, CustomRecoveryModeFactory}

import org.apache.spark.{SecurityManager, SparkConf, SparkFunSuite}
import org.apache.spark.deploy._
import org.apache.spark.deploy.DeployMessages._
import org.apache.spark.internal.config._
import org.apache.spark.internal.config.Deploy._
import org.apache.spark.internal.config.UI._
import org.apache.spark.internal.config.Worker._
import org.apache.spark.resource.{ResourceInformation, ResourceRequirement}
import org.apache.spark.resource.ResourceUtils.{FPGA, GPU}
import org.apache.spark.rpc.{RpcAddress, RpcEndpoint, RpcEndpointRef, RpcEnv}
import org.apache.spark.serializer
import org.apache.spark.util.Utils

object MockWorker {
  val counter = new AtomicInteger(10000)
}

class MockWorker(master: RpcEndpointRef, conf: SparkConf = new SparkConf) extends RpcEndpoint {
  val seq = MockWorker.counter.incrementAndGet()
  val id = seq.toString
  override val rpcEnv: RpcEnv = RpcEnv.create("worker", "localhost", seq,
    conf, new SecurityManager(conf))
  var apps = new mutable.HashMap[String, String]()
  val driverIdToAppId = new mutable.HashMap[String, String]()
  def newDriver(driverId: String): RpcEndpointRef = {
    val name = s"driver_${drivers.size}"
    rpcEnv.setupEndpoint(name, new RpcEndpoint {
      override val rpcEnv: RpcEnv = MockWorker.this.rpcEnv
      override def receive: PartialFunction[Any, Unit] = {
        case RegisteredApplication(appId, _) =>
          apps(appId) = appId
          driverIdToAppId(driverId) = appId
      }
    })
  }

  var decommissioned = false
  var appDesc = DeployTestUtils.createAppDesc()
  val drivers = mutable.HashSet[String]()
  val driverResources = new mutable.HashMap[String, Map[String, Set[String]]]
  val execResources = new mutable.HashMap[String, Map[String, Set[String]]]
  override def receive: PartialFunction[Any, Unit] = {
    case RegisteredWorker(masterRef, _, _, _) =>
      masterRef.send(WorkerLatestState(id, Nil, drivers.toSeq))
    case LaunchExecutor(_, appId, execId, _, _, _, resources_) =>
      execResources(appId + "/" + execId) = resources_.map(r => (r._1, r._2.addresses.toSet))
    case LaunchDriver(driverId, desc, resources_) =>
      drivers += driverId
      driverResources(driverId) = resources_.map(r => (r._1, r._2.addresses.toSet))
      master.send(RegisterApplication(appDesc, newDriver(driverId)))
    case KillDriver(driverId) =>
      master.send(DriverStateChanged(driverId, DriverState.KILLED, None))
      drivers -= driverId
      driverResources.remove(driverId)
      driverIdToAppId.get(driverId) match {
        case Some(appId) =>
          apps.remove(appId)
          master.send(UnregisterApplication(appId))
        case None =>
      }
      driverIdToAppId.remove(driverId)
    case DecommissionWorker =>
      decommissioned = true
  }
}

// This class is designed to handle the lifecycle of only one application.
class MockExecutorLaunchFailWorker(master: Master, conf: SparkConf = new SparkConf)
  extends MockWorker(master.self, conf) with Eventually {

  val appRegistered = new CountDownLatch(1)
  val launchExecutorReceived = new CountDownLatch(1)
  val appIdsToLaunchExecutor = new mutable.HashSet[String]
  var failedCnt = 0

  override def receive: PartialFunction[Any, Unit] = {
    case LaunchDriver(driverId, _, _) =>
      master.self.send(RegisterApplication(appDesc, newDriver(driverId)))

      // Below code doesn't make driver stuck, as newDriver opens another rpc endpoint for
      // handling driver related messages. To simplify logic, we will block handling
      // LaunchExecutor message until we validate registering app succeeds.
      eventually(timeout(5.seconds)) {
        // an app would be registered with Master once Driver set up
        assert(apps.nonEmpty)
        assert(master.idToApp.keySet.intersect(apps.keySet) == apps.keySet)
      }

      appRegistered.countDown()
    case LaunchExecutor(_, appId, execId, _, _, _, _) =>
      assert(appRegistered.await(10, TimeUnit.SECONDS))

      if (failedCnt == 0) {
        launchExecutorReceived.countDown()
      }
      assert(master.idToApp.contains(appId))
      appIdsToLaunchExecutor += appId
      failedCnt += 1
      master.self.send(ExecutorStateChanged(appId, execId, ExecutorState.FAILED, None, None))

    case otherMsg => super.receive(otherMsg)
  }
}

class MasterSuite extends SparkFunSuite
  with Matchers with Eventually with PrivateMethodTester with BeforeAndAfter {

  // regex to extract worker links from the master webui HTML
  // groups represent URL and worker ID
  val WORKER_LINK_RE = """<a href="(.+?)">\s*(worker-.+?)\s*</a>""".r

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
    conf.set(RECOVERY_MODE, "CUSTOM")
    conf.set(RECOVERY_MODE_FACTORY, classOf[CustomRecoveryModeFactory].getCanonicalName)
    conf.set(MASTER_REST_SERVER_ENABLED, false)

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
      webUiAddress = "http://localhost:80",
      Map.empty
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

  test("master correctly recover the application") {
    val conf = new SparkConf(loadDefaults = false)
    conf.set(RECOVERY_MODE, "CUSTOM")
    conf.set(RECOVERY_MODE_FACTORY, classOf[FakeRecoveryModeFactory].getCanonicalName)
    conf.set(MASTER_REST_SERVER_ENABLED, false)

    val fakeAppInfo = makeAppInfo(1024)
    val fakeWorkerInfo = makeWorkerInfo(8192, 16)
    val fakeDriverInfo = new DriverInfo(
      startTime = 0,
      id = "test_driver",
      desc = new DriverDescription(
        jarUrl = "",
        mem = 1024,
        cores = 1,
        supervise = false,
        command = new Command("", Nil, Map.empty, Nil, Nil, Nil)),
      submitDate = new Date())

    // Build the fake recovery data
    FakeRecoveryModeFactory.persistentData.put(s"app_${fakeAppInfo.id}", fakeAppInfo)
    FakeRecoveryModeFactory.persistentData.put(s"driver_${fakeDriverInfo.id}", fakeDriverInfo)
    FakeRecoveryModeFactory.persistentData.put(s"worker_${fakeWorkerInfo.id}", fakeWorkerInfo)

    var master: Master = null
    try {
      master = makeMaster(conf)
      master.rpcEnv.setupEndpoint(Master.ENDPOINT_NAME, master)
      // Wait until Master recover from checkpoint data.
      eventually(timeout(5.seconds), interval(100.milliseconds)) {
        master.workers.size should be(1)
      }

      master.idToApp.keySet should be(Set(fakeAppInfo.id))
      getDrivers(master) should be(Set(fakeDriverInfo))
      master.workers should be(Set(fakeWorkerInfo))

      // Notify Master about the executor and driver info to make it correctly recovered.
      val fakeExecutors = List(
        new ExecutorDescription(fakeAppInfo.id, 0, 8, ExecutorState.RUNNING),
        new ExecutorDescription(fakeAppInfo.id, 0, 7, ExecutorState.RUNNING))

      fakeAppInfo.state should be(ApplicationState.UNKNOWN)
      fakeWorkerInfo.coresFree should be(16)
      fakeWorkerInfo.coresUsed should be(0)

      master.self.send(MasterChangeAcknowledged(fakeAppInfo.id))
      eventually(timeout(1.second), interval(10.milliseconds)) {
        // Application state should be WAITING when "MasterChangeAcknowledged" event executed.
        fakeAppInfo.state should be(ApplicationState.WAITING)
      }
      val execResponse = fakeExecutors.map(exec =>
        WorkerExecutorStateResponse(exec, Map.empty[String, ResourceInformation]))
      val driverResponse = WorkerDriverStateResponse(
        fakeDriverInfo.id, Map.empty[String, ResourceInformation])
      master.self.send(WorkerSchedulerStateResponse(
        fakeWorkerInfo.id, execResponse, Seq(driverResponse)))

      eventually(timeout(5.seconds), interval(100.milliseconds)) {
        getState(master) should be(RecoveryState.ALIVE)
      }

      // If driver's resource is also counted, free cores should 0
      fakeWorkerInfo.coresFree should be(0)
      fakeWorkerInfo.coresUsed should be(16)
      // State of application should be RUNNING
      fakeAppInfo.state should be(ApplicationState.RUNNING)
    } finally {
      if (master != null) {
        master.rpcEnv.shutdown()
        master.rpcEnv.awaitTermination()
        master = null
        FakeRecoveryModeFactory.persistentData.clear()
      }
    }
  }

  test("master/worker web ui available") {
    implicit val formats = org.json4s.DefaultFormats
    val conf = new SparkConf()
    val localCluster = new LocalSparkCluster(2, 2, 512, conf)
    localCluster.start()
    val masterUrl = s"http://localhost:${localCluster.masterWebUIPort}"
    try {
      eventually(timeout(5.seconds), interval(100.milliseconds)) {
        val json = Utils
          .tryWithResource(Source.fromURL(s"$masterUrl/json"))(_.getLines().mkString("\n"))
        val JArray(workers) = (parse(json) \ "workers")
        workers.size should be (2)
        workers.foreach { workerSummaryJson =>
          val JString(workerWebUi) = workerSummaryJson \ "webuiaddress"
          val workerResponse = parse(Utils
            .tryWithResource(Source.fromURL(s"$workerWebUi/json"))(_.getLines().mkString("\n")))
          (workerResponse \ "cores").extract[Int] should be (2)
        }

        val html = Utils
          .tryWithResource(Source.fromURL(s"$masterUrl/"))(_.getLines().mkString("\n"))
        html should include ("Spark Master at spark://")
        val workerLinks = (WORKER_LINK_RE findAllMatchIn html).toList
        workerLinks.size should be (2)
        workerLinks foreach { case WORKER_LINK_RE(workerUrl, workerId) =>
          val workerHtml = Utils
            .tryWithResource(Source.fromURL(workerUrl))(_.getLines().mkString("\n"))
          workerHtml should include ("Spark Worker at")
          workerHtml should include ("Running Executors (0)")
        }
      }
    } finally {
      localCluster.stop()
    }
  }

  test("master/worker web ui available with reverseProxy") {
    implicit val formats = org.json4s.DefaultFormats
    val conf = new SparkConf()
    conf.set(UI_REVERSE_PROXY, true)
    val localCluster = new LocalSparkCluster(2, 2, 512, conf)
    localCluster.start()
    val masterUrl = s"http://localhost:${localCluster.masterWebUIPort}"
    try {
      eventually(timeout(5.seconds), interval(100.milliseconds)) {
        val json = Utils
          .tryWithResource(Source.fromURL(s"$masterUrl/json"))(_.getLines().mkString("\n"))
        val JArray(workers) = (parse(json) \ "workers")
        workers.size should be (2)
        workers.foreach { workerSummaryJson =>
          // the webuiaddress intentionally points to the local web ui.
          // explicitly construct reverse proxy url targeting the master
          val JString(workerId) = workerSummaryJson \ "id"
          val url = s"$masterUrl/proxy/${workerId}/json"
          val workerResponse = parse(
            Utils.tryWithResource(Source.fromURL(url))(_.getLines().mkString("\n")))
          (workerResponse \ "cores").extract[Int] should be (2)
        }

        val html = Utils
          .tryWithResource(Source.fromURL(s"$masterUrl/"))(_.getLines().mkString("\n"))
        html should include ("Spark Master at spark://")
        html should include ("""href="/static""")
        html should include ("""src="/static""")
        verifyWorkerUI(html, masterUrl)
      }
    } finally {
      localCluster.stop()
      System.getProperties().remove("spark.ui.proxyBase")
    }
  }

  test("master/worker web ui available behind front-end reverseProxy") {
    implicit val formats = org.json4s.DefaultFormats
    val reverseProxyUrl = "http://proxyhost:8080/path/to/spark"
    val conf = new SparkConf()
    conf.set(UI_REVERSE_PROXY, true)
    conf.set(UI_REVERSE_PROXY_URL, reverseProxyUrl)
    val localCluster = new LocalSparkCluster(2, 2, 512, conf)
    localCluster.start()
    val masterUrl = s"http://localhost:${localCluster.masterWebUIPort}"
    try {
      eventually(timeout(5.seconds), interval(100.milliseconds)) {
        val json = Utils
          .tryWithResource(Source.fromURL(s"$masterUrl/json"))(_.getLines().mkString("\n"))
        val JArray(workers) = (parse(json) \ "workers")
        workers.size should be (2)
        workers.foreach { workerSummaryJson =>
          // the webuiaddress intentionally points to the local web ui.
          // explicitly construct reverse proxy url targeting the master
          val JString(workerId) = workerSummaryJson \ "id"
          val url = s"$masterUrl/proxy/${workerId}/json"
          val workerResponse = parse(Utils
            .tryWithResource(Source.fromURL(url))(_.getLines().mkString("\n")))
          (workerResponse \ "cores").extract[Int] should be (2)
          (workerResponse \ "masterwebuiurl").extract[String] should be (reverseProxyUrl + "/")
        }

        System.getProperty("spark.ui.proxyBase") should be (reverseProxyUrl)
        val html = Utils
          .tryWithResource(Source.fromURL(s"$masterUrl/"))(_.getLines().mkString("\n"))
        html should include ("Spark Master at spark://")
        verifyStaticResourcesServedByProxy(html, reverseProxyUrl)
        verifyWorkerUI(html, masterUrl, reverseProxyUrl)
      }
    } finally {
      localCluster.stop()
      System.getProperties().remove("spark.ui.proxyBase")
    }
  }

  private def verifyWorkerUI(masterHtml: String, masterUrl: String,
      reverseProxyUrl: String = ""): Unit = {
    val workerLinks = (WORKER_LINK_RE findAllMatchIn masterHtml).toList
    workerLinks.size should be (2)
    workerLinks foreach {
      case WORKER_LINK_RE(workerUrl, workerId) =>
        workerUrl should be (s"$reverseProxyUrl/proxy/$workerId")
        // there is no real front-end proxy as defined in $reverseProxyUrl
        // construct url directly targeting the master
        val url = s"$masterUrl/proxy/$workerId/"
        System.setProperty("spark.ui.proxyBase", workerUrl)
        val workerHtml = Utils
          .tryWithResource(Source.fromURL(url))(_.getLines().mkString("\n"))
        workerHtml should include ("Spark Worker at")
        workerHtml should include ("Running Executors (0)")
        verifyStaticResourcesServedByProxy(workerHtml, workerUrl)
      case _ => fail  // make sure we don't accidentially skip the tests
    }
  }

  private def verifyStaticResourcesServedByProxy(html: String, proxyUrl: String): Unit = {
    html should not include ("""href="/static""")
    html should include (s"""href="$proxyUrl/static""")
    html should not include ("""src="/static""")
    html should include (s"""src="$proxyUrl/static""")
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

  private val _scheduleExecutorsOnWorkers =
    PrivateMethod[Array[Int]](Symbol("scheduleExecutorsOnWorkers"))
  private val _drivers = PrivateMethod[HashSet[DriverInfo]](Symbol("drivers"))
  private val _state = PrivateMethod[RecoveryState.Value](Symbol("state"))

  private val workerInfo = makeWorkerInfo(4096, 10)
  private val workerInfos = Array(workerInfo, workerInfo, workerInfo)

  private def makeMaster(conf: SparkConf = new SparkConf): Master = {
    assert(_master === null, "Some Master's RpcEnv is leaked in tests")
    val securityMgr = new SecurityManager(conf)
    val rpcEnv = RpcEnv.create(Master.SYSTEM_NAME, "localhost", 0, conf, securityMgr)
    _master = new Master(rpcEnv, rpcEnv.address, 0, securityMgr, conf)
    _master
  }

  def makeAliveMaster(conf: SparkConf = new SparkConf): Master = {
    val master = makeMaster(conf)
    master.rpcEnv.setupEndpoint(Master.ENDPOINT_NAME, master)
    eventually(timeout(10.seconds)) {
      val masterState = master.self.askSync[MasterStateResponse](RequestMasterState)
      assert(masterState.status === RecoveryState.ALIVE, "Master is not alive")
    }
    master
  }

  private def makeAppInfo(
      memoryPerExecutorMb: Int,
      coresPerExecutor: Option[Int] = None,
      maxCores: Option[Int] = None): ApplicationInfo = {
    val desc = new ApplicationDescription(
      "test", maxCores, memoryPerExecutorMb, null, "", None, None, coresPerExecutor)
    val appId = System.currentTimeMillis.toString
    val endpointRef = mock(classOf[RpcEndpointRef])
    val mockAddress = mock(classOf[RpcAddress])
    when(endpointRef.address).thenReturn(mockAddress)
    new ApplicationInfo(0, appId, desc, new Date, endpointRef, Int.MaxValue)
  }

  private def makeWorkerInfo(memoryMb: Int, cores: Int): WorkerInfo = {
    val workerId = System.currentTimeMillis.toString
    val endpointRef = mock(classOf[RpcEndpointRef])
    val mockAddress = mock(classOf[RpcAddress])
    when(endpointRef.address).thenReturn(mockAddress)
    new WorkerInfo(workerId, "host", 100, cores, memoryMb,
      endpointRef, "http://localhost:80", Map.empty)
  }

  private def scheduleExecutorsOnWorkers(
      master: Master,
      appInfo: ApplicationInfo,
      workerInfos: Array[WorkerInfo],
      spreadOut: Boolean): Array[Int] = {
    master.invokePrivate(_scheduleExecutorsOnWorkers(appInfo, workerInfos, spreadOut))
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
      1024,
      "http://localhost:8080",
      RpcAddress("localhost", 9999)))
    val executors = (0 until 3).map { i =>
      new ExecutorDescription(appId = i.toString, execId = i, 2, ExecutorState.RUNNING)
    }
    master.self.send(WorkerLatestState("1", executors, driverIds = Seq("0", "1", "2")))

    eventually(timeout(10.seconds)) {
      assert(killedExecutors.asScala.toList.sorted === List("0" -> 0, "1" -> 1, "2" -> 2))
      assert(killedDrivers.asScala.toList.sorted === List("0", "1", "2"))
    }
  }

  test("SPARK-20529: Master should reply the address received from worker") {
    val master = makeAliveMaster()
    @volatile var receivedMasterAddress: RpcAddress = null
    val fakeWorker = master.rpcEnv.setupEndpoint("worker", new RpcEndpoint {
      override val rpcEnv: RpcEnv = master.rpcEnv

      override def receive: PartialFunction[Any, Unit] = {
        case RegisteredWorker(_, _, masterAddress, _) =>
          receivedMasterAddress = masterAddress
      }
    })

    master.self.send(RegisterWorker(
      "1",
      "localhost",
      9999,
      fakeWorker,
      10,
      1024,
      "http://localhost:8080",
      RpcAddress("localhost2", 10000)))

    eventually(timeout(10.seconds)) {
      assert(receivedMasterAddress === RpcAddress("localhost2", 10000))
    }
  }

  test("SPARK-27510: Master should avoid dead loop while launching executor failed in Worker") {
    val master = makeAliveMaster()
    var worker: MockExecutorLaunchFailWorker = null
    try {
      val conf = new SparkConf()
      // SPARK-32250: When running test on GitHub Action machine, the available processors in JVM
      // is only 2, while on Jenkins it's 32. For this specific test, 2 available processors, which
      // also decides number of threads in Dispatcher, is not enough to consume the messages. In
      // the worst situation, MockExecutorLaunchFailWorker would occupy these 2 threads for
      // handling messages LaunchDriver, LaunchExecutor at the same time but leave no thread for
      // the driver to handle the message RegisteredApplication. At the end, it results in the dead
      // lock situation. Therefore, we need to set more threads to avoid the dead lock.
      conf.set(Network.RPC_NETTY_DISPATCHER_NUM_THREADS, 6)
      worker = new MockExecutorLaunchFailWorker(master, conf)
      worker.rpcEnv.setupEndpoint("worker", worker)
      val workerRegMsg = RegisterWorker(
        worker.id,
        "localhost",
        9999,
        worker.self,
        10,
        1234 * 3,
        "http://localhost:8080",
        master.rpcEnv.address)
      master.self.send(workerRegMsg)
      val driver = DeployTestUtils.createDriverDesc()
      // mimic DriverClient to send RequestSubmitDriver to master
      master.self.askSync[SubmitDriverResponse](RequestSubmitDriver(driver))

      // LaunchExecutor message should have been received in worker side
      assert(worker.launchExecutorReceived.await(10, TimeUnit.SECONDS))

      eventually(timeout(10.seconds)) {
        val appIds = worker.appIdsToLaunchExecutor
        // Master would continually launch executors until reach MAX_EXECUTOR_RETRIES
        assert(worker.failedCnt == master.conf.get(MAX_EXECUTOR_RETRIES))
        // Master would remove the app if no executor could be launched for it
        assert(master.idToApp.keySet.intersect(appIds).isEmpty)
      }
    } finally {
      if (worker != null) {
        worker.rpcEnv.shutdown()
      }
      if (master != null) {
        master.rpcEnv.shutdown()
      }
    }
  }

  def testWorkerDecommissioning(
      numWorkers: Int,
      numWorkersExpectedToDecom: Int,
      hostnames: Seq[String]): Unit = {
    val conf = new SparkConf()
    val master = makeAliveMaster(conf)
    val workers = (1 to numWorkers).map { idx =>
      val worker = new MockWorker(master.self, conf)
      worker.rpcEnv.setupEndpoint(s"worker-$idx", worker)
      val workerReg = RegisterWorker(
        worker.id,
        "localhost",
        worker.self.address.port,
        worker.self,
        10,
        1024,
        "http://localhost:8080",
        RpcAddress("localhost", 10000))
      master.self.send(workerReg)
      worker
    }

    eventually(timeout(10.seconds)) {
      val masterState = master.self.askSync[MasterStateResponse](RequestMasterState)
      assert(masterState.workers.length === numWorkers)
      assert(masterState.workers.forall(_.state == WorkerState.ALIVE))
      assert(masterState.workers.map(_.id).toSet == workers.map(_.id).toSet)
    }

    val decomWorkersCount = master.self.askSync[Integer](DecommissionWorkersOnHosts(hostnames))
    assert(decomWorkersCount === numWorkersExpectedToDecom)

    // Decommissioning is actually async ... wait for the workers to actually be decommissioned by
    // polling the master's state.
    eventually(timeout(30.seconds)) {
      val masterState = master.self.askSync[MasterStateResponse](RequestMasterState)
      assert(masterState.workers.length === numWorkers)
      val workersActuallyDecomed = masterState.workers
        .filter(_.state == WorkerState.DECOMMISSIONED).map(_.id)
      val decommissionedWorkers = workers.filter(w => workersActuallyDecomed.contains(w.id))
      assert(workersActuallyDecomed.length === numWorkersExpectedToDecom)
      assert(decommissionedWorkers.forall(_.decommissioned))
    }

    // Decommissioning a worker again should return the same answer since we want this call to be
    // idempotent.
    val decomWorkersCountAgain = master.self.askSync[Integer](DecommissionWorkersOnHosts(hostnames))
    assert(decomWorkersCountAgain === numWorkersExpectedToDecom)
  }

  test("All workers on a host should be decommissioned") {
    testWorkerDecommissioning(2, 2, Seq("LoCalHost", "localHOST"))
  }

  test("No workers should be decommissioned with invalid host") {
    testWorkerDecommissioning(2, 0, Seq("NoSuchHost1", "NoSuchHost2"))
  }

  test("Only worker on host should be decommissioned") {
    testWorkerDecommissioning(1, 1, Seq("lOcalHost", "NoSuchHost"))
  }

  test("SPARK-19900: there should be a corresponding driver for the app after relaunching driver") {
    val conf = new SparkConf().set(WORKER_TIMEOUT, 1L)
    val master = makeAliveMaster(conf)
    var worker1: MockWorker = null
    var worker2: MockWorker = null
    try {
      worker1 = new MockWorker(master.self)
      worker1.rpcEnv.setupEndpoint("worker", worker1)
      val worker1Reg = RegisterWorker(
        worker1.id,
        "localhost",
        9998,
        worker1.self,
        10,
        1024,
        "http://localhost:8080",
        RpcAddress("localhost2", 10000))
      master.self.send(worker1Reg)
      val driver = DeployTestUtils.createDriverDesc().copy(supervise = true)
      master.self.askSync[SubmitDriverResponse](RequestSubmitDriver(driver))

      eventually(timeout(10.seconds)) {
        assert(worker1.apps.nonEmpty)
      }

      eventually(timeout(10.seconds)) {
        val masterState = master.self.askSync[MasterStateResponse](RequestMasterState)
        assert(masterState.workers(0).state == WorkerState.DEAD)
      }

      worker2 = new MockWorker(master.self)
      worker2.rpcEnv.setupEndpoint("worker", worker2)
      master.self.send(RegisterWorker(
        worker2.id,
        "localhost",
        9999,
        worker2.self,
        10,
        1024,
        "http://localhost:8081",
        RpcAddress("localhost", 10001)))
      eventually(timeout(10.seconds)) {
        assert(worker2.apps.nonEmpty)
      }

      master.self.send(worker1Reg)
      eventually(timeout(10.seconds)) {
        val masterState = master.self.askSync[MasterStateResponse](RequestMasterState)

        val worker = masterState.workers.filter(w => w.id == worker1.id)
        assert(worker.length == 1)
        // make sure the `DriverStateChanged` arrives at Master.
        assert(worker(0).drivers.isEmpty)
        assert(worker1.apps.isEmpty)
        assert(worker1.drivers.isEmpty)
        assert(worker2.apps.size == 1)
        assert(worker2.drivers.size == 1)
        assert(masterState.activeDrivers.length == 1)
        assert(masterState.activeApps.length == 1)
      }
    } finally {
      if (worker1 != null) {
        worker1.rpcEnv.shutdown()
      }
      if (worker2 != null) {
        worker2.rpcEnv.shutdown()
      }
    }
  }

  test("assign/recycle resources to/from driver") {
    val master = makeAliveMaster()
    val masterRef = master.self
    val resourceReqs = Seq(ResourceRequirement(GPU, 3), ResourceRequirement(FPGA, 3))
    val driver = DeployTestUtils.createDriverDesc().copy(resourceReqs = resourceReqs)
    val driverId = masterRef.askSync[SubmitDriverResponse](
      RequestSubmitDriver(driver)).driverId.get
    var status = masterRef.askSync[DriverStatusResponse](RequestDriverStatus(driverId))
    assert(status.state === Some(DriverState.SUBMITTED))
    val worker = new MockWorker(masterRef)
    worker.rpcEnv.setupEndpoint(s"worker", worker)
    val resources = Map(GPU -> new ResourceInformation(GPU, Array("0", "1", "2")),
      FPGA -> new ResourceInformation(FPGA, Array("f1", "f2", "f3")))
    val regMsg = RegisterWorker(worker.id, "localhost", 7077, worker.self, 10, 1024,
      "http://localhost:8080", RpcAddress("localhost", 10000), resources)
    masterRef.send(regMsg)
    eventually(timeout(10.seconds)) {
      status = masterRef.askSync[DriverStatusResponse](RequestDriverStatus(driverId))
      assert(status.state === Some(DriverState.RUNNING))
      assert(worker.drivers.head === driverId)
      assert(worker.driverResources(driverId) === Map(GPU -> Set("0", "1", "2"),
        FPGA -> Set("f1", "f2", "f3")))
      val workerResources = master.workers.head.resources
      assert(workerResources(GPU).availableAddrs.length === 0)
      assert(workerResources(GPU).assignedAddrs.toSet === Set("0", "1", "2"))
      assert(workerResources(FPGA).availableAddrs.length === 0)
      assert(workerResources(FPGA).assignedAddrs.toSet === Set("f1", "f2", "f3"))
    }
    val driverFinished = DriverStateChanged(driverId, DriverState.FINISHED, None)
    masterRef.send(driverFinished)
    eventually(timeout(10.seconds)) {
      val workerResources = master.workers.head.resources
      assert(workerResources(GPU).availableAddrs.length === 3)
      assert(workerResources(GPU).assignedAddrs.toSet === Set())
      assert(workerResources(FPGA).availableAddrs.length === 3)
      assert(workerResources(FPGA).assignedAddrs.toSet === Set())
    }
  }

  test("assign/recycle resources to/from executor") {

    def makeWorkerAndRegister(
        master: RpcEndpointRef,
        workerResourceReqs: Map[String, Int] = Map.empty)
    : MockWorker = {
      val worker = new MockWorker(master)
      worker.rpcEnv.setupEndpoint(s"worker", worker)
      val resources = workerResourceReqs.map { case (rName, amount) =>
        val shortName = rName.charAt(0)
        val addresses = (0 until amount).map(i => s"$shortName$i").toArray
        rName -> new ResourceInformation(rName, addresses)
      }
      val reg = RegisterWorker(worker.id, "localhost", 8077, worker.self, 10, 2048,
        "http://localhost:8080", RpcAddress("localhost", 10000), resources)
      master.send(reg)
      worker
    }

    val master = makeAliveMaster()
    val masterRef = master.self
    val resourceReqs = Seq(ResourceRequirement(GPU, 3), ResourceRequirement(FPGA, 3))
    val worker = makeWorkerAndRegister(masterRef, Map(GPU -> 6, FPGA -> 6))
    worker.appDesc = worker.appDesc.copy(resourceReqsPerExecutor = resourceReqs)
    val driver = DeployTestUtils.createDriverDesc().copy(resourceReqs = resourceReqs)
    val driverId = masterRef.askSync[SubmitDriverResponse](RequestSubmitDriver(driver)).driverId
    val status = masterRef.askSync[DriverStatusResponse](RequestDriverStatus(driverId.get))
    assert(status.state === Some(DriverState.RUNNING))
    val workerResources = master.workers.head.resources
    eventually(timeout(10.seconds)) {
      assert(workerResources(GPU).availableAddrs.length === 0)
      assert(workerResources(FPGA).availableAddrs.length === 0)
      assert(worker.driverResources.size === 1)
      assert(worker.execResources.size === 1)
      val driverResources = worker.driverResources.head._2
      val execResources = worker.execResources.head._2
      val gpuAddrs = driverResources(GPU).union(execResources(GPU))
      val fpgaAddrs = driverResources(FPGA).union(execResources(FPGA))
      assert(gpuAddrs === Set("g0", "g1", "g2", "g3", "g4", "g5"))
      assert(fpgaAddrs === Set("f0", "f1", "f2", "f3", "f4", "f5"))
    }
    val appId = worker.apps.head._1
    masterRef.send(UnregisterApplication(appId))
    masterRef.send(DriverStateChanged(driverId.get, DriverState.FINISHED, None))
    eventually(timeout(10.seconds)) {
      assert(workerResources(GPU).availableAddrs.length === 6)
      assert(workerResources(FPGA).availableAddrs.length === 6)
    }
  }

  private def getDrivers(master: Master): HashSet[DriverInfo] = {
    master.invokePrivate(_drivers())
  }

  private def getState(master: Master): RecoveryState.Value = {
    master.invokePrivate(_state())
  }
}

private class FakeRecoveryModeFactory(conf: SparkConf, ser: serializer.Serializer)
    extends StandaloneRecoveryModeFactory(conf, ser) {
  import FakeRecoveryModeFactory.persistentData

  override def createPersistenceEngine(): PersistenceEngine = new PersistenceEngine {

    override def unpersist(name: String): Unit = {
      persistentData.remove(name)
    }

    override def persist(name: String, obj: Object): Unit = {
      persistentData(name) = obj
    }

    override def read[T: ClassTag](prefix: String): Seq[T] = {
      persistentData.filter(_._1.startsWith(prefix)).map(_._2.asInstanceOf[T]).toSeq
    }
  }

  override def createLeaderElectionAgent(master: LeaderElectable): LeaderElectionAgent = {
    new MonarchyLeaderAgent(master)
  }
}

private object FakeRecoveryModeFactory {
  val persistentData = new HashMap[String, Object]()
}
