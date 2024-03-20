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
import java.util.concurrent.{CountDownLatch, TimeUnit}
import java.util.concurrent.atomic.AtomicInteger

import scala.collection.mutable
import scala.collection.mutable.{HashMap, HashSet}
import scala.concurrent.duration._
import scala.io.Source
import scala.reflect.ClassTag

import org.mockito.ArgumentMatchers.any
import org.mockito.Mockito._
import org.scalatest.{BeforeAndAfter, PrivateMethodTester}
import org.scalatest.concurrent.Eventually
import org.scalatest.matchers.must.Matchers
import org.scalatest.matchers.should.Matchers._

import org.apache.spark.{serializer, SecurityManager, SparkConf, SparkFunSuite}
import org.apache.spark.deploy._
import org.apache.spark.deploy.DeployMessages._
import org.apache.spark.internal.config.Deploy._
import org.apache.spark.resource.ResourceProfile.DEFAULT_RESOURCE_PROFILE_ID
import org.apache.spark.rpc.{RpcAddress, RpcEndpoint, RpcEndpointRef, RpcEnv}
import org.apache.spark.util.Utils

object MockWorker {
  val counter = new AtomicInteger(10000)
}

class MockWorker(master: RpcEndpointRef, conf: SparkConf = new SparkConf) extends RpcEndpoint {
  val seq = MockWorker.counter.incrementAndGet()
  val id = seq.toString
  // Use port 0 to start the server with a random free port
  override val rpcEnv: RpcEnv = RpcEnv.create("worker", "localhost", 0,
    conf, new SecurityManager(conf))
  val apps = new mutable.HashMap[String, String]()
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
  val launchedExecutors = new mutable.HashMap[String, LaunchExecutor]
  override def receive: PartialFunction[Any, Unit] = {
    case RegisteredWorker(masterRef, _, _, _) =>
      masterRef.send(WorkerLatestState(id, Nil, drivers.toSeq))
    case l @ LaunchExecutor(_, appId, execId, _, _, _, _, resources_) =>
      execResources(appId + "/" + execId) = resources_.map(r => (r._1, r._2.addresses.toSet))
      launchedExecutors(appId + "/" + execId) = l
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
    case LaunchExecutor(_, appId, execId, _, _, _, _, _) =>
      assert(appRegistered.await(10, TimeUnit.SECONDS))

      if (failedCnt == 0) {
        launchExecutorReceived.countDown()
      }
      assert(master.idToApp.contains(appId))
      appIdsToLaunchExecutor += appId
      failedCnt += 1
      master.self.askSync(ExecutorStateChanged(appId, execId,
        ExecutorState.FAILED, None, None))

    case otherMsg => super.receive(otherMsg)
  }
}

trait MasterSuiteBase extends SparkFunSuite
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

  protected def verifyWorkerUI(masterHtml: String, masterUrl: String,
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
      case _ => fail()  // make sure we don't accidentially skip the tests
    }
  }

  protected def verifyStaticResourcesServedByProxy(html: String, proxyUrl: String): Unit = {
    html should not include ("""href="/static""")
    html should include (s"""href="$proxyUrl/static""")
    html should not include ("""src="/static""")
    html should include (s"""src="$proxyUrl/static""")
  }

  protected def verifyDrivers(
      spreadOut: Boolean, answer1: Int, answer2: Int, answer3: Int): Unit = {
    val master = makeMaster(new SparkConf().set(SPREAD_OUT_DRIVERS, spreadOut))
    val worker1 = makeWorkerInfo(512, 10)
    val worker2 = makeWorkerInfo(512, 10)
    val worker3 = makeWorkerInfo(512, 10)
    master.state = RecoveryState.ALIVE
    master.workers += worker1
    master.workers += worker2
    master.workers += worker3
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
    assert(worker1.drivers.size === answer1)
    assert(worker2.drivers.size === answer2)
    assert(worker3.drivers.size === answer3)
  }

  protected def scheduleExecutorsForAppWithMultiRPs(withMaxCores: Boolean): Unit = {
    val appInfo: ApplicationInfo = if (withMaxCores) {
      makeAppInfo(
        128, maxCores = Some(4), initialExecutorLimit = Some(0))
    } else {
      makeAppInfo(
        128, maxCores = None, initialExecutorLimit = Some(0))
    }

    val master = makeAliveMaster()
    val conf = new SparkConf()
    val workers = (1 to 3).map { idx =>
      val worker = new MockWorker(master.self, conf)
      worker.rpcEnv.setupEndpoint(s"worker-$idx", worker)
      val workerReg = RegisterWorker(
        worker.id,
        "localhost",
        worker.self.address.port,
        worker.self,
        2,
        512,
        "http://localhost:8080",
        RpcAddress("localhost", 10000))
      master.self.send(workerReg)
      worker
    }

    // Register app and schedule.
    master.registerApplication(appInfo)
    startExecutorsOnWorkers(master)
    assert(appInfo.executors.isEmpty)

    // Request executors with multiple resource profile.
    // rp1 with 15 cores per executor, rp2 with 8192MB memory per executor, no worker can
    // fulfill the resource requirement.
    val rp1 = DeployTestUtils.createResourceProfile(Some(256), Map.empty, Some(3))
    val rp2 = DeployTestUtils.createResourceProfile(Some(1024), Map.empty, Some(1))
    val rp3 = DeployTestUtils.createResourceProfile(Some(256), Map.empty, Some(1))
    val rp4 = DeployTestUtils.createResourceProfile(Some(256), Map.empty, Some(1))
    val requests = Map(
      appInfo.desc.defaultProfile -> 1,
      rp1 -> 1,
      rp2 -> 1,
      rp3 -> 1,
      rp4 -> 2
    )
    eventually(timeout(10.seconds)) {
      master.self.askSync[Boolean](RequestExecutors(appInfo.id, requests))
      assert(appInfo.executors.size === workers.map(_.launchedExecutors.size).sum)
    }

    if (withMaxCores) {
      assert(appInfo.executors.size === 3)
      assert(appInfo.getOrUpdateExecutorsForRPId(DEFAULT_RESOURCE_PROFILE_ID).size === 1)
      assert(appInfo.getOrUpdateExecutorsForRPId(rp1.id).size === 0)
      assert(appInfo.getOrUpdateExecutorsForRPId(rp2.id).size === 0)
      assert(appInfo.getOrUpdateExecutorsForRPId(rp3.id).size === 1)
      assert(appInfo.getOrUpdateExecutorsForRPId(rp4.id).size === 1)
    } else {
      assert(appInfo.executors.size === 4)
      assert(appInfo.getOrUpdateExecutorsForRPId(DEFAULT_RESOURCE_PROFILE_ID).size === 1)
      assert(appInfo.getOrUpdateExecutorsForRPId(rp1.id).size === 0)
      assert(appInfo.getOrUpdateExecutorsForRPId(rp2.id).size === 0)
      assert(appInfo.getOrUpdateExecutorsForRPId(rp3.id).size === 1)
      assert(appInfo.getOrUpdateExecutorsForRPId(rp4.id).size === 2)
    }

    // Verify executor information.
    val executorForRp3 = appInfo.executors(appInfo.getOrUpdateExecutorsForRPId(rp3.id).head)
    assert(executorForRp3.cores === 1)
    assert(executorForRp3.memory === 256)
    assert(executorForRp3.rpId === rp3.id)

    // Verify LaunchExecutor message.
    val launchExecutorMsg = workers
      .find(_.id === executorForRp3.worker.id)
      .map(_.launchedExecutors(appInfo.id + "/" + executorForRp3.id))
      .get
    assert(launchExecutorMsg.cores === 1)
    assert(launchExecutorMsg.memory === 256)
    assert(launchExecutorMsg.rpId === rp3.id)
  }

  protected def basicScheduling(spreadOut: Boolean): Unit = {
    val master = makeMaster()
    val appInfo = makeAppInfo(128)
    val scheduledCores = scheduleExecutorsOnWorkers(master, appInfo, workerInfos, spreadOut)
    assert(scheduledCores === Array(10, 10, 10))
  }

  protected def basicSchedulingWithMoreMemory(spreadOut: Boolean): Unit = {
    val master = makeMaster()
    val appInfo = makeAppInfo(384)
    val scheduledCores = scheduleExecutorsOnWorkers(master, appInfo, workerInfos, spreadOut)
    assert(scheduledCores === Array(10, 10, 10))
  }

  protected def schedulingWithMaxCores(spreadOut: Boolean): Unit = {
    val master = makeMaster()
    val appInfo1 = makeAppInfo(128, maxCores = Some(8))
    val appInfo2 = makeAppInfo(128, maxCores = Some(16))
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

  protected def schedulingWithCoresPerExecutor(spreadOut: Boolean): Unit = {
    val master = makeMaster()
    val appInfo1 = makeAppInfo(128, coresPerExecutor = Some(2))
    val appInfo2 = makeAppInfo(32, coresPerExecutor = Some(2))
    val appInfo3 = makeAppInfo(32, coresPerExecutor = Some(3))
    val scheduledCores1 = scheduleExecutorsOnWorkers(master, appInfo1, workerInfos, spreadOut)
    val scheduledCores2 = scheduleExecutorsOnWorkers(master, appInfo2, workerInfos, spreadOut)
    val scheduledCores3 = scheduleExecutorsOnWorkers(master, appInfo3, workerInfos, spreadOut)
    assert(scheduledCores1 === Array(8, 8, 8)) // 4 * 2 because of memory limits
    assert(scheduledCores2 === Array(10, 10, 10)) // 5 * 2
    assert(scheduledCores3 === Array(9, 9, 9)) // 3 * 3
  }

  // Sorry for the long method name!
  protected def schedulingWithCoresPerExecutorAndMaxCores(spreadOut: Boolean): Unit = {
    val master = makeMaster()
    val appInfo1 = makeAppInfo(32, coresPerExecutor = Some(2), maxCores = Some(4))
    val appInfo2 = makeAppInfo(32, coresPerExecutor = Some(2), maxCores = Some(20))
    val appInfo3 = makeAppInfo(32, coresPerExecutor = Some(3), maxCores = Some(20))
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

  protected def schedulingWithExecutorLimit(spreadOut: Boolean): Unit = {
    val master = makeMaster()
    val appInfo = makeAppInfo(32)
    appInfo.requestExecutors(Map(appInfo.desc.defaultProfile -> 0))
    val scheduledCores1 = scheduleExecutorsOnWorkers(master, appInfo, workerInfos, spreadOut)
    appInfo.requestExecutors(Map(appInfo.desc.defaultProfile -> 2))
    val scheduledCores2 = scheduleExecutorsOnWorkers(master, appInfo, workerInfos, spreadOut)
    appInfo.requestExecutors(Map(appInfo.desc.defaultProfile -> 5))
    val scheduledCores3 = scheduleExecutorsOnWorkers(master, appInfo, workerInfos, spreadOut)
    assert(scheduledCores1 === Array(0, 0, 0))
    assert(scheduledCores2 === Array(10, 10, 0))
    assert(scheduledCores3 === Array(10, 10, 10))
  }

  protected def schedulingWithExecutorLimitAndMaxCores(spreadOut: Boolean): Unit = {
    val master = makeMaster()
    val appInfo = makeAppInfo(32, maxCores = Some(16))
    appInfo.requestExecutors(Map(appInfo.desc.defaultProfile -> 0))
    val scheduledCores1 = scheduleExecutorsOnWorkers(master, appInfo, workerInfos, spreadOut)
    appInfo.requestExecutors(Map(appInfo.desc.defaultProfile -> 2))
    val scheduledCores2 = scheduleExecutorsOnWorkers(master, appInfo, workerInfos, spreadOut)
    appInfo.requestExecutors(Map(appInfo.desc.defaultProfile -> 5))
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

  protected def schedulingWithExecutorLimitAndCoresPerExecutor(spreadOut: Boolean): Unit = {
    val master = makeMaster()
    val appInfo = makeAppInfo(32, coresPerExecutor = Some(4))
    appInfo.requestExecutors(Map(appInfo.desc.defaultProfile -> 0))
    val scheduledCores1 = scheduleExecutorsOnWorkers(master, appInfo, workerInfos, spreadOut)
    appInfo.requestExecutors(Map(appInfo.desc.defaultProfile -> 2))
    val scheduledCores2 = scheduleExecutorsOnWorkers(master, appInfo, workerInfos, spreadOut)
    appInfo.requestExecutors(Map(appInfo.desc.defaultProfile -> 5))
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
  protected def schedulingWithEverything(spreadOut: Boolean): Unit = {
    val master = makeMaster()
    val appInfo = makeAppInfo(32, coresPerExecutor = Some(4), maxCores = Some(18))
    appInfo.requestExecutors(Map(appInfo.desc.defaultProfile -> 0))
    val scheduledCores1 = scheduleExecutorsOnWorkers(master, appInfo, workerInfos, spreadOut)
    appInfo.requestExecutors(Map(appInfo.desc.defaultProfile -> 2))
    val scheduledCores2 = scheduleExecutorsOnWorkers(master, appInfo, workerInfos, spreadOut)
    appInfo.requestExecutors(Map(appInfo.desc.defaultProfile -> 5))
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

  protected val _schedule = PrivateMethod[Unit](Symbol("schedule"))
  private val _scheduleExecutorsOnWorkers =
    PrivateMethod[Array[Int]](Symbol("scheduleExecutorsOnWorkers"))
  private val _startExecutorsOnWorkers =
    PrivateMethod[Unit](Symbol("startExecutorsOnWorkers"))
  private val _drivers = PrivateMethod[HashSet[DriverInfo]](Symbol("drivers"))
  protected val _waitingDrivers =
    PrivateMethod[mutable.ArrayBuffer[DriverInfo]](Symbol("waitingDrivers"))
  private val _state = PrivateMethod[RecoveryState.Value](Symbol("state"))
  protected val _newDriverId = PrivateMethod[String](Symbol("newDriverId"))
  protected val _newApplicationId = PrivateMethod[String](Symbol("newApplicationId"))
  protected val _createApplication = PrivateMethod[ApplicationInfo](Symbol("createApplication"))
  protected val _persistenceEngine = PrivateMethod[PersistenceEngine](Symbol("persistenceEngine"))

  protected val workerInfo = makeWorkerInfo(512, 10)
  private val workerInfos = Array(workerInfo, workerInfo, workerInfo)

  protected def makeMaster(conf: SparkConf = new SparkConf): Master = {
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

  protected def makeAppInfo(
      memoryPerExecutorMb: Int,
      coresPerExecutor: Option[Int] = None,
      maxCores: Option[Int] = None,
      customResources: Map[String, Int] = Map.empty,
      initialExecutorLimit: Option[Int] = None): ApplicationInfo = {
    val rp = DeployTestUtils.createDefaultResourceProfile(
      memoryPerExecutorMb, customResources, coresPerExecutor)

    val desc = new ApplicationDescription(
      "test", maxCores, null, "", rp, None, None, initialExecutorLimit)
    val appId = System.nanoTime().toString
    val endpointRef = mock(classOf[RpcEndpointRef])
    val mockAddress = mock(classOf[RpcAddress])
    when(endpointRef.address).thenReturn(mockAddress)
    doNothing().when(endpointRef).send(any())
    new ApplicationInfo(0, appId, desc, new Date, endpointRef, Int.MaxValue)
  }

  protected def makeWorkerInfo(memoryMb: Int, cores: Int): WorkerInfo = {
    val workerId = System.nanoTime().toString
    val endpointRef = mock(classOf[RpcEndpointRef])
    val mockAddress = mock(classOf[RpcAddress])
    when(endpointRef.address).thenReturn(mockAddress)
    new WorkerInfo(workerId, "host", 100, cores, memoryMb,
      endpointRef, "http://localhost:80", Map.empty)
  }

  // Schedule executors for default resource profile.
  private def scheduleExecutorsOnWorkers(
      master: Master,
      appInfo: ApplicationInfo,
      workerInfos: Array[WorkerInfo],
      spreadOut: Boolean): Array[Int] = {
    val defaultResourceDesc = appInfo.getResourceDescriptionForRpId(DEFAULT_RESOURCE_PROFILE_ID)
    master.invokePrivate(_scheduleExecutorsOnWorkers(
      appInfo, DEFAULT_RESOURCE_PROFILE_ID, defaultResourceDesc, workerInfos, spreadOut))
  }

  protected def startExecutorsOnWorkers(master: Master): Unit = {
    master.invokePrivate(_startExecutorsOnWorkers())
  }

  protected def testWorkerDecommissioning(
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
        128,
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

  protected def getDrivers(master: Master): HashSet[DriverInfo] = {
    master.invokePrivate(_drivers())
  }

  protected def getState(master: Master): RecoveryState.Value = {
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
