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
import java.util.concurrent.TimeUnit

import scala.concurrent.duration._

import org.mockito.ArgumentMatchers.{eq => meq}
import org.mockito.Mockito.{times, verify, when}
import org.scalatest.matchers.should.Matchers._
import org.scalatestplus.mockito.MockitoSugar.{mock => smock}
import other.supplier.{CustomPersistenceEngine, CustomRecoveryModeFactory}

import org.apache.spark.SparkConf
import org.apache.spark.deploy._
import org.apache.spark.deploy.DeployMessages._
import org.apache.spark.internal.config._
import org.apache.spark.internal.config.Deploy._
import org.apache.spark.internal.config.Worker.WORKER_TIMEOUT
import org.apache.spark.io.LZ4CompressionCodec
import org.apache.spark.resource.{ResourceInformation, ResourceProfile, ResourceRequirement}
import org.apache.spark.resource.ResourceUtils.{FPGA, GPU}
import org.apache.spark.rpc.{RpcAddress, RpcEndpoint, RpcEndpointRef, RpcEnv}
import org.apache.spark.serializer.JavaSerializer

class RecoverySuite extends MasterSuiteBase {
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
        command = commandToPersist,
        appUiUrl = "",
        defaultProfile = DeployTestUtils.defaultResourceProfile,
        eventLogDir = None,
        eventLogCodec = None),
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

  test("SPARK-46664: master should recover quickly in case of zero workers and apps") {
    val conf = new SparkConf(loadDefaults = false)
    conf.set(RECOVERY_MODE, "CUSTOM")
    conf.set(RECOVERY_MODE_FACTORY, classOf[FakeRecoveryModeFactory].getCanonicalName)
    conf.set(MASTER_REST_SERVER_ENABLED, false)

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
    FakeRecoveryModeFactory.persistentData.put(s"driver_${fakeDriverInfo.id}", fakeDriverInfo)

    var master: Master = null
    try {
      master = makeMaster(conf)
      master.rpcEnv.setupEndpoint(Master.ENDPOINT_NAME, master)
      eventually(timeout(2.seconds), interval(100.milliseconds)) {
        getState(master) should be(RecoveryState.ALIVE)
      }
      master.workers.size should be(0)
    } finally {
      if (master != null) {
        master.rpcEnv.shutdown()
        master.rpcEnv.awaitTermination()
        master = null
        FakeRecoveryModeFactory.persistentData.clear()
      }
    }
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
      val rpId = ResourceProfile.DEFAULT_RESOURCE_PROFILE_ID
      val fakeExecutors = List(
        new ExecutorDescription(fakeAppInfo.id, 0, rpId, 8, 1024, ExecutorState.RUNNING),
        new ExecutorDescription(fakeAppInfo.id, 0, rpId, 7, 1024, ExecutorState.RUNNING))

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
        workerResourceReqs: Map[String, Int] = Map.empty): MockWorker = {
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
    worker.appDesc = DeployTestUtils.createAppDesc(Map(GPU -> 3, FPGA -> 3))
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

  test("SPARK-46216: Recovery without compression") {
    val conf = new SparkConf(loadDefaults = false)
    conf.set(RECOVERY_MODE, "FILESYSTEM")
    conf.set(RECOVERY_DIRECTORY, System.getProperty("java.io.tmpdir"))

    var master: Master = null
    try {
      master = makeAliveMaster(conf)
      val e = master.persistenceEngine.asInstanceOf[FileSystemPersistenceEngine]
      assert(e.codec.isEmpty)
    } finally {
      if (master != null) {
        master.rpcEnv.shutdown()
        master.rpcEnv.awaitTermination()
        master = null
      }
    }
  }

  test("SPARK-46216: Recovery with compression") {
    val conf = new SparkConf(loadDefaults = false)
    conf.set(RECOVERY_MODE, "FILESYSTEM")
    conf.set(RECOVERY_DIRECTORY, System.getProperty("java.io.tmpdir"))
    conf.set(RECOVERY_COMPRESSION_CODEC, "lz4")

    var master: Master = null
    try {
      master = makeAliveMaster(conf)
      val e = master.persistenceEngine.asInstanceOf[FileSystemPersistenceEngine]
      assert(e.codec.get.isInstanceOf[LZ4CompressionCodec])
    } finally {
      if (master != null) {
        master.rpcEnv.shutdown()
        master.rpcEnv.awaitTermination()
        master = null
      }
    }
  }

  test("SPARK-46258: Recovery with RocksDB") {
    val conf = new SparkConf(loadDefaults = false)
    conf.set(RECOVERY_MODE, "ROCKSDB")
    conf.set(RECOVERY_DIRECTORY, System.getProperty("java.io.tmpdir"))

    var master: Master = null
    try {
      master = makeAliveMaster(conf)
      val e = master.persistenceEngine.asInstanceOf[RocksDBPersistenceEngine]
      assert(e.serializer.isInstanceOf[JavaSerializer])
    } finally {
      if (master != null) {
        master.rpcEnv.shutdown()
        master.rpcEnv.awaitTermination()
        master = null
      }
    }
  }

  test("SPARK-46353: handleRegisterWorker in STANDBY mode") {
    val master = makeMaster()
    val masterRpcAddress = smock[RpcAddress]
    val worker = smock[RpcEndpointRef]

    assert(master.state === RecoveryState.STANDBY)
    master.handleRegisterWorker("worker-0", "localhost", 1024, worker, 10, 4096,
      "http://localhost:8081", masterRpcAddress, Map.empty)
    verify(worker, times(1)).send(meq(MasterInStandby))
    verify(worker, times(0))
      .send(meq(RegisteredWorker(master.self, null, masterRpcAddress, duplicate = true)))
    verify(worker, times(0))
      .send(meq(RegisteredWorker(master.self, null, masterRpcAddress, duplicate = false)))
    assert(master.workers.isEmpty)
    assert(master.idToWorker.isEmpty)
  }

  test("SPARK-46353: handleRegisterWorker in RECOVERING mode without workers") {
    val master = makeMaster()
    val masterRpcAddress = smock[RpcAddress]
    val worker = smock[RpcEndpointRef]

    master.state = RecoveryState.RECOVERING
    master.persistenceEngine = new BlackHolePersistenceEngine()
    master.handleRegisterWorker("worker-0", "localhost", 1024, worker, 10, 4096,
      "http://localhost:8081", masterRpcAddress, Map.empty)
    verify(worker, times(0)).send(meq(MasterInStandby))
    verify(worker, times(1))
      .send(meq(RegisteredWorker(master.self, null, masterRpcAddress, duplicate = false)))
    assert(master.workers.size === 1)
    assert(master.idToWorker.size === 1)
  }

  test("SPARK-46353: handleRegisterWorker in RECOVERING mode with a unknown worker") {
    val master = makeMaster()
    val masterRpcAddress = smock[RpcAddress]
    val worker = smock[RpcEndpointRef]
    val workerInfo = smock[WorkerInfo]
    when(workerInfo.state).thenReturn(WorkerState.UNKNOWN)

    master.state = RecoveryState.RECOVERING
    master.workers.add(workerInfo)
    master.idToWorker("worker-0") = workerInfo
    master.persistenceEngine = new BlackHolePersistenceEngine()
    master.handleRegisterWorker("worker-0", "localhost", 1024, worker, 10, 4096,
      "http://localhost:8081", masterRpcAddress, Map.empty)
    verify(worker, times(0)).send(meq(MasterInStandby))
    verify(worker, times(1))
      .send(meq(RegisteredWorker(master.self, null, masterRpcAddress, duplicate = true)))
    assert(master.state === RecoveryState.RECOVERING)
    assert(master.workers.nonEmpty)
    assert(master.idToWorker.nonEmpty)
  }
}
