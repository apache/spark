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
import org.apache.spark.io.LZ4CompressionCodec
import org.apache.spark.resource.{ResourceInformation, ResourceProfile}
import org.apache.spark.rpc.{RpcAddress, RpcEndpointRef}
import org.apache.spark.serializer.KryoSerializer

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

  test("SPARK-46205: Recovery with Kryo Serializer") {
    val conf = new SparkConf(loadDefaults = false)
    conf.set(RECOVERY_MODE, "FILESYSTEM")
    conf.set(RECOVERY_SERIALIZER, "Kryo")
    conf.set(RECOVERY_DIRECTORY, System.getProperty("java.io.tmpdir"))

    var master: Master = null
    try {
      master = makeAliveMaster(conf)
      val e = master.invokePrivate(_persistenceEngine()).asInstanceOf[FileSystemPersistenceEngine]
      assert(e.serializer.isInstanceOf[KryoSerializer])
    } finally {
      if (master != null) {
        master.rpcEnv.shutdown()
        master.rpcEnv.awaitTermination()
        master = null
      }
    }
  }

  test("SPARK-46216: Recovery without compression") {
    val conf = new SparkConf(loadDefaults = false)
    conf.set(RECOVERY_MODE, "FILESYSTEM")
    conf.set(RECOVERY_DIRECTORY, System.getProperty("java.io.tmpdir"))

    var master: Master = null
    try {
      master = makeAliveMaster(conf)
      val e = master.invokePrivate(_persistenceEngine()).asInstanceOf[FileSystemPersistenceEngine]
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
      val e = master.invokePrivate(_persistenceEngine()).asInstanceOf[FileSystemPersistenceEngine]
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
    conf.set(RECOVERY_SERIALIZER, "Kryo")
    conf.set(RECOVERY_DIRECTORY, System.getProperty("java.io.tmpdir"))

    var master: Master = null
    try {
      master = makeAliveMaster(conf)
      val e = master.invokePrivate(_persistenceEngine()).asInstanceOf[RocksDBPersistenceEngine]
      assert(e.serializer.isInstanceOf[KryoSerializer])
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
