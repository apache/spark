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

package org.apache.spark.deploy.worker

import java.io.{File, IOException}
import java.util.concurrent.atomic.AtomicBoolean
import java.util.function.Supplier

import scala.concurrent.duration._

import org.json4s.{DefaultFormats, Extraction, Formats}
import org.mockito.{Mock, MockitoAnnotations}
import org.mockito.Answers.RETURNS_SMART_NULLS
import org.mockito.ArgumentMatchers.any
import org.mockito.Mockito._
import org.mockito.invocation.InvocationOnMock
import org.scalatest.{BeforeAndAfter, PrivateMethodTester}
import org.scalatest.concurrent.Eventually.{eventually, interval, timeout}
import org.scalatest.matchers.must.Matchers
import org.scalatest.matchers.should.Matchers._

import org.apache.spark.{SecurityManager, SparkConf, SparkFunSuite}
import org.apache.spark.TestUtils.{createTempJsonFile, createTempScriptWithExpectedOutput}
import org.apache.spark.deploy.{Command, ExecutorState, ExternalShuffleService}
import org.apache.spark.deploy.DeployMessages.{DriverStateChanged, ExecutorStateChanged, WorkDirCleanup}
import org.apache.spark.deploy.master.DriverState
import org.apache.spark.internal.config
import org.apache.spark.internal.config.SHUFFLE_SERVICE_DB_BACKEND
import org.apache.spark.internal.config.Worker._
import org.apache.spark.network.shuffledb.DBBackend
import org.apache.spark.resource.{ResourceAllocation, ResourceInformation}
import org.apache.spark.resource.ResourceUtils._
import org.apache.spark.resource.TestResourceIDs.{WORKER_FPGA_ID, WORKER_GPU_ID}
import org.apache.spark.rpc.{RpcAddress, RpcEnv}
import org.apache.spark.util.Utils

class WorkerSuite extends SparkFunSuite with Matchers with BeforeAndAfter with PrivateMethodTester {

  import org.apache.spark.deploy.DeployTestUtils._

  @Mock(answer = RETURNS_SMART_NULLS) private var shuffleService: ExternalShuffleService = _

  def cmd(javaOpts: String*): Command = {
    Command("", Seq.empty, Map.empty, Seq.empty, Seq.empty, Seq(javaOpts : _*))
  }
  def conf(opts: (String, String)*): SparkConf = new SparkConf(loadDefaults = false).setAll(opts)

  implicit val formats: Formats = DefaultFormats

  private val _generateWorkerId = PrivateMethod[String](Symbol("generateWorkerId"))

  private var _worker: Worker = _

  private def makeWorker(
      conf: SparkConf = new SparkConf(),
      shuffleServiceSupplier: Supplier[ExternalShuffleService] = null,
      local: Boolean = false): Worker = {
    assert(_worker === null, "Some Worker's RpcEnv is leaked in tests")
    val securityMgr = new SecurityManager(conf)
    val rpcEnv = RpcEnv.create("test", "localhost", 12345, conf, securityMgr)
    val resourcesFile = conf.get(SPARK_WORKER_RESOURCE_FILE)
    val workDir = Utils.createTempDir(namePrefix = this.getClass.getSimpleName).toString
    val localWorker = new Worker(rpcEnv, 50000, 20, 1234 * 5,
      Array.fill(1)(RpcAddress("1.2.3.4", 1234)), "Worker", workDir,
      conf, securityMgr, resourcesFile, shuffleServiceSupplier)
    if (local) {
      localWorker
    } else {
      _worker = localWorker
      _worker
    }
  }

  before {
    MockitoAnnotations.openMocks(this).close()
  }

  after {
    if (_worker != null) {
      _worker.rpcEnv.shutdown()
      _worker.rpcEnv.awaitTermination()
      _worker = null
    }
  }

  test("test isUseLocalNodeSSLConfig") {
    Worker.isUseLocalNodeSSLConfig(cmd("-Dasdf=dfgh")) shouldBe false
    Worker.isUseLocalNodeSSLConfig(cmd("-Dspark.ssl.useNodeLocalConf=true")) shouldBe true
    Worker.isUseLocalNodeSSLConfig(cmd("-Dspark.ssl.useNodeLocalConf=false")) shouldBe false
    Worker.isUseLocalNodeSSLConfig(cmd("-Dspark.ssl.useNodeLocalConf=")) shouldBe false
  }

  test("test maybeUpdateSSLSettings") {
    Worker.maybeUpdateSSLSettings(
      cmd("-Dasdf=dfgh", "-Dspark.ssl.opt1=x"),
      conf("spark.ssl.opt1" -> "y", "spark.ssl.opt2" -> "z"))
        .javaOpts should contain theSameElementsInOrderAs Seq(
          "-Dasdf=dfgh", "-Dspark.ssl.opt1=x")

    Worker.maybeUpdateSSLSettings(
      cmd("-Dspark.ssl.useNodeLocalConf=false", "-Dspark.ssl.opt1=x"),
      conf("spark.ssl.opt1" -> "y", "spark.ssl.opt2" -> "z"))
        .javaOpts should contain theSameElementsInOrderAs Seq(
          "-Dspark.ssl.useNodeLocalConf=false", "-Dspark.ssl.opt1=x")

    Worker.maybeUpdateSSLSettings(
      cmd("-Dspark.ssl.useNodeLocalConf=true", "-Dspark.ssl.opt1=x"),
      conf("spark.ssl.opt1" -> "y", "spark.ssl.opt2" -> "z"))
        .javaOpts should contain theSameElementsAs Seq(
          "-Dspark.ssl.useNodeLocalConf=true", "-Dspark.ssl.opt1=y", "-Dspark.ssl.opt2=z")

  }

  test("test clearing of finishedExecutors (small number of executors)") {
    val conf = new SparkConf()
    conf.set(WORKER_UI_RETAINED_EXECUTORS, 2)
    val worker = makeWorker(conf)
    // initialize workers
    for (i <- 0 until 5) {
      worker.executors += s"app1/$i" -> createExecutorRunner(i)
    }
    // initialize ExecutorStateChanged Message
    worker.handleExecutorStateChanged(
      ExecutorStateChanged("app1", 0, ExecutorState.EXITED, None, None))
    assert(worker.finishedExecutors.size === 1)
    assert(worker.executors.size === 4)
    for (i <- 1 until 5) {
      worker.handleExecutorStateChanged(
        ExecutorStateChanged("app1", i, ExecutorState.EXITED, None, None))
      assert(worker.finishedExecutors.size === 2)
      if (i > 1) {
        assert(!worker.finishedExecutors.contains(s"app1/${i - 2}"))
      }
      assert(worker.executors.size === 4 - i)
    }
  }

  test("test clearing of finishedExecutors (more executors)") {
    val conf = new SparkConf()
    conf.set(WORKER_UI_RETAINED_EXECUTORS, 30)
    val worker = makeWorker(conf)
    // initialize workers
    for (i <- 0 until 50) {
      worker.executors += s"app1/$i" -> createExecutorRunner(i)
    }
    // initialize ExecutorStateChanged Message
    worker.handleExecutorStateChanged(
      ExecutorStateChanged("app1", 0, ExecutorState.EXITED, None, None))
    assert(worker.finishedExecutors.size === 1)
    assert(worker.executors.size === 49)
    for (i <- 1 until 50) {
      val expectedValue = {
        if (worker.finishedExecutors.size < 30) {
          worker.finishedExecutors.size + 1
        } else {
          28
        }
      }
      worker.handleExecutorStateChanged(
        ExecutorStateChanged("app1", i, ExecutorState.EXITED, None, None))
      if (expectedValue == 28) {
        for (j <- i - 30 until i - 27) {
          assert(!worker.finishedExecutors.contains(s"app1/$j"))
        }
      }
      assert(worker.executors.size === 49 - i)
      assert(worker.finishedExecutors.size === expectedValue)
    }
  }

  test("test clearing of finishedDrivers (small number of drivers)") {
    val conf = new SparkConf()
    conf.set(WORKER_UI_RETAINED_DRIVERS, 2)
    val worker = makeWorker(conf)
    // initialize workers
    for (i <- 0 until 5) {
      val driverId = s"driverId-$i"
      worker.drivers += driverId -> createDriverRunner(driverId)
    }
    // initialize DriverStateChanged Message
    worker.handleDriverStateChanged(DriverStateChanged("driverId-0", DriverState.FINISHED, None))
    assert(worker.drivers.size === 4)
    assert(worker.finishedDrivers.size === 1)
    for (i <- 1 until 5) {
      val driverId = s"driverId-$i"
      worker.handleDriverStateChanged(DriverStateChanged(driverId, DriverState.FINISHED, None))
      if (i > 1) {
        assert(!worker.finishedDrivers.contains(s"driverId-${i - 2}"))
      }
      assert(worker.drivers.size === 4 - i)
      assert(worker.finishedDrivers.size === 2)
    }
  }

  test("test clearing of finishedDrivers (more drivers)") {
    val conf = new SparkConf()
    conf.set(WORKER_UI_RETAINED_DRIVERS, 30)
    val worker = makeWorker(conf)
    // initialize workers
    for (i <- 0 until 50) {
      val driverId = s"driverId-$i"
      worker.drivers += driverId -> createDriverRunner(driverId)
    }
    // initialize DriverStateChanged Message
    worker.handleDriverStateChanged(DriverStateChanged("driverId-0", DriverState.FINISHED, None))
    assert(worker.finishedDrivers.size === 1)
    assert(worker.drivers.size === 49)
    for (i <- 1 until 50) {
      val expectedValue = {
        if (worker.finishedDrivers.size < 30) {
          worker.finishedDrivers.size + 1
        } else {
          28
        }
      }
      val driverId = s"driverId-$i"
      worker.handleDriverStateChanged(DriverStateChanged(driverId, DriverState.FINISHED, None))
      if (expectedValue == 28) {
        for (j <- i - 30 until i - 27) {
          assert(!worker.finishedDrivers.contains(s"driverId-$j"))
        }
      }
      assert(worker.drivers.size === 49 - i)
      assert(worker.finishedDrivers.size === expectedValue)
    }
  }

  test("worker could be launched without any resources") {
    val worker = makeWorker()
    worker.rpcEnv.setupEndpoint("worker", worker)
    eventually(timeout(10.seconds)) {
      assert(worker.resources === Map.empty)
      worker.rpcEnv.shutdown()
      worker.rpcEnv.awaitTermination()
    }
  }

  test("worker could load resources from resources file while launching") {
    val conf = new SparkConf()
    withTempDir { dir =>
      val gpuArgs = ResourceAllocation(WORKER_GPU_ID, Seq("0", "1"))
      val fpgaArgs =
        ResourceAllocation(WORKER_FPGA_ID, Seq("f1", "f2", "f3"))
      val ja = Extraction.decompose(Seq(gpuArgs, fpgaArgs))
      val f1 = createTempJsonFile(dir, "resources", ja)
      conf.set(SPARK_WORKER_RESOURCE_FILE.key, f1)
      conf.set(WORKER_GPU_ID.amountConf, "2")
      conf.set(WORKER_FPGA_ID.amountConf, "3")
      val worker = makeWorker(conf)
      worker.rpcEnv.setupEndpoint("worker", worker)
      eventually(timeout(10.seconds)) {
        assert(worker.resources === Map(GPU -> gpuArgs.toResourceInformation,
          FPGA -> fpgaArgs.toResourceInformation))
        worker.rpcEnv.shutdown()
        worker.rpcEnv.awaitTermination()
      }
    }
  }

  test("worker could load resources from discovery script while launching") {
    val conf = new SparkConf()
    withTempDir { dir =>
      val scriptPath = createTempScriptWithExpectedOutput(dir, "fpgaDiscoverScript",
        """{"name": "fpga","addresses":["f1", "f2", "f3"]}""")
      conf.set(WORKER_FPGA_ID.discoveryScriptConf, scriptPath)
      conf.set(WORKER_FPGA_ID.amountConf, "3")
      val worker = makeWorker(conf)
      worker.rpcEnv.setupEndpoint("worker", worker)
      eventually(timeout(10.seconds)) {
        assert(worker.resources === Map(FPGA ->
          new ResourceInformation(FPGA, Array("f1", "f2", "f3"))))
        worker.rpcEnv.shutdown()
        worker.rpcEnv.awaitTermination()
      }
    }
  }

  test("worker could load resources from resources file and discovery script while launching") {
    val conf = new SparkConf()
    withTempDir { dir =>
      val gpuArgs = ResourceAllocation(WORKER_GPU_ID, Seq("0", "1"))
      val ja = Extraction.decompose(Seq(gpuArgs))
      val resourcesPath = createTempJsonFile(dir, "resources", ja)
      val scriptPath = createTempScriptWithExpectedOutput(dir, "fpgaDiscoverScript",
        """{"name": "fpga","addresses":["f1", "f2", "f3"]}""")
      conf.set(SPARK_WORKER_RESOURCE_FILE.key, resourcesPath)
      conf.set(WORKER_FPGA_ID.discoveryScriptConf, scriptPath)
      conf.set(WORKER_FPGA_ID.amountConf, "3")
      conf.set(WORKER_GPU_ID.amountConf, "2")
      val worker = makeWorker(conf)
      worker.rpcEnv.setupEndpoint("worker", worker)
      eventually(timeout(10.seconds)) {
        assert(worker.resources === Map(GPU -> gpuArgs.toResourceInformation,
          FPGA -> new ResourceInformation(FPGA, Array("f1", "f2", "f3"))))
        worker.rpcEnv.shutdown()
        worker.rpcEnv.awaitTermination()
      }
    }
  }

  test("cleanup non-shuffle files after executor exits when config " +
      "spark.storage.cleanupFilesAfterExecutorExit=true") {
    testCleanupFilesWithConfig(true)
  }

  test("don't cleanup non-shuffle files after executor exits when config " +
      "spark.storage.cleanupFilesAfterExecutorExit=false") {
    testCleanupFilesWithConfig(false)
  }

  private def testCleanupFilesWithConfig(value: Boolean): Unit = {
    val conf = new SparkConf().set(config.STORAGE_CLEANUP_FILES_AFTER_EXECUTOR_EXIT, value)

    val cleanupCalled = new AtomicBoolean(false)
    when(shuffleService.executorRemoved(any[String], any[String])).thenAnswer(
      (_: InvocationOnMock) => cleanupCalled.set(true))
    val externalShuffleServiceSupplier = new Supplier[ExternalShuffleService] {
      override def get: ExternalShuffleService = shuffleService
    }
    val worker = makeWorker(conf, externalShuffleServiceSupplier)
    // initialize workers
    for (i <- 0 until 10) {
      worker.executors += s"app1/$i" -> createExecutorRunner(i)
    }
    worker.handleExecutorStateChanged(
      ExecutorStateChanged("app1", 0, ExecutorState.EXITED, None, None))
    assert(cleanupCalled.get() == value)
  }

  test("WorkDirCleanup cleans app dirs and shuffle metadata when " +
    "spark.shuffle.service.db.enabled=true, spark.shuffle.service.db.backend=RocksDB") {
    testWorkDirCleanupAndRemoveMetadataWithConfig(true, DBBackend.ROCKSDB)
  }

  test("WorkDirCleanup cleans app dirs and shuffle metadata when " +
    "spark.shuffle.service.db.enabled=true, spark.shuffle.service.db.backend=LevelDB") {
    assume(!Utils.isMacOnAppleSilicon)
    testWorkDirCleanupAndRemoveMetadataWithConfig(true, DBBackend.LEVELDB)
  }

  test("WorkDirCleanup cleans only app dirs when" +
    "spark.shuffle.service.db.enabled=false") {
    testWorkDirCleanupAndRemoveMetadataWithConfig(false)
  }

  private def testWorkDirCleanupAndRemoveMetadataWithConfig(
      dbCleanupEnabled: Boolean, shuffleDBBackend: DBBackend = null): Unit = {
    val conf = new SparkConf().set("spark.shuffle.service.db.enabled", dbCleanupEnabled.toString)
    if (dbCleanupEnabled) {
      assert(shuffleDBBackend != null)
      conf.set(SHUFFLE_SERVICE_DB_BACKEND.key, shuffleDBBackend.name())
    }
    conf.set("spark.worker.cleanup.appDataTtl", "60")
    conf.set("spark.shuffle.service.enabled", "true")

    val appId = "app1"
    val execId = "exec1"
    val cleanupCalled = new AtomicBoolean(false)
    when(shuffleService.applicationRemoved(any[String])).thenAnswer(
      (_: InvocationOnMock) => cleanupCalled.set(true))
    val externalShuffleServiceSupplier = new Supplier[ExternalShuffleService] {
      override def get: ExternalShuffleService = shuffleService
    }
    val worker = makeWorker(conf, externalShuffleServiceSupplier)
    val workDir = Utils.createTempDir(namePrefix = "work")
    // initialize workers
    worker.workDir = workDir
    // Create the executor's working directory
    val executorDir = new File(worker.workDir, appId + "/" + execId)

    if (!executorDir.exists && !executorDir.mkdirs()) {
      throw new IOException("Failed to create directory " + executorDir)
    }
    executorDir.setLastModified(System.currentTimeMillis - (1000 * 120))
    worker.receive(WorkDirCleanup)
    eventually(timeout(1.second), interval(10.milliseconds)) {
      assert(!executorDir.exists())
      assert(cleanupCalled.get() == dbCleanupEnabled)
    }
  }

  test("SPARK-45867: Support worker id pattern") {
    val worker = makeWorker(new SparkConf().set(WORKER_ID_PATTERN, "my-worker-%2$s"))
    assert(worker.invokePrivate(_generateWorkerId()) === "my-worker-localhost")
  }

  test("SPARK-45867: Prevent invalid worker id patterns") {
    val m = intercept[IllegalArgumentException] {
      makeWorker(new SparkConf().set(WORKER_ID_PATTERN, "my worker"))
    }.getMessage
    assert(m.contains("Whitespace is not allowed"))
  }
}
