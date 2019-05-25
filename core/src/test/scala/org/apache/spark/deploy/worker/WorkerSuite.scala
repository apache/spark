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

import org.mockito.{Mock, MockitoAnnotations}
import org.mockito.Answers.RETURNS_SMART_NULLS
import org.mockito.ArgumentMatchers.any
import org.mockito.Mockito._
import org.mockito.invocation.InvocationOnMock
import org.scalatest.{BeforeAndAfter, Matchers}
import org.scalatest.concurrent.Eventually.{eventually, interval, timeout}

import org.apache.spark.{SecurityManager, SparkConf, SparkFunSuite}
import org.apache.spark.deploy.{Command, ExecutorState, ExternalShuffleService}
import org.apache.spark.deploy.DeployMessages.{DriverStateChanged, ExecutorStateChanged, WorkDirCleanup}
import org.apache.spark.deploy.master.DriverState
import org.apache.spark.internal.config
import org.apache.spark.internal.config.Worker._
import org.apache.spark.rpc.{RpcAddress, RpcEnv}
import org.apache.spark.util.Utils

class WorkerSuite extends SparkFunSuite with Matchers with BeforeAndAfter {

  import org.apache.spark.deploy.DeployTestUtils._

  @Mock(answer = RETURNS_SMART_NULLS) private var shuffleService: ExternalShuffleService = _

  def cmd(javaOpts: String*): Command = {
    Command("", Seq.empty, Map.empty, Seq.empty, Seq.empty, Seq(javaOpts : _*))
  }
  def conf(opts: (String, String)*): SparkConf = new SparkConf(loadDefaults = false).setAll(opts)

  private var _worker: Worker = _

  private def makeWorker(
      conf: SparkConf,
      shuffleServiceSupplier: Supplier[ExternalShuffleService] = null): Worker = {
    assert(_worker === null, "Some Worker's RpcEnv is leaked in tests")
    val securityMgr = new SecurityManager(conf)
    val rpcEnv = RpcEnv.create("test", "localhost", 12345, conf, securityMgr)
    _worker = new Worker(rpcEnv, 50000, 20, 1234 * 5, Array.fill(1)(RpcAddress("1.2.3.4", 1234)),
      "Worker", "/tmp", conf, securityMgr, shuffleServiceSupplier)
    _worker
  }

  before {
    MockitoAnnotations.initMocks(this)
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
    "spark.shuffle.service.db.enabled=true") {
    testWorkDirCleanupAndRemoveMetadataWithConfig(true)
  }

  test("WorkdDirCleanup cleans only app dirs when" +
    "spark.shuffle.service.db.enabled=false") {
    testWorkDirCleanupAndRemoveMetadataWithConfig(false)
  }

  private def testWorkDirCleanupAndRemoveMetadataWithConfig(dbCleanupEnabled: Boolean): Unit = {
    val conf = new SparkConf().set("spark.shuffle.service.db.enabled", dbCleanupEnabled.toString)
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
}
