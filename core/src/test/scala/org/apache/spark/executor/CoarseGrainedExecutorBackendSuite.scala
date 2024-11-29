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

package org.apache.spark.executor

import java.io.File
import java.nio.ByteBuffer
import java.util.Properties
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.atomic.AtomicInteger

import scala.collection.concurrent.TrieMap
import scala.concurrent.duration._

import org.json4s.{DefaultFormats, Extraction, Formats}
import org.json4s.JsonAST.{JArray, JObject}
import org.json4s.JsonDSL._
import org.mockito.ArgumentMatchers.any
import org.mockito.Mockito._
import org.scalatest.concurrent.Eventually.{eventually, timeout}
import org.scalatestplus.mockito.MockitoSugar

import org.apache.spark._
import org.apache.spark.TestUtils._
import org.apache.spark.api.plugin.{DriverPlugin, ExecutorPlugin, PluginContext, SparkPlugin}
import org.apache.spark.internal.config.{EXECUTOR_MEMORY, PLUGINS}
import org.apache.spark.resource._
import org.apache.spark.resource.ResourceUtils._
import org.apache.spark.resource.TestResourceIDs._
import org.apache.spark.rpc.RpcEnv
import org.apache.spark.scheduler.{SparkListener, SparkListenerExecutorAdded, SparkListenerExecutorRemoved, TaskDescription}
import org.apache.spark.scheduler.cluster.CoarseGrainedClusterMessages.{KillTask, LaunchTask}
import org.apache.spark.serializer.JavaSerializer
import org.apache.spark.util.{SerializableBuffer, SslTestUtils, ThreadUtils, Utils}

class CoarseGrainedExecutorBackendSuite extends SparkFunSuite
    with LocalSparkContext with MockitoSugar {

  implicit val formats: Formats = DefaultFormats

  def createSparkConf(): SparkConf = {
    new SparkConf()
  }

  test("parsing no resources") {
    val conf = createSparkConf()
    val resourceProfile = ResourceProfile.getOrCreateDefaultProfile(conf)
    val serializer = new JavaSerializer(conf)
    val env = createMockEnv(conf, serializer)

    // we don't really use this, just need it to get at the parser function
    val backend = new CoarseGrainedExecutorBackend( env.rpcEnv, "driverurl", "1", "host1", "host1",
      4, env, None, resourceProfile)
    withTempDir { tmpDir =>
      val testResourceArgs: JObject = ("" -> "")
      val ja = JArray(List(testResourceArgs))
      val f1 = createTempJsonFile(tmpDir, "resources", ja)
      val error = intercept[SparkException] {
        val parsedResources = backend.parseOrFindResources(Some(f1))
      }.getMessage()

      assert(error.contains("Error parsing resources file"),
        s"Calling with no resources didn't error as expected, error: $error")
    }
  }

  test("parsing one resource") {
    val conf = createSparkConf()
    conf.set(EXECUTOR_GPU_ID.amountConf, "2")
    val serializer = new JavaSerializer(conf)
    val env = createMockEnv(conf, serializer)
    // we don't really use this, just need it to get at the parser function
    val backend = new CoarseGrainedExecutorBackend( env.rpcEnv, "driverurl", "1", "host1", "host1",
      4, env, None, ResourceProfile.getOrCreateDefaultProfile(conf))
    withTempDir { tmpDir =>
      val ra = ResourceAllocation(EXECUTOR_GPU_ID, Seq("0", "1"))
      val ja = Extraction.decompose(Seq(ra))
      val f1 = createTempJsonFile(tmpDir, "resources", ja)
      val parsedResources = backend.parseOrFindResources(Some(f1))

      assert(parsedResources.size === 1)
      assert(parsedResources.get(GPU).nonEmpty)
      assert(parsedResources.get(GPU).get.name === GPU)
      assert(parsedResources.get(GPU).get.addresses.sameElements(Array("0", "1")))
    }
  }

  test("parsing multiple resources resource profile") {
    val rpBuilder = new ResourceProfileBuilder
    val ereqs = new ExecutorResourceRequests().resource(GPU, 2)
    ereqs.resource(FPGA, 3)
    val rp = rpBuilder.require(ereqs).build()
    testParsingMultipleResources(createSparkConf(), rp)
  }

  test("parsing multiple resources") {
    val conf = createSparkConf()
    conf.set(EXECUTOR_GPU_ID.amountConf, "2")
    conf.set(EXECUTOR_FPGA_ID.amountConf, "3")
    testParsingMultipleResources(conf, ResourceProfile.getOrCreateDefaultProfile(conf))
  }

  def testParsingMultipleResources(conf: SparkConf, resourceProfile: ResourceProfile): Unit = {
    val serializer = new JavaSerializer(conf)
    val env = createMockEnv(conf, serializer)
    // we don't really use this, just need it to get at the parser function
    val backend = new CoarseGrainedExecutorBackend( env.rpcEnv, "driverurl", "1", "host1", "host1",
      4, env, None, resourceProfile)

    withTempDir { tmpDir =>
      val gpuArgs = ResourceAllocation(EXECUTOR_GPU_ID, Seq("0", "1"))
      val fpgaArgs =
        ResourceAllocation(EXECUTOR_FPGA_ID, Seq("f1", "f2", "f3"))
      val ja = Extraction.decompose(Seq(gpuArgs, fpgaArgs))
      val f1 = createTempJsonFile(tmpDir, "resources", ja)
      val parsedResources = backend.parseOrFindResources(Some(f1))

      assert(parsedResources.size === 2)
      assert(parsedResources.get(GPU).nonEmpty)
      assert(parsedResources.get(GPU).get.name === GPU)
      assert(parsedResources.get(GPU).get.addresses.sameElements(Array("0", "1")))
      assert(parsedResources.get(FPGA).nonEmpty)
      assert(parsedResources.get(FPGA).get.name === FPGA)
      assert(parsedResources.get(FPGA).get.addresses.sameElements(Array("f1", "f2", "f3")))
    }
  }

  test("error checking parsing resources and executor and task configs") {
    val conf = createSparkConf()
    conf.set(EXECUTOR_GPU_ID.amountConf, "2")
    val serializer = new JavaSerializer(conf)
    val env = createMockEnv(conf, serializer)
    // we don't really use this, just need it to get at the parser function
    val backend = new CoarseGrainedExecutorBackend(env.rpcEnv, "driverurl", "1", "host1", "host1",
      4, env, None, ResourceProfile.getOrCreateDefaultProfile(conf))

    // not enough gpu's on the executor
    withTempDir { tmpDir =>
      val gpuArgs = ResourceAllocation(EXECUTOR_GPU_ID, Seq("0"))
      val ja = Extraction.decompose(Seq(gpuArgs))
      val f1 = createTempJsonFile(tmpDir, "resources", ja)

      val error = intercept[IllegalArgumentException] {
        val parsedResources = backend.parseOrFindResources(Some(f1))
      }.getMessage()

      assert(error.contains("Resource: gpu, with addresses: 0 is less than what the " +
        "user requested: 2"))
    }

    // missing resource on the executor
    withTempDir { tmpDir =>
      val fpga = ResourceAllocation(EXECUTOR_FPGA_ID, Seq("0"))
      val ja = Extraction.decompose(Seq(fpga))
      val f1 = createTempJsonFile(tmpDir, "resources", ja)

      val error = intercept[SparkException] {
        val parsedResources = backend.parseOrFindResources(Some(f1))
      }.getMessage()

      assert(error.contains("User is expecting to use resource: gpu, but didn't " +
        "specify a discovery script!"))
    }
  }

  test("executor resource found less than required resource profile") {
    val rpBuilder = new ResourceProfileBuilder
    val ereqs = new ExecutorResourceRequests().resource(GPU, 4)
    val treqs = new TaskResourceRequests().resource(GPU, 1)
    val rp = rpBuilder.require(ereqs).require(treqs).build()
    testExecutorResourceFoundLessThanRequired(createSparkConf(), rp)
  }

  test("executor resource found less than required") {
    val conf = createSparkConf()
    conf.set(EXECUTOR_GPU_ID.amountConf, "4")
    conf.set(TASK_GPU_ID.amountConf, "1")
    testExecutorResourceFoundLessThanRequired(conf, ResourceProfile.getOrCreateDefaultProfile(conf))
  }

  private def testExecutorResourceFoundLessThanRequired(
      conf: SparkConf,
      resourceProfile: ResourceProfile) = {
    val serializer = new JavaSerializer(conf)
    val env = createMockEnv(conf, serializer)
    // we don't really use this, just need it to get at the parser function
    val backend = new CoarseGrainedExecutorBackend(env.rpcEnv, "driverurl", "1", "host1", "host1",
      4, env, None, resourceProfile)

    // executor resources < required
    withTempDir { tmpDir =>
      val gpuArgs = ResourceAllocation(EXECUTOR_GPU_ID, Seq("0", "1"))
      val ja = Extraction.decompose(Seq(gpuArgs))
      val f1 = createTempJsonFile(tmpDir, "resources", ja)

      val error = intercept[IllegalArgumentException] {
        val parsedResources = backend.parseOrFindResources(Some(f1))
      }.getMessage()

      assert(error.contains("Resource: gpu, with addresses: 0,1 is less than what the " +
        "user requested: 4"))
    }
  }

  test("use resource discovery") {
    val conf = createSparkConf()
    conf.set(EXECUTOR_FPGA_ID.amountConf, "3")
    assume(!(Utils.isWindows))
    withTempDir { dir =>
      val scriptPath = createTempScriptWithExpectedOutput(dir, "fpgaDiscoverScript",
        """{"name": "fpga","addresses":["f1", "f2", "f3"]}""")
      conf.set(EXECUTOR_FPGA_ID.discoveryScriptConf, scriptPath)

      val serializer = new JavaSerializer(conf)
      val env = createMockEnv(conf, serializer)

      // we don't really use this, just need it to get at the parser function
      val backend = new CoarseGrainedExecutorBackend(env.rpcEnv, "driverurl", "1", "host1", "host1",
        4, env, None, ResourceProfile.getOrCreateDefaultProfile(conf))

      val parsedResources = backend.parseOrFindResources(None)

      assert(parsedResources.size === 1)
      assert(parsedResources.get(FPGA).nonEmpty)
      assert(parsedResources.get(FPGA).get.name === FPGA)
      assert(parsedResources.get(FPGA).get.addresses.sameElements(Array("f1", "f2", "f3")))
    }
  }

  test("use resource discovery and allocated file option with resource profile") {
    assume(!(Utils.isWindows))
    withTempDir { dir =>
      val scriptPath = createTempScriptWithExpectedOutput(dir, "fpgaDiscoverScript",
        """{"name": "fpga","addresses":["f1", "f2", "f3"]}""")
      val rpBuilder = new ResourceProfileBuilder
      val ereqs = new ExecutorResourceRequests().resource(FPGA, 3, scriptPath)
      ereqs.resource(GPU, 2)
      val rp = rpBuilder.require(ereqs).build()
      allocatedFileAndConfigsResourceDiscoveryTestFpga(dir, createSparkConf(), rp)
    }
  }

  test("use resource discovery and allocated file option") {
    assume(!(Utils.isWindows))
    withTempDir { dir =>
      val scriptPath = createTempScriptWithExpectedOutput(dir, "fpgaDiscoverScript",
        """{"name": "fpga","addresses":["f1", "f2", "f3"]}""")
      val conf = createSparkConf()
      conf.set(EXECUTOR_FPGA_ID.amountConf, "3")
      conf.set(EXECUTOR_FPGA_ID.discoveryScriptConf, scriptPath)
      conf.set(EXECUTOR_GPU_ID.amountConf, "2")
      val rp = ResourceProfile.getOrCreateDefaultProfile(conf)
      allocatedFileAndConfigsResourceDiscoveryTestFpga(dir, conf, rp)
    }
  }

  private def allocatedFileAndConfigsResourceDiscoveryTestFpga(
      dir: File,
      conf: SparkConf,
      resourceProfile: ResourceProfile) = {
    val serializer = new JavaSerializer(conf)
    val env = createMockEnv(conf, serializer)

    // we don't really use this, just need it to get at the parser function
    val backend = new CoarseGrainedExecutorBackend(env.rpcEnv, "driverurl", "1", "host1", "host1",
      4, env, None, resourceProfile)
    val gpuArgs = ResourceAllocation(EXECUTOR_GPU_ID, Seq("0", "1"))
    val ja = Extraction.decompose(Seq(gpuArgs))
    val f1 = createTempJsonFile(dir, "resources", ja)
    val parsedResources = backend.parseOrFindResources(Some(f1))

    assert(parsedResources.size === 2)
    assert(parsedResources.get(GPU).nonEmpty)
    assert(parsedResources.get(GPU).get.name === GPU)
    assert(parsedResources.get(GPU).get.addresses.sameElements(Array("0", "1")))
    assert(parsedResources.get(FPGA).nonEmpty)
    assert(parsedResources.get(FPGA).get.name === FPGA)
    assert(parsedResources.get(FPGA).get.addresses.sameElements(Array("f1", "f2", "f3")))
  }

  test("track allocated resources by taskId") {
    val conf = createSparkConf()
    val securityMgr = new SecurityManager(conf)
    val serializer = new JavaSerializer(conf)
    var backend: CoarseGrainedExecutorBackend = null

    try {
      val rpcEnv = RpcEnv.create("1", "localhost", 0, conf, securityMgr)
      val env = createMockEnv(conf, serializer, Some(rpcEnv))
        backend = new CoarseGrainedExecutorBackend(env.rpcEnv, rpcEnv.address.hostPort, "1",
        "host1", "host1", 4, env, None,
          resourceProfile = ResourceProfile.getOrCreateDefaultProfile(conf))

      val taskId = 1000000L
      val resourcesAmounts = Map(GPU -> Map(
        "0" -> ResourceAmountUtils.toInternalResource(0.15),
        "1" -> ResourceAmountUtils.toInternalResource(0.76)))
      // We don't really verify the data, just pass it around.
      val data = ByteBuffer.wrap(Array[Byte](1, 2, 3, 4))
      val taskDescription = new TaskDescription(taskId, 2, "1", "TASK 1000000", 19,
        1, JobArtifactSet.emptyJobArtifactSet, new Properties, 1, resourcesAmounts, data)
      val serializedTaskDescription = TaskDescription.encode(taskDescription)
      backend.rpcEnv.setupEndpoint("Executor 1", backend)
      backend.executor = mock[Executor](CALLS_REAL_METHODS)
      val executor = backend.executor
      // Mock the executor.
      val threadPool = ThreadUtils.newDaemonFixedThreadPool(1, "test-executor")
      when(executor.threadPool).thenReturn(threadPool)
      val runningTasks = new ConcurrentHashMap[Long, Executor#TaskRunner]
      when(executor.runningTasks).thenAnswer(_ => runningTasks)
      when(executor.conf).thenReturn(conf)

      def getFakeTaskRunner(taskDescription: TaskDescription): Executor#TaskRunner = {
        new executor.TaskRunner(backend, taskDescription, None) {
          override def run(): Unit = {
            logInfo(s"task ${this.taskDescription.taskId} runs.")
          }

          override def kill(interruptThread: Boolean, reason: String): Unit = {
            logInfo(s"task ${this.taskDescription.taskId} killed.")
          }
        }
      }

      // Feed the fake task-runners to be executed by the executor.
      doAnswer(_ => getFakeTaskRunner(taskDescription))
        .when(executor).createTaskRunner(any(), any())

      // Launch a new task shall add an entry to `taskResources` map.
      backend.self.send(LaunchTask(new SerializableBuffer(serializedTaskDescription)))
      eventually(timeout(10.seconds)) {
        assert(runningTasks.size == 1)
        val resources = backend.executor.runningTasks.get(taskId).taskDescription.resources
        assert(resources(GPU).keys.toArray.sorted sameElements Array("0", "1"))
        assert(executor.runningTasks.get(taskId).taskDescription.resources
          === resourcesAmounts)
      }

      // Update the status of a running task shall not affect `taskResources` map.
      backend.statusUpdate(taskId, TaskState.RUNNING, data)
      val resources = backend.executor.runningTasks.get(taskId).taskDescription.resources
      assert(resources(GPU).keys.toArray.sorted sameElements Array("0", "1"))
      assert(executor.runningTasks.get(taskId).taskDescription.resources
        === resourcesAmounts)

      // Update the status of a finished task shall remove the entry from `taskResources` map.
      backend.statusUpdate(taskId, TaskState.FINISHED, data)
    } finally {
      if (backend != null) {
        backend.rpcEnv.shutdown()
      }
    }
  }

  test("SPARK-24203 when bindAddress is not set, it defaults to hostname") {
    val args1 = Array(
      "--driver-url", "driverurl",
      "--executor-id", "1",
      "--hostname", "host1",
      "--cores", "1",
      "--app-id", "app1")

    val arg = CoarseGrainedExecutorBackend.parseArguments(args1, "")
    assert(arg.bindAddress == "host1")
  }

  test("SPARK-24203 when bindAddress is different, it does not default to hostname") {
    val args1 = Array(
      "--driver-url", "driverurl",
      "--executor-id", "1",
      "--hostname", "host1",
      "--bind-address", "bindaddress1",
      "--cores", "1",
      "--app-id", "app1")

    val arg = CoarseGrainedExecutorBackend.parseArguments(args1, "")
    assert(arg.bindAddress == "bindaddress1")
  }

  /**
   * This testcase is to verify that [[Executor.killTask()]] will always cancel a task that is
   * being executed in [[Executor.TaskRunner]].
   */
  test(s"Tasks launched should always be cancelled.")  {
    val conf = createSparkConf()
    val securityMgr = new SecurityManager(conf)
    val serializer = new JavaSerializer(conf)
    val threadPool = ThreadUtils.newDaemonFixedThreadPool(32, "test-executor")
    var backend: CoarseGrainedExecutorBackend = null

    try {
      val rpcEnv = RpcEnv.create("1", "localhost", 0, conf, securityMgr)
      val env = createMockEnv(conf, serializer, Some(rpcEnv))
      backend = new CoarseGrainedExecutorBackend(env.rpcEnv, rpcEnv.address.hostPort, "1",
        "host1", "host1", 4, env, None,
        resourceProfile = ResourceProfile.getOrCreateDefaultProfile(conf))

      backend.rpcEnv.setupEndpoint("Executor 1", backend)
      backend.executor = mock[Executor](CALLS_REAL_METHODS)
      val executor = backend.executor
      // Mock the executor.
      when(executor.threadPool).thenReturn(threadPool)
      val runningTasks = spy[ConcurrentHashMap[Long, Executor#TaskRunner]](
        new ConcurrentHashMap[Long, Executor#TaskRunner])
      when(executor.runningTasks).thenAnswer(_ => runningTasks)
      when(executor.conf).thenReturn(conf)

      // We don't really verify the data, just pass it around.
      val data = ByteBuffer.wrap(Array[Byte](1, 2, 3, 4))

      val numTasks = 1000
      val tasksKilled = new TrieMap[Long, Boolean]()
      val tasksExecuted = new TrieMap[Long, Boolean]()

      val resourcesAmounts = Map(GPU -> Map(
        "0" -> ResourceAmountUtils.toInternalResource(0.15),
        "1" -> ResourceAmountUtils.toInternalResource(0.76)))

      // Fake tasks with different taskIds.
      val taskDescriptions = (1 to numTasks).map {
        taskId => new TaskDescription(taskId, 2, "1", s"TASK $taskId", 19,
          1, JobArtifactSet.emptyJobArtifactSet, new Properties, 1, resourcesAmounts, data)
      }
      assert(taskDescriptions.length == numTasks)

      def getFakeTaskRunner(taskDescription: TaskDescription): Executor#TaskRunner = {
        new executor.TaskRunner(backend, taskDescription, None) {
          override def run(): Unit = {
            tasksExecuted.put(this.taskDescription.taskId, true)
            logInfo(s"task ${this.taskDescription.taskId} runs.")
          }

          override def kill(interruptThread: Boolean, reason: String): Unit = {
            logInfo(s"task ${this.taskDescription.taskId} killed.")
            tasksKilled.put(this.taskDescription.taskId, true)
          }
        }
      }

      // Feed the fake task-runners to be executed by the executor.
      val firstLaunchTask = getFakeTaskRunner(taskDescriptions(1))
      val otherTasks = taskDescriptions.slice(1, numTasks).map(getFakeTaskRunner(_)).toArray
      assert (otherTasks.length == numTasks - 1)
      // Workaround for compilation issue around Mockito.doReturn
      doReturn(firstLaunchTask, otherTasks: _*).when(executor).
        createTaskRunner(any(), any())

      // Launch tasks and quickly kill them so that TaskRunner.killTask will be triggered.
      taskDescriptions.foreach { taskDescription =>
        val buffer = new SerializableBuffer(TaskDescription.encode(taskDescription))
        backend.self.send(LaunchTask(buffer))
        Thread.sleep(1)
        backend.self.send(KillTask(taskDescription.taskId, "exec1", false, "test"))
      }

      eventually(timeout(10.seconds)) {
        verify(runningTasks, times(numTasks)).put(any(), any())
      }

      assert(tasksExecuted.size == tasksKilled.size,
        s"Tasks killed ${tasksKilled.size} != tasks executed ${tasksExecuted.size}")
      assert(tasksExecuted.keySet == tasksKilled.keySet)
      logInfo(s"Task executed ${tasksExecuted.size}, task killed ${tasksKilled.size}")
    } finally {
      if (backend != null) {
        backend.rpcEnv.shutdown()
      }
      threadPool.shutdownNow()
    }
  }

  /**
   * This testcase is to verify that [[Executor.killTask()]] will always cancel a task even if
   * it has not been launched yet.
   */
  test(s"Tasks not launched should always be cancelled.")  {
    val conf = createSparkConf()
    val securityMgr = new SecurityManager(conf)
    val serializer = new JavaSerializer(conf)
    val threadPool = ThreadUtils.newDaemonFixedThreadPool(32, "test-executor")
    var backend: CoarseGrainedExecutorBackend = null

    try {
      val rpcEnv = RpcEnv.create("1", "localhost", 0, conf, securityMgr)
      val env = createMockEnv(conf, serializer, Some(rpcEnv))
      backend = new CoarseGrainedExecutorBackend(env.rpcEnv, rpcEnv.address.hostPort, "1",
        "host1", "host1", 4, env, None,
        resourceProfile = ResourceProfile.getOrCreateDefaultProfile(conf))

      backend.rpcEnv.setupEndpoint("Executor 1", backend)
      backend.executor = mock[Executor](CALLS_REAL_METHODS)
      val executor = backend.executor
      // Mock the executor.
      when(executor.threadPool).thenReturn(threadPool)
      val runningTasks = spy[ConcurrentHashMap[Long, Executor#TaskRunner]](
        new ConcurrentHashMap[Long, Executor#TaskRunner])
      when(executor.runningTasks).thenAnswer(_ => runningTasks)
      when(executor.conf).thenReturn(conf)

      // We don't really verify the data, just pass it around.
      val data = ByteBuffer.wrap(Array[Byte](1, 2, 3, 4))

      val numTasks = 1000
      val tasksKilled = new TrieMap[Long, Boolean]()
      val tasksExecuted = new TrieMap[Long, Boolean]()

      val resourcesAmounts = Map(GPU -> Map(
        "0" -> ResourceAmountUtils.toInternalResource(0.15),
        "1" -> ResourceAmountUtils.toInternalResource(0.76)))

      // Fake tasks with different taskIds.
      val taskDescriptions = (1 to numTasks).map {
        taskId => new TaskDescription(taskId, 2, "1", s"TASK $taskId", 19,
          1, JobArtifactSet.emptyJobArtifactSet, new Properties, 1, resourcesAmounts, data)
      }
      assert(taskDescriptions.length == numTasks)

      def getFakeTaskRunner(taskDescription: TaskDescription): Executor#TaskRunner = {
        new executor.TaskRunner(backend, taskDescription, None) {
          override def run(): Unit = {
            tasksExecuted.put(this.taskDescription.taskId, true)
            logInfo(s"task ${this.taskDescription.taskId} runs.")
          }

          override def kill(interruptThread: Boolean, reason: String): Unit = {
            logInfo(s"task ${this.taskDescription.taskId} killed.")
            tasksKilled.put(this.taskDescription.taskId, true)
          }
        }
      }

      // Feed the fake task-runners to be executed by the executor.
      val firstLaunchTask = getFakeTaskRunner(taskDescriptions(1))
      val otherTasks = taskDescriptions.slice(1, numTasks).map(getFakeTaskRunner(_)).toArray
      assert (otherTasks.length == numTasks - 1)
      // Workaround for compilation issue around Mockito.doReturn
      doReturn(firstLaunchTask, otherTasks: _*).when(executor).
        createTaskRunner(any(), any())

      // The reverse order of events can happen when the scheduler tries to cancel a task right
      // after launching it.
      taskDescriptions.foreach { taskDescription =>
        val buffer = new SerializableBuffer(TaskDescription.encode(taskDescription))
        backend.self.send(KillTask(taskDescription.taskId, "exec1", false, "test"))
        backend.self.send(LaunchTask(buffer))
      }

      eventually(timeout(10.seconds)) {
        verify(runningTasks, times(numTasks)).put(any(), any())
      }

      assert(tasksExecuted.size == tasksKilled.size,
        s"Tasks killed ${tasksKilled.size} != tasks executed ${tasksExecuted.size}")
      assert(tasksExecuted.keySet == tasksKilled.keySet)
      logInfo(s"Task executed ${tasksExecuted.size}, task killed ${tasksKilled.size}")
    } finally {
      if (backend != null) {
        backend.rpcEnv.shutdown()
      }
      threadPool.shutdownNow()
    }
  }

  /**
   * A fatal error occurred when [[Executor]] was initialized, this should be caught by
   * [[SparkUncaughtExceptionHandler]] and [[Executor]] can exit by itself.
   */
  test("SPARK-40320 Executor should exit when initialization failed for fatal error") {
    val conf = createSparkConf()
      .setMaster("local-cluster[1, 1, 512]")
      .set(EXECUTOR_MEMORY.key, "512m")
      .set(PLUGINS, Seq(classOf[TestFatalErrorPlugin].getName))
      .setAppName("test")
    sc = new SparkContext(conf)
    val executorAddCounter = new AtomicInteger(0)
    val executorRemovedCounter = new AtomicInteger(0)

    val listener = new SparkListener() {
      override def onExecutorAdded(executorAdded: SparkListenerExecutorAdded): Unit = {
        executorAddCounter.getAndIncrement()
      }

      override def onExecutorRemoved(executorRemoved: SparkListenerExecutorRemoved): Unit = {
        executorRemovedCounter.getAndIncrement()
      }
    }
    try {
      sc.addSparkListener(listener)
      eventually(timeout(30.seconds)) {
        assert(executorAddCounter.get() >= 2)
        assert(executorRemovedCounter.get() >= 2)
      }
    } finally {
      sc.removeSparkListener(listener)
    }
  }

  private def createMockEnv(conf: SparkConf, serializer: JavaSerializer,
      rpcEnv: Option[RpcEnv] = None): SparkEnv = {
    val mockEnv = mock[SparkEnv]
    val mockRpcEnv = mock[RpcEnv]
    when(mockEnv.conf).thenReturn(conf)
    when(mockEnv.serializer).thenReturn(serializer)
    when(mockEnv.closureSerializer).thenReturn(serializer)
    when(mockEnv.rpcEnv).thenReturn(rpcEnv.getOrElse(mockRpcEnv))
    SparkEnv.set(mockEnv)
    mockEnv
  }
}

private class TestFatalErrorPlugin extends SparkPlugin {
  override def driverPlugin(): DriverPlugin = new TestDriverPlugin()

  override def executorPlugin(): ExecutorPlugin = new TestErrorExecutorPlugin()
}

private class TestDriverPlugin extends DriverPlugin {
}

private class TestErrorExecutorPlugin extends ExecutorPlugin {

  override def init(_ctx: PluginContext, extraConf: java.util.Map[String, String]): Unit = {
    // scalastyle:off throwerror
    /**
     * A fatal error. See nonFatal definition in [[NonFatal]].
     */
    throw new UnsatisfiedLinkError("Mock throws fatal error.")
    // scalastyle:on throwerror
  }
}

class SslCoarseGrainedExecutorBackendSuite extends CoarseGrainedExecutorBackendSuite
  with LocalSparkContext with MockitoSugar {

  override def createSparkConf(): SparkConf = {
    SslTestUtils.updateWithSSLConfig(super.createSparkConf())
  }
}
