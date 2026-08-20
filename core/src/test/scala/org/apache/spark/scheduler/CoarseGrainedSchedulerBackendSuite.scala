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

package org.apache.spark.scheduler

import java.util.Properties
import java.util.concurrent.{Callable, CountDownLatch, FutureTask, LinkedBlockingQueue, TimeUnit}
import java.util.concurrent.atomic.{AtomicBoolean, AtomicReference}

import scala.collection.mutable
import scala.concurrent.Future
import scala.concurrent.duration._
import scala.language.postfixOps
import scala.reflect.ClassTag

import org.mockito.ArgumentMatchers.any
import org.mockito.Mockito.when
import org.mockito.invocation.InvocationOnMock
import org.scalatest.concurrent.Eventually
import org.scalatestplus.mockito.MockitoSugar._

import org.apache.spark._
import org.apache.spark.TestUtils.createTempScriptWithExpectedOutput
import org.apache.spark.internal.config._
import org.apache.spark.internal.config.Network.RPC_MESSAGE_MAX_SIZE
import org.apache.spark.rdd.RDD
import org.apache.spark.resource.{ExecutorResourceRequests, ResourceInformation, ResourceProfile, ResourceProfileBuilder, TaskResourceRequests}
import org.apache.spark.resource.ResourceAmountUtils.ONE_ENTIRE_RESOURCE
import org.apache.spark.resource.ResourceUtils._
import org.apache.spark.resource.TestResourceIDs._
import org.apache.spark.rpc.{RpcAddress, RpcEndpointRef, RpcEnv, RpcTimeout}
import org.apache.spark.scheduler.cluster.{CoarseGrainedSchedulerBackend, ExecutorInfo}
import org.apache.spark.scheduler.cluster.CoarseGrainedClusterMessages._
import org.apache.spark.util.{RpcUtils, SerializableBuffer, Utils}

class CoarseGrainedSchedulerBackendSuite extends SparkFunSuite with LocalSparkContext
    with Eventually {

  private val executorUpTimeout = 1.minute

  test("serialized task larger than max RPC message size") {
    val conf = new SparkConf
    conf.set(RPC_MESSAGE_MAX_SIZE, 1)
    conf.set(DEFAULT_PARALLELISM.key, "1")
    sc = new SparkContext("local-cluster[2, 1, 1024]", "test", conf)
    val frameSize = RpcUtils.maxMessageSizeBytes(sc.conf)
    val buffer = new SerializableBuffer(java.nio.ByteBuffer.allocate(2 * frameSize))
    val larger = sc.parallelize(Seq(buffer))
    val thrown = intercept[SparkException] {
      larger.collect()
    }
    assert(thrown.getMessage.contains("using broadcast variables for large values"))
    val smaller = sc.parallelize(1 to 4).collect()
    assert(smaller.length === 4)
  }

  test("compute max number of concurrent tasks can be launched") {
    val conf = new SparkConf()
      .setMaster("local-cluster[4, 3, 1024]")
      .setAppName("test")
    sc = new SparkContext(conf)
    eventually(timeout(executorUpTimeout)) {
      // Ensure all executors have been launched.
      assert(sc.getExecutorIds().length == 4)
    }
    assert(sc.maxNumConcurrentTasks(ResourceProfile.getOrCreateDefaultProfile(conf)) == 12)
  }

  test("compute max number of concurrent tasks can be launched when spark.task.cpus > 1") {
    val conf = new SparkConf()
      .set(CPUS_PER_TASK, BigDecimal(2))
      .setMaster("local-cluster[4, 3, 1024]")
      .setAppName("test")
    sc = new SparkContext(conf)
    eventually(timeout(executorUpTimeout)) {
      // Ensure all executors have been launched.
      assert(sc.getExecutorIds().length == 4)
    }
    // Each executor can only launch one task since `spark.task.cpus` is 2.
    assert(sc.maxNumConcurrentTasks(ResourceProfile.getOrCreateDefaultProfile(conf)) == 4)
  }

  test("compute max number of concurrent tasks can be launched when some executors are busy") {
    val conf = new SparkConf()
      .set(CPUS_PER_TASK, BigDecimal(2))
      .setMaster("local-cluster[4, 3, 1024]")
      .setAppName("test")
    sc = new SparkContext(conf)
    val rdd = sc.parallelize(1 to 10, 4).mapPartitions { iter =>
      Thread.sleep(5000)
      iter
    }
    val taskStarted = new AtomicBoolean(false)
    val taskEnded = new AtomicBoolean(false)
    val listener = new SparkListener() {
      override def onTaskStart(taskStart: SparkListenerTaskStart): Unit = {
        taskStarted.set(true)
      }

      override def onTaskEnd(taskEnd: SparkListenerTaskEnd): Unit = {
        taskEnded.set(true)
      }
    }

    try {
      sc.addSparkListener(listener)
      eventually(timeout(executorUpTimeout)) {
        // Ensure all executors have been launched.
        assert(sc.getExecutorIds().length == 4)
      }

      // Submit a job to trigger some tasks on active executors.
      testSubmitJob(sc, rdd)

      eventually(timeout(10.seconds)) {
        // Ensure some tasks have started and no task finished, so some executors must be busy.
        assert(taskStarted.get())
        assert(taskEnded.get() == false)
        // Assert we count in slots on both busy and free executors.
        assert(
          sc.maxNumConcurrentTasks(ResourceProfile.getOrCreateDefaultProfile(conf)) == 4)
      }
    } finally {
      sc.removeSparkListener(listener)
    }
  }

  test("SPARK-47458 compute max number of concurrent tasks with resources limiting") {
    withTempDir { dir =>
      val discoveryScript = createTempScriptWithExpectedOutput(
        dir, "gpuDiscoveryScript", """{"name": "gpu","addresses":["0", "1", "2", "3"]}""")
      val conf = new SparkConf()
        .set(CPUS_PER_TASK, BigDecimal(1))
        .setMaster("local-cluster[1, 20, 1024]")
        .setAppName("test")
        .set(WORKER_GPU_ID.amountConf, "4")
        .set(WORKER_GPU_ID.discoveryScriptConf, discoveryScript)
        .set(EXECUTOR_GPU_ID.amountConf, "4")
        .set(TASK_GPU_ID.amountConf, "0.2")
      sc = new SparkContext(conf)
      eventually(timeout(executorUpTimeout)) {
        // Ensure all executors have been launched.
        assert(sc.getExecutorIds().length == 1)
      }
      // The concurrent tasks should be min of {20/1, 4 * (1/0.2)}
      assert(sc.maxNumConcurrentTasks(ResourceProfile.getOrCreateDefaultProfile(conf)) == 20)

      val gpuTaskAmountToExpectedTasks = Map(
        0.3 -> 12,  // 4 * (1/0.3).toInt
        0.4 -> 8,   // 4 * (1/0.4).toInt
        0.5 -> 8,   // 4 * (1/0.5).toInt
        0.8 -> 4,   // 4 * (1/0.8).toInt
        1.0 -> 4,   // 4 / 1
        2.0 -> 2,   // 4 / 2
        3.0 -> 1,   // 4 / 3
        4.0 -> 1    // 4 / 4
      )

      // It's the GPU resource that limits the concurrent number
      gpuTaskAmountToExpectedTasks.keys.foreach { taskGpu =>
        val treqs = new TaskResourceRequests().cpus(1).resource(GPU, taskGpu)
        val rp: ResourceProfile = new ResourceProfileBuilder().require(treqs).build()
        sc.resourceProfileManager.addResourceProfile(rp)
        assert(sc.maxNumConcurrentTasks(rp) == gpuTaskAmountToExpectedTasks(taskGpu))
      }
    }
  }

  // Every item corresponds to (CPU resources per task, GPU resources per task,
  // and the GPU addresses assigned to all tasks).
  Seq(
    (1, 1, Array(Array("0"), Array("1"), Array("2"), Array("3"))),
    (1, 2, Array(Array("0", "1"), Array("2", "3"))),
    (1, 4, Array(Array("0", "1", "2", "3"))),
    (2, 1, Array(Array("0"), Array("1"))),
    (4, 1, Array(Array("0"))),
    (4, 2, Array(Array("0", "1"))),
    (2, 2, Array(Array("0", "1"), Array("2", "3"))),
    (4, 4, Array(Array("0", "1", "2", "3"))),
    (1, 3, Array(Array("0", "1", "2"))),
    (3, 1, Array(Array("0")))
  ).foreach { case (taskCpus, taskGpus, expectedGpuAddresses) =>
    test(s"SPARK-47663 end to end test validating if task cpus:${taskCpus} and " +
      s"task gpus: ${taskGpus} works") {
      withTempDir { dir =>
        val discoveryScript = createTempScriptWithExpectedOutput(
          dir, "gpuDiscoveryScript", """{"name": "gpu","addresses":["0", "1", "2", "3"]}""")
        val conf = new SparkConf()
          .set(CPUS_PER_TASK, BigDecimal(taskCpus))
          .setMaster("local-cluster[1, 4, 1024]")
          .setAppName("test")
          .set(WORKER_GPU_ID.amountConf, "4")
          .set(WORKER_GPU_ID.discoveryScriptConf, discoveryScript)
          .set(EXECUTOR_GPU_ID.amountConf, "4")
          .set(TASK_GPU_ID.amountConf, taskGpus.toString)

        sc = new SparkContext(conf)
        eventually(timeout(executorUpTimeout)) {
          // Ensure all executors have been launched.
          assert(sc.getExecutorIds().length == 1)
        }

        val numPartitions = Seq(4 / taskCpus, 4 / taskGpus).min
        val ret = sc.parallelize(1 to 20, numPartitions).mapPartitions { _ =>
          val tc = TaskContext.get()
          assert(tc.cpuAmount() == taskCpus)
          val gpus = tc.resources()("gpu").addresses
          Iterator.single(gpus)
        }.collect()

        assert(ret === expectedGpuAddresses)
      }
    }
  }

  // Here we just have test for one happy case instead of all cases: other cases are covered in
  // FsHistoryProviderSuite.
  test("custom log url for Spark UI is applied") {
    val customExecutorLogUrl = "http://newhost:9999/logs/clusters/{{CLUSTER_ID}}/users/{{USER}}" +
      "/containers/{{CONTAINER_ID}}/{{FILE_NAME}}"

    val conf = new SparkConf()
      .set(UI.CUSTOM_EXECUTOR_LOG_URL, customExecutorLogUrl)
      .setMaster("local-cluster[0, 3, 1024]")
      .setAppName("test")

    sc = new SparkContext(conf)
    val backend = sc.schedulerBackend.asInstanceOf[CoarseGrainedSchedulerBackend]
    val mockEndpointRef = mock[RpcEndpointRef]
    val mockAddress = mock[RpcAddress]

    val logUrls = Map(
      "stdout" -> "http://oldhost:8888/logs/dummy/stdout",
      "stderr" -> "http://oldhost:8888/logs/dummy/stderr")
    val attributes = Map(
      "CLUSTER_ID" -> "cl1",
      "USER" -> "dummy",
      "CONTAINER_ID" -> "container1",
      "LOG_FILES" -> "stdout,stderr")
    val baseUrl = s"http://newhost:9999/logs/clusters/${attributes("CLUSTER_ID")}" +
      s"/users/${attributes("USER")}/containers/${attributes("CONTAINER_ID")}"

    var executorAddedCount: Int = 0
    val listener = new SparkListener() {
      override def onExecutorAdded(executorAdded: SparkListenerExecutorAdded): Unit = {
        executorAddedCount += 1
        assert(executorAdded.executorInfo.logUrlMap === Seq("stdout", "stderr").map { file =>
          file -> (baseUrl + s"/$file")
        }.toMap)
      }
    }

    sc.addSparkListener(listener)

    backend.driverEndpoint.askSync[Boolean](
      RegisterExecutor("1", mockEndpointRef, mockAddress.host, 1, logUrls, attributes,
        Map.empty, ResourceProfile.DEFAULT_RESOURCE_PROFILE_ID))
    backend.driverEndpoint.askSync[Boolean](
      RegisterExecutor("2", mockEndpointRef, mockAddress.host, 1, logUrls, attributes,
        Map.empty, ResourceProfile.DEFAULT_RESOURCE_PROFILE_ID))
    backend.driverEndpoint.askSync[Boolean](
      RegisterExecutor("3", mockEndpointRef, mockAddress.host, 1, logUrls, attributes,
        Map.empty, ResourceProfile.DEFAULT_RESOURCE_PROFILE_ID))

    sc.listenerBus.waitUntilEmpty(executorUpTimeout.toMillis)
    assert(executorAddedCount === 3)
  }

  test("extra resources from executor") {

    val testStartTime = System.currentTimeMillis()

    val execCores = 3
    val conf = new SparkConf()
      .set(EXECUTOR_CORES, execCores)
      .set(SCHEDULER_REVIVE_INTERVAL.key, "1m") // don't let it auto revive during test
      .set(EXECUTOR_INSTANCES, 0) // avoid errors about duplicate executor registrations
      .setMaster(
      "coarseclustermanager[org.apache.spark.scheduler.TestCoarseGrainedSchedulerBackend]")
      .setAppName("test")
    conf.set(TASK_GPU_ID.amountConf, "1")
    conf.set(EXECUTOR_GPU_ID.amountConf, "1")

    sc = new SparkContext(conf)
    val execGpu = new ExecutorResourceRequests().cores(1).resource(GPU, 3)
    val taskGpu = new TaskResourceRequests().cpus(1).resource(GPU, 1)
    val rp = new ResourceProfile(execGpu.requests, taskGpu.requests)
    sc.resourceProfileManager.addResourceProfile(rp)
    assert(rp.id > ResourceProfile.DEFAULT_RESOURCE_PROFILE_ID)
    val backend = sc.schedulerBackend.asInstanceOf[TestCoarseGrainedSchedulerBackend]
    // Note we get two in default profile and one in the new rp
    // we need to put a req time in for all of them.
    backend.requestTotalExecutors(Map((rp.id, 1)), Map(), Map())
    backend.requestExecutors(3)
    val mockEndpointRef = mock[RpcEndpointRef]
    val mockAddress = mock[RpcAddress]
    when(mockEndpointRef.send(LaunchTask)).thenAnswer((_: InvocationOnMock) => {})

    val resources = Map(GPU -> new ResourceInformation(GPU, Array("0", "1", "3")))

    var executorAddedCount: Int = 0
    val infos = scala.collection.mutable.ArrayBuffer[ExecutorInfo]()
    val listener = new SparkListener() {
      override def onExecutorAdded(executorAdded: SparkListenerExecutorAdded): Unit = {
        // Lets check that the exec allocation times "make sense"
        val info = executorAdded.executorInfo
        infos += info
        executorAddedCount += 1
      }
    }

    sc.addSparkListener(listener)

    backend.driverEndpoint.askSync[Boolean](
      RegisterExecutor("1", mockEndpointRef, mockAddress.host, 1, Map.empty, Map.empty, resources,
        ResourceProfile.DEFAULT_RESOURCE_PROFILE_ID))
    backend.driverEndpoint.askSync[Boolean](
      RegisterExecutor("2", mockEndpointRef, mockAddress.host, 1, Map.empty, Map.empty, resources,
        ResourceProfile.DEFAULT_RESOURCE_PROFILE_ID))
    backend.driverEndpoint.askSync[Boolean](
      RegisterExecutor("3", mockEndpointRef, mockAddress.host, 1, Map.empty, Map.empty, resources,
        rp.id))

    val frameSize = RpcUtils.maxMessageSizeBytes(sc.conf)
    val bytebuffer = java.nio.ByteBuffer.allocate(frameSize - 100)
    val buffer = new SerializableBuffer(bytebuffer)

    var execResources = backend.getExecutorAvailableResources("1")
    assert(execResources(GPU).availableAddrs.sorted === Array("0", "1", "3"))

    val exec3ResourceProfileId = backend.getExecutorResourceProfileId("3")
    assert(exec3ResourceProfileId === rp.id)

    val taskResources = Map(GPU -> Map("0" -> ONE_ENTIRE_RESOURCE))
    val taskCpus = 1
    val taskDescs: Seq[Seq[TaskDescription]] = Seq(Seq(new TaskDescription(1, 0, "1",
      "t1", 0, 1, JobArtifactSet.emptyJobArtifactSet, new Properties(),
      taskCpus, taskResources, None, bytebuffer)))
    val ts = backend.getTaskSchedulerImpl()
    when(ts.resourceOffers(any[IndexedSeq[WorkerOffer]], any[Boolean])).thenReturn(taskDescs)

    backend.driverEndpoint.send(ReviveOffers)

    eventually(timeout(5 seconds)) {
      execResources = backend.getExecutorAvailableResources("1")
      assert(execResources(GPU).availableAddrs.sorted === Array("1", "3"))
      assert(execResources(GPU).assignedAddrs === Array("0"))
    }

    // To avoid allocating any resources immediately after releasing the resource from the task to
    // make sure that `availableAddrs` below won't change
    when(ts.resourceOffers(any[IndexedSeq[WorkerOffer]], any[Boolean])).thenReturn(Seq.empty)
    backend.driverEndpoint.send(
      StatusUpdate("1", 1, TaskState.FINISHED, buffer, taskCpus, taskResources))

    eventually(timeout(5 seconds)) {
      execResources = backend.getExecutorAvailableResources("1")
      assert(execResources(GPU).availableAddrs.sorted === Array("0", "1", "3"))
      assert(execResources(GPU).assignedAddrs.isEmpty)
    }
    sc.listenerBus.waitUntilEmpty(executorUpTimeout.toMillis)
    assert(executorAddedCount === 3)
    infos.foreach { info =>
      assert(info.requestTime.get > 0,
        "Exec allocation and request times don't make sense")
      assert(info.requestTime.get > testStartTime,
        "Exec allocation and request times don't make sense")
      assert(info.registrationTime.get >= info.requestTime.get,
        "Exec allocation and request times don't make sense")
    }
  }

  test("exec alloc decrease.") {

    val testStartTime = System.currentTimeMillis()

    val execCores = 3
    val conf = new SparkConf()
      .set(EXECUTOR_CORES, execCores)
      .set(SCHEDULER_REVIVE_INTERVAL.key, "1m") // don't let it auto revive during test
      .set(EXECUTOR_INSTANCES, 0) // avoid errors about duplicate executor registrations
      .setMaster(
      "coarseclustermanager[org.apache.spark.scheduler.TestCoarseGrainedSchedulerBackend]")
      .setAppName("test")
    conf.set(TASK_GPU_ID.amountConf, "1")
    conf.set(EXECUTOR_GPU_ID.amountConf, "1")

    sc = new SparkContext(conf)
    val execGpu = new ExecutorResourceRequests().cores(1).resource(GPU, 3)
    val taskGpu = new TaskResourceRequests().cpus(1).resource(GPU, 1)
    val rp = new ResourceProfile(execGpu.requests, taskGpu.requests)
    sc.resourceProfileManager.addResourceProfile(rp)
    assert(rp.id > ResourceProfile.DEFAULT_RESOURCE_PROFILE_ID)
    val backend = sc.schedulerBackend.asInstanceOf[TestCoarseGrainedSchedulerBackend]
    // Note we get two in default profile and one in the new rp
    // we need to put a req time in for all of them.
    backend.requestTotalExecutors(Map((rp.id, 1)), Map(), Map())
    // Decrease the number of execs requested in the new rp.
    backend.requestTotalExecutors(Map((rp.id, 0)), Map(), Map())
    // Request execs in the default profile.
    backend.requestExecutors(3)
    val mockEndpointRef = mock[RpcEndpointRef]
    val mockAddress = mock[RpcAddress]
    when(mockEndpointRef.send(LaunchTask)).thenAnswer((_: InvocationOnMock) => {})

    val resources = Map(GPU -> new ResourceInformation(GPU, Array("0", "1", "3")))

    var executorAddedCount: Int = 0
    val infos = scala.collection.mutable.ArrayBuffer[ExecutorInfo]()
    val listener = new SparkListener() {
      override def onExecutorAdded(executorAdded: SparkListenerExecutorAdded): Unit = {
        // Lets check that the exec allocation times "make sense"
        val info = executorAdded.executorInfo
        infos += info
        executorAddedCount += 1
      }
    }

    sc.addSparkListener(listener)

    backend.driverEndpoint.askSync[Boolean](
      RegisterExecutor("1", mockEndpointRef, mockAddress.host, 1, Map.empty, Map.empty, resources,
        ResourceProfile.DEFAULT_RESOURCE_PROFILE_ID))
    backend.driverEndpoint.askSync[Boolean](
      RegisterExecutor("2", mockEndpointRef, mockAddress.host, 1, Map.empty, Map.empty, resources,
        ResourceProfile.DEFAULT_RESOURCE_PROFILE_ID))
    backend.driverEndpoint.askSync[Boolean](
      RegisterExecutor("3", mockEndpointRef, mockAddress.host, 1, Map.empty, Map.empty, resources,
        rp.id))

    val frameSize = RpcUtils.maxMessageSizeBytes(sc.conf)
    val bytebuffer = java.nio.ByteBuffer.allocate(frameSize - 100)
    val buffer = new SerializableBuffer(bytebuffer)

    var execResources = backend.getExecutorAvailableResources("1")
    assert(execResources(GPU).availableAddrs.sorted === Array("0", "1", "3"))

    val exec3ResourceProfileId = backend.getExecutorResourceProfileId("3")
    assert(exec3ResourceProfileId === rp.id)

    val taskResources = Map(GPU -> Map("0" -> ONE_ENTIRE_RESOURCE))
    val taskCpus = 1
    val taskDescs: Seq[Seq[TaskDescription]] = Seq(Seq(new TaskDescription(1, 0, "1",
      "t1", 0, 1, JobArtifactSet.emptyJobArtifactSet, new Properties(),
      taskCpus, taskResources, None, bytebuffer)))
    val ts = backend.getTaskSchedulerImpl()
    when(ts.resourceOffers(any[IndexedSeq[WorkerOffer]], any[Boolean])).thenReturn(taskDescs)

    backend.driverEndpoint.send(ReviveOffers)

    eventually(timeout(5 seconds)) {
      execResources = backend.getExecutorAvailableResources("1")
      assert(execResources(GPU).availableAddrs.sorted === Array("1", "3"))
      assert(execResources(GPU).assignedAddrs === Array("0"))
    }

    // To avoid allocating any resources immediately after releasing the resource from the task to
    // make sure that `availableAddrs` below won't change
    when(ts.resourceOffers(any[IndexedSeq[WorkerOffer]], any[Boolean])).thenReturn(Seq.empty)
    backend.driverEndpoint.send(
      StatusUpdate("1", 1, TaskState.FINISHED, buffer, taskCpus, taskResources))

    eventually(timeout(5 seconds)) {
      execResources = backend.getExecutorAvailableResources("1")
      assert(execResources(GPU).availableAddrs.sorted === Array("0", "1", "3"))
      assert(execResources(GPU).assignedAddrs.isEmpty)
    }
    sc.listenerBus.waitUntilEmpty(executorUpTimeout.toMillis)
    assert(executorAddedCount === 3)
    infos.foreach { info =>
      info.requestTime.map { t =>
        assert(t > 0,
          "Exec request times don't make sense")
        assert(t >= testStartTime,
          "Exec allocation and request times don't make sense")
        assert(t <= info.registrationTime.get,
          "Exec allocation and request times don't make sense")
      }
    }
    assert(infos.filter(_.requestTime.isEmpty).length === 1,
      "Our unexpected executor does not have a request time.")
  }

  test("SPARK-41848: executor cores should be decreased based on taskCpus") {
    val testStartTime = System.currentTimeMillis()

    val execCores = 3
    val conf = new SparkConf()
      .set(EXECUTOR_CORES, execCores)
      .set(SCHEDULER_REVIVE_INTERVAL.key, "1m") // don't let it auto revive during test
      .set(EXECUTOR_INSTANCES, 0)
      .setMaster(
        "coarseclustermanager[org.apache.spark.scheduler.TestCoarseGrainedSchedulerBackend]")
      .setAppName("test")

    sc = new SparkContext(conf)

    val backend = sc.schedulerBackend.asInstanceOf[TestCoarseGrainedSchedulerBackend]
    // Request execs in the default profile.
    backend.requestExecutors(1)
    val mockEndpointRef = mock[RpcEndpointRef]
    val mockAddress = mock[RpcAddress]
    when(mockEndpointRef.send(LaunchTask)).thenAnswer((_: InvocationOnMock) => {})

    var executorAddedCount: Int = 0
    val infos = mutable.ArrayBuffer[ExecutorInfo]()
    val listener = new SparkListener() {
      override def onExecutorAdded(executorAdded: SparkListenerExecutorAdded): Unit = {
        // Lets check that the exec allocation times "make sense"
        val info = executorAdded.executorInfo
        infos += info
        executorAddedCount += 1
      }
    }

    sc.addSparkListener(listener)

    val ts = backend.getTaskSchedulerImpl()
    when(ts.resourceOffers(any[IndexedSeq[WorkerOffer]], any[Boolean])).thenReturn(Seq.empty)
    backend.driverEndpoint.askSync[Boolean](
      RegisterExecutor("1", mockEndpointRef, mockAddress.host, execCores, Map.empty, Map.empty,
        Map.empty, ResourceProfile.DEFAULT_RESOURCE_PROFILE_ID))
    backend.driverEndpoint.send(LaunchedExecutor("1"))
    eventually(timeout(5 seconds)) {
      assert(backend.getExecutorAvailableCpus("1").contains(3))
    }

    val frameSize = RpcUtils.maxMessageSizeBytes(sc.conf)
    val bytebuffer = java.nio.ByteBuffer.allocate(frameSize - 100)
    val buffer = new SerializableBuffer(bytebuffer)

    val defaultRp = ResourceProfile.getOrCreateDefaultProfile(conf)
    assert(ResourceProfile.getTaskCpusOrDefaultForProfile(defaultRp, conf) == 1)
    // Task cpus can be different from default resource profile when TaskResourceProfile is used.
    val taskCpus = 2
    val taskDescs: Seq[Seq[TaskDescription]] = Seq(Seq(new TaskDescription(1, 0, "1",
      "t1", 0, 1, JobArtifactSet.emptyJobArtifactSet, new Properties(),
      taskCpus, Map.empty, None, bytebuffer)))
    when(ts.resourceOffers(any[IndexedSeq[WorkerOffer]], any[Boolean])).thenReturn(taskDescs)

    backend.driverEndpoint.send(ReviveOffers)

    eventually(timeout(5 seconds)) {
      assert(backend.getExecutorAvailableCpus("1").contains(1))
    }

    // To avoid allocating any resources immediately after releasing the resource from the task to
    // make sure that executor's available cpus below won't change
    when(ts.resourceOffers(any[IndexedSeq[WorkerOffer]], any[Boolean])).thenReturn(Seq.empty)
    backend.driverEndpoint.send(
      StatusUpdate("1", 1, TaskState.FINISHED, buffer, taskCpus))

    eventually(timeout(5 seconds)) {
      assert(backend.getExecutorAvailableCpus("1").contains(3))
    }
    sc.listenerBus.waitUntilEmpty(executorUpTimeout.toMillis)
    assert(executorAddedCount === 1)
    infos.foreach { info =>
      info.requestTime.map { t =>
        assert(t > 0,
          "Exec request times don't make sense")
        assert(t >= testStartTime,
          "Exec allocation and request times don't make sense")
        assert(t <= info.registrationTime.get,
          "Exec allocation and request times don't make sense")
      }
    }
  }

  Seq(false, true).foreach { isBarrier =>
    test("SPARK-58879: idle decommission rejects tasks assigned before LaunchTask " +
      s"(barrier=$isBarrier)") {
      val numTasks = if (isBarrier) 2 else 1
      val conf = new SparkConf().set(EXECUTOR_CORES, 2)
      val backend = createDecommissionBackend(conf)
      val scheduler = backend.taskScheduler
      val executor = registerDecommissionExecutor(backend, "1", 2)
      val decommissionCalled = new AtomicBoolean(false)
      backend.beforeDecommission = (_, _, _) => decommissionCalled.set(true)
      val launchEntered = new CountDownLatch(1)
      val allowLaunch = new CountDownLatch(1)
      executor.beforeLaunch = _ => {
        launchEntered.countDown()
        require(allowLaunch.await(30, TimeUnit.SECONDS), "LaunchTask was not released")
      }
      val taskSet = if (isBarrier) {
        FakeTask.createBarrierTaskSet(numTasks)
      } else {
        FakeTask.createTaskSet(numTasks)
      }
      scheduler.submitTasks(taskSet)
      backend.driverEndpoint.send(ReviveOffers)

      var requestThread: Thread = null
      try {
        assert(launchEntered.await(10, TimeUnit.SECONDS))
        assert(scheduler.runningTasksByExecutors("1") === numTasks)
        val (thread, request) = startDecommissionRequest {
          backend.decommissionExecutorsIfIdle(
            Array("1" -> ExecutorDecommissionInfo("idle timeout")), false)
        }
        requestThread = thread
        assert(request.get(10, TimeUnit.SECONDS).isEmpty)
        assert(backend.isExecutorActive("1"))
        assert(!executor.decommissionReceived)
        assert(!decommissionCalled.get())
      } finally {
        allowLaunch.countDown()
        if (requestThread != null) {
          requestThread.join(TimeUnit.SECONDS.toMillis(10))
          assert(!requestThread.isAlive)
        }
      }

      val tasks = (0 until numTasks).map(_ => executor.nextTask())
      flushDecommissionBackend(backend)
      tasks.foreach(completeDecommissionTestTask(backend, _))
      assert(!scheduler.isExecutorBusy("1"))
      assert(backend.getExecutorAvailableCpus("1").contains(BigDecimal(2)))
      assert(backend.decommissionExecutorsIfIdle(
        Array("1" -> ExecutorDecommissionInfo("idle timeout")), false) === Seq("1"))
      assert(executor.decommissionReceived)
    }
  }

  test("SPARK-58879: idle decommission fences an executor before concurrent resource offers") {
    val backend = createDecommissionBackend()
    val retired = registerDecommissionExecutor(backend, "1")
    val survivor = registerDecommissionExecutor(backend, "2")
    backend.taskScheduler.submitTasks(FakeTask.createTaskSet(1))
    val admissionEntered = new CountDownLatch(1)
    val allowAdmission = new CountDownLatch(1)
    val offerEntered = new CountDownLatch(1)
    val locksHeld = new AtomicBoolean(false)
    val decommissionReleased = new AtomicBoolean(false)
    backend.beforeDecommission = (_, _, _) => {
      locksHeld.set(Thread.holdsLock(backend.taskScheduler) && Thread.holdsLock(backend))
      admissionEntered.countDown()
      decommissionReleased.set(allowAdmission.await(30, TimeUnit.SECONDS))
    }
    backend.beforeOffers = () => offerEntered.countDown()

    val (requestThread, request) = startDecommissionRequest {
      backend.decommissionExecutorsIfIdle(
        Array("1" -> ExecutorDecommissionInfo("idle timeout")), false)
    }
    try {
      assert(admissionEntered.await(10, TimeUnit.SECONDS))
      backend.driverEndpoint.send(ReviveOffers)
      assert(offerEntered.await(10, TimeUnit.SECONDS))
    } finally {
      allowAdmission.countDown()
      requestThread.join(TimeUnit.SECONDS.toMillis(10))
      assert(!requestThread.isAlive)
    }
    assert(request.get(10, TimeUnit.SECONDS) === Seq("1"))
    assert(locksHeld.get())
    assert(decommissionReleased.get())
    flushDecommissionBackend(backend)
    assert(retired.launchedTasks.isEmpty)
    assert(retired.decommissionReceived)
    val task = survivor.nextTask()
    assert(task.executorId === "2")
    completeDecommissionTestTask(backend, task)
  }

  test("SPARK-58879: idle decommission skips duplicates and does not replay rejected requests") {
    // Retention defaults to zero. Enable it so an incorrectly queued request is observable.
    val conf = new SparkConf().set(SCHEDULER_MAX_RETAINED_UNKNOWN_EXECUTORS, 1)
    val backend = createDecommissionBackend(conf)
    val first = registerDecommissionExecutor(backend, "1")
    val second = registerDecommissionExecutor(backend, "2")
    val info = ExecutorDecommissionInfo("idle timeout")
    val requests = mutable.ArrayBuffer.empty[(Seq[String], Boolean, Boolean)]
    backend.beforeDecommission = (ids, adjustTarget, triggeredByExecutor) => {
      requests += ((ids, adjustTarget, triggeredByExecutor))
    }

    assert(backend.decommissionExecutorsIfIdle(
      Array("1" -> info, "1" -> info, "3" -> info), false) === Seq("1"))
    assert(backend.decommissionExecutorsIfIdle(Array("1" -> info), false).isEmpty)
    assert(requests.toSeq === Seq((Seq("1"), false, false)))
    assert(first.decommissionReceived)
    assert(!second.decommissionReceived)
    assert(!backend.hasUnknownDecommission("1"))
    assert(!backend.hasUnknownDecommission("3"))

    val third = registerDecommissionExecutor(backend, "3")
    assert(!third.decommissionReceived)
    assert(backend.isExecutorActive("3"))

    assert(backend.decommissionExecutors(Array("4" -> info), false, false).isEmpty)
    assert(backend.hasUnknownDecommission("4"))
    val fourth = registerDecommissionExecutor(backend, "4")
    assert(fourth.decommissionReceived)
    assert(!backend.isExecutorActive("4"))
    assert(!backend.hasUnknownDecommission("4"))
  }

  test("SPARK-58879: forced decommission still accepts a busy executor") {
    val backend = createDecommissionBackend()
    val executor = registerDecommissionExecutor(backend, "1")
    backend.taskScheduler.submitTasks(FakeTask.createTaskSet(1))
    backend.driverEndpoint.send(ReviveOffers)
    val task = executor.nextTask()
    assert(backend.taskScheduler.isExecutorBusy("1"))

    assert(backend.decommissionExecutors(
      Array("1" -> ExecutorDecommissionInfo("host drain")), false, false) === Seq("1"))
    assert(executor.decommissionReceived)
    assert(!backend.isExecutorActive("1"))
    completeDecommissionTestTask(backend, task)
    assert(!backend.taskScheduler.isExecutorBusy("1"))
    assert(executor.launchedTasks.isEmpty)
  }

  test("SPARK-41766: New registered executor should receive decommission request" +
    " sent before registration") {
    val conf = new SparkConf()
      .setMaster("local-cluster[0, 3, 1024]")
      .setAppName("test")
      .set(SCHEDULER_MAX_RETAINED_UNKNOWN_EXECUTORS.key, "1")

    sc = new SparkContext(conf)
    val backend = sc.schedulerBackend.asInstanceOf[CoarseGrainedSchedulerBackend]
    val mockEndpointRef = new MockExecutorRpcEndpointRef(conf)
    val mockAddress = mock[RpcAddress]
    val executorId = "1"
    val executorDecommissionInfo = ExecutorDecommissionInfo(
      s"Executor $executorId is decommissioned")

    backend.decommissionExecutor(executorId, executorDecommissionInfo, false)
    assert(!mockEndpointRef.decommissionReceived)

    backend.driverEndpoint.askSync[Boolean](
      RegisterExecutor("1", mockEndpointRef, mockAddress.host, 1, Map(), Map(),
        Map.empty, ResourceProfile.DEFAULT_RESOURCE_PROFILE_ID))

    sc.listenerBus.waitUntilEmpty(executorUpTimeout.toMillis)
    assert(mockEndpointRef.decommissionReceived)
  }

  test("SPARK-58886: requestExecutors should saturate instead of overflowing a huge" +
    " requested total") {
    val conf = new SparkConf()
      .setMaster("local-cluster[0, 3, 1024]")
      .setAppName("test")

    sc = new SparkContext(conf)
    val backend = sc.schedulerBackend.asInstanceOf[CoarseGrainedSchedulerBackend]

    sc.requestTotalExecutors(Int.MaxValue - 1, 0, Map.empty)
    backend.requestExecutors(2)

    val defaultProf = sc.resourceProfileManager.defaultResourceProfile
    assert(backend.getRequestedTotalExecutors()(defaultProf) === Int.MaxValue)

    // Only the applied increase (1, not the requested 2) may be recorded as a pending
    // request time. Shrink the total to 1 to consume the huge seed entry, leaving just
    // that increment: exactly one of the two executors registered below should get a
    // request time.
    sc.requestTotalExecutors(1, 0, Map.empty)

    val infos = mutable.ArrayBuffer[ExecutorInfo]()
    sc.addSparkListener(new SparkListener() {
      override def onExecutorAdded(executorAdded: SparkListenerExecutorAdded): Unit = {
        infos += executorAdded.executorInfo
      }
    })
    val mockAddress = mock[RpcAddress]
    Seq("1", "2").foreach { id =>
      backend.driverEndpoint.askSync[Boolean](
        RegisterExecutor(id, new MockExecutorRpcEndpointRef(conf), mockAddress.host, 1,
          Map(), Map(), Map.empty, ResourceProfile.DEFAULT_RESOURCE_PROFILE_ID))
    }
    sc.listenerBus.waitUntilEmpty(executorUpTimeout.toMillis)
    assert(infos.size === 2)
    assert(infos.head.requestTime.isDefined)
    assert(infos.last.requestTime.isEmpty)
  }

  test("UpdateUserCredentials is broadcast to all registered executors") {
    val conf = new SparkConf()
      .setMaster("local-cluster[0, 3, 1024]")
      .setAppName("test")

    sc = new SparkContext(conf)
    val backend = sc.schedulerBackend.asInstanceOf[CoarseGrainedSchedulerBackend]

    // Register two mock executors
    val mockEndpointRef1 = new MockExecutorRpcEndpointRef(conf)
    val mockEndpointRef2 = new MockExecutorRpcEndpointRef(conf)
    val mockAddress = mock[RpcAddress]

    backend.driverEndpoint.askSync[Boolean](
      RegisterExecutor("1", mockEndpointRef1, mockAddress.host, 1, Map(), Map(),
        Map.empty, ResourceProfile.DEFAULT_RESOURCE_PROFILE_ID))
    backend.driverEndpoint.askSync[Boolean](
      RegisterExecutor("2", mockEndpointRef2, mockAddress.host, 1, Map(), Map(),
        Map.empty, ResourceProfile.DEFAULT_RESOURCE_PROFILE_ID))

    sc.listenerBus.waitUntilEmpty(executorUpTimeout.toMillis)

    // Neither executor should have received credentials yet
    assert(mockEndpointRef1.receivedUserCredentials.isEmpty)
    assert(mockEndpointRef2.receivedUserCredentials.isEmpty)

    // Send UpdateUserCredentials via DriverEndpoint
    val testCredentials = Array[Byte](1, 2, 3, 4, 5)
    backend.driverEndpoint.send(UpdateUserCredentials(1L, testCredentials))

    // Wait for the message to be processed
    eventually(timeout(5 seconds)) {
      assert(mockEndpointRef1.receivedUserCredentials.isDefined)
      assert(mockEndpointRef2.receivedUserCredentials.isDefined)
    }

    assert(mockEndpointRef1.receivedUserCredentials.get._2 === testCredentials)
    assert(mockEndpointRef2.receivedUserCredentials.get._2 === testCredentials)

    // Verify SparkEnv credential store is also updated
    assert(SparkEnv.get.userCredentials.get().bytes === testCredentials)
  }

  test("SparkAppConfig includes current user credentials for late-registering executors") {
    val conf = new SparkConf()
      .setMaster("local-cluster[0, 3, 1024]")
      .setAppName("test")

    sc = new SparkContext(conf)
    val backend = sc.schedulerBackend.asInstanceOf[CoarseGrainedSchedulerBackend]

    // Simulate credential acquisition by setting credentials before any executor registers
    val testCredentials = Array[Byte](10, 20, 30, 40, 50)
    backend.driverEndpoint.send(UpdateUserCredentials(1L, testCredentials))

    // Wait for the message to be processed
    eventually(timeout(5 seconds)) {
      assert(SparkEnv.get.userCredentials.get() != null)
    }

    // Now retrieve SparkAppConfig as a late-registering executor would
    val appConfig = backend.driverEndpoint.askSync[SparkAppConfig](
      RetrieveSparkAppConfig(ResourceProfile.DEFAULT_RESOURCE_PROFILE_ID))

    // Verify that userCredentials is present in the response
    assert(appConfig.userCredentials.isDefined,
      "SparkAppConfig should include user credentials for late-registering executors")
    assert(appConfig.userCredentials.get._2 === testCredentials)
  }

  test("version guard prevents stale credentials from overwriting newer ones") {
    val conf = new SparkConf()
      .setMaster("local-cluster[0, 3, 1024]")
      .setAppName("test")

    sc = new SparkContext(conf)
    val backend = sc.schedulerBackend.asInstanceOf[CoarseGrainedSchedulerBackend]

    // Send version 3 credentials first
    val credsV3 = Array[Byte](30, 30, 30)
    backend.driverEndpoint.send(UpdateUserCredentials(3L, credsV3))

    eventually(timeout(5 seconds)) {
      assert(SparkEnv.get.userCredentials.get() != null)
      assert(SparkEnv.get.userCredentials.get().version === 3L)
    }

    // Now send version 1 (stale) -- should be rejected
    val credsV1 = Array[Byte](10, 10, 10)
    backend.driverEndpoint.send(UpdateUserCredentials(1L, credsV1))

    // Flush the DriverEndpoint mailbox by sending a synchronous request.
    // Since DriverEndpoint is single-threaded, when this returns, v1 has been processed.
    backend.driverEndpoint.askSync[SparkAppConfig](
      RetrieveSparkAppConfig(ResourceProfile.DEFAULT_RESOURCE_PROFILE_ID))

    // Store should still hold v3 (stale v1 was rejected by version guard)
    assert(SparkEnv.get.userCredentials.get().version === 3L,
      "Stale version 1 should not overwrite newer version 3")

    // Send version 5 (newer) -- should be accepted
    val credsV5 = Array[Byte](50, 50, 50)
    backend.driverEndpoint.send(UpdateUserCredentials(5L, credsV5))

    eventually(timeout(5 seconds)) {
      assert(SparkEnv.get.userCredentials.get().version === 5L)
    }

    // Verify version 5 credentials are in the store (not v1 or v3)
    assert(SparkEnv.get.userCredentials.get().bytes === credsV5)
  }

  test("executor-side credential store version guard rejects stale and accepts newer") {
    // This tests VersionedCredentials.updateIfNewer -- the same method used in
    // CoarseGrainedExecutorBackend.receive and Executor.TaskRunner.
    val store = new AtomicReference[VersionedCredentials]()

    // Initial write to null store should succeed
    VersionedCredentials.updateIfNewer(store, 2L, Array[Byte](20, 20))
    assert(store.get().version === 2L)
    assert(store.get().bytes === Array[Byte](20, 20))

    // Stale version (1) should be rejected
    VersionedCredentials.updateIfNewer(store, 1L, Array[Byte](10, 10))
    assert(store.get().version === 2L, "Stale version should not overwrite newer")

    // Same version (2) should also be rejected (strict >)
    VersionedCredentials.updateIfNewer(store, 2L, Array[Byte](22, 22))
    assert(store.get().version === 2L)
    assert(store.get().bytes === Array[Byte](20, 20), "Same version should not overwrite")

    // Newer version (5) should be accepted
    VersionedCredentials.updateIfNewer(store, 5L, Array[Byte](50, 50))
    assert(store.get().version === 5L)
    assert(store.get().bytes === Array[Byte](50, 50))
  }

  private def createDecommissionBackend(
      conf: SparkConf = new SparkConf()): DecommissionTestSchedulerBackend = {
    conf.setMaster(s"coarseclustermanager[${classOf[DecommissionTestSchedulerBackend].getName}]")
      .setAppName("idle decommission test")
      .set(EXECUTOR_INSTANCES, 0)
      .set(DYN_ALLOCATION_ENABLED, false)
      .set(DECOMMISSION_ENABLED, true)
    sc = new SparkContext(conf)
    sc.schedulerBackend.asInstanceOf[DecommissionTestSchedulerBackend]
  }

  private def registerDecommissionExecutor(
      backend: DecommissionTestSchedulerBackend,
      executorId: String,
      cores: Int = 1)
      : DecommissionTestExecutorRpcEndpointRef = {
    val executor = new DecommissionTestExecutorRpcEndpointRef(sc.conf, executorId)
    assert(backend.driverEndpoint.askSync[Boolean](
      RegisterExecutor(executorId, executor, "localhost", cores, Map.empty, Map.empty,
        Map.empty, ResourceProfile.DEFAULT_RESOURCE_PROFILE_ID)))
    backend.driverEndpoint.send(LaunchedExecutor(executorId))
    flushDecommissionBackend(backend)
    executor
  }

  private def flushDecommissionBackend(backend: DecommissionTestSchedulerBackend): Unit = {
    // Any synchronous request flushes earlier driver-endpoint messages. Ignore its result:
    // executor "1" may not be registered or may already be retired.
    backend.driverEndpoint.askSync[Boolean](IsExecutorAlive("1"))
  }

  private def completeDecommissionTestTask(
      backend: DecommissionTestSchedulerBackend,
      task: TaskDescription): Unit = {
    val result = new DirectTaskResult[Int](
      sc.env.serializer.newInstance().serialize(0), Seq.empty, Array.emptyLongArray)
    val serializedResult = sc.env.closureSerializer.newInstance().serialize(result)
    backend.driverEndpoint.send(StatusUpdate(
      task.executorId, task.taskId, TaskState.FINISHED, new SerializableBuffer(serializedResult),
      task.cpus, task.resources))
    flushDecommissionBackend(backend)
  }

  private def startDecommissionRequest[T](body: => T): (Thread, FutureTask[T]) = {
    val request = new FutureTask[T](new Callable[T] {
      override def call(): T = body
    })
    val thread = new Thread(request, "idle-decommission-test")
    thread.setDaemon(true)
    thread.start()
    (thread, request)
  }

  private def testSubmitJob(sc: SparkContext, rdd: RDD[Int]): Unit = {
    sc.submitJob(
      rdd,
      (iter: Iterator[Int]) => iter.toArray,
      rdd.partitions.indices,
      { case (_, _) => return }: (Int, Array[Int]) => Unit,
      { return }
    )
  }
}

/** Cluster manager for the mock resource tests and real-scheduler decommission tests. */
private class CSMockExternalClusterManager extends ExternalClusterManager {

  private var ts: TaskSchedulerImpl = _

  private val MOCK_REGEX = """coarseclustermanager\[(.*)\]""".r
  override def canCreate(masterURL: String): Boolean = MOCK_REGEX.findFirstIn(masterURL).isDefined

  override def createTaskScheduler(
      sc: SparkContext,
      masterURL: String): TaskScheduler = {
    masterURL match {
      case MOCK_REGEX(backendClassName)
          if backendClassName == classOf[DecommissionTestSchedulerBackend].getName =>
        ts = new TaskSchedulerImpl(sc, sc.conf.get(TASK_MAX_FAILURES))
      case _ =>
        ts = mock[TaskSchedulerImpl]
        when(ts.sc).thenReturn(sc)
        when(ts.applicationId()).thenReturn("appid1")
        when(ts.applicationAttemptId()).thenReturn(Some("attempt1"))
        when(ts.schedulingMode).thenReturn(SchedulingMode.FIFO)
        when(ts.excludedNodes()).thenReturn(Set.empty[String])
    }
    ts
  }

  override def createSchedulerBackend(
      sc: SparkContext,
      masterURL: String,
      scheduler: TaskScheduler): SchedulerBackend = {
    masterURL match {
      case MOCK_REGEX(backendClassName) =>
        val backendClass = Utils.classForName(backendClassName)
        val ctor = backendClass.getConstructor(classOf[TaskSchedulerImpl], classOf[RpcEnv])
        ctor.newInstance(scheduler, sc.env.rpcEnv).asInstanceOf[SchedulerBackend]
    }
  }

  override def initialize(scheduler: TaskScheduler, backend: SchedulerBackend): Unit = {
    scheduler.asInstanceOf[TaskSchedulerImpl].initialize(backend)
  }
}

private[spark]
class TestCoarseGrainedSchedulerBackend(scheduler: TaskSchedulerImpl, override val rpcEnv: RpcEnv)
  extends CoarseGrainedSchedulerBackend(scheduler, rpcEnv) {

  def getTaskSchedulerImpl(): TaskSchedulerImpl = scheduler
}

private[spark] class DecommissionTestSchedulerBackend(
    scheduler: TaskSchedulerImpl,
    override val rpcEnv: RpcEnv)
  extends CoarseGrainedSchedulerBackend(scheduler, rpcEnv) {

  val taskScheduler = scheduler
  @volatile var beforeOffers: () => Unit = () => ()
  @volatile var beforeDecommission: (Seq[String], Boolean, Boolean) => Unit = (_, _, _) => ()

  // Tests drive the real offer paths explicitly, without periodic or scheduler-triggered offers.
  override protected def createDriverEndpoint(): DriverEndpoint = new DriverEndpoint {
    override def onStart(): Unit = {}

    override def receive: PartialFunction[Any, Unit] = {
      case ReviveOffers =>
        beforeOffers()
        super.receive(ReviveOffers)
      case message => super.receive(message)
    }
  }

  override def reviveOffers(): Unit = {}

  override def decommissionExecutors(
      executorsAndDecomInfo: Array[(String, ExecutorDecommissionInfo)],
      adjustTargetNumExecutors: Boolean,
      triggeredByExecutor: Boolean): Seq[String] = {
    beforeDecommission(
      executorsAndDecomInfo.map(_._1).toSeq, adjustTargetNumExecutors, triggeredByExecutor)
    super.decommissionExecutors(
      executorsAndDecomInfo, adjustTargetNumExecutors, triggeredByExecutor)
  }

  def hasUnknownDecommission(executorId: String): Boolean = synchronized {
    unknownExecutorsPendingDecommission.getIfPresent(executorId) != null
  }
}

private[spark] class DecommissionTestExecutorRpcEndpointRef(
    conf: SparkConf,
    executorId: String) extends RpcEndpointRef(conf) {

  val launchedTasks = new LinkedBlockingQueue[TaskDescription]()
  @volatile var decommissionReceived = false
  @volatile var beforeLaunch: TaskDescription => Unit = (_: TaskDescription) => ()

  override def address: RpcAddress = RpcAddress("localhost", 10000 + executorId.toInt)
  override def name: String = s"executor-$executorId"

  override def send(message: Any): Unit = message match {
    case LaunchTask(data) =>
      val task = TaskDescription.decode(data.value)
      beforeLaunch(task)
      launchedTasks.add(task)
    case DecommissionExecutor => decommissionReceived = true
    case _ =>
  }

  override def ask[T: ClassTag](message: Any, timeout: RpcTimeout): Future[T] = {
    Future.successful(true.asInstanceOf[T])
  }

  def nextTask(): TaskDescription = {
    val task = launchedTasks.poll(10, TimeUnit.SECONDS)
    require(task != null, s"No task was launched on executor $executorId")
    task
  }
}

private[spark] class MockExecutorRpcEndpointRef(conf: SparkConf) extends RpcEndpointRef(conf) {
  // scalastyle:off executioncontextglobal
  import scala.concurrent.ExecutionContext.Implicits.global
  // scalastyle:on executioncontextglobal

  @volatile var decommissionReceived = false
  @volatile var receivedUserCredentials: Option[(Long, Array[Byte])] = None

  override def address: RpcAddress = null
  override def name: String = "executor"
  override def send(message: Any): Unit =
    message match {
      case DecommissionExecutor => decommissionReceived = true
      case UpdateUserCredentials(version, creds) => receivedUserCredentials = Some((version, creds))
      case _ =>
    }
  override def ask[T: ClassTag](message: Any, timeout: RpcTimeout): Future[T] = {
    Future{true.asInstanceOf[T]}
  }
}
