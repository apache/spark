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
import java.util.concurrent.atomic.AtomicBoolean

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
    conf.set("spark.default.parallelism", "1")
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
      .set(CPUS_PER_TASK, 2)
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
      .set(CPUS_PER_TASK, 2)
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
        .set(CPUS_PER_TASK, 1)
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
          .set(CPUS_PER_TASK, taskCpus)
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
          assert(tc.cpus() == taskCpus)
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
      taskCpus, taskResources, bytebuffer)))
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
      taskCpus, taskResources, bytebuffer)))
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
      taskCpus, Map.empty, bytebuffer)))
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

/** Simple cluster manager that wires up our mock backend for the resource tests. */
private class CSMockExternalClusterManager extends ExternalClusterManager {

  private var ts: TaskSchedulerImpl = _

  private val MOCK_REGEX = """coarseclustermanager\[(.*)\]""".r
  override def canCreate(masterURL: String): Boolean = MOCK_REGEX.findFirstIn(masterURL).isDefined

  override def createTaskScheduler(
      sc: SparkContext,
      masterURL: String): TaskScheduler = {
    ts = mock[TaskSchedulerImpl]
    when(ts.sc).thenReturn(sc)
    when(ts.applicationId()).thenReturn("appid1")
    when(ts.applicationAttemptId()).thenReturn(Some("attempt1"))
    when(ts.schedulingMode).thenReturn(SchedulingMode.FIFO)
    when(ts.excludedNodes()).thenReturn(Set.empty[String])
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

private[spark] class MockExecutorRpcEndpointRef(conf: SparkConf) extends RpcEndpointRef(conf) {
  // scalastyle:off executioncontextglobal
  import scala.concurrent.ExecutionContext.Implicits.global
  // scalastyle:on executioncontextglobal

  var decommissionReceived = false

  override def address: RpcAddress = null
  override def name: String = "executor"
  override def send(message: Any): Unit =
    message match {
      case DecommissionExecutor => decommissionReceived = true
    }
  override def ask[T: ClassTag](message: Any, timeout: RpcTimeout): Future[T] = {
    Future{true.asInstanceOf[T]}
  }
}
