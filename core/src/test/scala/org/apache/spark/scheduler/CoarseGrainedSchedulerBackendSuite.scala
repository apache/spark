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
import scala.concurrent.duration._
import scala.language.postfixOps

import org.mockito.ArgumentMatchers.any
import org.mockito.Mockito.when
import org.mockito.invocation.InvocationOnMock
import org.scalatest.concurrent.Eventually
import org.scalatestplus.mockito.MockitoSugar._

import org.apache.spark._
import org.apache.spark.internal.config._
import org.apache.spark.internal.config.Network.RPC_MESSAGE_MAX_SIZE
import org.apache.spark.rdd.RDD
import org.apache.spark.resource.{ExecutorResourceRequests, ResourceInformation, ResourceProfile, TaskResourceRequests}
import org.apache.spark.resource.ResourceUtils._
import org.apache.spark.resource.TestResourceIDs._
import org.apache.spark.rpc.{RpcAddress, RpcEndpointRef, RpcEnv}
import org.apache.spark.scheduler.cluster.CoarseGrainedClusterMessages._
import org.apache.spark.scheduler.cluster.CoarseGrainedSchedulerBackend
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
    assert(smaller.size === 4)
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
    val mockEndpointRef = mock[RpcEndpointRef]
    val mockAddress = mock[RpcAddress]
    when(mockEndpointRef.send(LaunchTask)).thenAnswer((_: InvocationOnMock) => {})

    val resources = Map(GPU -> new ResourceInformation(GPU, Array("0", "1", "3")))

    var executorAddedCount: Int = 0
    val listener = new SparkListener() {
      override def onExecutorAdded(executorAdded: SparkListenerExecutorAdded): Unit = {
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

    val taskResources = Map(GPU -> new ResourceInformation(GPU, Array("0")))
    val taskDescs: Seq[Seq[TaskDescription]] = Seq(Seq(new TaskDescription(1, 0, "1",
      "t1", 0, 1, mutable.Map.empty[String, Long],
      mutable.Map.empty[String, Long], mutable.Map.empty[String, Long],
      new Properties(), 1, taskResources, bytebuffer)))
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
      StatusUpdate("1", 1, TaskState.FINISHED, buffer, taskResources))

    eventually(timeout(5 seconds)) {
      execResources = backend.getExecutorAvailableResources("1")
      assert(execResources(GPU).availableAddrs.sorted === Array("0", "1", "3"))
      assert(execResources(GPU).assignedAddrs.isEmpty)
    }
    sc.listenerBus.waitUntilEmpty(executorUpTimeout.toMillis)
    assert(executorAddedCount === 3)
  }

  private def testSubmitJob(sc: SparkContext, rdd: RDD[Int]): Unit = {
    sc.submitJob(
      rdd,
      (iter: Iterator[Int]) => iter.toArray,
      0 until rdd.partitions.length,
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
