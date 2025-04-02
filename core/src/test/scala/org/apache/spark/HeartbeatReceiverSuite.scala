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

package org.apache.spark

import java.util.concurrent.{ExecutorService, TimeUnit}

import scala.collection.mutable
import scala.concurrent.Future
import scala.concurrent.duration._

import org.mockito.ArgumentMatchers.{any, eq => meq}
import org.mockito.Mockito.{mock, spy, verify, when}
import org.scalatest.{BeforeAndAfterEach, PrivateMethodTester}
import org.scalatest.concurrent.Eventually._

import org.apache.spark.executor.{ExecutorMetrics, TaskMetrics}
import org.apache.spark.internal.config.{DYN_ALLOCATION_TESTING, STORAGE_BLOCKMANAGER_HEARTBEAT_TIMEOUT}
import org.apache.spark.internal.config.Network.NETWORK_TIMEOUT
import org.apache.spark.resource.{ResourceProfile, ResourceProfileManager}
import org.apache.spark.rpc.{RpcCallContext, RpcEndpoint, RpcEndpointRef, RpcEnv}
import org.apache.spark.scheduler._
import org.apache.spark.scheduler.cluster.CoarseGrainedClusterMessages._
import org.apache.spark.scheduler.cluster.CoarseGrainedSchedulerBackend
import org.apache.spark.storage.BlockManagerId
import org.apache.spark.util.{ManualClock, ThreadUtils}

/**
 * A test suite for the heartbeating behavior between the driver and the executors.
 */
class HeartbeatReceiverSuite
  extends SparkFunSuite
  with BeforeAndAfterEach
  with PrivateMethodTester
  with LocalSparkContext {

  private val executorId1 = "1"
  private val executorId2 = "2"

  // Shared state that must be reset before and after each test
  private var scheduler: TaskSchedulerImpl = null
  private var heartbeatReceiver: HeartbeatReceiver = null
  private var heartbeatReceiverRef: RpcEndpointRef = null
  private var heartbeatReceiverClock: ManualClock = null

  // Helper private method accessors for HeartbeatReceiver
  private val _executorLastSeen =
    PrivateMethod[collection.Map[String, Long]](Symbol("executorLastSeen"))
  private val _executorTimeoutMs = PrivateMethod[Long](Symbol("executorTimeoutMs"))
  private val _killExecutorThread = PrivateMethod[ExecutorService](Symbol("killExecutorThread"))
  var conf: SparkConf = _

  /**
   * Before each test, set up the SparkContext and a custom [[HeartbeatReceiver]]
   * that uses a manual clock.
   */
  override def beforeEach(): Unit = {
    super.beforeEach()
    conf = new SparkConf()
      .setMaster("local[2]")
      .setAppName("test")
      .set(DYN_ALLOCATION_TESTING, true)
    sc = spy[SparkContext](new SparkContext(conf))
    scheduler = mock(classOf[TaskSchedulerImpl])
    when(sc.taskScheduler).thenReturn(scheduler)
    when(scheduler.excludedNodes()).thenReturn(Predef.Set[String]())
    when(scheduler.sc).thenReturn(sc)
    heartbeatReceiverClock = new ManualClock
    heartbeatReceiver = new HeartbeatReceiver(sc, heartbeatReceiverClock)
    heartbeatReceiverRef = sc.env.rpcEnv.setupEndpoint("heartbeat", heartbeatReceiver)
    when(scheduler.executorHeartbeatReceived(any(), any(), any(), any())).thenReturn(true)
  }

  /**
   * After each test, clean up all state and stop the [[SparkContext]].
   */
  override def afterEach(): Unit = {
    super.afterEach()
    scheduler = null
    heartbeatReceiver = null
    heartbeatReceiverRef = null
    heartbeatReceiverClock = null
  }

  test("task scheduler is set correctly") {
    assert(heartbeatReceiver.scheduler === null)
    heartbeatReceiverRef.askSync[Boolean](TaskSchedulerIsSet)
    assert(heartbeatReceiver.scheduler !== null)
  }

  test("normal heartbeat") {
    heartbeatReceiverRef.askSync[Boolean](TaskSchedulerIsSet)
    addExecutorAndVerify(executorId1)
    addExecutorAndVerify(executorId2)
    triggerHeartbeat(executorId1, executorShouldReregister = false)
    triggerHeartbeat(executorId2, executorShouldReregister = false)
    val trackedExecutors = getTrackedExecutors
    assert(trackedExecutors.size === 2)
    assert(trackedExecutors.contains(executorId1))
    assert(trackedExecutors.contains(executorId2))
  }

  test("reregister if scheduler is not ready yet") {
    addExecutorAndVerify(executorId1)
    // Task scheduler is not set yet in HeartbeatReceiver, so executors should reregister
    triggerHeartbeat(executorId1, executorShouldReregister = true)
  }

  test("reregister if heartbeat from unregistered executor") {
    heartbeatReceiverRef.askSync[Boolean](TaskSchedulerIsSet)
    // Received heartbeat from unknown executor, so we ask it to re-register
    triggerHeartbeat(executorId1, executorShouldReregister = true)
    assert(getTrackedExecutors.isEmpty)
  }

  test("reregister if heartbeat from removed executor") {
    heartbeatReceiverRef.askSync[Boolean](TaskSchedulerIsSet)
    addExecutorAndVerify(executorId1)
    addExecutorAndVerify(executorId2)
    // Remove the second executor but not the first
    removeExecutorAndVerify(executorId2)
    // Now trigger the heartbeats
    // A heartbeat from the second executor should require reregistering
    triggerHeartbeat(executorId1, executorShouldReregister = false)
    triggerHeartbeat(executorId2, executorShouldReregister = true)
    val trackedExecutors = getTrackedExecutors
    assert(trackedExecutors.size === 1)
    assert(trackedExecutors.contains(executorId1))
    assert(!trackedExecutors.contains(executorId2))
  }

  test("expire dead hosts") {
    val executorTimeout = heartbeatReceiver.invokePrivate(_executorTimeoutMs())
    heartbeatReceiverRef.askSync[Boolean](TaskSchedulerIsSet)
    addExecutorAndVerify(executorId1)
    addExecutorAndVerify(executorId2)
    triggerHeartbeat(executorId1, executorShouldReregister = false)
    triggerHeartbeat(executorId2, executorShouldReregister = false)
    // Advance the clock and only trigger a heartbeat for the first executor
    heartbeatReceiverClock.advance(executorTimeout / 2)
    triggerHeartbeat(executorId1, executorShouldReregister = false)
    heartbeatReceiverClock.advance(executorTimeout)
    heartbeatReceiverRef.askSync[Boolean](ExpireDeadHosts)
    // Only the second executor should be expired as a dead host
    val trackedExecutors = getTrackedExecutors
    assert(trackedExecutors.size === 1)
    assert(trackedExecutors.contains(executorId1))
    assert(!trackedExecutors.contains(executorId2))
  }

  test("expire dead hosts should kill executors with replacement (SPARK-8119)") {
    // Set up a fake backend and cluster manager to simulate killing executors
    val rpcEnv = sc.env.rpcEnv
    val fakeClusterManager = new FakeClusterManager(rpcEnv, conf)
    val fakeClusterManagerRef = rpcEnv.setupEndpoint("fake-cm", fakeClusterManager)
    val fakeSchedulerBackend =
      new FakeSchedulerBackend(scheduler, rpcEnv, fakeClusterManagerRef, sc.resourceProfileManager)
    when(sc.schedulerBackend).thenReturn(fakeSchedulerBackend)

    // Register fake executors with our fake scheduler backend
    // This is necessary because the backend refuses to kill executors it does not know about
    fakeSchedulerBackend.start()
    val dummyExecutorEndpoint1 = new FakeExecutorEndpoint(rpcEnv)
    val dummyExecutorEndpoint2 = new FakeExecutorEndpoint(rpcEnv)
    val dummyExecutorEndpointRef1 = rpcEnv.setupEndpoint("fake-executor-1", dummyExecutorEndpoint1)
    val dummyExecutorEndpointRef2 = rpcEnv.setupEndpoint("fake-executor-2", dummyExecutorEndpoint2)
    fakeSchedulerBackend.driverEndpoint.askSync[Boolean](
      RegisterExecutor(executorId1, dummyExecutorEndpointRef1, "1.2.3.4", 0, Map.empty, Map.empty,
        Map.empty, ResourceProfile.DEFAULT_RESOURCE_PROFILE_ID))
    fakeSchedulerBackend.driverEndpoint.askSync[Boolean](
      RegisterExecutor(executorId2, dummyExecutorEndpointRef2, "1.2.3.5", 0, Map.empty, Map.empty,
        Map.empty, ResourceProfile.DEFAULT_RESOURCE_PROFILE_ID))
    heartbeatReceiverRef.askSync[Boolean](TaskSchedulerIsSet)
    addExecutorAndVerify(executorId1)
    addExecutorAndVerify(executorId2)
    triggerHeartbeat(executorId1, executorShouldReregister = false)
    triggerHeartbeat(executorId2, executorShouldReregister = false)

    // Adjust the target number of executors on the cluster manager side
    assert(fakeClusterManager.getTargetNumExecutors === 0)
    sc.requestTotalExecutors(2, 0, Map.empty)
    assert(fakeClusterManager.getTargetNumExecutors === 2)
    assert(fakeClusterManager.getExecutorIdsToKill.isEmpty)

    // Expire the executors. This should trigger our fake backend to kill the executors.
    // Since the kill request is sent to the cluster manager asynchronously, we need to block
    // on the kill thread to ensure that the cluster manager actually received our requests.
    // Here we use a timeout of O(seconds), but in practice this whole test takes O(10ms).
    val executorTimeout = heartbeatReceiver.invokePrivate(_executorTimeoutMs())
    heartbeatReceiverClock.advance(executorTimeout * 2)
    heartbeatReceiverRef.askSync[Boolean](ExpireDeadHosts)
    val killThread = heartbeatReceiver.invokePrivate(_killExecutorThread())
    killThread.shutdown() // needed for awaitTermination
    killThread.awaitTermination(10L, TimeUnit.SECONDS)

    // The target number of executors should not change! Otherwise, having an expired
    // executor means we permanently adjust the target number downwards until we
    // explicitly request new executors. For more detail, see SPARK-8119.
    assert(fakeClusterManager.getTargetNumExecutors === 2)
    assert(fakeClusterManager.getExecutorIdsToKill === Set(executorId1, executorId2))
    // [SPARK-27348] HeartbeatReceiver should remove lost executor from scheduler backend
    eventually(timeout(5.seconds)) {
      assert(!fakeSchedulerBackend.getExecutorIds().contains(executorId1))
      assert(!fakeSchedulerBackend.getExecutorIds().contains(executorId2))
    }
    fakeSchedulerBackend.stop()
  }

  test("SPARK-34273: Do not reregister BlockManager when SparkContext is stopped") {
    val blockManagerId = BlockManagerId(executorId1, "localhost", 12345)

    heartbeatReceiverRef.askSync[Boolean](TaskSchedulerIsSet)
    val response = heartbeatReceiverRef.askSync[HeartbeatResponse](
      Heartbeat(executorId1, Array.empty, blockManagerId, mutable.Map.empty))
    assert(response.reregisterBlockManager)

    try {
      sc.stopped.set(true)
      val response = heartbeatReceiverRef.askSync[HeartbeatResponse](
        Heartbeat(executorId1, Array.empty, blockManagerId, mutable.Map.empty))
      assert(!response.reregisterBlockManager)
    } finally {
      sc.stopped.set(false)
    }
  }

  test("SPARK-44726: Show spark.network.timeout config error message") {
    sc.stop()
    val conf = new SparkConf()
      .setMaster("local[2]")
      .setAppName("test")
      .set(NETWORK_TIMEOUT.key, "30s")
    val m = intercept[IllegalArgumentException] {
      new SparkContext(conf)
    }.getMessage
    assert(m.contains("spark.network.timeoutInterval should be less than or equal to " +
      NETWORK_TIMEOUT.key))
  }

  test("SPARK-44726: Show spark.storage.blockManagerHeartbeatTimeoutMs error message") {
    sc.stop()
    val conf = new SparkConf()
      .setMaster("local[2]")
      .setAppName("test")
      .set(STORAGE_BLOCKMANAGER_HEARTBEAT_TIMEOUT.key, "30s")
    val m = intercept[IllegalArgumentException] {
      new SparkContext(conf)
    }.getMessage
    assert(m.contains("spark.network.timeoutInterval should be less than or equal to " +
      STORAGE_BLOCKMANAGER_HEARTBEAT_TIMEOUT.key))
  }

  /** Manually send a heartbeat and return the response. */
  private def triggerHeartbeat(
      executorId: String,
      executorShouldReregister: Boolean): Unit = {
    val metrics = TaskMetrics.empty
    val blockManagerId = BlockManagerId(executorId, "localhost", 12345)
    val executorMetrics = new ExecutorMetrics(Array(123456L, 543L, 12345L, 1234L, 123L,
      12L, 432L, 321L, 654L, 765L))
    val executorUpdates = mutable.Map((0, 0) -> executorMetrics)
    val response = heartbeatReceiverRef.askSync[HeartbeatResponse](
      Heartbeat(executorId, Array(1L -> metrics.accumulators()), blockManagerId, executorUpdates))
    if (executorShouldReregister) {
      assert(response.reregisterBlockManager)
    } else {
      assert(!response.reregisterBlockManager)
      // Additionally verify that the scheduler callback is called with the correct parameters
      verify(scheduler).executorHeartbeatReceived(
        meq(executorId),
        meq(Array(1L -> metrics.accumulators())),
        meq(blockManagerId),
        meq(executorUpdates))
    }
  }

  private def addExecutorAndVerify(executorId: String): Unit = {
    assert(
      heartbeatReceiver.addExecutor(executorId).map { f =>
        ThreadUtils.awaitResult(f, 10.seconds)
      } === Some(true))
  }

  private def removeExecutorAndVerify(executorId: String): Unit = {
    assert(
      heartbeatReceiver.removeExecutor(executorId).map { f =>
        ThreadUtils.awaitResult(f, 10.seconds)
      } === Some(true))
  }

  private def getTrackedExecutors: collection.Map[String, Long] = {
    // We may receive undesired SparkListenerExecutorAdded from LocalSchedulerBackend,
    // so exclude it from the map. See SPARK-10800.
    heartbeatReceiver.invokePrivate(_executorLastSeen()).
      filter { case (k, _) => k != SparkContext.DRIVER_IDENTIFIER }
  }
}

// TODO: use these classes to add end-to-end tests for dynamic allocation!

/**
 * Dummy RPC endpoint to simulate executors.
 */
private class FakeExecutorEndpoint(override val rpcEnv: RpcEnv) extends RpcEndpoint {

  override def receive: PartialFunction[Any, Unit] = {
    case _ =>
  }
}

/**
 * Dummy scheduler backend to simulate executor allocation requests to the cluster manager.
 */
private class FakeSchedulerBackend(
    scheduler: TaskSchedulerImpl,
    rpcEnv: RpcEnv,
    clusterManagerEndpoint: RpcEndpointRef,
    resourceProfileManager: ResourceProfileManager)
  extends CoarseGrainedSchedulerBackend(scheduler, rpcEnv) {

  def this() = this(null, null, null, null)

  protected override def doRequestTotalExecutors(
      resourceProfileToTotalExecs: Map[ResourceProfile, Int]): Future[Boolean] = {
    clusterManagerEndpoint.ask[Boolean](
      RequestExecutors(resourceProfileToTotalExecs, numLocalityAwareTasksPerResourceProfileId,
        rpHostToLocalTaskCount, Set.empty))
}

  protected override def doKillExecutors(executorIds: Seq[String]): Future[Boolean] = {
    clusterManagerEndpoint.ask[Boolean](KillExecutors(executorIds))
  }
}

/**
 * Dummy cluster manager to simulate responses to executor allocation requests.
 */
private class FakeClusterManager(override val rpcEnv: RpcEnv, conf: SparkConf) extends RpcEndpoint {
  private var targetNumExecutors = 0
  private val executorIdsToKill = new mutable.HashSet[String]

  def getTargetNumExecutors: Int = targetNumExecutors
  def getExecutorIdsToKill: Set[String] = executorIdsToKill.toSet

  override def receiveAndReply(context: RpcCallContext): PartialFunction[Any, Unit] = {
    case RequestExecutors(resourceProfileToTotalExecs, _, _, _) =>
      targetNumExecutors =
        resourceProfileToTotalExecs(ResourceProfile.getOrCreateDefaultProfile(conf))
      context.reply(true)
    case KillExecutors(executorIds) =>
      executorIdsToKill ++= executorIds
      context.reply(true)
  }
}
