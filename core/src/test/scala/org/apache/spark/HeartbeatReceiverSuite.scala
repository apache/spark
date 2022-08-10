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
import scala.collection.mutable.ArrayBuffer
import scala.concurrent.Future
import scala.concurrent.duration._

import org.mockito.ArgumentMatchers.{any, eq => meq}
import org.mockito.Mockito.{mock, spy, verify, when}
import org.scalatest.{BeforeAndAfterEach, PrivateMethodTester}
import org.scalatest.concurrent.Eventually._

import org.apache.spark.deploy.ApplicationDescription
import org.apache.spark.deploy.client.{StandaloneAppClient, StandaloneAppClientListener}
import org.apache.spark.executor.{ExecutorMetrics, TaskMetrics}
import org.apache.spark.internal.config.{DYN_ALLOCATION_TESTING, HEARTBEAT_RECEIVER_CHECK_WORKER_LAST_HEARTBEAT, Network}
import org.apache.spark.resource.{ResourceProfile, ResourceProfileManager}
import org.apache.spark.rpc.{RpcCallContext, RpcEndpoint, RpcEndpointRef, RpcEnv}
import org.apache.spark.scheduler._
import org.apache.spark.scheduler.cluster.{CoarseGrainedSchedulerBackend, StandaloneSchedulerBackend}
import org.apache.spark.scheduler.cluster.CoarseGrainedClusterMessages._
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
  private val _executorExpiryCandidates =
    PrivateMethod[mutable.HashMap[String, Long]](Symbol("executorExpiryCandidates"))
  private val _expiryCandidatesTimeout = PrivateMethod[Long](Symbol("expiryCandidatesTimeout"))

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
    sc = spy(new SparkContext(conf))
    scheduler = mock(classOf[TaskSchedulerImpl])
    when(sc.taskScheduler).thenReturn(scheduler)
    when(scheduler.excludedNodes).thenReturn(Predef.Set[String]())
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

  test("Check workerLastHeartbeat before expiring the executor") {
    sc.stop()
    conf = new SparkConf()
      .setMaster("local-cluster[1, 1, 1024]")
      .setAppName("test")
      .set(DYN_ALLOCATION_TESTING, true)
      .set(HEARTBEAT_RECEIVER_CHECK_WORKER_LAST_HEARTBEAT, true)
    sc = spy(new SparkContext(conf))
    scheduler = mock(classOf[TaskSchedulerImpl])
    when(sc.taskScheduler).thenReturn(scheduler)
    when(scheduler.excludedNodes).thenReturn(Predef.Set[String]())
    when(scheduler.sc).thenReturn(sc)
    heartbeatReceiverClock = new ManualClock
    heartbeatReceiver = new HeartbeatReceiver(sc, heartbeatReceiverClock)
    heartbeatReceiverRef = sc.env.rpcEnv.setupEndpoint("heartbeat", heartbeatReceiver)
    when(scheduler.executorHeartbeatReceived(any(), any(), any(), any())).thenReturn(true)

    // executorTimeout = 120000
    val executorTimeout = heartbeatReceiver.invokePrivate(_executorTimeoutMs())
    heartbeatReceiverRef.askSync[Boolean](TaskSchedulerIsSet)
    addExecutorAndVerify(executorId1)

    // Setup FakeAppClient
    val rpcEnv = sc.env.rpcEnv
    val fakeAppClient = new FakeAppClient(rpcEnv,
      Array[String](),
      mock(classOf[ApplicationDescription]),
      mock(classOf[StandaloneAppClientListener]),
      conf,
      executorTimeout)
    val schedulerBackend = sc.schedulerBackend.asInstanceOf[StandaloneSchedulerBackend]
    schedulerBackend.client = fakeAppClient

    // executorLastSeen = Map(1 -> 0); now = 0
    triggerHeartbeat(executorId1, executorShouldReregister = false)

    // now = 120001
    heartbeatReceiverClock.advance(executorTimeout + 1)
    // Executor1 will not be killed because workerLastHeartbeat is 120000. Executor1 will be removed
    // from executorLastSeen (trackedExecutors.size === 0) and moved to executorExpiryCandidates
    // (executorExpiryCandidates.size === 1). In other words, executorExpiryCandidates is equal to
    // Map(1 -> 120000).
    heartbeatReceiverRef.askSync[Boolean](ExpireDeadHosts)

    var trackedExecutors = getTrackedExecutors
    var executorExpiryCandidates = heartbeatReceiver.invokePrivate(_executorExpiryCandidates())
    assert(trackedExecutors.size === 0)
    assert(executorExpiryCandidates.size === 1)
    assert(executorExpiryCandidates.contains(executorId1))

    // now = 240001
    // The executor will be removed from executorExpiryCandidates because
    // (now - executorExpiryCandidates(1)) is larger than (executorTimeoutMs / 2).
    heartbeatReceiverClock.advance(executorTimeout)
    heartbeatReceiverRef.askSync[Boolean](ExpireDeadHosts)
    trackedExecutors = getTrackedExecutors
    executorExpiryCandidates = heartbeatReceiver.invokePrivate(_executorExpiryCandidates())
    assert(trackedExecutors.size === 0)
    assert(executorExpiryCandidates.size === 0)
  }

  test("Remove executor from executorExpiryCandidates when receiving a heartbeat") {
    sc.stop()
    conf = new SparkConf()
      .setMaster("local-cluster[1, 1, 1024]")
      .setAppName("test")
      .set(DYN_ALLOCATION_TESTING, true)
      .set(HEARTBEAT_RECEIVER_CHECK_WORKER_LAST_HEARTBEAT, true)
    sc = spy(new SparkContext(conf))
    scheduler = mock(classOf[TaskSchedulerImpl])
    when(sc.taskScheduler).thenReturn(scheduler)
    when(scheduler.excludedNodes).thenReturn(Predef.Set[String]())
    when(scheduler.sc).thenReturn(sc)
    heartbeatReceiverClock = new ManualClock
    heartbeatReceiver = new HeartbeatReceiver(sc, heartbeatReceiverClock)
    heartbeatReceiverRef = sc.env.rpcEnv.setupEndpoint("heartbeat", heartbeatReceiver)
    when(scheduler.executorHeartbeatReceived(any(), any(), any(), any())).thenReturn(true)

    // executorTimeout = 120000
    val executorTimeout = heartbeatReceiver.invokePrivate(_executorTimeoutMs())
    heartbeatReceiverRef.askSync[Boolean](TaskSchedulerIsSet)
    addExecutorAndVerify(executorId1)

    val rpcEnv = sc.env.rpcEnv
    val fakeAppClient = new FakeAppClient(rpcEnv,
      Array[String](),
      mock(classOf[ApplicationDescription]),
      mock(classOf[StandaloneAppClientListener]),
      conf,
      executorTimeout)
    val schedulerBackend = sc.schedulerBackend.asInstanceOf[StandaloneSchedulerBackend]
    schedulerBackend.client = fakeAppClient

    // executorLastSeen = Map(1 -> 0); now = 0
    triggerHeartbeat(executorId1, executorShouldReregister = false)

    // now = 120001
    heartbeatReceiverClock.advance(executorTimeout + 1)
    // Executor1 will not be killed because workerLastHeartbeat is 120000. Executor1 will be removed
    // from executorLastSeen (trackedExecutors.size === 0) and moved to executorExpiryCandidates
    // (executorExpiryCandidates.size === 1). In other words, executorExpiryCandidates is equal to
    // Map(1 -> 120000).
    heartbeatReceiverRef.askSync[Boolean](ExpireDeadHosts)

    var trackedExecutors = getTrackedExecutors
    var executorExpiryCandidates = heartbeatReceiver.invokePrivate(_executorExpiryCandidates())
    assert(trackedExecutors.size === 0)
    assert(executorExpiryCandidates.size === 1)
    assert(executorExpiryCandidates.contains(executorId1))

    // now = 120001
    // When heartbeatReceiver receives a heartbeat from executor1, executor1 will be removed from
    // executorExpiryCandidates and moved to executorLastSeen.
    triggerHeartbeat(executorId1, executorShouldReregister = false)
    trackedExecutors = getTrackedExecutors
    executorExpiryCandidates = heartbeatReceiver.invokePrivate(_executorExpiryCandidates())
    assert(trackedExecutors.size === 1)
    assert(executorExpiryCandidates.size === 0)
  }

  test("Remove executor from both executorExpiryCandidates and executorLastSeen when receiving" +
    "an ExecutorRemoved msg") {
    sc.stop()
    conf = new SparkConf()
      .setMaster("local-cluster[1, 1, 1024]")
      .setAppName("test")
      .set(DYN_ALLOCATION_TESTING, true)
      .set(HEARTBEAT_RECEIVER_CHECK_WORKER_LAST_HEARTBEAT, true)
    sc = spy(new SparkContext(conf))
    scheduler = mock(classOf[TaskSchedulerImpl])
    when(sc.taskScheduler).thenReturn(scheduler)
    when(scheduler.excludedNodes).thenReturn(Predef.Set[String]())
    when(scheduler.sc).thenReturn(sc)
    heartbeatReceiverClock = new ManualClock
    heartbeatReceiver = new HeartbeatReceiver(sc, heartbeatReceiverClock)
    heartbeatReceiverRef = sc.env.rpcEnv.setupEndpoint("heartbeat", heartbeatReceiver)
    when(scheduler.executorHeartbeatReceived(any(), any(), any(), any())).thenReturn(true)

    // executorTimeout = 120000
    val executorTimeout = heartbeatReceiver.invokePrivate(_executorTimeoutMs())
    heartbeatReceiverRef.askSync[Boolean](TaskSchedulerIsSet)
    addExecutorAndVerify(executorId1)
    addExecutorAndVerify(executorId2)

    val rpcEnv = sc.env.rpcEnv
    val fakeAppClient = new FakeAppClient(rpcEnv,
      Array[String](),
      mock(classOf[ApplicationDescription]),
      mock(classOf[StandaloneAppClientListener]),
      conf,
      executorTimeout)
    val schedulerBackend = sc.schedulerBackend.asInstanceOf[StandaloneSchedulerBackend]
    schedulerBackend.client = fakeAppClient

    triggerHeartbeat(executorId1, executorShouldReregister = false)
    triggerHeartbeat(executorId2, executorShouldReregister = false)
    var trackedExecutors = getTrackedExecutors
    var executorExpiryCandidates = heartbeatReceiver.invokePrivate(_executorExpiryCandidates())
    assert(trackedExecutors.size === 2)
    assert(executorExpiryCandidates.size === 0)

    // HEARTBEAT_RECEIVER_CHECK_WORKER_LAST_HEARTBEAT is enabled, but we did not set the
    // value of HEARTBEAT_EXPIRY_CANDIDATES_TIMEOUT. Hence, expiryCandidatesTimeout is same as
    // default value (30s = 30000ms).
    val expiryCandidatesTimeout = heartbeatReceiver.invokePrivate(_expiryCandidatesTimeout())
    assert(expiryCandidatesTimeout == 30000)

    // Advance the clock and only trigger a heartbeat for the first executor
    heartbeatReceiverClock.advance(expiryCandidatesTimeout)
    triggerHeartbeat(executorId1, executorShouldReregister = false)
    heartbeatReceiverClock.advance(executorTimeout)
    heartbeatReceiverRef.askSync[Boolean](ExpireDeadHosts)

    // Only the second executor will be put into executorExpiryCandidates
    trackedExecutors = getTrackedExecutors
    assert(trackedExecutors.size === 1)
    assert(trackedExecutors.contains(executorId1))
    executorExpiryCandidates = heartbeatReceiver.invokePrivate(_executorExpiryCandidates())
    assert(executorExpiryCandidates.size === 1)
    assert(executorExpiryCandidates.contains(executorId2))

    // When heartbeatReceiver receives ExecutorRemoved, it will remove the executor from both
    // executorLastSeen and executorExpiryCandidates.
    heartbeatReceiverRef.askSync[Boolean](ExecutorRemoved(executorId1))
    heartbeatReceiverRef.askSync[Boolean](ExecutorRemoved(executorId2))
    trackedExecutors = getTrackedExecutors
    executorExpiryCandidates = heartbeatReceiver.invokePrivate(_executorExpiryCandidates())
    assert(trackedExecutors.size === 0)
    assert(executorExpiryCandidates.size === 0)
  }

  test("Remove all executors if HeartbeatReceiver cannot connect to master") {
    sc.stop()
    conf = new SparkConf()
      .setMaster("local-cluster[1, 1, 1024]")
      .setAppName("test")
      .set(DYN_ALLOCATION_TESTING, true)
      .set(HEARTBEAT_RECEIVER_CHECK_WORKER_LAST_HEARTBEAT, true)
    sc = spy(new SparkContext(conf))
    scheduler = mock(classOf[TaskSchedulerImpl])
    when(sc.taskScheduler).thenReturn(scheduler)
    when(scheduler.excludedNodes).thenReturn(Predef.Set[String]())
    when(scheduler.sc).thenReturn(sc)
    heartbeatReceiverClock = new ManualClock
    heartbeatReceiver = new HeartbeatReceiver(sc, heartbeatReceiverClock)
    heartbeatReceiverRef = sc.env.rpcEnv.setupEndpoint("heartbeat", heartbeatReceiver)
    when(scheduler.executorHeartbeatReceived(any(), any(), any(), any())).thenReturn(true)

    // executorTimeout = 120000
    val executorTimeout = heartbeatReceiver.invokePrivate(_executorTimeoutMs())
    heartbeatReceiverRef.askSync[Boolean](TaskSchedulerIsSet)
    addExecutorAndVerify(executorId1)
    addExecutorAndVerify(executorId2)

    val rpcEnv = sc.env.rpcEnv
    // Set the value of lastHeartbeat to -1 to simulate master disconnection.
    val fakeAppClient = new FakeAppClient(rpcEnv,
      Array[String](),
      mock(classOf[ApplicationDescription]),
      mock(classOf[StandaloneAppClientListener]),
      conf,
      -1)
    val schedulerBackend = sc.schedulerBackend.asInstanceOf[StandaloneSchedulerBackend]
    schedulerBackend.client = fakeAppClient

    triggerHeartbeat(executorId1, executorShouldReregister = false)
    triggerHeartbeat(executorId2, executorShouldReregister = false)
    var trackedExecutors = getTrackedExecutors
    var executorExpiryCandidates = heartbeatReceiver.invokePrivate(_executorExpiryCandidates())
    assert(trackedExecutors.size === 2)
    assert(executorExpiryCandidates.size === 0)

    // When (now - executorLastSeen(execID)) is larger than executorTimeout, heartbeatReceiver will
    // ask master node for workerLastHeartbeat. However, in this test, heartbeatReceiver cannot
    // reach master node, and thus we will remove the executor.
    heartbeatReceiverClock.advance(executorTimeout + 1)
    heartbeatReceiverRef.askSync[Boolean](ExpireDeadHosts)

    // Both executors will be removed from executorLastSeen and executorExpiryCandidates.
    trackedExecutors = getTrackedExecutors
    executorExpiryCandidates = heartbeatReceiver.invokePrivate(_executorExpiryCandidates())
    assert(trackedExecutors.size === 0)
    assert(executorExpiryCandidates.size === 0)
  }

  test("executorTimeout will fallback to NETWORK_TIMEOUT if NETWORK_EXECUTOR_TIMEOUT is not set") {
    sc.stop()
    conf = new SparkConf()
      .setMaster("local-cluster[1, 1, 1024]")
      .setAppName("test")
      .set(DYN_ALLOCATION_TESTING, true)
      .set(Network.NETWORK_TIMEOUT.key, "100s")
      .set(Network.HEARTBEAT_EXPIRY_CANDIDATES_TIMEOUT.key, "50s")
    sc = spy(new SparkContext(conf))
    scheduler = mock(classOf[TaskSchedulerImpl])
    when(sc.taskScheduler).thenReturn(scheduler)
    when(scheduler.excludedNodes).thenReturn(Predef.Set[String]())
    when(scheduler.sc).thenReturn(sc)
    heartbeatReceiverClock = new ManualClock
    heartbeatReceiver = new HeartbeatReceiver(sc, heartbeatReceiverClock)
    val executorTimeout = heartbeatReceiver.invokePrivate(_executorTimeoutMs())
    assert(executorTimeout == 100000)

    // HEARTBEAT_RECEIVER_CHECK_WORKER_LAST_HEARTBEAT is not enabled, so the value
    // of expiryCandidatesTimeout is 0.
    val expiryCandidatesTimeout = heartbeatReceiver.invokePrivate(_expiryCandidatesTimeout())
    assert(expiryCandidatesTimeout == 0)
  }

  test("check executorTimeout value when NETWORK_EXECUTOR_TIMEOUT is set") {
    sc.stop()
    conf = new SparkConf()
      .setMaster("local-cluster[1, 1, 1024]")
      .setAppName("test")
      .set(DYN_ALLOCATION_TESTING, true)
      .set(Network.NETWORK_EXECUTOR_TIMEOUT.key, "50s")
      .set(Network.NETWORK_TIMEOUT_INTERVAL.key, "15s")
      .set(Network.HEARTBEAT_EXPIRY_CANDIDATES_TIMEOUT.key, "20s")
      .set(HEARTBEAT_RECEIVER_CHECK_WORKER_LAST_HEARTBEAT, true)
    sc = spy(new SparkContext(conf))
    scheduler = mock(classOf[TaskSchedulerImpl])
    when(sc.taskScheduler).thenReturn(scheduler)
    when(scheduler.excludedNodes).thenReturn(Predef.Set[String]())
    when(scheduler.sc).thenReturn(sc)
    heartbeatReceiverClock = new ManualClock
    heartbeatReceiver = new HeartbeatReceiver(sc, heartbeatReceiverClock)
    val executorTimeout = heartbeatReceiver.invokePrivate(_executorTimeoutMs())
    assert(executorTimeout == 50000)

    // HEARTBEAT_RECEIVER_CHECK_WORKER_LAST_HEARTBEAT is enabled.
    val expiryCandidatesTimeout = heartbeatReceiver.invokePrivate(_expiryCandidatesTimeout())
    assert(expiryCandidatesTimeout == 20000)
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
      filterKeys(_ != SparkContext.DRIVER_IDENTIFIER).toMap
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

private class FakeAppClient(rpcEnv: RpcEnv,
  masterUrls: Array[String],
  appDescription: ApplicationDescription,
  listener: StandaloneAppClientListener,
  conf: SparkConf,
  lastHeartbeat: Long = 0)
  extends StandaloneAppClient(rpcEnv, masterUrls, appDescription, listener, conf) {

  private val workerLastHeartbeat: Long = lastHeartbeat
  override def workerLastHeartbeat(appId: String,
    executorFullIds: ArrayBuffer[String]): Option[ArrayBuffer[Long]] = {
    lastHeartbeat match {
      case -1 => None
      case _ => Some(ArrayBuffer.fill[Long](executorFullIds.size)(workerLastHeartbeat))
    }
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
