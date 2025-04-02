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

package org.apache.spark.deploy

import scala.collection.mutable
import scala.concurrent.duration._

import org.mockito.ArgumentMatchers.any
import org.mockito.Mockito.{mock, when}
import org.scalatest.{BeforeAndAfterAll, PrivateMethodTester}
import org.scalatest.concurrent.Eventually._

import org.apache.spark._
import org.apache.spark.deploy.DeployMessages._
import org.apache.spark.deploy.master.ApplicationInfo
import org.apache.spark.deploy.master.Master
import org.apache.spark.deploy.worker.Worker
import org.apache.spark.internal.config
import org.apache.spark.resource.ResourceProfile
import org.apache.spark.rpc.{RpcAddress, RpcEndpointRef, RpcEnv}
import org.apache.spark.scheduler.TaskSchedulerImpl
import org.apache.spark.scheduler.cluster._
import org.apache.spark.scheduler.cluster.CoarseGrainedClusterMessages.{LaunchedExecutor, RegisterExecutor}

/**
 * End-to-end tests for dynamic allocation in standalone mode.
 */
class StandaloneDynamicAllocationSuite
  extends SparkFunSuite
  with LocalSparkContext
  with BeforeAndAfterAll
  with PrivateMethodTester {

  private val numWorkers = 2
  private val conf = new SparkConf()
  private val securityManager = new SecurityManager(conf)

  private var masterRpcEnv: RpcEnv = null
  private var workerRpcEnvs: Seq[RpcEnv] = null
  private var master: Master = null
  private var workers: Seq[Worker] = null

  /**
   * Start the local cluster.
   * Note: local-cluster mode is insufficient because we want a reference to the Master.
   */
  override def beforeAll(): Unit = {
    super.beforeAll()
    masterRpcEnv = RpcEnv.create(Master.SYSTEM_NAME, "localhost", 0, conf, securityManager)
    workerRpcEnvs = (0 until numWorkers).map { i =>
      RpcEnv.create(Worker.SYSTEM_NAME + i, "localhost", 0, conf, securityManager)
    }
    master = makeMaster()
    workers = makeWorkers(10, 2048)
    // Wait until all workers register with master successfully
    eventually(timeout(1.minute), interval(10.milliseconds)) {
      assert(getMasterState.workers.length === numWorkers)
    }
  }

  override def afterAll(): Unit = {
    try {
      masterRpcEnv.shutdown()
      workerRpcEnvs.foreach(_.shutdown())
      master.stop()
      workers.foreach(_.stop())
      masterRpcEnv = null
      workerRpcEnvs = null
      master = null
      workers = null
    } finally {
      super.afterAll()
    }
  }

  test("dynamic allocation default behavior") {
    sc = new SparkContext(appConf)
    val appId = sc.applicationId
    eventually(timeout(10.seconds), interval(10.millis)) {
      val apps = getApplications()
      assert(apps.length === 1)
      assert(apps.head.id === appId)
      assert(apps.head.executors.size === 2)
      assert(apps.head.getExecutorLimit === Int.MaxValue)
    }
    // kill all executors
    assert(killAllExecutors(sc))
    var apps = getApplications()
    assert(apps.head.executors.size === 0)
    assert(apps.head.getExecutorLimit === 0)
    // request 1
    assert(sc.requestExecutors(1))
    apps = getApplications()
    assert(apps.head.executors.size === 1)
    assert(apps.head.getExecutorLimit === 1)
    // request 1 more
    assert(sc.requestExecutors(1))
    apps = getApplications()
    assert(apps.head.executors.size === 2)
    assert(apps.head.getExecutorLimit === 2)
    // request 1 more; this one won't go through
    assert(sc.requestExecutors(1))
    apps = getApplications()
    assert(apps.head.executors.size === 2)
    assert(apps.head.getExecutorLimit === 3)
    // kill all existing executors; we should end up with 3 - 2 = 1 executor
    assert(killAllExecutors(sc))
    apps = getApplications()
    assert(apps.head.executors.size === 1)
    assert(apps.head.getExecutorLimit === 1)
    // kill all executors again; this time we'll have 1 - 1 = 0 executors left
    assert(killAllExecutors(sc))
    apps = getApplications()
    assert(apps.head.executors.size === 0)
    assert(apps.head.getExecutorLimit === 0)
    // request many more; this increases the limit well beyond the cluster capacity
    assert(sc.requestExecutors(1000))
    apps = getApplications()
    assert(apps.head.executors.size === 2)
    assert(apps.head.getExecutorLimit === 1000)
  }

  test("dynamic allocation with max cores <= cores per worker") {
    sc = new SparkContext(appConf.set(config.CORES_MAX, 8))
    val appId = sc.applicationId
    eventually(timeout(10.seconds), interval(10.millis)) {
      val apps = getApplications()
      assert(apps.length === 1)
      assert(apps.head.id === appId)
      assert(apps.head.executors.size === 2)
      assert(apps.head.executors.values.map(_.cores).toArray === Array(4, 4))
      assert(apps.head.getExecutorLimit === Int.MaxValue)
    }
    // kill all executors
    assert(killAllExecutors(sc))
    var apps = getApplications()
    assert(apps.head.executors.size === 0)
    assert(apps.head.getExecutorLimit === 0)
    // request 1
    assert(sc.requestExecutors(1))
    apps = getApplications()
    assert(apps.head.executors.size === 1)
    assert(apps.head.executors.values.head.cores === 8)
    assert(apps.head.getExecutorLimit === 1)
    // request 1 more; this one won't go through because we're already at max cores.
    // This highlights a limitation of using dynamic allocation with max cores WITHOUT
    // setting cores per executor: once an application scales down and then scales back
    // up, its executors may not be spread out anymore!
    assert(sc.requestExecutors(1))
    apps = getApplications()
    assert(apps.head.executors.size === 1)
    assert(apps.head.getExecutorLimit === 2)
    // request 1 more; this one also won't go through for the same reason
    assert(sc.requestExecutors(1))
    apps = getApplications()
    assert(apps.head.executors.size === 1)
    assert(apps.head.getExecutorLimit === 3)
    // kill all existing executors; we should end up with 3 - 1 = 2 executor
    // Note: we scheduled these executors together, so their cores should be evenly distributed
    assert(killAllExecutors(sc))
    apps = getApplications()
    assert(apps.head.executors.size === 2)
    assert(apps.head.executors.values.map(_.cores).toArray === Array(4, 4))
    assert(apps.head.getExecutorLimit === 2)
    // kill all executors again; this time we'll have 1 - 1 = 0 executors left
    assert(killAllExecutors(sc))
    apps = getApplications()
    assert(apps.head.executors.size === 0)
    assert(apps.head.getExecutorLimit === 0)
    // request many more; this increases the limit well beyond the cluster capacity
    assert(sc.requestExecutors(1000))
    apps = getApplications()
    assert(apps.head.executors.size === 2)
    assert(apps.head.executors.values.map(_.cores).toArray === Array(4, 4))
    assert(apps.head.getExecutorLimit === 1000)
  }

  test("dynamic allocation with max cores > cores per worker") {
    sc = new SparkContext(appConf.set(config.CORES_MAX, 16))
    val appId = sc.applicationId
    eventually(timeout(10.seconds), interval(10.millis)) {
      val apps = getApplications()
      assert(apps.length === 1)
      assert(apps.head.id === appId)
      assert(apps.head.executors.size === 2)
      assert(apps.head.executors.values.map(_.cores).toArray === Array(8, 8))
      assert(apps.head.getExecutorLimit === Int.MaxValue)
    }
    // kill all executors
    assert(killAllExecutors(sc))
    var apps = getApplications()
    assert(apps.head.executors.size === 0)
    assert(apps.head.getExecutorLimit === 0)
    // request 1
    assert(sc.requestExecutors(1))
    apps = getApplications()
    assert(apps.head.executors.size === 1)
    assert(apps.head.executors.values.head.cores === 10)
    assert(apps.head.getExecutorLimit === 1)
    // request 1 more
    // Note: the cores are not evenly distributed because we scheduled these executors 1 by 1
    assert(sc.requestExecutors(1))
    apps = getApplications()
    assert(apps.head.executors.size === 2)
    assert(apps.head.executors.values.map(_.cores).toSet === Set(10, 6))
    assert(apps.head.getExecutorLimit === 2)
    // request 1 more; this one won't go through
    assert(sc.requestExecutors(1))
    apps = getApplications()
    assert(apps.head.executors.size === 2)
    assert(apps.head.getExecutorLimit === 3)
    // kill all existing executors; we should end up with 3 - 2 = 1 executor
    assert(killAllExecutors(sc))
    apps = getApplications()
    assert(apps.head.executors.size === 1)
    assert(apps.head.executors.values.head.cores === 10)
    assert(apps.head.getExecutorLimit === 1)
    // kill all executors again; this time we'll have 1 - 1 = 0 executors left
    assert(killAllExecutors(sc))
    apps = getApplications()
    assert(apps.head.executors.size === 0)
    assert(apps.head.getExecutorLimit === 0)
    // request many more; this increases the limit well beyond the cluster capacity
    assert(sc.requestExecutors(1000))
    apps = getApplications()
    assert(apps.head.executors.size === 2)
    assert(apps.head.executors.values.map(_.cores).toArray === Array(8, 8))
    assert(apps.head.getExecutorLimit === 1000)
  }

  test("dynamic allocation with cores per executor") {
    sc = new SparkContext(appConf.set(config.EXECUTOR_CORES, 2))
    val appId = sc.applicationId
    eventually(timeout(10.seconds), interval(10.millis)) {
      val apps = getApplications()
      assert(apps.length === 1)
      assert(apps.head.id === appId)
      assert(apps.head.executors.size === 10) // 20 cores total
      assert(apps.head.getExecutorLimit === Int.MaxValue)
    }
    // kill all executors
    assert(killAllExecutors(sc))
    var apps = getApplications()
    assert(apps.head.executors.size === 0)
    assert(apps.head.getExecutorLimit === 0)
    // request 1
    assert(sc.requestExecutors(1))
    apps = getApplications()
    assert(apps.head.executors.size === 1)
    assert(apps.head.getExecutorLimit === 1)
    // request 3 more
    assert(sc.requestExecutors(3))
    apps = getApplications()
    assert(apps.head.executors.size === 4)
    assert(apps.head.getExecutorLimit === 4)
    // request 10 more; only 6 will go through
    assert(sc.requestExecutors(10))
    apps = getApplications()
    assert(apps.head.executors.size === 10)
    assert(apps.head.getExecutorLimit === 14)
    // kill 2 executors; we should get 2 back immediately
    assert(killNExecutors(sc, 2))
    apps = getApplications()
    assert(apps.head.executors.size === 10)
    assert(apps.head.getExecutorLimit === 12)
    // kill 4 executors; we should end up with 12 - 4 = 8 executors
    assert(killNExecutors(sc, 4))
    apps = getApplications()
    assert(apps.head.executors.size === 8)
    assert(apps.head.getExecutorLimit === 8)
    // kill all executors; this time we'll have 8 - 8 = 0 executors left
    assert(killAllExecutors(sc))
    apps = getApplications()
    assert(apps.head.executors.size === 0)
    assert(apps.head.getExecutorLimit === 0)
    // request many more; this increases the limit well beyond the cluster capacity
    assert(sc.requestExecutors(1000))
    apps = getApplications()
    assert(apps.head.executors.size === 10)
    assert(apps.head.getExecutorLimit === 1000)
  }

  test("dynamic allocation with cores per executor AND max cores") {
    sc = new SparkContext(appConf
      .set(config.EXECUTOR_CORES, 2)
      .set(config.CORES_MAX, 8))
    val appId = sc.applicationId
    eventually(timeout(10.seconds), interval(10.millis)) {
      val apps = getApplications()
      assert(apps.length === 1)
      assert(apps.head.id === appId)
      assert(apps.head.executors.size === 4) // 8 cores total
      assert(apps.head.getExecutorLimit === Int.MaxValue)
    }
    // kill all executors
    assert(killAllExecutors(sc))
    var apps = getApplications()
    assert(apps.head.executors.size === 0)
    assert(apps.head.getExecutorLimit === 0)
    // request 1
    assert(sc.requestExecutors(1))
    apps = getApplications()
    assert(apps.head.executors.size === 1)
    assert(apps.head.getExecutorLimit === 1)
    // request 3 more
    assert(sc.requestExecutors(3))
    apps = getApplications()
    assert(apps.head.executors.size === 4)
    assert(apps.head.getExecutorLimit === 4)
    // request 10 more; none will go through
    assert(sc.requestExecutors(10))
    apps = getApplications()
    assert(apps.head.executors.size === 4)
    assert(apps.head.getExecutorLimit === 14)
    // kill all executors; 4 executors will be launched immediately
    assert(killAllExecutors(sc))
    apps = getApplications()
    assert(apps.head.executors.size === 4)
    assert(apps.head.getExecutorLimit === 10)
    // ... and again
    assert(killAllExecutors(sc))
    apps = getApplications()
    assert(apps.head.executors.size === 4)
    assert(apps.head.getExecutorLimit === 6)
    // ... and again; now we end up with 6 - 4 = 2 executors left
    assert(killAllExecutors(sc))
    apps = getApplications()
    assert(apps.head.executors.size === 2)
    assert(apps.head.getExecutorLimit === 2)
    // ... and again; this time we have 2 - 2 = 0 executors left
    assert(killAllExecutors(sc))
    apps = getApplications()
    assert(apps.head.executors.size === 0)
    assert(apps.head.getExecutorLimit === 0)
    // request many more; this increases the limit well beyond the cluster capacity
    assert(sc.requestExecutors(1000))
    apps = getApplications()
    assert(apps.head.executors.size === 4)
    assert(apps.head.getExecutorLimit === 1000)
  }

  test("kill the same executor twice (SPARK-9795)") {
    sc = new SparkContext(appConf)
    val appId = sc.applicationId
    sc.requestExecutors(2)
    eventually(timeout(10.seconds), interval(10.millis)) {
      val apps = getApplications()
      assert(apps.length === 1)
      assert(apps.head.id === appId)
      assert(apps.head.executors.size === 2)
      assert(apps.head.getExecutorLimit === 2)
    }
    // sync executors between the Master and the driver, needed because
    // the driver refuses to kill executors it does not know about
    syncExecutors(sc)
    // kill the same executor twice
    val executors = getExecutorIds(sc)
    assert(executors.size === 2)
    assert(sc.killExecutor(executors.head))
    assert(!sc.killExecutor(executors.head))
    val apps = getApplications()
    assert(apps.head.executors.size === 1)
    // The limit should not be lowered twice
    assert(apps.head.getExecutorLimit === 1)
  }

  test("the pending replacement executors should not be lost (SPARK-10515)") {
    sc = new SparkContext(appConf)
    val appId = sc.applicationId
    sc.requestExecutors(2)
    eventually(timeout(10.seconds), interval(10.millis)) {
      val apps = getApplications()
      assert(apps.length === 1)
      assert(apps.head.id === appId)
      assert(apps.head.executors.size === 2)
      assert(apps.head.getExecutorLimit === 2)
    }
    // sync executors between the Master and the driver, needed because
    // the driver refuses to kill executors it does not know about
    syncExecutors(sc)
    val executors = getExecutorIds(sc)
    val executorIdsBefore = executors.toSet
    assert(executors.size === 2)
    // kill and replace an executor
    assert(sc.killAndReplaceExecutor(executors.head))
    eventually(timeout(10.seconds), interval(10.millis)) {
      val apps = getApplications()
      assert(apps.head.executors.size === 2)
      val executorIdsAfter = getExecutorIds(sc).toSet
      // make sure the executor was killed and replaced
      assert(executorIdsBefore != executorIdsAfter)
    }

    // kill old executor (which is killedAndReplaced) should fail
    assert(!sc.killExecutor(executors.head))

    // refresh executors list
    val newExecutors = getExecutorIds(sc)
    syncExecutors(sc)

    // kill newly created executor and do not replace it
    assert(sc.killExecutor(newExecutors(1)))
    val apps = getApplications()
    assert(apps.head.executors.size === 1)
    assert(apps.head.getExecutorLimit === 1)
  }

  test("disable force kill for busy executors (SPARK-9552)") {
    sc = new SparkContext(appConf)
    val appId = sc.applicationId
    eventually(timeout(10.seconds), interval(10.millis)) {
      val apps = getApplications()
      assert(apps.length === 1)
      assert(apps.head.id === appId)
      assert(apps.head.executors.size === 2)
      assert(apps.head.getExecutorLimit === Int.MaxValue)
    }
    var apps = getApplications()
    // sync executors between the Master and the driver, needed because
    // the driver refuses to kill executors it does not know about
    syncExecutors(sc)
    val executors = getExecutorIds(sc)
    assert(executors.size === 2)

    // simulate running a task on the executor
    val getMap = PrivateMethod[mutable.HashMap[String, mutable.HashSet[Long]]](
      Symbol("executorIdToRunningTaskIds"))
    val taskScheduler = sc.taskScheduler.asInstanceOf[TaskSchedulerImpl]
    val executorIdToRunningTaskIds = taskScheduler invokePrivate getMap()
    executorIdToRunningTaskIds(executors.head) = mutable.HashSet(1L)
    // kill the busy executor without force; this should fail
    assert(killExecutor(sc, executors.head, force = false).isEmpty)
    apps = getApplications()
    assert(apps.head.executors.size === 2)

    // force kill busy executor
    assert(killExecutor(sc, executors.head, force = true).nonEmpty)
    apps = getApplications()
    // kill executor successfully
    assert(apps.head.executors.size === 1)
  }

  test("initial executor limit") {
    val initialExecutorLimit = 1
    val myConf = appConf
      .set(config.DYN_ALLOCATION_ENABLED, true)
      .set(config.SHUFFLE_SERVICE_ENABLED, true)
      .set(config.DYN_ALLOCATION_INITIAL_EXECUTORS, initialExecutorLimit)
    sc = new SparkContext(myConf)
    val appId = sc.applicationId
    eventually(timeout(10.seconds), interval(10.millis)) {
      val apps = getApplications()
      assert(apps.length === 1)
      assert(apps.head.id === appId)
      assert(apps.head.executors.size === initialExecutorLimit)
      assert(apps.head.getExecutorLimit === initialExecutorLimit)
    }
  }

  test("kill all executors on localhost") {
    sc = new SparkContext(appConf)
    val appId = sc.applicationId
    eventually(timeout(10.seconds), interval(10.millis)) {
      val apps = getApplications()
      assert(apps.length === 1)
      assert(apps.head.id === appId)
      assert(apps.head.executors.size === 2)
      assert(apps.head.getExecutorLimit === Int.MaxValue)
    }
    val beforeList = getApplications().head.executors.keys.toSet
    syncExecutors(sc)

    sc.schedulerBackend match {
      case b: CoarseGrainedSchedulerBackend =>
        b.killExecutorsOnHost("localhost")
      case _ => fail("expected coarse grained scheduler")
    }

    eventually(timeout(10.seconds), interval(100.millis)) {
      val afterList = getApplications().head.executors.keys.toSet
      assert(beforeList.intersect(afterList).size == 0)
    }
  }

  test("executor registration on a excluded host must fail") {
    // The context isn't really used by the test, but it helps with creating a test scheduler,
    // since CoarseGrainedSchedulerBackend makes a lot of calls to the context instance.
    sc = new SparkContext(appConf.set(config.EXCLUDE_ON_FAILURE_ENABLED.key, "true"))

    val endpointRef = mock(classOf[RpcEndpointRef])
    val mockAddress = mock(classOf[RpcAddress])
    when(endpointRef.address).thenReturn(mockAddress)
    val message = RegisterExecutor("one", endpointRef, "excluded-host", 10, Map.empty,
      Map.empty, Map.empty, ResourceProfile.DEFAULT_RESOURCE_PROFILE_ID)

    val taskScheduler = mock(classOf[TaskSchedulerImpl])
    when(taskScheduler.excludedNodes()).thenReturn(Set("excluded-host"))
    when(taskScheduler.resourceOffers(any(), any[Boolean])).thenReturn(Nil)
    when(taskScheduler.sc).thenReturn(sc)

    val rpcEnv = RpcEnv.create("test-rpcenv", "localhost", 0, conf, securityManager)
    try {
      val scheduler = new CoarseGrainedSchedulerBackend(taskScheduler, rpcEnv)
      try {
        scheduler.start()
        val e = intercept[SparkException] {
          scheduler.driverEndpoint.askSync[Boolean](message)
        }
        assert(e.getCause().isInstanceOf[IllegalStateException])
        assert(scheduler.getExecutorIds().isEmpty)
      } finally {
        scheduler.stop()
      }
    } finally {
      rpcEnv.shutdown()
    }
  }

  // ===============================
  // | Utility methods for testing |
  // ===============================

  /** Return a SparkConf for applications that want to talk to our Master. */
  private def appConf: SparkConf = {
    new SparkConf()
      .setMaster(masterRpcEnv.address.toSparkURL)
      .setAppName("test")
      .set(config.EXECUTOR_MEMORY.key, "256m")
      // Because we're faking executor launches in the Worker, set the config so that the driver
      // will not timeout anything related to executors.
      .set(config.Network.NETWORK_TIMEOUT.key, "2h")
      .set(config.EXECUTOR_HEARTBEAT_INTERVAL.key, "1h")
      .set(config.STORAGE_BLOCKMANAGER_HEARTBEAT_TIMEOUT.key, "1h")
  }

  /** Make a master to which our application will send executor requests. */
  private def makeMaster(): Master = {
    val master = new Master(masterRpcEnv, masterRpcEnv.address, 0, securityManager, conf)
    masterRpcEnv.setupEndpoint(Master.ENDPOINT_NAME, master)
    master
  }

  /** Make a few workers that talk to our master. */
  private def makeWorkers(cores: Int, memory: Int): Seq[Worker] = {
    (0 until numWorkers).map { i =>
      val rpcEnv = workerRpcEnvs(i)
      val worker = new TestWorker(rpcEnv, cores, memory)
      rpcEnv.setupEndpoint(Worker.ENDPOINT_NAME, worker)
      worker
    }
  }

  /** Get the Master state */
  private def getMasterState: MasterStateResponse = {
    master.self.askSync[MasterStateResponse](RequestMasterState)
  }

  /** Get the applications that are active from Master */
  private def getApplications(): Array[ApplicationInfo] = {
    getMasterState.activeApps
  }

  /** Kill all executors belonging to this application. */
  private def killAllExecutors(sc: SparkContext): Boolean = {
    killNExecutors(sc, Int.MaxValue)
  }

  /** Kill N executors belonging to this application. */
  private def killNExecutors(sc: SparkContext, n: Int): Boolean = {
    syncExecutors(sc)
    sc.killExecutors(getExecutorIds(sc).take(n))
  }

  /** Kill the given executor, specifying whether to force kill it. */
  private def killExecutor(sc: SparkContext, executorId: String, force: Boolean): Seq[String] = {
    syncExecutors(sc)
    sc.schedulerBackend match {
      case b: CoarseGrainedSchedulerBackend =>
        b.killExecutors(Seq(executorId), adjustTargetNumExecutors = true, countFailures = false,
          force)
      case _ => fail("expected coarse grained scheduler")
    }
  }

  /**
   * Return a list of executor IDs belonging to this application.
   *
   * Note that we must use the executor IDs according to the Master, which has the most
   * updated view. We cannot rely on the executor IDs according to the driver because we
   * don't wait for executors to register. Otherwise the tests will take much longer to run.
   */
  private def getExecutorIds(sc: SparkContext): Seq[String] = {
    val app = getApplications().find(_.id == sc.applicationId)
    assert(app.isDefined)
    // Although executors is transient, master is in the same process so the message won't be
    // serialized and it's safe here.
    app.get.executors.keys.map(_.toString).toSeq
  }

  /**
   * Sync executor IDs between the driver and the Master.
   *
   * This allows us to avoid waiting for new executors to register with the driver before
   * we submit a request to kill them. This must be called before each kill request.
   */
  private def syncExecutors(sc: SparkContext): Unit = {
    val backend = sc.schedulerBackend.asInstanceOf[CoarseGrainedSchedulerBackend]
    val driverExecutors = backend.getExecutorIds()
    val masterExecutors = getExecutorIds(sc)
    val missingExecutors = masterExecutors.toSet.diff(driverExecutors.toSet).toSeq.sorted
    missingExecutors.foreach { id =>
      // Fake an executor registration so the driver knows about us
      val endpointRef = mock(classOf[RpcEndpointRef])
      val mockAddress = mock(classOf[RpcAddress])
      when(endpointRef.address).thenReturn(mockAddress)
      val message = RegisterExecutor(id, endpointRef, "localhost", 10, Map.empty, Map.empty,
        Map.empty, ResourceProfile.DEFAULT_RESOURCE_PROFILE_ID)
      backend.driverEndpoint.askSync[Boolean](message)
      backend.driverEndpoint.send(LaunchedExecutor(id))
    }
  }

  /**
   * Worker implementation that does not actually launch any executors, but reports them as
   * running so the Master keeps track of them. This requires that `syncExecutors` be used
   * to make sure the Master instance and the SparkContext under test agree about what
   * executors are running.
   */
  private class TestWorker(rpcEnv: RpcEnv, cores: Int, memory: Int)
    extends Worker(
      rpcEnv, 0, cores, memory, Array(masterRpcEnv.address), Worker.ENDPOINT_NAME,
      null, conf, securityManager) {

    override def receive: PartialFunction[Any, Unit] = testReceive.orElse(super.receive)

    private def testReceive: PartialFunction[Any, Unit] = synchronized {
      case LaunchExecutor(_, appId, execId, _, _, _, _, _) =>
        self.send(ExecutorStateChanged(appId, execId, ExecutorState.RUNNING, None, None))
    }

  }

}
