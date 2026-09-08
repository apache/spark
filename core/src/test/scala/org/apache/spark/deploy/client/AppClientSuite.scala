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

package org.apache.spark.deploy.client

import java.io.Closeable
import java.util.concurrent.{ConcurrentHashMap, ConcurrentLinkedQueue}

import scala.concurrent.Future
import scala.concurrent.duration._
import scala.jdk.CollectionConverters._

import org.scalatest.concurrent.{Eventually, ScalaFutures}

import org.apache.spark._
import org.apache.spark.deploy.{ApplicationDescription, Command, DeployTestUtils}
import org.apache.spark.deploy.DeployMessages.{MasterStateResponse, RequestApplicationHold, RequestMasterState, WorkerDecommissioning}
import org.apache.spark.deploy.master.{ApplicationInfo, Master}
import org.apache.spark.deploy.worker.Worker
import org.apache.spark.internal.{config, Logging}
import org.apache.spark.resource.{ExecutorResourceRequests, ResourceProfileBuilder}
import org.apache.spark.resource.ResourceProfile.DEFAULT_RESOURCE_PROFILE_ID
import org.apache.spark.rpc.RpcEnv
import org.apache.spark.scheduler.ExecutorDecommissionInfo
import org.apache.spark.util.Utils

/**
 * End-to-end tests for application client in standalone mode.
 */
class AppClientSuite
    extends SparkFunSuite
    with LocalSparkContext
    with Eventually
    with ScalaFutures {
  private val numWorkers = 2

  private var conf: SparkConf = null
  private var masterRpcEnv: RpcEnv = null
  private var workerRpcEnvs: Seq[RpcEnv] = null
  private var master: Master = null
  private var workers: Seq[Worker] = null
  private var securityManager: SecurityManager = null

  /**
   * Start the local cluster.
   * Note: local-cluster mode is insufficient because we want a reference to the Master.
   */
  override def beforeAll(): Unit = {
    super.beforeAll()
    conf = new SparkConf().set(config.DECOMMISSION_ENABLED.key, "true")
    securityManager = new SecurityManager(conf)
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
      workerRpcEnvs.foreach(_.shutdown())
      masterRpcEnv.shutdown()
      workers.foreach(_.stop())
      master.stop()
      workerRpcEnvs = null
      masterRpcEnv = null
      workers = null
      master = null
    } finally {
      super.afterAll()
    }
  }

  test("interface methods of AppClient using local Master") {
    Utils.tryWithResource(new AppClientInst(masterRpcEnv.address.toSparkURL)) { ci =>

      ci.client.start()

      // Client should connect with one Master which registers the application
      eventually(timeout(10.seconds), interval(10.millis)) {
        val apps = getApplications()
        assert(ci.listener.connectedIdList.size === 1, "client listener should have one connection")
        assert(apps.length === 1, "master should have 1 registered app")
      }

      // Send message to Master to request Executors, verify request by change in executor limit
      val numExecutorsRequested = 1
      whenReady(
        ci.client.requestTotalExecutors(numExecutorsRequested),
        timeout(10.seconds),
        interval(10.millis)) { acknowledged =>
        assert(acknowledged)
      }

      eventually(timeout(10.seconds), interval(10.millis)) {
        val apps = getApplications()
        assert(apps.head.getExecutorLimit === numExecutorsRequested, s"executor request failed")
      }


      // Save the executor id before decommissioning so we can kill it
      val application = getApplications().head
      val executors = application.executors
      val executorId: String = executors.head._2.fullId

      // Send a decommission self to all the workers
      // Note: normally the worker would send this on their own.
      workers.foreach { worker =>
        worker.decommissionSelf()
        // send the notice to Master to tell the decommission of Workers
        master.self.send(WorkerDecommissioning(worker.workerId, worker.self))
      }

      // Decommissioning is async.
      eventually(timeout(1.seconds), interval(10.millis)) {
        // We only record decommissioning for the executor we've requested
        assert(ci.listener.execDecommissionedMap.size === 1)
        val decommissionInfo = ci.listener.execDecommissionedMap.get(executorId)
        assert(decommissionInfo != null && decommissionInfo.workerHost.isDefined,
          s"$executorId should have been decommissioned along with its worker")
      }

      // Send request to kill executor, verify request was made
      whenReady(
        ci.client.killExecutors(Seq(executorId)),
        timeout(10.seconds),
        interval(10.millis)) { acknowledged =>
        assert(acknowledged)
      }

      // Verify that asking for executors on the decommissioned workers fails
      whenReady(
        ci.client.requestTotalExecutors(numExecutorsRequested),
        timeout(10.seconds),
        interval(10.millis)) { acknowledged =>
        assert(acknowledged)
      }
      assert(getApplications().head.executors.size === 0)

      // Issue stop command for Client to disconnect from Master
      ci.client.stop()

      // Verify Client is marked dead and unregistered from Master
      eventually(timeout(10.seconds), interval(10.millis)) {
        val apps = getApplications()
        assert(ci.listener.deadReasonList.size === 1, "client should have been marked dead")
        assert(apps.isEmpty, "master should have 0 registered apps")
      }
    }
  }

  test("request executors with multi resource profiles") {
    Utils.tryWithResource(new AppClientInst(masterRpcEnv.address.toSparkURL)) { ci =>
      ci.client.start()

      // Client should connect with one Master which registers the application
      eventually(timeout(10.seconds), interval(10.millis)) {
        val apps = getApplications()
        assert(ci.listener.connectedIdList.size === 1, "client listener should have one connection")
        assert(apps.length === 1, "master should have 1 registered app")
      }

      // Send message to Master to request Executors with multiple resource profiles.
      val rpBuilder = new ResourceProfileBuilder()
      val ereqs = new ExecutorResourceRequests()
      ereqs.cores(5)
      ereqs.memory("1024m")
      rpBuilder.require(ereqs)
      val rp = rpBuilder.build()
      val resourceProfileToTotalExecs = Map(
        ci.desc.defaultProfile -> 1,
        rp -> 2
      )
      whenReady(
        ci.client.requestTotalExecutors(resourceProfileToTotalExecs),
        timeout(10.seconds),
        interval(10.millis)) { acknowledged =>
        assert(acknowledged)
      }

      eventually(timeout(10.seconds), interval(10.millis)) {
        val app = getApplications().head
        assert(app.getRequestedRPIds().length == 2)
        assert(app.getResourceProfileById(DEFAULT_RESOURCE_PROFILE_ID)
          === ci.desc.defaultProfile)
        assert(app.getResourceProfileById(rp.id) === rp)
        assert(app.getTargetExecutorNumForRPId(DEFAULT_RESOURCE_PROFILE_ID) === 1)
        assert(app.getTargetExecutorNumForRPId(rp.id) === 2)
      }

      // Issue stop command for Client to disconnect from Master
      ci.client.stop()

      // Verify Client is marked dead and unregistered from Master
      eventually(timeout(10.seconds), interval(10.millis)) {
        val apps = getApplications()
        assert(ci.listener.deadReasonList.size === 1, "client should have been marked dead")
        assert(apps.isEmpty, "master should have 0 registered apps")
      }
    }
  }

  test("SPARK-59055: report the hold status of the application to the Master") {
    Utils.tryWithResource(new AppClientInst(masterRpcEnv.address.toSparkURL)) { ci =>
      ci.client.start()

      eventually(timeout(10.seconds), interval(10.millis)) {
        assert(getApplications().length === 1, "master should have 1 registered app")
      }

      // The Master knows nothing about the hold until the driver reports it.
      val app = getApplications().head
      assert(!app.holdSupported && !app.held && app.numDrainingExecutors === 0)

      ci.client.reportHoldStatus(supported = true, held = false)
      eventually(timeout(10.seconds), interval(10.millis)) {
        assert(getApplications().head.holdSupported)
      }
      assert(!getApplications().head.held)

      ci.client.reportHoldStatus(supported = true, held = true)
      eventually(timeout(10.seconds), interval(10.millis)) {
        assert(getApplications().head.held)
      }

      ci.client.reportHoldStatus(supported = true, held = false)
      eventually(timeout(10.seconds), interval(10.millis)) {
        assert(!getApplications().head.held)
      }

      // Issue stop command for Client to disconnect from Master
      ci.client.stop()

      eventually(timeout(10.seconds), interval(10.millis)) {
        assert(getApplications().isEmpty, "master should have 0 registered apps")
      }
    }
  }

  test("SPARK-59061: hold and resume an application from the Master") {
    Utils.tryWithResource(new AppClientInst(masterRpcEnv.address.toSparkURL)) { ci =>
      ci.client.start()

      eventually(timeout(10.seconds), interval(10.millis)) {
        assert(getApplications().length === 1, "master should have 1 registered app")
      }
      val appId = getApplications().head.id

      // A hold requested from the Master UI reaches the driver.
      master.self.send(RequestApplicationHold(appId, hold = true))
      eventually(timeout(10.seconds), interval(10.millis)) {
        assert(ci.listener.holdRequestList.asScala.toSeq === Seq(true))
      }

      master.self.send(RequestApplicationHold(appId, hold = false))
      eventually(timeout(10.seconds), interval(10.millis)) {
        assert(ci.listener.holdRequestList.asScala.toSeq === Seq(true, false))
      }

      ci.client.stop()

      eventually(timeout(10.seconds), interval(10.millis)) {
        assert(getApplications().isEmpty, "master should have 0 registered apps")
      }
    }
  }

  test("SPARK-59061: reject the hold request of an application that disabled holding") {
    Utils.tryWithResource(
        new AppClientInst(masterRpcEnv.address.toSparkURL, holdEnabled = false)) { ci =>
      ci.client.start()

      eventually(timeout(10.seconds), interval(10.millis)) {
        assert(getApplications().length === 1, "master should have 1 registered app")
      }
      val app = getApplications().head

      // The Master rejects without forwarding, so the driver never sees the request. The
      // askSync afterwards is a barrier: the Master processes its messages in order, so once
      // it answers, the hold request has been handled.
      master.self.send(RequestApplicationHold(app.id, hold = true))
      getApplications()
      Thread.sleep(100)
      assert(ci.listener.holdRequestList.isEmpty)

      ci.client.stop()

      eventually(timeout(10.seconds), interval(10.millis)) {
        assert(getApplications().isEmpty, "master should have 0 registered apps")
      }
    }
  }

  test("request from AppClient before initialized with master") {
    Utils.tryWithResource(new AppClientInst(masterRpcEnv.address.toSparkURL)) { ci =>

      // requests to master should fail immediately
      whenReady(ci.client.requestTotalExecutors(3), timeout(1.seconds)) { success =>
        assert(success === false)
      }
    }
  }

  // ===============================
  // | Utility methods for testing |
  // ===============================

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
      val worker = new Worker(rpcEnv, 0, cores, memory, Array(masterRpcEnv.address),
        Worker.ENDPOINT_NAME, null, conf, securityManager)
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

  /** Application Listener to collect events */
  private class AppClientCollector extends StandaloneAppClientListener with Logging {
    val connectedIdList = new ConcurrentLinkedQueue[String]()
    @volatile var disconnectedCount: Int = 0
    val deadReasonList = new ConcurrentLinkedQueue[String]()
    val execAddedList = new ConcurrentLinkedQueue[String]()
    val execRemovedList = new ConcurrentLinkedQueue[String]()
    val execDecommissionedMap = new ConcurrentHashMap[String, ExecutorDecommissionInfo]()
    val holdRequestList = new ConcurrentLinkedQueue[Boolean]()

    def connected(id: String): Unit = {
      connectedIdList.add(id)
    }

    def disconnected(): Unit = {
      synchronized {
        disconnectedCount += 1
      }
    }

    def dead(reason: String): Unit = {
      deadReasonList.add(reason)
    }

    def executorAdded(
        id: String,
        workerId: String,
        hostPort: String,
        cores: Int,
        memory: Int): Unit = {
      execAddedList.add(id)
    }

    def executorRemoved(
        id: String, message: String, exitStatus: Option[Int], workerHost: Option[String]): Unit = {
      execRemovedList.add(id)
    }

    def executorDecommissioned(id: String, decommissionInfo: ExecutorDecommissionInfo): Unit = {
      val previousDecommissionInfo = execDecommissionedMap.putIfAbsent(id, decommissionInfo)
      assert(previousDecommissionInfo === null, s"Expected no previous decommission info for $id")
    }

    def workerRemoved(workerId: String, host: String, message: String): Unit = {}

    def holdApplication(hold: Boolean): Future[Boolean] = {
      holdRequestList.add(hold)
      Future.successful(true)
    }
  }

  /** Create AppClient and supporting objects */
  private class AppClientInst(masterUrl: String, holdEnabled: Boolean = true) extends Closeable {
    val rpcEnv = RpcEnv.create("spark", Utils.localHostName(), 0, conf, securityManager)
    private val cmd = new Command(TestExecutor.getClass.getCanonicalName.stripSuffix("$"),
      List(), Map(), Seq(), Seq(), Seq())
    private val defaultRp = DeployTestUtils.createDefaultResourceProfile(512)
    val desc =
      ApplicationDescription("AppClientSuite", Some(1), cmd, "ignored", defaultRp,
        holdEnabled = holdEnabled)
    val listener = new AppClientCollector
    val client = new StandaloneAppClient(rpcEnv, Array(masterUrl), desc, listener, new SparkConf)

    override def close(): Unit = {
      rpcEnv.shutdown()
    }
  }

}
