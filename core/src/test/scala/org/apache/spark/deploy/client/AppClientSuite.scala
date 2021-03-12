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
import java.util.concurrent.ConcurrentLinkedQueue

import scala.concurrent.duration._

import org.scalatest.BeforeAndAfterAll
import org.scalatest.concurrent.{Eventually, ScalaFutures}

import org.apache.spark._
import org.apache.spark.deploy.{ApplicationDescription, Command}
import org.apache.spark.deploy.DeployMessages.{MasterStateResponse, RequestMasterState}
import org.apache.spark.deploy.master.{ApplicationInfo, Master}
import org.apache.spark.deploy.worker.Worker
import org.apache.spark.internal.Logging
import org.apache.spark.rpc.RpcEnv
import org.apache.spark.util.Utils

/**
 * End-to-end tests for application client in standalone mode.
 */
class AppClientSuite
    extends SparkFunSuite
    with LocalSparkContext
    with BeforeAndAfterAll
    with Eventually
    with ScalaFutures {
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
      assert(getMasterState.workers.size === numWorkers)
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
        assert(apps.size === 1, "master should have 1 registered app")
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

      // Send request to kill executor, verify request was made
      val executorId: String = getApplications().head.executors.head._2.fullId
      whenReady(
        ci.client.killExecutors(Seq(executorId)),
        timeout(10.seconds),
        interval(10.millis)) { acknowledged =>
        assert(acknowledged)
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

  /** Return a SparkConf for applications that want to talk to our Master. */
  private def appConf: SparkConf = {
    new SparkConf()
      .setMaster(masterRpcEnv.address.toSparkURL)
      .setAppName("test")
      .set("spark.executor.memory", "256m")
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
  private def getApplications(): Seq[ApplicationInfo] = {
    getMasterState.activeApps
  }

  /** Application Listener to collect events */
  private class AppClientCollector extends StandaloneAppClientListener with Logging {
    val connectedIdList = new ConcurrentLinkedQueue[String]()
    @volatile var disconnectedCount: Int = 0
    val deadReasonList = new ConcurrentLinkedQueue[String]()
    val execAddedList = new ConcurrentLinkedQueue[String]()
    val execRemovedList = new ConcurrentLinkedQueue[String]()

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
        id: String, message: String, exitStatus: Option[Int], workerLost: Boolean): Unit = {
      execRemovedList.add(id)
    }

    def workerRemoved(workerId: String, host: String, message: String): Unit = {}
  }

  /** Create AppClient and supporting objects */
  private class AppClientInst(masterUrl: String) extends Closeable {
    val rpcEnv = RpcEnv.create("spark", Utils.localHostName(), 0, conf, securityManager)
    private val cmd = new Command(TestExecutor.getClass.getCanonicalName.stripSuffix("$"),
      List(), Map(), Seq(), Seq(), Seq())
    private val desc = new ApplicationDescription("AppClientSuite", Some(1), 512, cmd, "ignored")
    val listener = new AppClientCollector
    val client = new StandaloneAppClient(rpcEnv, Array(masterUrl), desc, listener, new SparkConf)

    override def close(): Unit = {
      rpcEnv.shutdown()
    }
  }

}
