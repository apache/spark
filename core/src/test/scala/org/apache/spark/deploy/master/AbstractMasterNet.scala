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

package org.apache.spark.deploy.master

import java.io.File
import java.util.UUID
import java.util.concurrent.atomic.AtomicReference

import scala.collection.mutable
import scala.language.postfixOps

import com.google.common.io.Files
import org.scalatest.concurrent.Eventually
import org.scalatest.time.{Milliseconds, Seconds, Span}
import org.scalatest.{BeforeAndAfter, BeforeAndAfterAll, Matchers, PrivateMethodTester}

import org.apache.spark._
import org.apache.spark.deploy.ApplicationDescription
import org.apache.spark.deploy.DeployMessages._
import org.apache.spark.deploy.master.MasterMessages.BoundPortsResponse
import org.apache.spark.rpc.{RpcAddress, RpcCallContext, RpcEndpoint, RpcEndpointRef, RpcEnv}

abstract class AbstractMasterNet(rpcImpl: String)
    extends SparkFunSuite
    with Matchers
    with Eventually
    with PrivateMethodTester
    with BeforeAndAfter
    with BeforeAndAfterAll {

  var masterEnv: RpcEnv = _

  implicit val patienceCfg = PatienceConfig(Span(5, Seconds), Span(500, Milliseconds))

  private val defaultConf = new SparkConf(loadDefaults = false)
      .set("spark.rpc", rpcImpl)
      .set("spark.rpc.numRetries", "3")
      .set("spark.rpc.retry.wait", "500ms")
      .set("spark.network.timeout", "3s")
      .set("spark.deploy.recoveryMode", "FILESYSTEM")

  val masterConf: SparkConf = defaultConf.clone

  def schedulerSecretConf: SparkConf = defaultConf.clone()
      .set("spark.authenticate", "true")
      .set("spark.authenticate.secret", "1q2w3e")
      .set("spark.authenticate.enableSaslEncryption", "true")

  def user1SecretConf: SparkConf = defaultConf.clone()
      .set("spark.authenticate", "true")
      .set("spark.authenticate.user", "testUser1")
      .set("spark.authenticate.secret", "abc123")
      .set("spark.authenticate.enableSaslEncryption", "true")

  def user2SecretConf: SparkConf = defaultConf.clone()
      .set("spark.authenticate", "true")
      .set("spark.authenticate.user", "testUser2")
      .set("spark.authenticate.secret", "def123")
      .set("spark.authenticate.enableSaslEncryption", "true")

  def badSecretConf: SparkConf = defaultConf.clone()
      .set("spark.authenticate", "true")
      .set("spark.authenticate.secret", "bad")
      .set("spark.authenticate.enableSaslEncryption", "true")

  def noSecretConf: SparkConf = defaultConf.clone()

  val envs = new mutable.Stack[RpcEnv]

  override def afterAll() {
  }

  override def beforeAll(): Unit = {
  }

  var tmpDir: File = _

  before {
    tmpDir = Files.createTempDir()
    masterConf.set("spark.deploy.recoveryDirectory", tmpDir.getAbsolutePath)
    startMaster()
  }

  after {
    while (envs.nonEmpty) {
      shutdownEnv(envs.pop)
    }
  }

  def shutdownEnv(env: RpcEnv): Unit = {
    try {
      env.shutdown()
      env.awaitTermination()
    } catch {
      case ex: Exception => logError(s"Failed to close env bound to ${env.address }", ex)
    }
  }

  trait SimpleAppClient extends RpcEndpoint {
    @volatile var _appId: String = _
    @volatile var _masterRef: RpcEndpointRef = _

    def registerApp(app: ApplicationDescription): Unit

    def unregisterApp(): Unit

    def requestExecutors(n: Int): Boolean

    def state: Option[AnyRef]
  }

  def makeAppClient(env: RpcEnv, masterRef: RpcEndpointRef): SimpleAppClient = {
    val endpoint = new SimpleAppClient {
      _masterRef = masterRef
      val _state = new AtomicReference[AnyRef]()
      override val rpcEnv: RpcEnv = env

      override def state: Option[AnyRef] = Option(_state.get)

      override def receive(context: RpcCallContext): PartialFunction[Any, Unit] = {
        case RegisteredApplication(appId, _) =>
          _state.set("registered")
          _appId = appId
        case ApplicationRemoved(_) =>
          _state.set("removed")
        case MasterChanged(newMaster, _) =>
          _masterRef = newMaster
          _state.set("recovered")
          _masterRef.send(MasterChangeAcknowledged(_appId))
      }

      override def onDisconnected(remoteAddress: RpcAddress): Unit = _state.set("disconnected")

      override def onStart(): Unit = _state.set("started")

      override def onError(cause: Throwable): Unit = _state.set(cause)

      override def onNetworkError(cause: Throwable, addr: RpcAddress): Unit = _state.set(cause)

      override def registerApp(app: ApplicationDescription): Unit = {
        _masterRef.send(RegisterApplication(app, self))
      }

      override def requestExecutors(n: Int): Boolean = {
        _masterRef.askWithRetry[Boolean](RequestExecutors(_appId, n))
      }

      override def unregisterApp(): Unit = {
        _masterRef.send(UnregisterApplication(_appId))
      }
    }

    env.setupEndpoint("test" + UUID.randomUUID().toString, endpoint)

    endpoint
  }

  def makeEnv(conf: SparkConf): RpcEnv = {
    val confClone = conf.clone()
    val env = RpcEnv.create(
      name = "test" + UUID.randomUUID().toString,
      host = "localhost",
      port = 0,
      conf = confClone,
      securityManager = new SecurityManager(confClone))
    envs.push(env)
    env
  }

  def connectToMaster(clientEnv: RpcEnv): RpcEndpointRef =
    clientEnv.setupEndpointRef(
      systemName = Master.SYSTEM_NAME,
      address = RpcAddress("localhost", masterEnv.address.port),
      endpointName = Master.ENDPOINT_NAME)

  def testSchedulingMsgAsk(masterRef: RpcEndpointRef): Unit = {
    // RequestMasterState is an internal Spark scheduler message, so it should be handled by
    // standard RPC endpoint, but it should be rejected by submission RPC endpoint
    val masterStateResponse = masterRef.askWithRetry[MasterStateResponse](RequestMasterState)
    assert(masterStateResponse.status === RecoveryState.ALIVE)
  }

  def testSubmissionMsgAsk(masterRef: RpcEndpointRef): Unit = {
    // RequestDriverStatus is a submission message, so it should be handled by
    // standard RPC endpoint and by submission RPC endpoint
    val driverStatusResponse =
      masterRef.askWithRetry[DriverStatusResponse](RequestDriverStatus("1"))
    assert(driverStatusResponse.found === false)
    assert(driverStatusResponse.exception === None)
  }

  def testAppRegistration(
      clientEnv: RpcEnv, masterRef: RpcEndpointRef,
      doBeforeUnregister: SimpleAppClient => Unit = client => {}): Unit = {
    val app = makeAppInfo()
    val client = makeAppClient(clientEnv, masterRef)
    try {
      eventually { assert(client.state === Some("started")) }
      client.registerApp(app)
      eventually { assert(client.state === Some("registered")) }
      assert(client.requestExecutors(1) === true)
      doBeforeUnregister(client)
      client.unregisterApp()
      client.stop()
    } catch {
      case ex: Throwable =>
        val clientException = client.state.collectFirst {
          case t: Throwable => t
        }
        clientException.foreach(throw _)
        throw ex
    }
  }

  def testAppRegistrationWithMasterFailure(
      clientEnv: RpcEnv, masterRef: RpcEndpointRef): Unit = testAppRegistration(
    clientEnv, masterRef, client => {
      masterEnv.shutdown()
      masterEnv.awaitTermination()

      eventually { assert(client.state === Some("disconnected")) }

      startMaster()

      eventually { assert(client.state === Some("recovered")) }
      assert(client.requestExecutors(1) === true)
    })

  def testAppRegistrationWithAuthz(clientEnv: RpcEnv, masterRef: RpcEndpointRef): Unit =
    testAppRegistration(
      clientEnv, masterRef, client => {
        val clientEnv2 = makeEnv(user2SecretConf)
        try {
          val masterRef2 = connectToMaster(clientEnv2)
          val client2 = makeAppClient(clientEnv2, masterRef2)
          client2._appId = client._appId
          assert(client2.requestExecutors(1) === true)
        }
        assert(client.requestExecutors(1) === true)
      })

  def makeAppInfo(): ApplicationDescription = {
    new ApplicationDescription("test", None, 100, null, "", None, None, None)
  }

  def startMaster(_conf: SparkConf = masterConf): Unit = {
    val (env, uiPort, restPort) = Master.startRpcEnvAndEndpoint("localhost", 0, 0, _conf.clone)
    envs.push(env)

    assert(
      Set(
        env.address.port,
        uiPort,
        restPort.getOrElse(-1)).size === 3)

    masterEnv = env
  }

}
