/*
 * EAGLE 
 *
 * Copyright 2016 Operating Systems Laboratory EPFL
 *
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

package org.apache.spark.scheduler.eagle

import java.net.{URL, InetSocketAddress}
import java.nio.ByteBuffer
import java.util.{UUID, ArrayList}

import ch.epfl.eagle.daemon.util.{TClients, TServers, ThriftClientPool}
import ch.epfl.eagle.thrift.NodeMonitorService.AsyncClient
import ch.epfl.eagle.thrift.NodeMonitorService.AsyncClient.{sendFrontendMessage_call,
tasksFinished_call}
import ch.epfl.eagle.thrift.{BackendService, NodeMonitorService, TFullTaskId,
TUserGroupInfo}
import org.apache.spark.deploy.SparkHadoopUtil

import org.apache.spark.executor.{Executor, ExecutorBackend}
import org.apache.spark.rpc.{ThreadSafeRpcEndpoint, RpcEnv}
import org.apache.spark.scheduler.TaskDescription
import org.apache.spark.scheduler.cluster.CoarseGrainedClusterMessages.{RegisterExecutor, RegisterExecutorResponse, RetrieveSparkProps}
import org.apache.spark.serializer.SerializerInstance
import org.apache.spark.util.{ThreadUtils, Utils}
import org.apache.spark._
import eagle.org.apache.thrift.async.AsyncMethodCallback
import org.apache.spark.internal.Logging
import scala.collection.mutable
import scala.collection.mutable.HashMap
import scala.util.{Failure, Success}

/**
  * An Eagle version of an {@link ExecutorBackend}.
  *
  * Acts as an Eagle backend and listens for task launch requests from Eagle. Also passes
  * statusUpdate messages from a Spark executor back to Eagle.
  */
class EagleExecutorBackend(driverUrl: String,
    executorId: String,
    cores: Int,
    env: SparkEnv)
  extends ExecutorBackend with Logging with BackendService.Iface with ThreadSafeRpcEndpoint {
  private val executor: Executor = new Executor(env.executorId, Utils.localHostName, env)

  override def onStart() {
    logInfo("Connecting to driver: " + driverUrl)
    env.rpcEnv.asyncSetupEndpointRefByURI(driverUrl).flatMap { ref =>
      // This is a very fast action so we can use "ThreadUtils.sameThread"
      //driver = Some(ref)
      ref.ask[RegisterExecutorResponse](RegisterExecutor(executorId, self, cores,
        Map[String, String]()))
    }(ThreadUtils.sameThread).onComplete {
      // This is a very fast action so we can use "ThreadUtils.sameThread"
      case Success(msg) =>
        logInfo(s"got $msg from driver.")
        initialize()
      case Failure(e) => {
        logError(s"Cannot register with driver: $driverUrl", e)
        System.exit(1)
      }
    }(ThreadUtils.sameThread)
  }
  // If this ExecutorBackend is changed to support multiple threads, then this may need
  // to be changed so that we don't share the serializer instance across threads
  private[this] val ser: SerializerInstance = env.closureSerializer.newInstance()

  // TODO: Make this configurable.
  private val nodeMonitorAddress = new InetSocketAddress("localhost", 20501)
  private val appName = System.getProperty("eagle.app.name", "spark")

  private val taskIdToFullTaskId = new HashMap[Long, TFullTaskId]()
  private val clientPool = new ThriftClientPool[NodeMonitorService.AsyncClient](
    new ThriftClientPool.NodeMonitorServiceMakerFactory())

  /** Callback to use for asynchronous Thrift function calls. */
  class ThriftCallback[T](client: AsyncClient) extends AsyncMethodCallback[T] {
    def onComplete(response: T) {
      try {
        clientPool.returnClient(nodeMonitorAddress, client)
      } catch {
        case e: Exception => e.printStackTrace(System.err)
      }
    }

    def onError(exception: Exception) {
      exception.printStackTrace(System.err)
    }
  }

  def initialize() {
    val client = TClients.createBlockingNmClient(
      nodeMonitorAddress.getHostName(), nodeMonitorAddress.getPort())
    logInfo("Backend for app %s trying to register to nodemonitor".format(appName))
    client.registerBackend(appName, "localhost:" + EagleExecutorBackend.listenPort)
  }

  override def launchTask(message: ByteBuffer, taskId: TFullTaskId, user: TUserGroupInfo) =
    synchronized {
      val taskIdLong = taskId.taskId.toLong
      taskIdToFullTaskId(taskIdLong) = taskId
      val taskDesc = ser.deserialize[TaskDescription](message)
      logInfo(s"Launching..${taskDesc.toString}")
      executor.launchTask(this, taskIdLong, taskDesc.attemptNumber, taskDesc.name, taskDesc.serializedTask)
    }

  override def statusUpdate(taskId: Long, state: TaskState.TaskState, data: ByteBuffer): Unit =
    synchronized {
      if (state == TaskState.RUNNING) {
        // Ignore running messages, which just generate extra traffic.
        return
      }

      val fullId = taskIdToFullTaskId(taskId)

      if (state == TaskState.FINISHED) {
        val client = clientPool.borrowClient(nodeMonitorAddress)
        val finishedTasksList = new ArrayList[TFullTaskId]()
        finishedTasksList.add(fullId)
        client.tasksFinished(finishedTasksList, new ThriftCallback[tasksFinished_call](client))
      }

      // Use a new client here because asynchronous clients can only be used for one function call
      // at a time.
      val client = clientPool.borrowClient(nodeMonitorAddress)
      client.sendFrontendMessage(
        appName, fullId, state.id, data, new ThriftCallback[sendFrontendMessage_call](client))
    }

  override lazy val rpcEnv: RpcEnv = env.rpcEnv
}

object EagleExecutorBackend extends Logging {
  var listenPort = 33333

  private def run(
      driverUrl: String,
      executorId: String,
      hostname: String,
      cores: Int,
      appId: String) {

    Utils.initDaemon(log)

    // Debug code
    Utils.checkHost(hostname)

    // Bootstrap to fetch the driver's Spark properties.
    val executorConf = new SparkConf
    val port = executorConf.getInt("spark.executor.port", 0)
    val fetcher = RpcEnv.create("driverPropsFetcher", hostname, port, executorConf, new SecurityManager(executorConf), clientMode = true)
      
    logInfo("EagleExecutorBackend started, fetcher will get driver "+driverUrl)
    
    val driver = fetcher.setupEndpointRefByURI(driverUrl)
    val props = driver.askWithRetry[Seq[(String, String)]](RetrieveSparkProps) ++
      Seq[(String, String)](("spark.app.id", appId))
    fetcher.shutdown()

    // Create SparkEnv using properties we fetched from the driver.
    val driverConf = new SparkConf()
    for ((key, value) <- props) {
      // this is required for SSL in standalone mode
      if (SparkConf.isExecutorStartupConf(key)) {
        driverConf.setIfMissing(key, value)
      } else {
        driverConf.set(key, value)
      }
    }
    logInfo(s"driver conf: ${driverConf.getAll.mkString("#")}")
    val env = SparkEnv.createExecutorEnv(
      driverConf, executorId, hostname, port, cores, isLocal = false)

    lazy val backend =
    new EagleExecutorBackend(
        driverUrl, executorId, cores, env)
    env.rpcEnv.setupEndpoint("Executor", backend)

    val processor = new BackendService.Processor[BackendService.Iface](backend)
    var foundPort = false
    while (!foundPort) {
      try {
        TServers.launchThreadedThriftServer(listenPort, 4, processor)
        foundPort = true
      } catch {
        case e: java.io.IOException =>
          println("Failed to listen on port %d; trying next port ".format(listenPort))
          listenPort = listenPort + 1
      }
    }
    env.rpcEnv.awaitTermination()
    SparkHadoopUtil.get.stopExecutorDelegationTokenRenewer()
  }

  def main(args: Array[String]) {
    var driverUrl: String = null
    var executorId: String = null
    var hostname: String = null
    var cores: Int = 0
    var appId: String = null

    var argv = args.toList
    while (!argv.isEmpty) {
      argv match {
        case ("--driver-url") :: value :: tail =>
          driverUrl = value
          argv = tail
        case ("--executor-id") :: value :: tail =>
          executorId = value
          argv = tail
        case ("--hostname") :: value :: tail =>
          hostname = value
          argv = tail
        case ("--cores") :: value :: tail =>
          cores = value.toInt
          argv = tail
        case ("--app-id") :: value :: tail =>
          appId = value
          argv = tail
        case Nil =>
        case tail =>
          // scalastyle:off println
          System.err.println(s"Unrecognized options: ${tail.mkString(" ")}")
          // scalastyle:on println
          printUsageAndExit()
      }
    }
    if(driverUrl == null) {
      printUsageAndExit()
    }
    if (hostname == null || cores <= 0) {
      hostname = Utils.localHostName()
      cores = 1
    }
    if (executorId == null) {
      executorId =
        s"EagleExecutor_${hostname}_${UUID.randomUUID().toString.substring(4, 8)}"
    }
    if (appId == null) {
      appId = "spark"
    }
    run(driverUrl, executorId, hostname, cores, appId)
    System.exit(0)
  }

  private def printUsageAndExit() = {
    // scalastyle:off println
    System.err.println(
      """
        |Usage: EagleExecutorBackend [options]
        |
        | Options are:
        |   --driver-url <driverUrl>
        |   --executor-id <executorId>(optional)
        |   --hostname <hostname> (optional, defaults to auto detecting)
        |   --cores <cores> (defaults to 1)
        |   --app-id <appid> (defaults to 'spark')
        | """.stripMargin)
    // scalastyle:on println
    System.exit(1)
  }

}