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

package org.apache.spark.executor

import java.nio.ByteBuffer

import org.apache.spark.rpc.{AkkaRpcEnv, RpcEnv, RpcEndPointRef, RpcEndPoint}

import scala.concurrent.Await

import akka.actor.{ActorSystem, Props}
import akka.pattern.Patterns

import org.apache.spark.{Logging, SecurityManager, SparkConf, SparkEnv}
import org.apache.spark.TaskState.TaskState
import org.apache.spark.deploy.SparkHadoopUtil
import org.apache.spark.deploy.worker.WorkerWatcher
import org.apache.spark.scheduler.TaskDescription
import org.apache.spark.scheduler.cluster.CoarseGrainedClusterMessages._
import org.apache.spark.util.{AkkaUtils, SignalLogger, Utils}

private[spark] class CoarseGrainedExecutorBackend(
    driverUrl: String,
    executorId: String,
    hostPort: String,
    cores: Int,
    sparkProperties: Seq[(String, String)],
    actorSystem: ActorSystem)
  extends RpcEndPoint with ExecutorBackend with Logging {

  Utils.checkHostPort(hostPort, "Expected hostport")

  var executor: Executor = null
  var driver: RpcEndPointRef = null

  preStart()

  def preStart() {
    logInfo("Connecting to driver: " + driverUrl)
    val rpcEnv = new AkkaRpcEnv(actorSystem, conf)
    driver = rpcEnv.setupEndPointRefByUrl(driverUrl)
    driver.send(RegisterExecutor(executorId, hostPort, cores))
  }

  override def remoteConnectionTerminated(remoteAddress: String): Unit = {
    logError(s"Driver $remoteAddress disassociated! Shutting down.")
    System.exit(1)
  }

  override def receive(sender: RpcEndPointRef, message: Any): Unit = message match {
    case RegisteredExecutor =>
      logInfo("Successfully registered with driver")
      val (hostname, _) = Utils.parseHostPort(hostPort)
      executor = new Executor(executorId, hostname, sparkProperties, cores, isLocal = false,
        actorSystem)

    case RegisterExecutorFailed(failureMessage) =>
      logError("Slave registration failed: " + failureMessage)
      System.exit(1)

    case LaunchTask(data) =>
      if (executor == null) {
        logError("Received LaunchTask command but executor was null")
        System.exit(1)
      } else {
        val ser = SparkEnv.get.closureSerializer.newInstance()
        val taskDesc = ser.deserialize[TaskDescription](data.value)
        logInfo("Got assigned task " + taskDesc.taskId)
        executor.launchTask(this, taskDesc.taskId, taskDesc.name, taskDesc.serializedTask)
      }

    case KillTask(taskId, _, interruptThread) =>
      if (executor == null) {
        logError("Received KillTask command but executor was null")
        System.exit(1)
      } else {
        executor.killTask(taskId, interruptThread)
      }

    case StopExecutor =>
      logInfo("Driver commanded a shutdown")
      executor.stop()
      //rpcEnv.stop(this)
      // TODO(rxin): Stop this properly.
  }

  override def statusUpdate(taskId: Long, state: TaskState, data: ByteBuffer) {
    driver.send(StatusUpdate(executorId, taskId, state, data))
  }
}

private[spark] object CoarseGrainedExecutorBackend extends Logging {

  private def run(
      driverUrl: String,
      executorId: String,
      hostname: String,
      cores: Int,
      appId: String,
      workerUrl: Option[String]) {

    SignalLogger.register(log)

    SparkHadoopUtil.get.runAsSparkUser { () =>
      // Debug code
      Utils.checkHost(hostname)

      // Bootstrap to fetch the driver's Spark properties.
      val executorConf = new SparkConf
      val port = executorConf.getInt("spark.executor.port", 0)
      val (fetcher, _) = AkkaUtils.createActorSystem(
        "driverPropsFetcher", hostname, port, executorConf, new SecurityManager(executorConf))
      val driver = fetcher.actorSelection(driverUrl)
      val timeout = AkkaUtils.askTimeout(executorConf)
      val fut = Patterns.ask(driver, RetrieveSparkProps, timeout)
      val props = Await.result(fut, timeout).asInstanceOf[Seq[(String, String)]] ++
        Seq[(String, String)](("spark.app.id", appId))
      fetcher.shutdown()

      // Create a new ActorSystem using driver's Spark properties to run the backend.
      val driverConf = new SparkConf().setAll(props)
      val (actorSystem, boundPort) = AkkaUtils.createActorSystem(
        SparkEnv.executorActorSystemName,
        hostname, port, driverConf, new SecurityManager(driverConf))
      // set it
      val sparkHostPort = hostname + ":" + boundPort
      actorSystem.actorOf(
        Props(classOf[CoarseGrainedExecutorBackend],
          driverUrl, executorId, sparkHostPort, cores, props, actorSystem),
        name = "Executor")
      workerUrl.foreach { url =>
        actorSystem.actorOf(Props(classOf[WorkerWatcher], url), name = "WorkerWatcher")
      }
      actorSystem.awaitTermination()
    }
  }

  def main(args: Array[String]) {
    args.length match {
      case x if x < 5 =>
        System.err.println(
          // Worker url is used in spark standalone mode to enforce fate-sharing with worker
          "Usage: CoarseGrainedExecutorBackend <driverUrl> <executorId> <hostname> " +
          "<cores> <appid> [<workerUrl>] ")
        System.exit(1)

      // NB: These arguments are provided by SparkDeploySchedulerBackend (for standalone mode)
      // and CoarseMesosSchedulerBackend (for mesos mode).
      case 5 =>
        run(args(0), args(1), args(2), args(3).toInt, args(4), None)
      case x if x > 5 =>
        run(args(0), args(1), args(2), args(3).toInt, args(4), Some(args(5)))
    }
  }
}
