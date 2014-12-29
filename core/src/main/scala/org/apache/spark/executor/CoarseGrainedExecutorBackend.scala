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

import akka.actor.Props
import akka.remote.DisassociatedEvent

import org.apache.spark.{Logging, SecurityManager, SparkConf, SparkEnv}
import org.apache.spark.TaskState.TaskState
import org.apache.spark.deploy.SparkHadoopUtil
import org.apache.spark.deploy.worker.WorkerWatcher
import org.apache.spark.rpc.akka.AkkaRpcEnv
import org.apache.spark.rpc.{RpcEndpoint, RpcEndpointRef}
import org.apache.spark.scheduler.TaskDescription
import org.apache.spark.scheduler.cluster.CoarseGrainedClusterMessages._
import org.apache.spark.util.{AkkaUtils, SignalLogger, Utils}

private[spark] class CoarseGrainedExecutorBackend(
    driverUrl: String,
    executorId: String,
    hostPort: String,
    cores: Int,
    env: SparkEnv)
  extends RpcEndpoint with ExecutorBackend with Logging {

  override val rpcEnv = env.rpcEnv

  Utils.checkHostPort(hostPort, "Expected hostport")

  var executor: Executor = null
  var driver: RpcEndpointRef = _

  override def preStart(): Unit = {
    // self is valid now. So now we can use `send`
    logInfo("Connecting to driver: " + driverUrl)
    driver = rpcEnv.setupEndpointRefByUrl(driverUrl)
    driver.send(RegisterExecutor(executorId, hostPort, cores, self))
  }

  override def receive(sender: RpcEndpointRef) = {
    case RegisteredExecutor =>
      logInfo("Successfully registered with driver")
      val (hostname, _) = Utils.parseHostPort(hostPort)
      executor = new Executor(executorId, hostname, env, isLocal = false)

    case RegisterExecutorFailed(message) =>
      logError("Slave registration failed: " + message)
      System.exit(1)

    case LaunchTask(data) =>
      if (executor == null) {
        logError("Received LaunchTask command but executor was null")
        System.exit(1)
      } else {
        val ser = env.closureSerializer.newInstance()
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

    case x: DisassociatedEvent =>
      logError(s"Driver $x disassociated! Shutting down.")
      System.exit(1)

    case StopExecutor =>
      logInfo("Driver commanded a shutdown")
      executor.stop()
      stop()
      rpcEnv.stopAll()
      env.actorSystem.shutdown()
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
      val rpcEnv = new AkkaRpcEnv(fetcher, executorConf)
      val driver = rpcEnv.setupEndpointRefByUrl(driverUrl)
      val props = driver.askWithReply[Seq[(String, String)]](RetrieveSparkProps) ++
        Seq[(String, String)](("spark.app.id", appId))

      rpcEnv.stopAll()
      fetcher.shutdown()
      fetcher.awaitTermination()

      // Create SparkEnv using properties we fetched from the driver.
      val driverConf = new SparkConf().setAll(props)
      val env = SparkEnv.createExecutorEnv(
        driverConf, executorId, hostname, port, cores, isLocal = false)

      // SparkEnv sets spark.driver.port so it shouldn't be 0 anymore.
      val boundPort = env.conf.getInt("spark.executor.port", 0)
      assert(boundPort != 0)

      // Start the CoarseGrainedExecutorBackend actor.
      val sparkHostPort = hostname + ":" + boundPort
      env.rpcEnv.setupEndpoint("Executor", new CoarseGrainedExecutorBackend(
          driverUrl, executorId, sparkHostPort, cores, env))
      workerUrl.foreach { url =>
        env.actorSystem.actorOf(Props(classOf[WorkerWatcher], url), name = "WorkerWatcher")
      }
      env.actorSystem.awaitTermination()
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
