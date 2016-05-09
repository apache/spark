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

package org.apache.spark.ps

import java.net.URL
import java.nio.ByteBuffer

import scala.concurrent.Await
import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer

import akka.pattern.Patterns
import akka.remote.RemotingLifecycleEvent
import akka.actor.{Props, ActorSelection, Actor}

import org.apache.spark.executor.ExecutorBackend
import org.apache.spark.deploy.SparkHadoopUtil
import org.apache.spark.ps.CoarseGrainedParameterServerMessage._
import org.apache.spark.{SecurityManager, SparkConf, SparkEnv, Logging}
import org.apache.spark.scheduler.cluster.CoarseGrainedClusterMessages._
import org.apache.spark.TaskState._
import org.apache.spark.util._

/**
 * `parameter server` ServerBackend
 */
private[spark] class CoarseGrainedParameterServerBackend(
    driverUrl: String,
    executorId: String,
    hostPort: String,
    executorVCores: Int,
    userClassPath: Seq[URL],
    env: SparkEnv)
  extends Actor with ActorLogReceive with ExecutorBackend with Logging {

  Utils.checkHostPort(hostPort, "Expected hostport")

  var driver: ActorSelection = null

  var psServer: PSServer = null

  override def preStart() {
    logInfo("Connecting to driver: " + driverUrl)
    driver = context.actorSelection(driverUrl)
    driver ! RegisterServer(executorId, hostPort, executorVCores,
      System.getProperty("spark.yarn.container.id", ""))
    context.system.eventStream.subscribe(self, classOf[RemotingLifecycleEvent])
  }

  override def receiveWithLogging = {
    case SetParameter(key: String, value: Array[Double], clock: Int) =>
      logInfo(s"set $key to $value")
      val success: Boolean = psServer.setParameter[String, Array[Double]](key, value, clock)
      sender ! success

    case GetParameter(key: String, clock: Int) =>
      logInfo(s"request $key")
      val (success, value) = psServer.getParameter[String, Array[Double]](key, clock)
      logInfo(s"get parameter: $value")
      sender ! Parameter(success, value)

    case UpdateParameter(key: String, value: Array[Double], clock: Int) =>
      logInfo(s"update $key")
      val success: Boolean = psServer.updateParameter[String, Array[Double]](key, value, clock)
      sender ! success

    case RegisteredServer(serverId: Long) =>
      logInfo(s"registered server with serverId: $serverId")
      val sparkConf = env.conf
      val agg = (deltaKVs: ArrayBuffer[Array[Double]])
            => {
        val size = deltaKVs.size
        deltaKVs.reduce((a, b) => a.zip(b).map(e => {
          e._1 + e._2
        })).map(e => e / size)
      }
      val func = (arr1: Array[Double], arr2: Array[Double]) => arr1.zip(arr2).map(e => e._1 + e._2)
      psServer = new PSServer(context, sparkConf, serverId, agg, func)

    case UpdateClock(clientId: String, clock: Int) =>
      logInfo(s"update clock $clock from client $clientId")
      val pause: Boolean = psServer.updateClock(clientId, clock)
      sender ! pause

    case InitPSClient(clientId: String) =>
      logInfo(s"client $clientId is coming.")
      psServer.initPSClient(clientId)

    case NotifyServer(executorUrl: String) =>
      println("notify server: " + executorUrl)
      psServer.addPSClient(executorUrl)

  }

  override def statusUpdate(taskId: Long, state: TaskState, data: ByteBuffer) {

  }

  override def getPSClient: Option[PSClient] = None

}

object CoarseGrainedParameterServerBackend extends Logging {

  private def run(
      driverUrl: String,
      executorId: String,
      hostname: String,
      cores: Int,
      appId: String,
      workerUrl: Option[String],
      userClassPath: Seq[URL]) {

    SignalLogger.register(log)

    SparkHadoopUtil.get.runAsSparkUser { () =>
    // Debug code
      Utils.checkHost(hostname)

      // Bootstrap to fetch the driver's Spark properties.
      val executorConf = new SparkConf
      val port = executorConf.getInt("spark.executor.port", 0)
      val (fetcher, _) = AkkaUtils.createActorSystem(
        "driverPropsFetcher",
        hostname,
        port,
        executorConf,
        new SecurityManager(executorConf))
      val driver = fetcher.actorSelection(driverUrl)
      val timeout = AkkaUtils.askTimeout(executorConf)
      val fut = Patterns.ask(driver, RetrieveSparkProps, timeout)
      val props = Await.result(fut, timeout).asInstanceOf[Seq[(String, String)]] ++
        Seq[(String, String)](("spark.app.id", appId))
      fetcher.shutdown()

      // Create SparkEnv using properties we fetched from the driver.
      val driverConf = new SparkConf()
      val executorVCores = cores * driverConf.get("spark.cores.ratio", "1").toInt
      for ((key, value) <- props) {
        // this is required for SSL in standalone mode
        if (SparkConf.isExecutorStartupConf(key)) {
          driverConf.setIfMissing(key, value)
        } else {
          driverConf.set(key, value)
        }
      }
      val env = SparkEnv.createExecutorEnv(
        driverConf, executorId, hostname, port, executorVCores, isLocal = false)

      // SparkEnv sets spark.driver.port so it shouldn't be 0 anymore.
      val boundPort = env.conf.getInt("spark.executor.port", 0)
      assert(boundPort != 0)

      // Start the CoarseGrainedExecutorBackend actor.
      val sparkHostPort = hostname + ":" + boundPort
      env.actorSystem.actorOf(
        Props(classOf[CoarseGrainedParameterServerBackend],
          driverUrl, executorId, sparkHostPort, executorVCores, userClassPath, env),
        name = "Server")

      env.actorSystem.awaitTermination()
    }
  }

  def main(args: Array[String]) {
    var driverUrl: String = null
    var executorId: String = null
    var hostname: String = null
    var cores: Int = 0
    var appId: String = null
    var workerUrl: Option[String] = None
    val userClassPath = new mutable.ListBuffer[URL]()

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
        case ("--worker-url") :: value :: tail =>
          // Worker url is used in spark standalone mode to enforce fate-sharing with worker
          workerUrl = Some(value)
          argv = tail
        case ("--user-class-path") :: value :: tail =>
          userClassPath += new URL(value)
          argv = tail
        case Nil =>
        case tail =>
          System.err.println(s"Unrecognized options: ${tail.mkString(" ")}")
          printUsageAndExit()
      }
    }

    if (driverUrl == null || executorId == null || hostname == null || cores <= 0 ||
      appId == null) {
      printUsageAndExit()
    }

    run(driverUrl, executorId, hostname, cores, appId, workerUrl, userClassPath)
  }

  private def printUsageAndExit() = {
    System.err.println(
      """
        |"Usage: CoarseGrainedParameterServerBackend [options]
        |
        | Options are:
        |   --driver-url <driverUrl>
        |   --executor-id <executorId>
        |   --hostname <hostname>
        |   --cores <cores>
        |   --app-id <appid>
        |   --worker-url <workerUrl>
        |   --user-class-path <url>
        |""".stripMargin)
    System.exit(1)
  }
}
