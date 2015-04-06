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

package org.apache.spark.deploy.mesos

import java.io.File
import java.util.concurrent.CountDownLatch

import org.apache.spark
import org.apache.spark.deploy.mesos.MesosClusterDispatcher.ClusterDispatcherArguments
import org.apache.spark.deploy.mesos.ui.MesosClusterUI
import org.apache.spark.deploy.rest.mesos.MesosRestServer
import org.apache.spark.scheduler.cluster.mesos._
import org.apache.spark.util.{IntParam, SignalLogger, Utils}
import org.apache.spark.{Logging, SecurityManager, SparkConf}

/*
 * A dispatcher that is responsible for managing and launching drivers, and is intended to
 * be used for Mesos cluster mode. The dispatcher is launched by the user in the cluster,
 * which it launches a [[MesosRestServer]] for listening for driver requests, and launches a
 * [[MesosClusterScheduler]] to launch these drivers in the Mesos cluster.
 *
 * A typical new driver lifecycle is the following:
 *
 * - Driver submitted via spark-submit talking to the [[MesosRestServer]]
 * - [[MesosRestServer]] queues the driver request to [[MesosClusterScheduler]]
 * - [[MesosClusterScheduler]] gets resource offers and launches the drivers that are in queue
 *
 * This dispatcher supports both Mesos fine-grain or coarse-grain mode as the mode is configurable
 * per driver launched.
 * This class is needed since Mesos doesn't manage frameworks, so the dispatcher acts as
 * a daemon to launch drivers as Mesos frameworks upon request.
 */
private[mesos] class MesosClusterDispatcher(
    args: ClusterDispatcherArguments,
    conf: SparkConf)
  extends Logging {

  private def publicAddress(conf: SparkConf, host: String): String = {
    val envVar = conf.getenv("SPARK_PUBLIC_DNS")
    if (envVar != null) envVar else host
  }

  private val recoveryMode = conf.get("spark.deploy.recoveryMode", "NONE").toUpperCase()
  logInfo("Recovery mode in Mesos dispatcher set to: " + recoveryMode)

  private val engineFactory = recoveryMode match {
    case "NONE" => new BlackHoleMesosClusterPersistenceEngineFactory
    case "ZOOKEEPER" => new ZookeeperMesosClusterPersistenceEngineFactory(conf)
  }

  private val scheduler = new MesosClusterSchedulerDriver(engineFactory, conf)

  private val server = new MesosRestServer(args.host, args.port, conf, scheduler)
  private val webUi = new MesosClusterUI(
    new SecurityManager(conf),
    args.webUiPort,
    conf,
    publicAddress(conf, args.host),
    scheduler)

  private val shutdownLatch = new CountDownLatch(1)
  private val sparkHome = new File(Option(conf.getenv("SPARK_HOME")).getOrElse("."))

  def start(): Unit = {
    webUi.bind()
    scheduler.frameworkUrl = webUi.activeWebUiUrl
    scheduler.start()
    server.start()
  }

  def awaitShutdown(): Unit = {
    shutdownLatch.await()
  }

  def stop(): Unit = {
    webUi.stop()
    server.stop()
    scheduler.stop()
    shutdownLatch.countDown()
  }
}

private[mesos] object MesosClusterDispatcher extends spark.Logging {
  def main(args: Array[String]) {
    SignalLogger.register(log)

    val conf = new SparkConf
    val dispatcherArgs = new ClusterDispatcherArguments(args, conf)

    conf.setMaster(dispatcherArgs.masterUrl)
    conf.setAppName("Spark Cluster")

    dispatcherArgs.zookeeperUrl.foreach { z =>
      conf.set("spark.deploy.recoveryMode", "ZOOKEEPER")
      conf.set("spark.deploy.zookeeper.url", z)
    }

    val dispatcher = new MesosClusterDispatcher(
      dispatcherArgs,
      conf)

    dispatcher.start()

    val shutdownHook = new Thread() {
      override def run() {
        logInfo("Shutdown hook is shutting down dispatcher")
        dispatcher.stop()
        dispatcher.awaitShutdown()
      }
    }

    Runtime.getRuntime.addShutdownHook(shutdownHook)

    dispatcher.awaitShutdown()
  }

  private class ClusterDispatcherArguments(args: Array[String], conf: SparkConf) {
    var host = Utils.localHostName()
    var port = 7077
    var webUiPort = 8081
    var masterUrl: String = _
    var zookeeperUrl: Option[String] = None
    var propertiesFile: String = _

    parse(args.toList)

    propertiesFile = Utils.loadDefaultSparkProperties(conf, propertiesFile)

    private def parse(args: List[String]): Unit = args match {
      case ("--host" | "-h") :: value :: tail =>
        Utils.checkHost(value, "Please use hostname " + value)
        host = value
        parse(tail)

      case ("--port" | "-p") :: IntParam(value) :: tail =>
        port = value
        parse(tail)

      case ("--webui-port" | "-p") :: IntParam(value) :: tail =>
        webUiPort = value
        parse(tail)

      case ("--zk" | "-z") :: value :: tail =>
        zookeeperUrl = Some(value)
        parse(tail)

      case ("--master" | "-m") :: value :: tail =>
        if (!value.startsWith("mesos://")) {
          System.err.println("Cluster dispatcher only supports mesos (uri begins with mesos://)")
          System.exit(1)
        }
        masterUrl = value.stripPrefix("mesos://")
        parse(tail)

      case ("--properties-file") :: value :: tail =>
        propertiesFile = value
        parse(tail)

      case ("--help") :: tail =>
        printUsageAndExit(0)

      case Nil => {
        if (masterUrl == null) {
          System.err.println("--master is required")
          System.exit(1)
        }
      }

      case _ =>
        printUsageAndExit(1)
    }

    /**
     * Print usage and exit JVM with the given exit code.
     */
    def printUsageAndExit(exitCode: Int): Unit = {
      System.err.println(
        "Usage: MesosClusterDispatcher [options]\n" +
          "\n" +
          "Options:\n" +
          "  -h HOST, --host HOST    Hostname to listen on\n" +
          "  -p PORT, --port PORT    Port to listen on (default: 7077)\n" +
          "  --webui-port WEBUI_PORT WebUI Port to listen on (default: 8081)\n" +
          "  -m --master MASTER      URI for connecting to Mesos master\n" +
          "  -z --zk ZOOKEEPER       Comma delimited URLs for connecting to \n" +
          "                          Zookeeper for persistence\n" +
          "  --properties-file FILE  Path to a custom Spark properties file.\n" +
          "                          Default is conf/spark-defaults.conf.")
      System.exit(exitCode)
    }
  }
}
