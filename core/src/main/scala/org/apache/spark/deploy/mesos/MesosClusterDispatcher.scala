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

import org.apache.spark.{Logging, SecurityManager, SparkConf}
import org.apache.spark.util.{SignalLogger, IntParam, Utils}

import java.io.File
import java.util.concurrent.{TimeUnit, CountDownLatch}

import org.apache.spark.deploy.mesos.ui.MesosClusterUI
import org.apache.spark.deploy.rest.MesosRestServer
import org.apache.spark.scheduler.cluster.mesos._
import org.apache.spark.deploy.mesos.MesosClusterDispatcher.ClusterDispatcherArguments
import org.apache.mesos.state.{ZooKeeperState, InMemoryState}

/*
 * A dispatcher actor that is responsible for managing drivers, that is intended to
 * used for Mesos cluster mode.
 * This class is needed since Mesos doesn't manage frameworks, so the dispatcher acts as
 * a daemon to launch drivers as Mesos frameworks upon request.
 */
private [spark] class MesosClusterDispatcher(
    args: ClusterDispatcherArguments,
    conf: SparkConf) extends Logging {

  def dispatcherPublicAddress(conf: SparkConf, host: String): String = {
    val envVar = conf.getenv("SPARK_PUBLIC_DNS")
    if (envVar != null) envVar else host
  }

  val recoveryMode = conf.get("spark.deploy.recoveryMode", "NONE").toUpperCase()
  logInfo("Recovery mode in Mesos dispatcher set to: " + recoveryMode)

  val engineFactory = recoveryMode match {
    case "NONE" => new BlackHolePersistenceEngineFactory
    case "ZOOKEEPER" => {
      new ZookeeperClusterPersistenceEngineFactory(conf)
    }
  }

  val scheduler = new MesosClusterScheduler(
    engineFactory,
    conf)

  val server = new MesosRestServer(args.host, args.port, conf, scheduler)

  val webUi = new MesosClusterUI(
    new SecurityManager(conf),
    args.webUiPort,
    conf,
    dispatcherPublicAddress(conf, args.host),
    scheduler)

  val shutdownLatch = new CountDownLatch(1)

  val sparkHome =
    new File(sys.env.get("SPARK_HOME").getOrElse("."))

  def start() {
    webUi.bind()
    scheduler.frameworkUrl = webUi.activeWebUiUrl
    scheduler.start()
    server.start()
  }

  def awaitShutdown() {
    shutdownLatch.await()
  }

  def stop() {
    webUi.stop()
    server.stop()
    scheduler.stop()
    shutdownLatch.countDown()
  }
}

private[mesos] object MesosClusterDispatcher extends Logging {
  def main(args: Array[String]) {
    SignalLogger.register(log)

    val conf = new SparkConf
    val dispatcherArgs = new ClusterDispatcherArguments(args, conf)

    conf.setMaster(dispatcherArgs.masterUrl)
    conf.setAppName("Mesos Cluster Dispatcher")

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

  class ClusterDispatcherArguments(args: Array[String], conf: SparkConf) {
    var host = Utils.localHostName()
    var port = 7077
    var webUiPort = 8081
    var masterUrl: String = _
    var propertiesFile: String = _

    parse(args.toList)

    propertiesFile = Utils.loadDefaultSparkProperties(conf, propertiesFile)

    def parse(args: List[String]): Unit = args match {
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
    def printUsageAndExit(exitCode: Int) {
      System.err.println(
        "Usage: MesosClusterDispatcher [options]\n" +
          "\n" +
          "Options:\n" +
          "  -h HOST, --host HOST   Hostname to listen on\n" +
          "  -p PORT, --port PORT   Port to listen on (default: 7077)\n" +
          "  --webui-port WEBUI_PORT   WebUI Port to listen on (default: 8081)\n" +
          "  -m --master MASTER      URI for connecting to Mesos master\n" +
          "  --properties-file FILE Path to a custom Spark properties file.\n" +
          "                         Default is conf/spark-defaults.conf.")
      System.exit(exitCode)
    }
  }
}
