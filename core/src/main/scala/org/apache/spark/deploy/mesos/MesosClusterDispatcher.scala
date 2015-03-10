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
import org.apache.spark.util.{IntParam, Utils}

import java.io.File
import org.apache.spark.deploy.mesos.ui.MesosClusterUI
import org.apache.spark.deploy.rest.MesosRestServer
import org.apache.spark.scheduler.cluster.mesos.{ClusterScheduler, MesosClusterScheduler}

/*
 * A dispatcher actor that is responsible for managing drivers, that is intended to
 * used for Mesos cluster mode.
 * This class is needed since Mesos doesn't manage frameworks, so the dispatcher acts as
 * a daemon to launch drivers as Mesos frameworks upon request.
 */
private [spark] class MesosClusterDispatcher(
    host: String,
    serverPort: Int,
    webUiPort: Int,
    conf: SparkConf,
    scheduler: ClusterScheduler) extends Logging {

  val server = new MesosRestServer(host, serverPort, conf, scheduler)

  val dispatcherPublicAddress = {
    val envVar = System.getenv("SPARK_PUBLIC_DNS")
    if (envVar != null) envVar else host
  }

  val webUi = new MesosClusterUI(
    new SecurityManager(conf), webUiPort, conf, dispatcherPublicAddress, scheduler)

  val sparkHome =
    new File(sys.env.get("SPARK_HOME").getOrElse("."))

  def start() {
    server.start()
    webUi.bind()
  }

  def stop() {
    webUi.stop()
    server.stop()
  }
}

object MesosClusterDispatcher {
  def main(args: Array[String]) {
    val conf = new SparkConf
    val dispatcherArgs = new ClusterDispatcherArguments(args, conf)
    conf.setMaster(dispatcherArgs.masterUrl)
    conf.setAppName("Mesos Cluster Dispatcher")
    val scheduler = new MesosClusterScheduler(conf)
    scheduler.start()
    new MesosClusterDispatcher(
      dispatcherArgs.host,
      dispatcherArgs.port,
      dispatcherArgs.webUiPort,
      conf,
      scheduler).start()
    this.synchronized {
      // block indefinitely
      this.wait() // TODO: bad
    }
  }

  class ClusterDispatcherArguments(args: Array[String], conf: SparkConf) {
    var host = Utils.localHostName()
    var port = 7077
    var webUiPort = 8081
    var masterUrl: String = null

    parse(args.toList)

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
          "  -m --master MASTER      URI for connecting to Mesos master\n")
      System.exit(exitCode)
    }
  }
}
