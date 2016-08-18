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

import java.util.concurrent.CountDownLatch

import org.apache.spark.{SecurityManager, SparkConf}
import org.apache.spark.deploy.mesos.ui.MesosClusterUI
import org.apache.spark.deploy.rest.mesos.MesosRestServer
import org.apache.spark.internal.Logging
import org.apache.spark.scheduler.cluster.mesos._
import org.apache.spark.util.{ShutdownHookManager, Utils}

/*
 * A dispatcher that is responsible for managing and launching drivers, and is intended to be
 * used for Mesos cluster mode. The dispatcher is a long-running process started by the user in
 * the cluster independently of Spark applications.
 * It contains a [[MesosRestServer]] that listens for requests to submit drivers and a
 * [[MesosClusterScheduler]] that processes these requests by negotiating with the Mesos master
 * for resources.
 *
 * A typical new driver lifecycle is the following:
 * - Driver submitted via spark-submit talking to the [[MesosRestServer]]
 * - [[MesosRestServer]] queues the driver request to [[MesosClusterScheduler]]
 * - [[MesosClusterScheduler]] gets resource offers and launches the drivers that are in queue
 *
 * This dispatcher supports both Mesos fine-grain or coarse-grain mode as the mode is configurable
 * per driver launched.
 * This class is needed since Mesos doesn't manage frameworks, so the dispatcher acts as
 * a daemon to launch drivers as Mesos frameworks upon request. The dispatcher is also started and
 * stopped by sbin/start-mesos-dispatcher and sbin/stop-mesos-dispatcher respectively.
 */
private[mesos] class MesosClusterDispatcher(
    args: MesosClusterDispatcherArguments,
    conf: SparkConf)
  extends Logging {

  private val publicAddress = Option(conf.getenv("SPARK_PUBLIC_DNS")).getOrElse(args.host)
  private val recoveryMode = conf.get("spark.deploy.recoveryMode", "NONE").toUpperCase()
  logInfo("Recovery mode in Mesos dispatcher set to: " + recoveryMode)

  private val engineFactory = recoveryMode match {
    case "NONE" => new BlackHoleMesosClusterPersistenceEngineFactory
    case "ZOOKEEPER" => new ZookeeperMesosClusterPersistenceEngineFactory(conf)
    case _ => throw new IllegalArgumentException("Unsupported recovery mode: " + recoveryMode)
  }

  private val scheduler = new MesosClusterScheduler(engineFactory, conf)

  private val server = new MesosRestServer(args.host, args.port, conf, scheduler)
  private val webUi = new MesosClusterUI(
    new SecurityManager(conf),
    args.webUiPort,
    conf,
    publicAddress,
    scheduler)

  private val shutdownLatch = new CountDownLatch(1)

  def start(): Unit = {
    webUi.bind()
    scheduler.frameworkUrl = conf.get("spark.mesos.dispatcher.webui.url", webUi.activeWebUiUrl)
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

private[mesos] object MesosClusterDispatcher extends Logging {
  def main(args: Array[String]) {
    Utils.initDaemon(log)
    val conf = new SparkConf
    val dispatcherArgs = new MesosClusterDispatcherArguments(args, conf)
    conf.setMaster(dispatcherArgs.masterUrl)
    conf.setAppName(dispatcherArgs.name)
    dispatcherArgs.zookeeperUrl.foreach { z =>
      conf.set("spark.deploy.recoveryMode", "ZOOKEEPER")
      conf.set("spark.deploy.zookeeper.url", z)
    }
    val dispatcher = new MesosClusterDispatcher(dispatcherArgs, conf)
    dispatcher.start()
    logDebug("Adding shutdown hook") // force eager creation of logger
    ShutdownHookManager.addShutdownHook { () =>
      logInfo("Shutdown hook is shutting down dispatcher")
      dispatcher.stop()
      dispatcher.awaitShutdown()
    }
    dispatcher.awaitShutdown()
  }
}
