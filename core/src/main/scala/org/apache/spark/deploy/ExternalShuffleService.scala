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

package org.apache.spark.deploy

import java.io.File
import java.util.concurrent.CountDownLatch

import scala.jdk.CollectionConverters._

import org.apache.spark.{SecurityManager, SparkConf}
import org.apache.spark.internal.{config, Logging, MDC}
import org.apache.spark.internal.LogKeys.{AUTH_ENABLED, PORT, SHUFFLE_DB_BACKEND_KEY, SHUFFLE_DB_BACKEND_NAME}
import org.apache.spark.metrics.{MetricsSystem, MetricsSystemInstances}
import org.apache.spark.network.TransportContext
import org.apache.spark.network.crypto.AuthServerBootstrap
import org.apache.spark.network.netty.SparkTransportConf
import org.apache.spark.network.server.{TransportServer, TransportServerBootstrap}
import org.apache.spark.network.shuffle.ExternalBlockHandler
import org.apache.spark.network.shuffledb.DBBackend
import org.apache.spark.network.util.TransportConf
import org.apache.spark.util.{ShutdownHookManager, Utils}

/**
 * Provides a server from which Executors can read shuffle files (rather than reading directly from
 * each other), to provide uninterrupted access to the files in the face of executors being turned
 * off or killed.
 *
 * Optionally requires SASL authentication in order to read. See [[SecurityManager]].
 */
private[deploy]
class ExternalShuffleService(sparkConf: SparkConf, securityManager: SecurityManager)
  extends Logging {
  protected val masterMetricsSystem =
    MetricsSystem.createMetricsSystem(MetricsSystemInstances.SHUFFLE_SERVICE, sparkConf)

  private val enabled = sparkConf.get(config.SHUFFLE_SERVICE_ENABLED)
  private val port = sparkConf.get(config.SHUFFLE_SERVICE_PORT)

  private val registeredExecutorsDB = "registeredExecutors"

  private val transportConf =
    SparkTransportConf.fromSparkConf(
      sparkConf,
      "shuffle",
      numUsableCores = 0,
      sslOptions = Some(securityManager.getRpcSSLOptions()))
  private val blockHandler = newShuffleBlockHandler(transportConf)
  private var transportContext: TransportContext = _

  private var server: TransportServer = _

  private val shuffleServiceSource = new ExternalShuffleServiceSource

  protected def findRegisteredExecutorsDBFile(dbName: String): File = {
    val localDirs = sparkConf.getOption("spark.local.dir").map(_.split(",")).getOrElse(Array())
    if (localDirs.length >= 1) {
      new File(localDirs.find(new File(_, dbName).exists()).getOrElse(localDirs(0)), dbName)
    } else {
      logWarning(s"'spark.local.dir' should be set first when we use db in " +
        s"ExternalShuffleService. Note that this only affects standalone mode.")
      null
    }
  }

  /** Get blockhandler  */
  def getBlockHandler: ExternalBlockHandler = {
    blockHandler
  }

  /** Create a new shuffle block handler. Factored out for subclasses to override. */
  protected def newShuffleBlockHandler(conf: TransportConf): ExternalBlockHandler = {
    if (sparkConf.get(config.SHUFFLE_SERVICE_DB_ENABLED) && enabled) {
      val shuffleDBName = sparkConf.get(config.SHUFFLE_SERVICE_DB_BACKEND)
      val dbBackend = DBBackend.byName(shuffleDBName)
      logInfo(log"Use ${MDC(SHUFFLE_DB_BACKEND_NAME, dbBackend.name())} as the implementation of " +
        log"${MDC(SHUFFLE_DB_BACKEND_KEY, config.SHUFFLE_SERVICE_DB_BACKEND.key)}")
      new ExternalBlockHandler(conf,
        findRegisteredExecutorsDBFile(dbBackend.fileName(registeredExecutorsDB)))
    } else {
      new ExternalBlockHandler(conf, null)
    }
  }

  /** Starts the external shuffle service if the user has configured us to. */
  def startIfEnabled(): Unit = {
    if (enabled) {
      start()
    }
  }

  /** Start the external shuffle service */
  def start(): Unit = {
    require(server == null, "Shuffle server already started")
    val authEnabled = securityManager.isAuthenticationEnabled()
    logInfo(log"Starting shuffle service on port ${MDC(PORT, port)}" +
      log" (auth enabled = ${MDC(AUTH_ENABLED, authEnabled)})")
    val bootstraps: Seq[TransportServerBootstrap] =
      if (authEnabled) {
        Seq(new AuthServerBootstrap(transportConf, securityManager))
      } else {
        Nil
      }
    transportContext = new TransportContext(transportConf, blockHandler, true)
    server = transportContext.createServer(port, bootstraps.asJava)

    shuffleServiceSource.registerMetricSet(server.getAllMetrics)
    blockHandler.getAllMetrics.getMetrics.put("numRegisteredConnections",
        server.getRegisteredConnections)
    shuffleServiceSource.registerMetricSet(blockHandler.getAllMetrics)
    masterMetricsSystem.registerSource(shuffleServiceSource)
    masterMetricsSystem.start()
  }

  /** Clean up all shuffle files associated with an application that has exited. */
  def applicationRemoved(appId: String): Unit = {
    blockHandler.applicationRemoved(appId, true /* cleanupLocalDirs */)
  }

  /** Clean up all the non-shuffle files associated with an executor that has exited. */
  def executorRemoved(executorId: String, appId: String): Unit = {
    blockHandler.executorRemoved(executorId, appId)
  }

  def stop(): Unit = {
    if (server != null) {
      server.close()
      server = null
    }
    if (transportContext != null) {
      transportContext.close()
      transportContext = null
    }
  }
}

/**
 * A main class for running the external shuffle service.
 */
object ExternalShuffleService extends Logging {
  @volatile
  private var server: ExternalShuffleService = _

  private val barrier = new CountDownLatch(1)

  def main(args: Array[String]): Unit = {
    main(args, (conf: SparkConf, sm: SecurityManager) => new ExternalShuffleService(conf, sm))
  }

  /** A helper main method that allows the caller to call this with a custom shuffle service. */
  private[spark] def main(
      args: Array[String],
      newShuffleService: (SparkConf, SecurityManager) => ExternalShuffleService): Unit = {
    Utils.initDaemon(log)
    val sparkConf = new SparkConf
    Utils.loadDefaultSparkProperties(sparkConf)
    val securityManager = new SecurityManager(sparkConf)

    // we override this value since this service is started from the command line
    // and we assume the user really wants it to be running
    sparkConf.set(config.SHUFFLE_SERVICE_ENABLED.key, "true")
    server = newShuffleService(sparkConf, securityManager)
    server.start()

    logDebug("Adding shutdown hook") // force eager creation of logger
    ShutdownHookManager.addShutdownHook { () =>
      logInfo("Shutting down shuffle service.")
      server.stop()
      barrier.countDown()
    }

    // keep running until the process is terminated
    barrier.await()
  }
}
