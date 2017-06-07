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

import java.util.concurrent.CountDownLatch

import scala.collection.JavaConverters._

import org.apache.spark.{SecurityManager, SparkConf}
import org.apache.spark.internal.Logging
import org.apache.spark.metrics.MetricsSystem
import org.apache.spark.network.TransportContext
import org.apache.spark.network.crypto.AuthServerBootstrap
import org.apache.spark.network.netty.SparkTransportConf
import org.apache.spark.network.server.{TransportServer, TransportServerBootstrap}
import org.apache.spark.network.shuffle.ExternalShuffleBlockHandler
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
    MetricsSystem.createMetricsSystem("shuffleService", sparkConf, securityManager)

  private val enabled = sparkConf.getBoolean("spark.shuffle.service.enabled", false)
  private val port = sparkConf.getInt("spark.shuffle.service.port", 7337)

  private val transportConf =
    SparkTransportConf.fromSparkConf(sparkConf, "shuffle", numUsableCores = 0)
  private val blockHandler = newShuffleBlockHandler(transportConf)
  private val transportContext: TransportContext =
    new TransportContext(transportConf, blockHandler, true)

  private var server: TransportServer = _

  private val shuffleServiceSource = new ExternalShuffleServiceSource(blockHandler)

  /** Create a new shuffle block handler. Factored out for subclasses to override. */
  protected def newShuffleBlockHandler(conf: TransportConf): ExternalShuffleBlockHandler = {
    new ExternalShuffleBlockHandler(conf, null)
  }

  /** Starts the external shuffle service if the user has configured us to. */
  def startIfEnabled() {
    if (enabled) {
      start()
    }
  }

  /** Start the external shuffle service */
  def start() {
    require(server == null, "Shuffle server already started")
    val authEnabled = securityManager.isAuthenticationEnabled()
    logInfo(s"Starting shuffle service on port $port (auth enabled = $authEnabled)")
    val bootstraps: Seq[TransportServerBootstrap] =
      if (authEnabled) {
        Seq(new AuthServerBootstrap(transportConf, securityManager))
      } else {
        Nil
      }
    server = transportContext.createServer(port, bootstraps.asJava)

    masterMetricsSystem.registerSource(shuffleServiceSource)
    masterMetricsSystem.start()
  }

  /** Clean up all shuffle files associated with an application that has exited. */
  def applicationRemoved(appId: String): Unit = {
    blockHandler.applicationRemoved(appId, true /* cleanupLocalDirs */)
  }

  def stop() {
    if (server != null) {
      server.close()
      server = null
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
    sparkConf.set("spark.shuffle.service.enabled", "true")
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
