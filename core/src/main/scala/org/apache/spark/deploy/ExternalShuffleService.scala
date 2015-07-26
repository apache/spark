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

import scala.collection.JavaConversions._

import org.apache.spark.{Logging, SparkConf, SecurityManager}
import org.apache.spark.network.TransportContext
import org.apache.spark.network.netty.SparkTransportConf
import org.apache.spark.network.sasl.SaslServerBootstrap
import org.apache.spark.network.server.TransportServer
import org.apache.spark.network.shuffle.ExternalShuffleBlockHandler
import org.apache.spark.util.Utils

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

  private val enabled = sparkConf.getBoolean("spark.shuffle.service.enabled", false)
  private val port = sparkConf.getInt("spark.shuffle.service.port", 7337)
  private val useSasl: Boolean = securityManager.isAuthenticationEnabled()

  private val transportConf = SparkTransportConf.fromSparkConf(sparkConf, numUsableCores = 0)
  private val blockHandler = new ExternalShuffleBlockHandler(transportConf)
  private val transportContext: TransportContext = new TransportContext(transportConf, blockHandler)

  private var server: TransportServer = _

  /** Starts the external shuffle service if the user has configured us to. */
  def startIfEnabled() {
    if (enabled) {
      start()
    }
  }

  /** Start the external shuffle service */
  def start() {
    require(server == null, "Shuffle server already started")
    logInfo(s"Starting shuffle service on port $port with useSasl = $useSasl")
    val bootstraps =
      if (useSasl) {
        Seq(new SaslServerBootstrap(transportConf, securityManager))
      } else {
        Nil
      }
    server = transportContext.createServer(port, bootstraps)
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
    val sparkConf = new SparkConf
    Utils.loadDefaultSparkProperties(sparkConf)
    val securityManager = new SecurityManager(sparkConf)

    // we override this value since this service is started from the command line
    // and we assume the user really wants it to be running
    sparkConf.set("spark.shuffle.service.enabled", "true")
    server = new ExternalShuffleService(sparkConf, securityManager)
    server.start()

    installShutdownHook()

    // keep running until the process is terminated
    barrier.await()
  }

  private def installShutdownHook(): Unit = {
    Runtime.getRuntime.addShutdownHook(new Thread("External Shuffle Service shutdown thread") {
      override def run() {
        logInfo("Shutting down shuffle service.")
        server.stop()
        barrier.countDown()
      }
    })
  }
}
