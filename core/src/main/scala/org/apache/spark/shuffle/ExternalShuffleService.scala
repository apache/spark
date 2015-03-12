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

package org.apache.spark.shuffle

import org.apache.spark.SparkConf
import org.apache.spark.SecurityManager
import org.apache.spark.network.netty.SparkTransportConf
import org.apache.spark.network.shuffle.ExternalShuffleBlockHandler
import org.apache.spark.network.TransportContext
import org.apache.spark.network.sasl.SaslRpcHandler
import org.apache.spark.network.server.TransportServer
import org.apache.spark.Logging
import org.apache.spark.util.Utils
import sun.misc.Signal
import sun.misc.SignalHandler
import java.util.concurrent.CountDownLatch

/**
 * A main class for runing the external shuffle service. This follows
 * closely how [[org.apache.spark.deploy.worker.StandaloneWorkerShuffleService]] works.
 *
 * @note This adds handlers for SIGINT and SIGTERM to terminate gracefully.
 */
object ExternalShuffleService extends Logging {
  @volatile
  private var server: TransportServer = _

  private val barrier = new CountDownLatch(1)

  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf
    Utils.loadDefaultSparkProperties(sparkConf)
    val securityManager = new SecurityManager(sparkConf)

    val port = sparkConf.getInt("spark.shuffle.service.port", 7337)
    val useSasl = securityManager.isAuthenticationEnabled()

    val transportConf = SparkTransportConf.fromSparkConf(sparkConf, numUsableCores = 0)
    val blockHandler = new ExternalShuffleBlockHandler(transportConf)

    val transportContext: TransportContext = {
      val handler = if (useSasl) new SaslRpcHandler(blockHandler, securityManager) else blockHandler
      new TransportContext(transportConf, handler)
    }

    installSignalHandlers()

    logInfo(s"Starting external shuffle service on port $port (authentication: $useSasl)")
    server = transportContext.createServer(port)

    // keep running until the process is terminated by SIGTERM or SIGINT
    barrier.await()
  }

  private def installSignalHandlers() = {
    new SigIntHandler("INT")
    new SigIntHandler("TERM")
  }

  class SigIntHandler(signal: String) extends SignalHandler {
    val prevHandler = Signal.handle(new Signal(signal), this)

    override def handle(sig: Signal): Unit = {
      if (server ne null) {
        logInfo("Shutting down shuffle service.")
        server.close()
        prevHandler.handle(sig)
        barrier.countDown()
      }
    }
  }
}
