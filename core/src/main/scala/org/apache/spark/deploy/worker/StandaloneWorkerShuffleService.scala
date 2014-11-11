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

package org.apache.spark.deploy.worker

import org.apache.spark.{Logging, SparkConf, SecurityManager}
import org.apache.spark.network.TransportContext
import org.apache.spark.network.netty.SparkTransportConf
import org.apache.spark.network.sasl.SaslRpcHandler
import org.apache.spark.network.server.TransportServer
import org.apache.spark.network.shuffle.ExternalShuffleBlockHandler

/**
 * Provides a server from which Executors can read shuffle files (rather than reading directly from
 * each other), to provide uninterrupted access to the files in the face of executors being turned
 * off or killed.
 *
 * Optionally requires SASL authentication in order to read. See [[SecurityManager]].
 */
private[worker]
class StandaloneWorkerShuffleService(sparkConf: SparkConf, securityManager: SecurityManager)
  extends Logging {

  private val enabled = sparkConf.getBoolean("spark.shuffle.service.enabled", false)
  private val port = sparkConf.getInt("spark.shuffle.service.port", 7337)
  private val useSasl: Boolean = securityManager.isAuthenticationEnabled()

  private val transportConf = SparkTransportConf.fromSparkConf(sparkConf)
  private val blockHandler = new ExternalShuffleBlockHandler(transportConf)
  private val transportContext: TransportContext = {
    val handler = if (useSasl) new SaslRpcHandler(blockHandler, securityManager) else blockHandler
    new TransportContext(transportConf, handler)
  }

  private var server: TransportServer = _

  /** Starts the external shuffle service if the user has configured us to. */
  def startIfEnabled() {
    if (enabled) {
      require(server == null, "Shuffle server already started")
      logInfo(s"Starting shuffle service on port $port with useSasl = $useSasl")
      server = transportContext.createServer(port)
    }
  }

  def stop() {
    if (enabled && server != null) {
      server.close()
      server = null
    }
  }
}
