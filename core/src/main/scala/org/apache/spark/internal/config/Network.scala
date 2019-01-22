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

package org.apache.spark.internal.config

import java.util.concurrent.TimeUnit

private[spark] object Network {

  private[spark] val NETWORK_CRYPTO_SASL_FALLBACK =
    ConfigBuilder("spark.network.crypto.saslFallback")
      .booleanConf
      .createWithDefault(true)

  private[spark] val NETWORK_CRYPTO_ENABLED =
    ConfigBuilder("spark.network.crypto.enabled")
      .booleanConf
      .createWithDefault(false)

  private[spark] val NETWORK_REMOTE_READ_NIO_BUFFER_CONVERSION =
    ConfigBuilder("spark.network.remoteReadNioBufferConversion")
      .booleanConf
      .createWithDefault(false)

  private[spark] val NETWORK_TIMEOUT =
    ConfigBuilder("spark.network.timeout")
      .timeConf(TimeUnit.SECONDS)
      .createWithDefaultString("120s")

  private[spark] val NETWORK_TIMEOUT_INTERVAL =
    ConfigBuilder("spark.network.timeoutInterval")
      .timeConf(TimeUnit.MILLISECONDS)
      .createWithDefaultString(STORAGE_BLOCKMANAGER_TIMEOUTINTERVAL.defaultValueString)

  private[spark] val RPC_ASK_TIMEOUT =
    ConfigBuilder("spark.rpc.askTimeout")
      .stringConf
      .createOptional

  private[spark] val RPC_CONNECT_THREADS =
    ConfigBuilder("spark.rpc.connect.threads")
      .intConf
      .createWithDefault(64)

  private[spark] val RPC_IO_NUM_CONNECTIONS_PER_PEER =
    ConfigBuilder("spark.rpc.io.numConnectionsPerPeer")
      .intConf
      .createWithDefault(1)

  private[spark] val RPC_IO_THREADS =
    ConfigBuilder("spark.rpc.io.threads")
      .intConf
      .createOptional

  private[spark] val RPC_LOOKUP_TIMEOUT =
    ConfigBuilder("spark.rpc.lookupTimeout")
      .stringConf
      .createOptional

  private[spark] val RPC_MESSAGE_MAX_SIZE =
    ConfigBuilder("spark.rpc.message.maxSize")
      .intConf
      .createWithDefault(128)

  private[spark] val RPC_NETTY_DISPATCHER_NUM_THREADS =
    ConfigBuilder("spark.rpc.netty.dispatcher.numThreads")
      .intConf
      .createOptional

  private[spark] val RPC_NUM_RETRIES =
    ConfigBuilder("spark.rpc.numRetries")
      .intConf
      .createWithDefault(3)

  private[spark] val RPC_RETRY_WAIT =
    ConfigBuilder("spark.rpc.retry.wait")
      .timeConf(TimeUnit.MILLISECONDS)
      .createWithDefaultString("3s")
}
