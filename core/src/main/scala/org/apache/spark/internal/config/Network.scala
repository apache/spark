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
      .version("2.2.0") // SPARK-19139, commit ID: 8f3f73abc1fe62496722476460c174af0250e3fe
      .booleanConf
      .createWithDefault(true)

  private[spark] val NETWORK_CRYPTO_ENABLED =
    ConfigBuilder("spark.network.crypto.enabled")
      .version("2.2.0") // SPARK-19139, commit ID: 8f3f73abc1fe62496722476460c174af0250e3fe
      .booleanConf
      .createWithDefault(false)

  private[spark] val NETWORK_REMOTE_READ_NIO_BUFFER_CONVERSION =
    ConfigBuilder("spark.network.remoteReadNioBufferConversion")
      .version("2.4.0") // SPARK-24307, commit ID: 2c82745686f4456c4d5c84040a431dcb5b6cb60b
      .booleanConf
      .createWithDefault(false)

  private[spark] val NETWORK_TIMEOUT =
    ConfigBuilder("spark.network.timeout")
      .version("1.3.0") // SPARK-4688, commit ID: d3f07fd23cc26a70f44c52e24445974d4885d58a
      .timeConf(TimeUnit.SECONDS)
      .createWithDefaultString("120s")

  private[spark] val NETWORK_TIMEOUT_INTERVAL =
    ConfigBuilder("spark.network.timeoutInterval")
      .version("1.3.2") // SPARK-5529, commit ID: ec196ab1c7569d7ab0a50c9d7338c2835f2c84d5
      .timeConf(TimeUnit.MILLISECONDS)
      .createWithDefaultString(STORAGE_BLOCKMANAGER_TIMEOUTINTERVAL.defaultValueString)

  private[spark] val RPC_ASK_TIMEOUT =
    ConfigBuilder("spark.rpc.askTimeout")
      .version("1.4.0") // SPARK-6490, commit ID: 8136810dfad12008ac300116df7bc8448740f1ae
      .stringConf
      .createOptional

  private[spark] val RPC_CONNECT_THREADS =
    ConfigBuilder("spark.rpc.connect.threads")
      .version("1.6.0") // SPARK-6028, commit ID: 084e4e126211d74a79e8dbd2d0e604dd3c650822
      .intConf
      .createWithDefault(64)

  private[spark] val RPC_IO_NUM_CONNECTIONS_PER_PEER =
    ConfigBuilder("spark.rpc.io.numConnectionsPerPeer")
      .version("1.6.0") // SPARK-10745, commit ID: 34a77679877bc40b58a10ec539a8da00fed7db39
      .intConf
      .createWithDefault(1)

  private[spark] val RPC_IO_THREADS =
    ConfigBuilder("spark.rpc.io.threads")
      .version("1.6.0") // SPARK-6028, commit ID: 084e4e126211d74a79e8dbd2d0e604dd3c650822
      .intConf
      .createOptional

  private[spark] val RPC_LOOKUP_TIMEOUT =
    ConfigBuilder("spark.rpc.lookupTimeout")
      .version("1.4.0") // SPARK-6490, commit ID: 8136810dfad12008ac300116df7bc8448740f1ae
      .stringConf
      .createOptional

  private[spark] val RPC_MESSAGE_MAX_SIZE =
    ConfigBuilder("spark.rpc.message.maxSize")
      .version("2.0.0") // SPARK-7997, commit ID: bc1babd63da4ee56e6d371eb24805a5d714e8295
      .intConf
      .createWithDefault(128)

  private[spark] val RPC_NETTY_DISPATCHER_NUM_THREADS =
    ConfigBuilder("spark.rpc.netty.dispatcher.numThreads")
      .version("1.6.0") // SPARK-11079, commit ID: 1797055dbf1d2fd7714d7c65c8d2efde2f15efc1
      .intConf
      .createOptional

  private[spark] val RPC_NUM_RETRIES =
    ConfigBuilder("spark.rpc.numRetries")
      .version("1.4.0") // SPARK-6490, commit ID: 8136810dfad12008ac300116df7bc8448740f1ae
      .intConf
      .createWithDefault(3)

  private[spark] val RPC_RETRY_WAIT =
    ConfigBuilder("spark.rpc.retry.wait")
      .version("1.4.0") // SPARK-6490, commit ID: 8136810dfad12008ac300116df7bc8448740f1ae
      .timeConf(TimeUnit.MILLISECONDS)
      .createWithDefaultString("3s")
}
