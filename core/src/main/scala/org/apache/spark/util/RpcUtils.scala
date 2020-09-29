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

package org.apache.spark.util

import scala.concurrent.duration._

import org.apache.spark.SparkConf
import org.apache.spark.internal.config
import org.apache.spark.internal.config.Network._
import org.apache.spark.rpc.{RpcAddress, RpcEndpointRef, RpcEnv, RpcTimeout}

private[spark] object RpcUtils {

  /**
   * Retrieve a `RpcEndpointRef` which is located in the driver via its name.
   */
  def makeDriverRef(name: String, conf: SparkConf, rpcEnv: RpcEnv): RpcEndpointRef = {
    val driverHost: String = conf.get(config.DRIVER_HOST_ADDRESS.key, "localhost")
    val driverPort: Int = conf.getInt(config.DRIVER_PORT.key, 7077)
    Utils.checkHost(driverHost)
    rpcEnv.setupEndpointRef(RpcAddress(driverHost, driverPort), name)
  }

  /** Returns the configured number of times to retry connecting */
  def numRetries(conf: SparkConf): Int = {
    conf.get(RPC_NUM_RETRIES)
  }

  /** Returns the configured number of milliseconds to wait on each retry */
  def retryWaitMs(conf: SparkConf): Long = {
    conf.get(RPC_RETRY_WAIT)
  }

  /** Returns the default Spark timeout to use for RPC ask operations. */
  def askRpcTimeout(conf: SparkConf): RpcTimeout = {
    RpcTimeout(conf, Seq(RPC_ASK_TIMEOUT.key, NETWORK_TIMEOUT.key), "120s")
  }

  /** Returns the default Spark timeout to use for RPC remote endpoint lookup. */
  def lookupRpcTimeout(conf: SparkConf): RpcTimeout = {
    RpcTimeout(conf, Seq(RPC_LOOKUP_TIMEOUT.key, NETWORK_TIMEOUT.key), "120s")
  }

  /**
   * Infinite timeout is used internally, so there's no timeout configuration property that
   * controls it. Therefore, we use "infinite" without any specific reason as its timeout
   * configuration property. And its timeout property should never be accessed since infinite
   * means we never timeout.
   */
  val INFINITE_TIMEOUT = new RpcTimeout(Long.MaxValue.nanos, "infinite")

  private val MAX_MESSAGE_SIZE_IN_MB = Int.MaxValue / 1024 / 1024

  /** Returns the configured max message size for messages in bytes. */
  def maxMessageSizeBytes(conf: SparkConf): Int = {
    val maxSizeInMB = conf.get(RPC_MESSAGE_MAX_SIZE)
    if (maxSizeInMB > MAX_MESSAGE_SIZE_IN_MB) {
      throw new IllegalArgumentException(
        s"${RPC_MESSAGE_MAX_SIZE.key} should not be greater than $MAX_MESSAGE_SIZE_IN_MB MB")
    }
    maxSizeInMB * 1024 * 1024
  }
}
