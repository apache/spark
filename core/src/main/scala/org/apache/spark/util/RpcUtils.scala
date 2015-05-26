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
import scala.language.postfixOps

import org.apache.spark.{SparkEnv, SparkConf}
import org.apache.spark.rpc.{RpcAddress, RpcEndpointRef, RpcEnv}

object RpcUtils {

  /**
   * Retrieve a [[RpcEndpointRef]] which is located in the driver via its name.
   */
  def makeDriverRef(name: String, conf: SparkConf, rpcEnv: RpcEnv): RpcEndpointRef = {
    val driverActorSystemName = SparkEnv.driverActorSystemName
    val driverHost: String = conf.get("spark.driver.host", "localhost")
    val driverPort: Int = conf.getInt("spark.driver.port", 7077)
    Utils.checkHost(driverHost, "Expected hostname")
    rpcEnv.setupEndpointRef(driverActorSystemName, RpcAddress(driverHost, driverPort), name)
  }

  /** Returns the configured number of times to retry connecting */
  def numRetries(conf: SparkConf): Int = {
    conf.getInt("spark.rpc.numRetries", 3)
  }

  /** Returns the configured number of milliseconds to wait on each retry */
  def retryWaitMs(conf: SparkConf): Long = {
    conf.getTimeAsMs("spark.rpc.retry.wait", "3s")
  }

  /** Returns the default Spark timeout to use for RPC ask operations. */
  def askTimeout(conf: SparkConf): FiniteDuration = {
    conf.getTimeAsSeconds("spark.rpc.askTimeout",
      conf.get("spark.network.timeout", "120s")) seconds
  }

  /** Returns the default Spark timeout to use for RPC remote endpoint lookup. */
  def lookupTimeout(conf: SparkConf): FiniteDuration = {
    conf.getTimeAsSeconds("spark.rpc.lookupTimeout",
      conf.get("spark.network.timeout", "120s")) seconds
  }
}
