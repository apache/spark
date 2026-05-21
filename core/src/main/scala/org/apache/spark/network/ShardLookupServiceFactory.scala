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

package org.apache.spark.network

import org.apache.spark.SparkConf
import org.apache.spark.internal.Logging
import org.apache.spark.internal.LogKeys.CLASS_NAME
import org.apache.spark.network.netty.NettyShardLookupService
import org.apache.spark.rpc.RpcEndpointRef
import org.apache.spark.util.Utils

/**
 * Factory for creating [[ShardLookupService]] instances.
 *
 * The implementation class is controlled by `spark.shard.service`.
 * Custom implementations (e.g. native backends) can be plugged in by setting
 * this config to the FQCN of a [[ShardLookupService]] subclass with a
 * constructor matching `(SparkConf, String, String, Int)`.
 */
private[spark] object ShardLookupServiceFactory extends Logging {

  private val SHARD_LOOKUP_SERVICE_CLASS_KEY = "spark.shard.service"

  private val DEFAULT_CLASS =
    classOf[NettyShardLookupService].getName

  def create(
      conf: SparkConf,
      bindAddress: String,
      advertiseAddress: String,
      port: Int,
      numCores: Int,
      masterEndpoint: RpcEndpointRef): ShardLookupService = {
    val className = conf.get(SHARD_LOOKUP_SERVICE_CLASS_KEY, DEFAULT_CLASS)
    if (className == DEFAULT_CLASS) {
      new NettyShardLookupService(conf, bindAddress, advertiseAddress, port,
        numCores, masterEndpoint)
    } else {
      try {
        logInfo(log"Creating custom ShardLookupService: ${MDC(CLASS_NAME, className)}")
        Utils.classForName(className)
          .getDeclaredConstructor(
            classOf[SparkConf], classOf[String], classOf[String],
            classOf[Int], classOf[Int], classOf[RpcEndpointRef])
          .newInstance(conf, bindAddress, advertiseAddress,
            port.asInstanceOf[AnyRef], numCores.asInstanceOf[AnyRef],
            masterEndpoint)
          .asInstanceOf[ShardLookupService]
      } catch {
        case e: Exception =>
          logWarning(log"Failed to create ${MDC(CLASS_NAME, className)}, falling back to Netty", e)
          new NettyShardLookupService(conf, bindAddress, advertiseAddress, port,
            numCores, masterEndpoint)
      }
    }
  }
}
