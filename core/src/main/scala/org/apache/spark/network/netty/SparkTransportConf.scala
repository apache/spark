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

package org.apache.spark.network.netty

import scala.collection.JavaConverters._

import org.apache.spark.SparkConf
import org.apache.spark.network.util.{ConfigProvider, NettyUtils, TransportConf}

/**
 * Provides a utility for transforming from a SparkConf inside a Spark JVM (e.g., Executor,
 * Driver, or a standalone shuffle service) into a TransportConf with details on our environment
 * like the number of cores that are allocated to this JVM.
 */
object SparkTransportConf {

  /**
   * Utility for creating a [[TransportConf]] from a [[SparkConf]].
   * @param _conf         the [[SparkConf]]
   * @param module         the module name
   * @param numUsableCores if nonzero, this will restrict the server and client threads to only
   *                       use the given number of cores, rather than all of the machine's cores.
   *                       This restriction will only occur if these properties are not already set.
   * @param role           optional role, could be driver, executor, worker and master. Default is
   *                      [[None]], means no role specific configurations.
   */
  def fromSparkConf(
      _conf: SparkConf,
      module: String,
      numUsableCores: Int = 0,
      role: Option[String] = None): TransportConf = {
    val conf = _conf.clone
    val (serverThreads, clientThreads) = getThreadsConfByRole(conf, module, role)
    // Or specify thread configuration based on our JVM's allocation of cores (rather than
    // necessarily assuming we have all the machine's cores).
    val numThreads = NettyUtils.defaultNumThreads(numUsableCores)
    // override threads configurations with role specific values if specified
    setThreads(conf, s"spark.$module.io.serverThreads", serverThreads, numThreads)
    setThreads(conf, s"spark.$module.io.clientThreads", clientThreads, numThreads)

    new TransportConf(module, new ConfigProvider {
      override def get(name: String): String = conf.get(name)
      override def get(name: String, defaultValue: String): String = conf.get(name, defaultValue)
      override def getAll(): java.lang.Iterable[java.util.Map.Entry[String, String]] = {
        conf.getAll.toMap.asJava.entrySet()
      }
    })
  }

  /**
   * get role specific number of threads of server and client
   * @param conf the [[SparkConf]]
   * @param module the module name
   * @param role  optional role, value could be driver, executor, worker and master.
   * @return a pair of server thread configuration and client thread configuration.
   *         If the configuration is not specified, set it to -1
   */
  def getThreadsConfByRole(
      conf: SparkConf,
      module: String,
      role: Option[String]): (Int, Int) = {
    if (role.isEmpty) {
      (-1, -1)
    } else {
      (conf.getInt(s"spark.${role.get}.$module.io.serverThreads", -1),
        conf.getInt(s"spark.${role.get}.$module.io.clientThreads", -1)
      )
    }
  }

  def setThreads(
      conf: SparkConf,
      key: String,
      specificValue: Int,
      defaultValue: Int): Unit = {
    if (specificValue > 0) {
      conf.set(key, specificValue.toString)
    } else {
      conf.setIfMissing(key, defaultValue.toString)
    }
  }
}
