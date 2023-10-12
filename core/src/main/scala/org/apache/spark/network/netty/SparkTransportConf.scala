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

import scala.jdk.CollectionConverters._

import org.apache.spark.{SparkConf, SSLOptions}
import org.apache.spark.network.util.{ConfigProvider, NettyUtils, TransportConf}

/**
 * Provides a utility for transforming from a SparkConf inside a Spark JVM (e.g., Executor,
 * Driver, or a standalone shuffle service) into a TransportConf with details on our environment
 * like the number of cores that are allocated to this JVM.
 */
object SparkTransportConf {

  /**
   * Utility for creating a [[TransportConf]] from a [[SparkConf]].
   * @param _conf the [[SparkConf]]
   * @param module the module name
   * @param numUsableCores if nonzero, this will restrict the server and client threads to only
   *                       use the given number of cores, rather than all of the machine's cores.
   *                       This restriction will only occur if these properties are not already set.
   * @param role           optional role, could be driver, executor, worker and master. Default is
   *                      [[None]], means no role specific configurations.
   * @param sslOptions SSL config options
   */
  def fromSparkConf(
      _conf: SparkConf,
      module: String,
      numUsableCores: Int = 0,
      role: Option[String] = None,
      sslOptions: Option[SSLOptions] = None): TransportConf = {
    val conf = _conf.clone
    // specify default thread configuration based on our JVM's allocation of cores (rather than
    // necessarily assuming we have all the machine's cores).
    val numThreads = NettyUtils.defaultNumThreads(numUsableCores)
    // override threads configurations with role specific values if specified
    // config order is role > module > default
    Seq("serverThreads", "clientThreads").foreach { suffix =>
      val value = role.flatMap { r => conf.getOption(s"spark.$r.$module.io.$suffix") }
        .getOrElse(
          conf.get(s"spark.$module.io.$suffix", numThreads.toString))
      conf.set(s"spark.$module.io.$suffix", value)
    }

    val configProvider = sslOptions.map(_.createConfigProvider(conf)).getOrElse(
      new ConfigProvider {
        override def get(name: String): String = conf.get(name)
        override def get(name: String, defaultValue: String): String = conf.get(name, defaultValue)
        override def getAll(): java.lang.Iterable[java.util.Map.Entry[String, String]] = {
          conf.getAll.toMap.asJava.entrySet()
        }
      })
    new TransportConf(module, configProvider)
  }
}
