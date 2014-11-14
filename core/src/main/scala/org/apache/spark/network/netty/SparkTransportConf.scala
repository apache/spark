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

import org.apache.spark.SparkConf
import org.apache.spark.network.util.{TransportConf, ConfigProvider}

object SparkTransportConf {
  /**
   * Utility for creating a [[TransportConf]] from a [[SparkConf]].
   * @param numUsableCores if nonzero, this will restrict the server and client threads to only
   *                       use the given number of cores, rather than all of the machine's cores.
   *                       This restriction will only occur if these properties are not already set.
   */
  def fromSparkConf(_conf: SparkConf, numUsableCores: Int = 0): TransportConf = {
    val conf = _conf.clone
    if (numUsableCores > 0) {
      // Only set if serverThreads/clientThreads not already set.
      conf.set("spark.shuffle.io.serverThreads",
        conf.get("spark.shuffle.io.serverThreads", numUsableCores.toString))
      conf.set("spark.shuffle.io.clientThreads",
        conf.get("spark.shuffle.io.clientThreads", numUsableCores.toString))
    }
    new TransportConf(new ConfigProvider {
      override def get(name: String): String = conf.get(name)
    })
  }
}
