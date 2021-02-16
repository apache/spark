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
import org.apache.spark.internal.config.SHUFFLE_IO_PLUGIN_CLASS
import org.apache.spark.shuffle.api.ShuffleDataIO
import org.apache.spark.util.Utils

private[spark] object ShuffleDataIOUtils {

  /**
   * The prefix of spark config keys that are passed from the driver to the executor.
   */
  val SHUFFLE_SPARK_CONF_PREFIX = "spark.shuffle.plugin.__config__."

  def loadShuffleDataIO(conf: SparkConf): ShuffleDataIO = {
    val configuredPluginClass = conf.get(SHUFFLE_IO_PLUGIN_CLASS)
    val maybeIO = Utils.loadExtensions(
      classOf[ShuffleDataIO], Seq(configuredPluginClass), conf)
    require(maybeIO.nonEmpty, s"A valid shuffle plugin must be specified by config " +
      s"${SHUFFLE_IO_PLUGIN_CLASS.key}, but $configuredPluginClass resulted in zero valid " +
      s"plugins.")
    maybeIO.head
  }

}
