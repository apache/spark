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

package org.apache.spark.internal.plugin

import org.apache.spark.SparkConf
import org.apache.spark.api.plugin.SparkWorkerPlugin
import org.apache.spark.internal.config.SUBPROCESS_PLUGINS
import org.apache.spark.util.Utils

/**
 * A container that delegates to a collection of [[SparkWorkerPlugin]] instances.
 */
class WorkerPluginContainer(plugins: Seq[SparkWorkerPlugin]) extends SparkWorkerPlugin {

  /**
   * Apply all contained plugins to the provided process builder.
   */
  override def apply(pb: ProcessBuilder): Unit = plugins.foreach(_.apply(pb))
}

object WorkerPluginContainer {

  def apply(conf: SparkConf): WorkerPluginContainer =
    new WorkerPluginContainer(
      Utils.loadExtensions(classOf[SparkWorkerPlugin], conf.get(SUBPROCESS_PLUGINS), conf))
}
