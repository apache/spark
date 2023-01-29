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

package org.apache.spark.sql.connect

import java.util

import scala.collection.JavaConverters._

import org.apache.spark.SparkContext
import org.apache.spark.api.plugin.{DriverPlugin, ExecutorPlugin, PluginContext, SparkPlugin}
import org.apache.spark.sql.connect.service.SparkConnectService

/**
 * This is the main entry point for Spark Connect.
 *
 * To decouple the build of Spark Connect and its dependencies from the core of Spark, we
 * implement it as a Driver Plugin. To enable Spark Connect, simply make sure that the appropriate
 * JAR is available in the CLASSPATH and the driver plugin is configured to load this class.
 */
class SparkConnectPlugin extends SparkPlugin {

  /**
   * Return the plugin's driver-side component.
   *
   * @return
   *   The driver-side component.
   */
  override def driverPlugin(): DriverPlugin = new DriverPlugin {

    override def init(
        sc: SparkContext,
        pluginContext: PluginContext): util.Map[String, String] = {
      SparkConnectService.start()
      Map.empty[String, String].asJava
    }

    override def shutdown(): Unit = {
      SparkConnectService.stop()
    }
  }

  override def executorPlugin(): ExecutorPlugin = null
}
