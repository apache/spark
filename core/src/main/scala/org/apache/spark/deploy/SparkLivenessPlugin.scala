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
package org.apache.spark.deploy

import java.util.{Map => JMap}
import java.util.concurrent.{ScheduledExecutorService, TimeUnit}

import scala.jdk.CollectionConverters._

import org.apache.spark.SparkContext
import org.apache.spark.api.plugin.{DriverPlugin, ExecutorPlugin, PluginContext, SparkPlugin}
import org.apache.spark.internal.Logging
import org.apache.spark.internal.LogKeys._
import org.apache.spark.internal.config._
import org.apache.spark.util.{SparkExitCode, ThreadUtils}

/**
 * A built-in plugin to check liveness of Spark essential components, e.g., SparkContext.
 */
class SparkLivenessPlugin extends SparkPlugin {
  override def driverPlugin(): DriverPlugin = new SparkLivenessDriverPlugin()

  // No-op
  override def executorPlugin(): ExecutorPlugin = null
}

class SparkLivenessDriverPlugin extends DriverPlugin with Logging {

  private val timer: ScheduledExecutorService =
    ThreadUtils.newDaemonSingleThreadScheduledExecutor("driver-liveness")

  override def init(sc: SparkContext, ctx: PluginContext): JMap[String, String] = {
    val checkInterval = sc.conf.get(DRIVER_SPARK_CONTEXT_LIVENESS_CHECK_INTERVAL)
    val terminateDelay = sc.conf.get(DRIVER_SPARK_CONTEXT_LIVENESS_TERMINATE_DELAY)
    if (checkInterval == 0) {
      logWarning("SparkContext liveness check is disabled.")
    } else {
      val task: Runnable = () => {
        if (sc.isStopped) {
          logWarning(log"SparkContext is stopped, will terminate Driver JVM " +
            log"after ${MDC(TIME_UNITS, terminateDelay)} seconds.")
          Thread.sleep(terminateDelay * 1000L)
          System.exit(SparkExitCode.SPARK_CONTEXT_STOPPED)
        }
      }
      timer.scheduleWithFixedDelay(task, checkInterval, checkInterval, TimeUnit.SECONDS)
    }
    Map.empty[String, String].asJava
  }
}
