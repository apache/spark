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
import org.apache.spark.internal.{Logging, MDC}
import org.apache.spark.internal.LogKeys._
import org.apache.spark.internal.config.DRIVER_TIMEOUT
import org.apache.spark.util.{SparkExitCode, ThreadUtils}

/**
 * A built-in plugin to provide Driver timeout feature.
 */
class DriverTimeoutPlugin extends SparkPlugin {
  override def driverPlugin(): DriverPlugin = new DriverTimeoutDriverPlugin()

  // No-op
  override def executorPlugin(): ExecutorPlugin = null
}

class DriverTimeoutDriverPlugin extends DriverPlugin with Logging {

  private val timeoutService: ScheduledExecutorService =
    ThreadUtils.newDaemonSingleThreadScheduledExecutor("driver-timeout")

  override def init(sc: SparkContext, ctx: PluginContext): JMap[String, String] = {
    val timeout = sc.conf.get(DRIVER_TIMEOUT)
    if (timeout == 0) {
      logWarning("Disabled with the timeout value 0.")
    } else {
      val task: Runnable = () => {
        logWarning(log"Terminate Driver JVM because it runs after " +
          log"${MDC(TIME_UNITS, timeout)} minute" +
          (if (timeout == 1) log"" else log"s"))
        // We cannot use 'SparkContext.stop' because SparkContext might be in abnormal situation.
        System.exit(SparkExitCode.DRIVER_TIMEOUT)
      }
      timeoutService.schedule(task, timeout, TimeUnit.MINUTES)
    }
    Map.empty[String, String].asJava
  }

  override def shutdown(): Unit = timeoutService.shutdown()
}
