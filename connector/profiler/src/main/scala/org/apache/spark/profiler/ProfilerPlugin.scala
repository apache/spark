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
package org.apache.spark.profiler

import java.util.{Map => JMap}

import scala.jdk.CollectionConverters._
import scala.util.Random

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.api.plugin.{DriverPlugin, ExecutorPlugin, PluginContext, SparkPlugin}
import org.apache.spark.internal.{Logging, MDC}
import org.apache.spark.internal.LogKeys.EXECUTOR_ID

/**
 * Spark plugin to do profiling
 */
class ProfilerPlugin extends SparkPlugin {
  override def driverPlugin(): DriverPlugin = new ProfilerDriverPlugin

  override def executorPlugin(): ExecutorPlugin = new ProfilerExecutorPlugin
}

class ProfilerDriverPlugin extends DriverPlugin with Logging {

  private var sparkConf: SparkConf = _
  private var pluginCtx: PluginContext = _
  private var profiler: SparkAsyncProfiler = _
  private var driverProfilingEnabled: Boolean = _

  override def init(sc: SparkContext, ctx: PluginContext): JMap[String, String] = {
    pluginCtx = ctx
    sparkConf = ctx.conf()
    driverProfilingEnabled = sparkConf.get(PROFILER_DRIVER_ENABLED)
    if (driverProfilingEnabled) {
      logInfo("Driver starting profiling")
      profiler = new SparkAsyncProfiler(sparkConf, pluginCtx.executorID())
      profiler.start()
    }

    Map.empty[String, String].asJava
  }

  override def shutdown(): Unit = {
    logInfo("Driver profiler shutting down")
    if (profiler != null) {
      profiler.stop()
    }
  }
}

class ProfilerExecutorPlugin extends ExecutorPlugin with Logging {

  private var sparkConf: SparkConf = _
  private var pluginCtx: PluginContext = _
  private var profiler: SparkAsyncProfiler = _
  private var executorProfilerEnabled: Boolean = _
  private var executorProfilerFraction: Double = _
  private val rand: Random = new Random(System.currentTimeMillis())

  override def init(ctx: PluginContext, extraConf: JMap[String, String]): Unit = {
    pluginCtx = ctx
    sparkConf = ctx.conf()
    executorProfilerEnabled = sparkConf.get(PROFILER_EXECUTOR_ENABLED)
    if (executorProfilerEnabled) {
      executorProfilerFraction = sparkConf.get(PROFILER_EXECUTOR_FRACTION)
      if (rand.nextInt(100) * 0.01 < executorProfilerFraction) {
        logInfo(log"Executor id ${MDC(EXECUTOR_ID, pluginCtx.executorID())} " +
          log"selected for profiling")
        profiler = new SparkAsyncProfiler(sparkConf, pluginCtx.executorID())
        profiler.start()
      }
    }
    Map.empty[String, String].asJava
  }

  override def shutdown(): Unit = {
    logInfo("Executor profiler shutting down")
    if (profiler != null) {
      profiler.stop()
    }
  }
}
