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

package org.apache.spark.metrics

import java.util.Properties
import java.util.concurrent.TimeUnit

import scala.collection.mutable

import com.codahale.metrics.{Counter, Gauge, Histogram, Meter, Metric, MetricRegistry, MetricRegistryListener, Timer}
import org.eclipse.jetty.servlet.ServletContextHandler

import org.apache.spark.{SecurityManager, SparkConf}
import org.apache.spark.internal.{Logging, MDC}
import org.apache.spark.internal.LogKeys
import org.apache.spark.internal.config._
import org.apache.spark.metrics.sink.{MetricsServlet, PrometheusServlet, Sink}
import org.apache.spark.metrics.source.{Source, StaticSources}
import org.apache.spark.util.Utils

/**
 * Spark Metrics System, created by a specific "instance", combined by source,
 * sink, periodically polls source metrics data to sink destinations.
 *
 * "instance" specifies "who" (the role) uses the metrics system. In Spark, there are several roles
 * like master, worker, executor, client driver. These roles will create metrics system
 * for monitoring. So, "instance" represents these roles. Currently in Spark, several instances
 * have already implemented: master, worker, executor, driver, applications.
 *
 * "source" specifies "where" (source) to collect metrics data from. In metrics system, there exists
 * two kinds of source:
 *   1. Spark internal source, like MasterSource, WorkerSource, etc, which will collect
 *   Spark component's internal state, these sources are related to instance and will be
 *   added after a specific metrics system is created.
 *   2. Common source, like JvmSource, which will collect low level state, is configured by
 *   configuration and loaded through reflection.
 *
 * "sink" specifies "where" (destination) to output metrics data to. Several sinks can
 * coexist and metrics can be flushed to all these sinks.
 *
 * Metrics configuration format is like below:
 * [instance].[sink|source].[name].[options] = xxxx
 *
 * [instance] can be "master", "worker", "executor", "driver", "applications" which means only
 * the specified instance has this property.
 * wild card "*" can be used to replace instance name, which means all the instances will have
 * this property.
 *
 * [sink|source] means this property belongs to source or sink. This field can only be
 * source or sink.
 *
 * [name] specify the name of sink or source, if it is custom defined.
 *
 * [options] represent the specific property of this source or sink.
 */
private[spark] class MetricsSystem private (
    val instance: String,
    conf: SparkConf,
    registry: MetricRegistry)
  extends Logging {

  private[this] val metricsConfig = new MetricsConfig(conf)

  private val sinks = new mutable.ArrayBuffer[Sink]
  private val sources = new mutable.ArrayBuffer[(Source, MetricRegistryListener)]

  private var running: Boolean = false

  // Treat MetricsServlet as a special sink as it should be exposed to add handlers to web ui
  private var metricsServlet: Option[MetricsServlet] = None
  private var prometheusServlet: Option[PrometheusServlet] = None

  /**
   * Get any UI handlers used by this metrics system; can only be called after start().
   */
  def getServletHandlers: Array[ServletContextHandler] = {
    require(running, "Can only call getServletHandlers on a running MetricsSystem")
    metricsServlet.map(_.getHandlers(conf)).getOrElse(Array()) ++
      prometheusServlet.map(_.getHandlers(conf)).getOrElse(Array())
  }

  metricsConfig.initialize()

  def start(registerStaticSources: Boolean = true): Unit = {
    require(!running, "Attempting to start a MetricsSystem that is already running")
    running = true
    if (registerStaticSources) {
      StaticSources.allSources.foreach(registerSource)
      registerSources()
    }
    registerSinks()
    sinks.foreach(_.start())
  }

  def stop(): Unit = {
    if (running) {
      sinks.foreach(_.stop())
      registry.removeMatching((_: String, _: Metric) => true)
      sources.synchronized {
        sources.foreach(s => removeSource(s._1))
      }
    } else {
      logWarning("Stopping a MetricsSystem that is not running")
    }
    running = false
  }

  def report(): Unit = {
    sinks.foreach(_.report())
  }

  /**
   * Build a name that uniquely identifies each metric source.
   * The name is structured as follows: <app ID>.<executor ID (or "driver")>.<source name>.
   * If either ID is not available, this defaults to just using <source name>.
   *
   * @param source Metric source to be named by this method.
   * @return A unique metric name for each combination of
   *         application, executor/driver and metric source.
   */
  private[spark] def buildRegistryName(source: Source): String = {
    val metricsNamespace = conf.get(METRICS_NAMESPACE).orElse(conf.getOption("spark.app.id"))

    val executorId = conf.get(EXECUTOR_ID)
    val defaultName = MetricRegistry.name(source.sourceName)

    if (instance == "driver" || instance == "executor") {
      if (metricsNamespace.isDefined && executorId.isDefined) {
        MetricRegistry.name(metricsNamespace.get, executorId.get, source.sourceName)
      } else {
        // Only Driver and Executor set spark.app.id and spark.executor.id.
        // Other instance types, e.g. Master and Worker, are not related to a specific application.
        if (metricsNamespace.isEmpty) {
          logWarning(log"Using default name ${MDC(LogKeys.DEFAULT_NAME, defaultName)} " +
            log"for source because neither " +
            log"${MDC(LogKeys.CONFIG, METRICS_NAMESPACE.key)} nor spark.app.id is set.")
        }
        if (executorId.isEmpty) {
          logWarning(log"Using default name ${MDC(LogKeys.DEFAULT_NAME, defaultName)} " +
            log"for source because spark.executor.id is not set.")
        }
        defaultName
      }
    } else { defaultName }
  }

  def getSourcesByName(sourceName: String): Seq[Source] = sources.synchronized {
    sources.filter(s => s._1.sourceName == sourceName).map(_._1).toSeq
  }

  def registerSource(source: Source): Unit = {
    val listener = new MetricsSystemListener(buildRegistryName(source))
    sources.synchronized {
      sources += (source, listener)
    }
    source.metricRegistry.addListener(listener)
  }

  def removeSource(source: Source): Unit = {
    sources.synchronized {
      val sourceIdx = sources.indexWhere(s => s._1 == source)
      if (sourceIdx != -1) {
        source.metricRegistry.removeListener(sources.remove(sourceIdx)._2)
      }
    }
    val regName = buildRegistryName(source)
    registry.removeMatching((name: String, _: Metric) => name.startsWith(regName))
  }

  private def registerSources(): Unit = {
    val instConfig = metricsConfig.getInstance(instance)
    val sourceConfigs = metricsConfig.subProperties(instConfig, MetricsSystem.SOURCE_REGEX)

    // Register all the sources related to instance
    sourceConfigs.foreach { kv =>
      val classPath = kv._2.getProperty("class")
      try {
        val source = Utils.classForName[Source](classPath).getConstructor().newInstance()
        registerSource(source)
      } catch {
        case e: Exception =>
          logError(log"Source class ${MDC(LogKeys.CLASS_NAME, classPath)} " +
            log"cannot be instantiated", e)
      }
    }
  }

  private def registerSinks(): Unit = {
    val instConfig = metricsConfig.getInstance(instance)
    val sinkConfigs = metricsConfig.subProperties(instConfig, MetricsSystem.SINK_REGEX)

    sinkConfigs.foreach { kv =>
      val classPath = kv._2.getProperty("class")
      if (null != classPath) {
        try {
          if (kv._1 == "servlet") {
            val servlet = Utils.classForName[MetricsServlet](classPath)
              .getConstructor(classOf[Properties], classOf[MetricRegistry])
              .newInstance(kv._2, registry)
            metricsServlet = Some(servlet)
          } else if (kv._1 == "prometheusServlet") {
            val servlet = Utils.classForName[PrometheusServlet](classPath)
              .getConstructor(classOf[Properties], classOf[MetricRegistry])
              .newInstance(kv._2, registry)
            prometheusServlet = Some(servlet)
          } else {
            val sink = try {
              Utils.classForName[Sink](classPath)
                .getConstructor(classOf[Properties], classOf[MetricRegistry])
                .newInstance(kv._2, registry)
            } catch {
              case _: NoSuchMethodException =>
                // Fallback to three-parameters constructor having SecurityManager
                Utils.classForName[Sink](classPath)
                  .getConstructor(
                    classOf[Properties], classOf[MetricRegistry], classOf[SecurityManager])
                  .newInstance(kv._2, registry, null)
            }
            sinks += sink
          }
        } catch {
          case e: Exception =>
            logError(log"Sink class ${MDC(LogKeys.CLASS_NAME, classPath)} " +
              log"cannot be instantiated")
            throw e
        }
      }
    }
  }

  def metricsProperties(): Properties = metricsConfig.properties

  private[spark] class MetricsSystemListener(prefix: String) extends MetricRegistryListener {
    def metricName(name: String): String = MetricRegistry.name(prefix, name)

    def registerMetric[T <: Metric](name: String, metric: T): Unit = {
      try {
        registry.register(metricName(name), metric)
      } catch {
        case e: IllegalArgumentException => logInfo("Metrics already registered", e)
      }
    }

    override def onHistogramAdded(name: String, histogram: Histogram): Unit =
      registerMetric(name, histogram)

    override def onCounterAdded(name: String, counter: Counter): Unit =
      registerMetric(name, counter)

    override def onMeterAdded(name: String, meter: Meter): Unit =
      registerMetric(name, meter)

    override def onGaugeAdded(name: String, gauge: Gauge[_]): Unit =
      registerMetric(name, gauge)

    override def onTimerAdded(name: String, timer: Timer): Unit =
      registerMetric(name, timer)

    override def onHistogramRemoved(name: String): Unit =
      registry.remove(metricName(name))

    override def onGaugeRemoved(name: String): Unit =
      registry.remove(metricName(name))

    override def onMeterRemoved(name: String): Unit =
      registry.remove(metricName(name))

    override def onCounterRemoved(name: String): Unit =
      registry.remove(metricName(name))

    override def onTimerRemoved(name: String): Unit =
      registry.remove(metricName(name))
  }
}

private[spark] object MetricsSystem {
  val SINK_REGEX = "^sink\\.(.+)\\.(.+)".r
  val SOURCE_REGEX = "^source\\.(.+)\\.(.+)".r

  private[this] val MINIMAL_POLL_UNIT = TimeUnit.SECONDS
  private[this] val MINIMAL_POLL_PERIOD = 1

  def checkMinimalPollingPeriod(pollUnit: TimeUnit, pollPeriod: Int): Unit = {
    val period = MINIMAL_POLL_UNIT.convert(pollPeriod, pollUnit)
    if (period < MINIMAL_POLL_PERIOD) {
      throw new IllegalArgumentException("Polling period " + pollPeriod + " " + pollUnit +
        " below than minimal polling period ")
    }
  }

  def createMetricsSystem(
      instance: String,
      conf: SparkConf,
      registry: MetricRegistry = new MetricRegistry): MetricsSystem = {
    new MetricsSystem(instance, conf, registry)
  }
}

private[spark] object MetricsSystemInstances {
  // The Spark standalone master process
  val MASTER = "master"

  // A component within the master which reports on various applications
  val APPLICATIONS = "applications"

  // A Spark standalone worker process
  val WORKER = "worker"

  // A Spark executor
  val EXECUTOR = "executor"

  // The Spark driver process (the process in which your SparkContext is created)
  val DRIVER = "driver"

  // The Spark shuffle service
  val SHUFFLE_SERVICE = "shuffleService"

  // The Spark ApplicationMaster when running on YARN
  val APPLICATION_MASTER = "applicationMaster"
}
