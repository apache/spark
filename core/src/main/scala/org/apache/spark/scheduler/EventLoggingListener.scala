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

package org.apache.spark.scheduler

import java.net.URI
import java.util.Properties

import scala.collection.mutable
import scala.jdk.CollectionConverters._

import org.apache.hadoop.conf.Configuration

import org.apache.spark.{SPARK_VERSION, SparkConf, SparkContext}
import org.apache.spark.deploy.SparkHadoopUtil
import org.apache.spark.deploy.history.EventLogFileWriter
import org.apache.spark.executor.ExecutorMetrics
import org.apache.spark.internal.Logging
import org.apache.spark.internal.config._
import org.apache.spark.util.{JsonProtocol, JsonProtocolOptions, Utils}

/**
 * A SparkListener that logs events to persistent storage.
 *
 * Event logging is specified by the following configurable parameters:
 *   spark.eventLog.enabled - Whether event logging is enabled.
 *   spark.eventLog.dir - Path to the directory in which events are logged.
 *   spark.eventLog.logBlockUpdates.enabled - Whether to log block updates
 *   spark.eventLog.logStageExecutorMetrics - Whether to log stage executor metrics
 *
 * Event log file writer maintains its own parameters: refer the doc of [[EventLogFileWriter]]
 * and its descendant for more details.
 */
private[spark] class EventLoggingListener(
    appId: String,
    appAttemptId : Option[String],
    logBaseDir: URI,
    sparkConf: SparkConf,
    hadoopConf: Configuration)
  extends SparkListener with Logging {

  import EventLoggingListener._

  def this(appId: String, appAttemptId : Option[String], logBaseDir: URI, sparkConf: SparkConf) =
    this(appId, appAttemptId, logBaseDir, sparkConf,
      SparkHadoopUtil.get.newConfiguration(sparkConf))

  // For testing.
  private[scheduler] val logWriter: EventLogFileWriter =
    EventLogFileWriter(appId, appAttemptId, logBaseDir, sparkConf, hadoopConf)

  // For testing. Keep track of all JSON serialized events that have been logged.
  private[scheduler] val loggedEvents = new mutable.ArrayBuffer[String]

  private val shouldLogBlockUpdates = sparkConf.get(EVENT_LOG_BLOCK_UPDATES)
  private val shouldLogStageExecutorMetrics = sparkConf.get(EVENT_LOG_STAGE_EXECUTOR_METRICS)
  private val testing = sparkConf.get(EVENT_LOG_TESTING)

  // map of (stageId, stageAttempt) to executor metric peaks per executor/driver for the stage
  private val liveStageExecutorMetrics =
    mutable.HashMap.empty[(Int, Int), mutable.HashMap[String, ExecutorMetrics]]

  private[this] val jsonProtocolOptions = new JsonProtocolOptions(sparkConf)

  /**
   * Creates the log file in the configured log directory.
   */
  def start(): Unit = {
    logWriter.start()
    initEventLog()
  }

  private def initEventLog(): Unit = {
    val metadata = SparkListenerLogStart(SPARK_VERSION)
    val eventJson = JsonProtocol.sparkEventToJsonString(metadata, jsonProtocolOptions)
    logWriter.writeEvent(eventJson, flushLogger = true)
    if (testing && loggedEvents != null) {
      loggedEvents += eventJson
    }
  }

  /** Log the event as JSON. */
  private def logEvent(event: SparkListenerEvent, flushLogger: Boolean = false): Unit = {
    val eventJson = JsonProtocol.sparkEventToJsonString(event, jsonProtocolOptions)
    logWriter.writeEvent(eventJson, flushLogger)
    if (testing) {
      loggedEvents += eventJson
    }
  }

  // Events that do not trigger a flush
  override def onStageSubmitted(event: SparkListenerStageSubmitted): Unit = {
    logEvent(event.copy(properties = redactProperties(event.properties)))
    if (shouldLogStageExecutorMetrics) {
      // record the peak metrics for the new stage
      liveStageExecutorMetrics.put((event.stageInfo.stageId, event.stageInfo.attemptNumber()),
        mutable.HashMap.empty[String, ExecutorMetrics])
    }
  }

  override def onTaskStart(event: SparkListenerTaskStart): Unit = logEvent(event)

  override def onTaskGettingResult(event: SparkListenerTaskGettingResult): Unit = logEvent(event)

  override def onTaskEnd(event: SparkListenerTaskEnd): Unit = {
    logEvent(event)
    if (shouldLogStageExecutorMetrics) {
      val stageKey = (event.stageId, event.stageAttemptId)
      liveStageExecutorMetrics.get(stageKey).map { metricsPerExecutor =>
        val metrics = metricsPerExecutor.getOrElseUpdate(
          event.taskInfo.executorId, new ExecutorMetrics())
        metrics.compareAndUpdatePeakValues(event.taskExecutorMetrics)
      }
    }
  }

  override def onEnvironmentUpdate(event: SparkListenerEnvironmentUpdate): Unit = {
    logEvent(redactEvent(sparkConf, event))
  }

  // Events that trigger a flush
  override def onStageCompleted(event: SparkListenerStageCompleted): Unit = {
    if (shouldLogStageExecutorMetrics) {
      // clear out any previous attempts, that did not have a stage completed event
      val prevAttemptId = event.stageInfo.attemptNumber() - 1
      for (attemptId <- 0 to prevAttemptId) {
        liveStageExecutorMetrics.remove((event.stageInfo.stageId, attemptId))
      }

      // log the peak executor metrics for the stage, for each live executor,
      // whether or not the executor is running tasks for the stage
      val executorOpt = liveStageExecutorMetrics.remove(
        (event.stageInfo.stageId, event.stageInfo.attemptNumber()))
      executorOpt.foreach { execMap =>
        execMap.foreach { case (executorId, peakExecutorMetrics) =>
            logEvent(new SparkListenerStageExecutorMetrics(executorId, event.stageInfo.stageId,
              event.stageInfo.attemptNumber(), peakExecutorMetrics))
        }
      }
    }

    // log stage completed event
    logEvent(event, flushLogger = true)
  }

  override def onJobStart(event: SparkListenerJobStart): Unit = {
    logEvent(event.copy(properties = redactProperties(event.properties)), flushLogger = true)
  }

  override def onJobEnd(event: SparkListenerJobEnd): Unit = logEvent(event, flushLogger = true)

  override def onBlockManagerAdded(event: SparkListenerBlockManagerAdded): Unit = {
    logEvent(event, flushLogger = true)
  }

  override def onBlockManagerRemoved(event: SparkListenerBlockManagerRemoved): Unit = {
    logEvent(event, flushLogger = true)
  }

  override def onUnpersistRDD(event: SparkListenerUnpersistRDD): Unit = {
    logEvent(event, flushLogger = true)
  }

  override def onApplicationStart(event: SparkListenerApplicationStart): Unit = {
    logEvent(event, flushLogger = true)
  }

  override def onApplicationEnd(event: SparkListenerApplicationEnd): Unit = {
    logEvent(event, flushLogger = true)
  }
  override def onExecutorAdded(event: SparkListenerExecutorAdded): Unit = {
    logEvent(event, flushLogger = true)
  }

  override def onExecutorRemoved(event: SparkListenerExecutorRemoved): Unit = {
    logEvent(event, flushLogger = true)
  }

  override def onExecutorBlacklisted(event: SparkListenerExecutorBlacklisted): Unit = {
    logEvent(event, flushLogger = true)
  }

  override def onExecutorExcluded(event: SparkListenerExecutorExcluded): Unit = {
    logEvent(event, flushLogger = true)
  }

  override def onExecutorBlacklistedForStage(
      event: SparkListenerExecutorBlacklistedForStage): Unit = {
    logEvent(event, flushLogger = true)
  }

  override def onExecutorExcludedForStage(
      event: SparkListenerExecutorExcludedForStage): Unit = {
    logEvent(event, flushLogger = true)
  }

  override def onNodeBlacklistedForStage(event: SparkListenerNodeBlacklistedForStage): Unit = {
    logEvent(event, flushLogger = true)
  }

  override def onNodeExcludedForStage(event: SparkListenerNodeExcludedForStage): Unit = {
    logEvent(event, flushLogger = true)
  }

  override def onExecutorUnblacklisted(event: SparkListenerExecutorUnblacklisted): Unit = {
    logEvent(event, flushLogger = true)
  }

  override def onExecutorUnexcluded(event: SparkListenerExecutorUnexcluded): Unit = {
    logEvent(event, flushLogger = true)
  }


  override def onNodeBlacklisted(event: SparkListenerNodeBlacklisted): Unit = {
    logEvent(event, flushLogger = true)
  }

  override def onNodeExcluded(event: SparkListenerNodeExcluded): Unit = {
    logEvent(event, flushLogger = true)
  }

  override def onNodeUnblacklisted(event: SparkListenerNodeUnblacklisted): Unit = {
    logEvent(event, flushLogger = true)
  }

  override def onNodeUnexcluded(event: SparkListenerNodeUnexcluded): Unit = {
    logEvent(event, flushLogger = true)
  }

  override def onBlockUpdated(event: SparkListenerBlockUpdated): Unit = {
    if (shouldLogBlockUpdates) {
      logEvent(event, flushLogger = true)
    }
  }

  override def onExecutorMetricsUpdate(event: SparkListenerExecutorMetricsUpdate): Unit = {
    if (shouldLogStageExecutorMetrics) {
      if (event.execId == SparkContext.DRIVER_IDENTIFIER) {
        logEvent(event)
      }
      event.executorUpdates.foreach { case (stageKey1, newPeaks) =>
        liveStageExecutorMetrics.foreach { case (stageKey2, metricsPerExecutor) =>
          // If the update came from the driver, stageKey1 will be the dummy key (-1, -1),
          // so record those peaks for all active stages.
          // Otherwise, record the peaks for the matching stage.
          if (stageKey1 == DRIVER_STAGE_KEY || stageKey1 == stageKey2) {
            val metrics = metricsPerExecutor.getOrElseUpdate(
              event.execId, new ExecutorMetrics())
            metrics.compareAndUpdatePeakValues(newPeaks)
          }
        }
      }
    }
  }

  override def onResourceProfileAdded(event: SparkListenerResourceProfileAdded): Unit = {
    logEvent(event, flushLogger = true)
  }

  override def onOtherEvent(event: SparkListenerEvent): Unit = {
    if (event.logEvent) {
      logEvent(event, flushLogger = true)
    }
  }

  /** Stop logging events. */
  def stop(): Unit = {
    logWriter.stop()
  }

  private def redactProperties(properties: Properties): Properties = {
    if (properties == null) {
      return properties
    }
    val redactedProperties = new Properties
    // properties may contain some custom local properties such as stage/job description
    // only properties in sparkConf need to be redacted.
    val (globalProperties, localProperties) = properties.asScala.toSeq.partition {
      case (key, _) => sparkConf.contains(key)
    }
    (Utils.redact(sparkConf, globalProperties) ++ localProperties).foreach {
      case (key, value) => redactedProperties.setProperty(key, value)
    }
    redactedProperties
  }
}

private[spark] object EventLoggingListener extends Logging {
  val DEFAULT_LOG_DIR = "/tmp/spark-events"
  // Dummy stage key used by driver in executor metrics updates
  val DRIVER_STAGE_KEY = (-1, -1)

  private[spark] def redactEvent(
      sparkConf: SparkConf,
      event: SparkListenerEnvironmentUpdate): SparkListenerEnvironmentUpdate = {
    // environmentDetails maps a string descriptor to a set of properties
    // Similar to:
    // "JVM Information" -> jvmInformation,
    // "Spark Properties" -> sparkProperties,
    // ...
    // where jvmInformation, sparkProperties, etc. are sequence of tuples.
    // We go through the various  of properties and redact sensitive information from them.
    val noRedactProps = Seq("Classpath Entries")
    val redactedProps = event.environmentDetails.map {
      case (name, props) if noRedactProps.contains(name) => name -> props
      case (name, props) => name -> Utils.redact(sparkConf, props)
    }
    SparkListenerEnvironmentUpdate(redactedProps)
  }
}
