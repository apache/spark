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

import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer

import org.apache.spark.Logging
import org.apache.spark.util.Utils

/**
 * A SparkListenerEvent bus that relays events to its listeners
 */
private[spark] trait SparkListenerBus extends Logging {

  // SparkListeners attached to this event bus
  protected val sparkListeners = new ArrayBuffer[SparkListener]
    with mutable.SynchronizedBuffer[SparkListener]

  def addListener(listener: SparkListener) {
    sparkListeners += listener
  }

  /**
   * Post an event to all attached listeners.
   * This does nothing if the event is SparkListenerShutdown.
   */
  def postToAll(event: SparkListenerEvent) {
    event match {
      case stageSubmitted: SparkListenerStageSubmitted =>
        foreachListener(_.onStageSubmitted(stageSubmitted))
      case stageCompleted: SparkListenerStageCompleted =>
        foreachListener(_.onStageCompleted(stageCompleted))
      case jobStart: SparkListenerJobStart =>
        foreachListener(_.onJobStart(jobStart))
      case jobEnd: SparkListenerJobEnd =>
        foreachListener(_.onJobEnd(jobEnd))
      case taskStart: SparkListenerTaskStart =>
        foreachListener(_.onTaskStart(taskStart))
      case taskGettingResult: SparkListenerTaskGettingResult =>
        foreachListener(_.onTaskGettingResult(taskGettingResult))
      case taskEnd: SparkListenerTaskEnd =>
        foreachListener(_.onTaskEnd(taskEnd))
      case environmentUpdate: SparkListenerEnvironmentUpdate =>
        foreachListener(_.onEnvironmentUpdate(environmentUpdate))
      case blockManagerAdded: SparkListenerBlockManagerAdded =>
        foreachListener(_.onBlockManagerAdded(blockManagerAdded))
      case blockManagerRemoved: SparkListenerBlockManagerRemoved =>
        foreachListener(_.onBlockManagerRemoved(blockManagerRemoved))
      case unpersistRDD: SparkListenerUnpersistRDD =>
        foreachListener(_.onUnpersistRDD(unpersistRDD))
      case applicationStart: SparkListenerApplicationStart =>
        foreachListener(_.onApplicationStart(applicationStart))
      case applicationEnd: SparkListenerApplicationEnd =>
        foreachListener(_.onApplicationEnd(applicationEnd))
      case metricsUpdate: SparkListenerExecutorMetricsUpdate =>
        foreachListener(_.onExecutorMetricsUpdate(metricsUpdate))
      case executorAdded: SparkListenerExecutorAdded =>
        foreachListener(_.onExecutorAdded(executorAdded))
      case executorRemoved: SparkListenerExecutorRemoved =>
        foreachListener(_.onExecutorRemoved(executorRemoved))
      case SparkListenerShutdown =>
    }
  }

  /**
   * Apply the given function to all attached listeners, catching and logging any exception.
   */
  private def foreachListener(f: SparkListener => Unit): Unit = {
    sparkListeners.foreach { listener =>
      try {
        f(listener)
      } catch {
        case e: Exception =>
          logError(s"Listener ${Utils.getFormattedClassName(listener)} threw an exception", e)
      }
    }
  }

}
