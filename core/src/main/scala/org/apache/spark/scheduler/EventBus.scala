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

/**
 * A SparkListenerEvent bus that relays events to its listeners.
 */
private[spark] trait EventBus {

  // SparkListeners attached to this event bus
  protected val sparkListeners = new ArrayBuffer[SparkListener]
    with mutable.SynchronizedBuffer[SparkListener]

  def addListener(listener: SparkListener) {
    sparkListeners += listener
  }

  /**
   * Post an event to all attached listeners. Return true if the shutdown event is posted.
   */
  protected def postToAll(event: SparkListenerEvent): Boolean = {
    postToListeners(event, sparkListeners)
  }

  /**
   * Post an event to a given list of listeners. Return true if the shutdown event is posted.
   */
  protected def postToListeners(
      event: SparkListenerEvent,
      listeners: Seq[SparkListener]): Boolean = {

    event match {
      case stageSubmitted: SparkListenerStageSubmitted =>
        listeners.foreach(_.onStageSubmitted(stageSubmitted))
      case stageCompleted: SparkListenerStageCompleted =>
        listeners.foreach(_.onStageCompleted(stageCompleted))
      case jobStart: SparkListenerJobStart =>
        listeners.foreach(_.onJobStart(jobStart))
      case jobEnd: SparkListenerJobEnd =>
        listeners.foreach(_.onJobEnd(jobEnd))
      case taskStart: SparkListenerTaskStart =>
        listeners.foreach(_.onTaskStart(taskStart))
      case taskGettingResult: SparkListenerTaskGettingResult =>
        listeners.foreach(_.onTaskGettingResult(taskGettingResult))
      case taskEnd: SparkListenerTaskEnd =>
        listeners.foreach(_.onTaskEnd(taskEnd))
      case applicationStart: SparkListenerApplicationStart =>
        listeners.foreach(_.onApplicationStart(applicationStart))
      case environmentUpdate: SparkListenerEnvironmentUpdate =>
        listeners.foreach(_.onEnvironmentUpdate(environmentUpdate))
      case executorsStateChange: SparkListenerExecutorsStateChange =>
        listeners.foreach(_.onExecutorsStateChange(executorsStateChange))
      case unpersistRDD: SparkListenerUnpersistRDD =>
        listeners.foreach(_.onUnpersistRDD(unpersistRDD))
      case SparkListenerShutdown =>
        return true
      case _ =>
    }
    false
  }
}
