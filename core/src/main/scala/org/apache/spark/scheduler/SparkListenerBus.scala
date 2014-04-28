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
 * A SparkListenerEvent bus that relays events to its listeners
 */
private[spark] trait SparkListenerBus {

  // SparkListeners attached to this event bus
  protected val sparkListeners = new ArrayBuffer[SparkListener]
    with mutable.SynchronizedBuffer[SparkListener]

  def addListener(listener: SparkListener) {
    sparkListeners += listener
  }

  /**
   * Post an event to all attached listeners. This does nothing if the event is
   * SparkListenerShutdown.
   */
  protected def postToAll(event: SparkListenerEvent) {
    event match {
      case stageSubmitted: SparkListenerStageSubmitted =>
        sparkListeners.foreach(_.onStageSubmitted(stageSubmitted))
      case stageCompleted: SparkListenerStageCompleted =>
        sparkListeners.foreach(_.onStageCompleted(stageCompleted))
      case jobStart: SparkListenerJobStart =>
        sparkListeners.foreach(_.onJobStart(jobStart))
      case jobEnd: SparkListenerJobEnd =>
        sparkListeners.foreach(_.onJobEnd(jobEnd))
      case taskStart: SparkListenerTaskStart =>
        sparkListeners.foreach(_.onTaskStart(taskStart))
      case taskGettingResult: SparkListenerTaskGettingResult =>
        sparkListeners.foreach(_.onTaskGettingResult(taskGettingResult))
      case taskEnd: SparkListenerTaskEnd =>
        sparkListeners.foreach(_.onTaskEnd(taskEnd))
      case environmentUpdate: SparkListenerEnvironmentUpdate =>
        sparkListeners.foreach(_.onEnvironmentUpdate(environmentUpdate))
      case blockManagerAdded: SparkListenerBlockManagerAdded =>
        sparkListeners.foreach(_.onBlockManagerAdded(blockManagerAdded))
      case blockManagerRemoved: SparkListenerBlockManagerRemoved =>
        sparkListeners.foreach(_.onBlockManagerRemoved(blockManagerRemoved))
      case unpersistRDD: SparkListenerUnpersistRDD =>
        sparkListeners.foreach(_.onUnpersistRDD(unpersistRDD))
      case applicationStart: SparkListenerApplicationStart =>
        sparkListeners.foreach(_.onApplicationStart(applicationStart))
      case applicationEnd: SparkListenerApplicationEnd =>
        sparkListeners.foreach(_.onApplicationEnd(applicationEnd))
      case SparkListenerShutdown =>
    }
  }
}
