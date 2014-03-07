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

package org.apache.spark.storage

import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer

import org.apache.spark.scheduler._

/**
 * A listener for BlockManager status updates.
 *
 * This listener provides a way to post executor storage status information as soon as it
 * is available (i.e. immediately after the associated BlockManager has registered with the
 * driver). This is necessary because the SparkContext is only notified when an executor is
 * launched, but by then the storage information is not ready yet.
 *
 * Further, it is possible for a BlockManager be registered before the listener bus on the
 * driver is initialized (e.g. the driver's own BlockManager), in which case the corresponding
 * event should be buffered.
 */
private[spark] class BlockManagerStatusListener extends SparkListener {

  private var _listenerBus: Option[SparkListenerBus] = None

  // Buffer any events received before the listener bus is ready
  private val bufferedEvents = new ArrayBuffer[SparkListenerEvent]
    with mutable.SynchronizedBuffer[SparkListenerEvent]

  /**
   * Set the listener bus. If there are buffered events, post them all to the listener bus.
   */
  def setListenerBus(listenerBus: SparkListenerBus) = {
    _listenerBus = Some(listenerBus)
    bufferedEvents.map(listenerBus.postToAll)
  }

  /**
   * Post the event if the listener bus is ready; otherwise, buffer it.
   */
  private def postOrBuffer(event: SparkListenerEvent) {
    _listenerBus.map(_.post(event)).getOrElse { bufferedEvents += event }
  }

  override def onBlockManagerGained(blockManagerGained: SparkListenerBlockManagerGained) =
    postOrBuffer(blockManagerGained)

  override def onBlockManagerLost(blockManagerLost: SparkListenerBlockManagerLost) =
    postOrBuffer(blockManagerLost)

}
