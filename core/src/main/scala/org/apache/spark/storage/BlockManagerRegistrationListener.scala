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

import org.apache.spark.scheduler._
import scala.collection.mutable.ArrayBuffer
import scala.collection.mutable

/** A listener for block manager registration */
private[spark] class BlockManagerRegistrationListener {

  private var _listenerBus: Option[SparkListenerBus] = None

  // Buffer any events received before the listener bus is ready
  private val bufferedEvents = new ArrayBuffer[SparkListenerEvent]
    with mutable.SynchronizedBuffer[SparkListenerEvent]

  /**
   * Set the listener bus. If there are buffered events, post them all to the listener bus at once.
   */
  def setListenerBus(listenerBus: SparkListenerBus) = {
    _listenerBus = Some(listenerBus)
    bufferedEvents.map(listenerBus.post)
  }

  /**
   * Called when a new BlockManager is registered with the master. If the listener bus is ready,
   * post the event; otherwise, buffer it.
   */
  def onBlockManagerRegister(storageStatus: Array[StorageStatus]) {
    val executorsStateChange = new SparkListenerExecutorsStateChange(storageStatus)
    _listenerBus.map(_.post(executorsStateChange)).getOrElse {
      bufferedEvents += executorsStateChange
    }
  }
}
