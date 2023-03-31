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

package org.apache.spark.broadcast

import scala.reflect.ClassTag

import org.apache.spark.SparkConf
import org.apache.spark.SparkEnv

/**
 * A [[org.apache.spark.broadcast.Broadcast]] implementation that is lightweight and low latency.
 * Refer to [[org.apache.spark.broadcast.SmallBroadcast]] for more details.
 */
private[spark] class SmallBroadcastFactory extends BroadcastFactory {

  private var isDriver = false

  override def initialize(isDriver: Boolean, conf: SparkConf): Unit = {
    this.isDriver = isDriver
  }

  override def newBroadcast[T: ClassTag](
    value_ : T,
    isLocal: Boolean,
    id: Long,
    serializedOnly: Boolean = false): Broadcast[T] = {
    assert(isDriver, "can only create SmallBroadcast on driver")
    // serializedOnly and isLocal params are not used in SmallBroadcast
    SparkEnv.get.broadcastManager.smallBroadcastTracker.register(id, value_)
    new SmallBroadcast[T](value_, id)
  }

  override def stop(): Unit = { }

  override def unbroadcast(id: Long, removeFromDriver: Boolean, blocking: Boolean): Unit = {
    SparkEnv.get.broadcastManager.smallBroadcastTracker.destroy(id, blocking)
  }
}
