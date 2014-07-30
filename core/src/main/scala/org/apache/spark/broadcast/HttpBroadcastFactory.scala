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

import org.apache.spark.{SecurityManager, SparkConf}

/**
 * A [[org.apache.spark.broadcast.BroadcastFactory]] implementation that uses a
 * HTTP server as the broadcast mechanism. Refer to
 * [[org.apache.spark.broadcast.HttpBroadcast]] for more details about this mechanism.
 */
class HttpBroadcastFactory extends BroadcastFactory {
  override def initialize(isDriver: Boolean, conf: SparkConf, securityMgr: SecurityManager) {
    HttpBroadcast.initialize(isDriver, conf, securityMgr)
  }

  override def newBroadcast[T: ClassTag](value_ : T, isLocal: Boolean, id: Long) =
    new HttpBroadcast[T](value_, isLocal, id)

  override def stop() { HttpBroadcast.stop() }

  /**
   * Remove all persisted state associated with the HTTP broadcast with the given ID.
   * @param removeFromDriver Whether to remove state from the driver
   * @param blocking Whether to block until unbroadcasted
   */
  override def unbroadcast(id: Long, removeFromDriver: Boolean, blocking: Boolean) {
    HttpBroadcast.unpersist(id, removeFromDriver, blocking)
  }
}
