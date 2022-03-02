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

import java.util.Collections
import java.util.concurrent.atomic.AtomicLong

import scala.reflect.ClassTag

import org.apache.commons.collections4.map.AbstractReferenceMap.ReferenceStrength
import org.apache.commons.collections4.map.ReferenceMap

import org.apache.spark.SparkConf
import org.apache.spark.api.python.PythonBroadcast
import org.apache.spark.internal.Logging

private[spark] class BroadcastManager(
    val isDriver: Boolean, conf: SparkConf) extends Logging {

  private var initialized = false
  private var broadcastFactory: BroadcastFactory = null

  initialize()

  // Called by SparkContext or Executor before using Broadcast
  private def initialize(): Unit = {
    synchronized {
      if (!initialized) {
        broadcastFactory = new TorrentBroadcastFactory
        broadcastFactory.initialize(isDriver, conf)
        initialized = true
      }
    }
  }

  def stop(): Unit = {
    broadcastFactory.stop()
  }

  private val nextBroadcastId = new AtomicLong(0)

  private[broadcast] val cachedValues =
    Collections.synchronizedMap(
      new ReferenceMap(ReferenceStrength.HARD, ReferenceStrength.WEAK)
        .asInstanceOf[java.util.Map[Any, Any]]
    )

  def newBroadcast[T: ClassTag](value_ : T, isLocal: Boolean): Broadcast[T] = {
    val bid = nextBroadcastId.getAndIncrement()
    value_ match {
      case pb: PythonBroadcast =>
        // SPARK-28486: attach this new broadcast variable's id to the PythonBroadcast,
        // so that underlying data file of PythonBroadcast could be mapped to the
        // BroadcastBlockId according to this id. Please see the specific usage of the
        // id in PythonBroadcast.readObject().
        pb.setBroadcastId(bid)

      case _ => // do nothing
    }
    broadcastFactory.newBroadcast[T](value_, isLocal, bid)
  }

  def unbroadcast(id: Long, removeFromDriver: Boolean, blocking: Boolean): Unit = {
    broadcastFactory.unbroadcast(id, removeFromDriver, blocking)
  }
}
