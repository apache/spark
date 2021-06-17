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

package org.apache.spark.deploy.history

import java.util.concurrent.atomic.AtomicLong

import scala.collection.mutable.HashMap

import org.apache.spark.SparkConf
import org.apache.spark.internal.Logging
import org.apache.spark.internal.config.History._
import org.apache.spark.util.Utils

/**
 * A class used to keep track of in-memory store usage by the SHS.
 */
private class HistoryServerMemoryManager(
    conf: SparkConf) extends Logging {

  private val maxUsage = conf.get(MAX_IN_MEMORY_STORE_USAGE)
  // Visible for testing.
  private[history] val currentUsage = new AtomicLong(0L)
  private[history] val active = new HashMap[(String, Option[String]), Long]()

  def initialize(): Unit = {
    logInfo("Initialized memory manager: " +
      s"current usage = ${Utils.bytesToString(currentUsage.get())}, " +
      s"max usage = ${Utils.bytesToString(maxUsage)}")
  }

  def lease(
      appId: String,
      attemptId: Option[String],
      eventLogSize: Long,
      codec: Option[String]): Unit = {
    val memoryUsage = approximateMemoryUsage(eventLogSize, codec)
    if (memoryUsage + currentUsage.get > maxUsage) {
      throw new RuntimeException("Not enough memory to create hybrid store " +
        s"for app $appId / $attemptId.")
    }
    active.synchronized {
      active(appId -> attemptId) = memoryUsage
    }
    currentUsage.addAndGet(memoryUsage)
    logInfo(s"Leasing ${Utils.bytesToString(memoryUsage)} memory usage for " +
      s"app $appId / $attemptId")
  }

  def release(appId: String, attemptId: Option[String]): Unit = {
    val memoryUsage = active.synchronized { active.remove(appId -> attemptId) }

    memoryUsage match {
      case Some(m) =>
        currentUsage.addAndGet(-m)
        logInfo(s"Released ${Utils.bytesToString(m)} memory usage for " +
          s"app $appId / $attemptId")
      case None =>
    }
  }

  private def approximateMemoryUsage(eventLogSize: Long, codec: Option[String]): Long = {
    codec match {
      case Some("zstd") =>
        eventLogSize * 10
      case Some(_) =>
        eventLogSize * 4
      case None =>
        eventLogSize / 2
    }
  }
}
