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

import java.nio.charset.StandardCharsets

import org.apache.hadoop.fs.Path

import org.apache.spark.SparkConf
import org.apache.spark.internal.config._

object EventLogTestHelper {
  def getUniqueApplicationId: String = "test-" + System.currentTimeMillis

  /**
   * Get a SparkConf with event logging enabled. It doesn't enable rolling event logs, so caller
   * should set it manually.
   */
  def getLoggingConf(logDir: Path, compressionCodec: Option[String] = None): SparkConf = {
    val conf = new SparkConf
    conf.set(EVENT_LOG_ENABLED, true)
    conf.set(EVENT_LOG_BLOCK_UPDATES, true)
    conf.set(EVENT_LOG_TESTING, true)
    conf.set(EVENT_LOG_DIR, logDir.toString)
    compressionCodec.foreach { codec =>
      conf.set(EVENT_LOG_COMPRESS, true)
      conf.set(EVENT_LOG_COMPRESSION_CODEC, codec)
    }
    conf.set(EVENT_LOG_STAGE_EXECUTOR_METRICS, true)
    conf
  }

  def writeTestEvents(
      writer: EventLogFileWriter,
      eventStr: String,
      desiredSize: Long): Seq[String] = {
    val stringLen = eventStr.getBytes(StandardCharsets.UTF_8).length
    val repeatCount = Math.floor(desiredSize / stringLen).toInt
    (0 until repeatCount).map { _ =>
      writer.writeEvent(eventStr, flushLogger = true)
      eventStr
    }
  }
}
