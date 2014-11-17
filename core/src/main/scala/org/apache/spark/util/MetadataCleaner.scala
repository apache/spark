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

package org.apache.spark.util

import java.util.{Timer, TimerTask}

import org.apache.spark.{Logging, SparkConf}

/**
 * Runs a timer task to periodically clean up metadata (e.g. old files or hashtable entries)
 */
private[spark] class MetadataCleaner(
    cleanerType: MetadataCleanerType.MetadataCleanerType,
    cleanupFunc: (Long) => Unit,
    conf: SparkConf)
  extends Logging
{
  val name = cleanerType.toString

  private val delaySeconds = MetadataCleaner.getDelaySeconds(conf, cleanerType)
  private val periodSeconds = math.max(10, delaySeconds / 10)
  private val timer = new Timer(name + " cleanup timer", true)


  private val task = new TimerTask {
    override def run() {
      try {
        cleanupFunc(System.currentTimeMillis() - (delaySeconds * 1000))
        logInfo("Ran metadata cleaner for " + name)
      } catch {
        case e: Exception => logError("Error running cleanup task for " + name, e)
      }
    }
  }

  if (delaySeconds > 0) {
    logDebug(
      "Starting metadata cleaner for " + name + " with delay of " + delaySeconds + " seconds " +
      "and period of " + periodSeconds + " secs")
    timer.schedule(task, delaySeconds * 1000, periodSeconds * 1000)
  }

  def cancel() {
    timer.cancel()
  }
}

private[spark] object MetadataCleanerType extends Enumeration {

  val MAP_OUTPUT_TRACKER, SPARK_CONTEXT, HTTP_BROADCAST, BLOCK_MANAGER,
  SHUFFLE_BLOCK_MANAGER, BROADCAST_VARS = Value

  type MetadataCleanerType = Value

  def systemProperty(which: MetadataCleanerType.MetadataCleanerType) =
      "spark.cleaner.ttl." + which.toString
}

// TODO: This mutates a Conf to set properties right now, which is kind of ugly when used in the
// initialization of StreamingContext. It's okay for users trying to configure stuff themselves.
private[spark] object MetadataCleaner {
  def getDelaySeconds(conf: SparkConf) = {
    conf.getInt("spark.cleaner.ttl", -1)
  }

  def getDelaySeconds(
      conf: SparkConf,
      cleanerType: MetadataCleanerType.MetadataCleanerType): Int = {
    conf.get(MetadataCleanerType.systemProperty(cleanerType), getDelaySeconds(conf).toString).toInt
  }

  def setDelaySeconds(
      conf: SparkConf,
      cleanerType: MetadataCleanerType.MetadataCleanerType,
      delay: Int) {
    conf.set(MetadataCleanerType.systemProperty(cleanerType),  delay.toString)
  }

  /**
   * Set the default delay time (in seconds).
   * @param conf SparkConf instance
   * @param delay default delay time to set
   * @param resetAll whether to reset all to default
   */
  def setDelaySeconds(conf: SparkConf, delay: Int, resetAll: Boolean = true) {
    conf.set("spark.cleaner.ttl", delay.toString)
    if (resetAll) {
      for (cleanerType <- MetadataCleanerType.values) {
        System.clearProperty(MetadataCleanerType.systemProperty(cleanerType))
      }
    }
  }
}

