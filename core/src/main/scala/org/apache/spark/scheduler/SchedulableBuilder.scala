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

import java.io.InputStream
import java.util.{Locale, NoSuchElementException, Properties}

import scala.util.control.NonFatal
import scala.xml.{Node, XML}

import org.apache.hadoop.fs.Path

import org.apache.spark.SparkContext
import org.apache.spark.internal.{Logging, MDC}
import org.apache.spark.internal.LogKey.PATH
import org.apache.spark.internal.config.{SCHEDULER_ALLOCATION_FILE, SCHEDULER_MODE}
import org.apache.spark.scheduler.SchedulingMode.SchedulingMode
import org.apache.spark.util.Utils

/**
 * An interface to build Schedulable tree
 * buildPools: build the tree nodes(pools)
 * addTaskSetManager: build the leaf nodes(TaskSetManagers)
 */
private[spark] trait SchedulableBuilder {
  def rootPool: Pool

  def buildPools(): Unit

  def addTaskSetManager(manager: Schedulable, properties: Properties): Unit
}

private[spark] class FIFOSchedulableBuilder(val rootPool: Pool)
  extends SchedulableBuilder with Logging {

  override def buildPools(): Unit = {
    // nothing
  }

  override def addTaskSetManager(manager: Schedulable, properties: Properties): Unit = {
    rootPool.addSchedulable(manager)
  }
}

private[spark] class FairSchedulableBuilder(val rootPool: Pool, sc: SparkContext)
  extends SchedulableBuilder with Logging {

  val schedulerAllocFile = sc.conf.get(SCHEDULER_ALLOCATION_FILE)
  val DEFAULT_SCHEDULER_FILE = "fairscheduler.xml"
  val FAIR_SCHEDULER_PROPERTIES = SparkContext.SPARK_SCHEDULER_POOL
  val DEFAULT_POOL_NAME = "default"
  val MINIMUM_SHARES_PROPERTY = "minShare"
  val SCHEDULING_MODE_PROPERTY = "schedulingMode"
  val WEIGHT_PROPERTY = "weight"
  val POOL_NAME_PROPERTY = "@name"
  val POOLS_PROPERTY = "pool"
  val DEFAULT_SCHEDULING_MODE = SchedulingMode.FIFO
  val DEFAULT_MINIMUM_SHARE = 0
  val DEFAULT_WEIGHT = 1

  override def buildPools(): Unit = {
    var fileData: Option[(InputStream, String)] = None
    try {
      fileData = schedulerAllocFile.map { f =>
        val filePath = new Path(f)
        val fis = filePath.getFileSystem(sc.hadoopConfiguration).open(filePath)
        logInfo(s"Creating Fair Scheduler pools from $f")
        Some((fis, f))
      }.getOrElse {
        val is = Utils.getSparkClassLoader.getResourceAsStream(DEFAULT_SCHEDULER_FILE)
        if (is != null) {
          logInfo(s"Creating Fair Scheduler pools from default file: $DEFAULT_SCHEDULER_FILE")
          Some((is, DEFAULT_SCHEDULER_FILE))
        } else {
          val schedulingMode = SchedulingMode.withName(sc.conf.get(SCHEDULER_MODE))
          rootPool.addSchedulable(new Pool(
            DEFAULT_POOL_NAME, schedulingMode, DEFAULT_MINIMUM_SHARE, DEFAULT_WEIGHT))
          logInfo("Fair scheduler configuration not found, created default pool: " +
            "%s, schedulingMode: %s, minShare: %d, weight: %d".format(
            DEFAULT_POOL_NAME, schedulingMode, DEFAULT_MINIMUM_SHARE, DEFAULT_WEIGHT))
          None
        }
      }

      fileData.foreach { case (is, fileName) => buildFairSchedulerPool(is, fileName) }
    } catch {
      case NonFatal(t) =>
        if (fileData.isDefined) {
          val fileName = fileData.get._2
          logError(log"Error while building the fair scheduler pools from ${MDC(PATH, fileName)}",
            t)
        } else {
          logError("Error while building the fair scheduler pools", t)
        }
        throw t
    } finally {
      fileData.foreach { case (is, fileName) => is.close() }
    }

    // finally create "default" pool
    buildDefaultPool()
  }

  private def buildDefaultPool(): Unit = {
    if (rootPool.getSchedulableByName(DEFAULT_POOL_NAME) == null) {
      val pool = new Pool(DEFAULT_POOL_NAME, DEFAULT_SCHEDULING_MODE,
        DEFAULT_MINIMUM_SHARE, DEFAULT_WEIGHT)
      rootPool.addSchedulable(pool)
      logInfo("Created default pool: %s, schedulingMode: %s, minShare: %d, weight: %d".format(
        DEFAULT_POOL_NAME, DEFAULT_SCHEDULING_MODE, DEFAULT_MINIMUM_SHARE, DEFAULT_WEIGHT))
    }
  }

  private def buildFairSchedulerPool(is: InputStream, fileName: String): Unit = {
    val xml = XML.load(is)
    for (poolNode <- (xml \\ POOLS_PROPERTY)) {

      val poolName = (poolNode \ POOL_NAME_PROPERTY).text

      val schedulingMode = getSchedulingModeValue(poolNode, poolName,
        DEFAULT_SCHEDULING_MODE, fileName)
      val minShare = getIntValue(poolNode, poolName, MINIMUM_SHARES_PROPERTY,
        DEFAULT_MINIMUM_SHARE, fileName)
      val weight = getIntValue(poolNode, poolName, WEIGHT_PROPERTY,
        DEFAULT_WEIGHT, fileName)

      rootPool.addSchedulable(new Pool(poolName, schedulingMode, minShare, weight))

      logInfo("Created pool: %s, schedulingMode: %s, minShare: %d, weight: %d".format(
        poolName, schedulingMode, minShare, weight))
    }
  }

  private def getSchedulingModeValue(
      poolNode: Node,
      poolName: String,
      defaultValue: SchedulingMode,
      fileName: String): SchedulingMode = {

    val xmlSchedulingMode =
      (poolNode \ SCHEDULING_MODE_PROPERTY).text.trim.toUpperCase(Locale.ROOT)
    val warningMessage = s"Unsupported schedulingMode: $xmlSchedulingMode found in " +
      s"Fair Scheduler configuration file: $fileName, using " +
      s"the default schedulingMode: $defaultValue for pool: $poolName"
    try {
      if (SchedulingMode.withName(xmlSchedulingMode) != SchedulingMode.NONE) {
        SchedulingMode.withName(xmlSchedulingMode)
      } else {
        logWarning(warningMessage)
        defaultValue
      }
    } catch {
      case e: NoSuchElementException =>
        logWarning(warningMessage)
        defaultValue
    }
  }

  private def getIntValue(
      poolNode: Node,
      poolName: String,
      propertyName: String,
      defaultValue: Int,
      fileName: String): Int = {

    val data = (poolNode \ propertyName).text.trim
    try {
      data.toInt
    } catch {
      case e: NumberFormatException =>
        logWarning(s"Error while loading fair scheduler configuration from $fileName: " +
          s"$propertyName is blank or invalid: $data, using the default $propertyName: " +
          s"$defaultValue for pool: $poolName")
        defaultValue
    }
  }

  override def addTaskSetManager(manager: Schedulable, properties: Properties): Unit = {
    val poolName = if (properties != null) {
        properties.getProperty(FAIR_SCHEDULER_PROPERTIES, DEFAULT_POOL_NAME)
      } else {
        DEFAULT_POOL_NAME
      }
    var parentPool = rootPool.getSchedulableByName(poolName)
    if (parentPool == null) {
      // we will create a new pool that user has configured in app
      // instead of being defined in xml file
      parentPool = new Pool(poolName, DEFAULT_SCHEDULING_MODE,
        DEFAULT_MINIMUM_SHARE, DEFAULT_WEIGHT)
      rootPool.addSchedulable(parentPool)
      logWarning(s"A job was submitted with scheduler pool $poolName, which has not been " +
        "configured. This can happen when the file that pools are read from isn't set, or " +
        s"when that file doesn't contain $poolName. Created $poolName with default " +
        s"configuration (schedulingMode: $DEFAULT_SCHEDULING_MODE, " +
        s"minShare: $DEFAULT_MINIMUM_SHARE, weight: $DEFAULT_WEIGHT)")
    }
    parentPool.addSchedulable(manager)
    logInfo("Added task set " + manager.name + " tasks to pool " + poolName)
  }
}
