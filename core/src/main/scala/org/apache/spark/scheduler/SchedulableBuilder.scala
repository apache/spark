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
import org.apache.spark.internal.LogKeys
import org.apache.spark.internal.LogKeys._
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
        logInfo(log"Creating Fair Scheduler pools from ${MDC(LogKeys.FILE_NAME, f)}")
        Some((fis, f))
      }.getOrElse {
        val is = Utils.getSparkClassLoader.getResourceAsStream(DEFAULT_SCHEDULER_FILE)
        if (is != null) {
          logInfo(log"Creating Fair Scheduler pools from default file: " +
            log"${MDC(LogKeys.FILE_NAME, DEFAULT_SCHEDULER_FILE)}")
          Some((is, DEFAULT_SCHEDULER_FILE))
        } else {
          val schedulingMode = SchedulingMode.withName(sc.conf.get(SCHEDULER_MODE))
          rootPool.addSchedulable(new Pool(
            DEFAULT_POOL_NAME, schedulingMode, DEFAULT_MINIMUM_SHARE, DEFAULT_WEIGHT))
          logInfo(log"Fair scheduler configuration not found, created default pool: " +
            log"${MDC(LogKeys.DEFAULT_NAME, DEFAULT_POOL_NAME)}, " +
            log"schedulingMode: ${MDC(LogKeys.SCHEDULING_MODE, schedulingMode)}, " +
            log"minShare: ${MDC(LogKeys.MIN_SHARE, DEFAULT_MINIMUM_SHARE)}, " +
            log"weight: ${MDC(LogKeys.WEIGHT, DEFAULT_WEIGHT)}")
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
      logInfo(log"Created default pool: ${MDC(LogKeys.POOL_NAME, DEFAULT_POOL_NAME)}, " +
        log"schedulingMode: ${MDC(LogKeys.SCHEDULING_MODE, DEFAULT_SCHEDULING_MODE)}, " +
        log"minShare: ${MDC(LogKeys.MIN_SHARE, DEFAULT_MINIMUM_SHARE)}, " +
        log"weight: ${MDC(LogKeys.WEIGHT, DEFAULT_WEIGHT)}")
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

      logInfo(log"Created pool: ${MDC(LogKeys.POOL_NAME, poolName)}, " +
        log"schedulingMode: ${MDC(LogKeys.SCHEDULING_MODE, schedulingMode)}, " +
        log"minShare: ${MDC(LogKeys.MIN_SHARE, minShare)}, " +
        log"weight: ${MDC(LogKeys.WEIGHT, weight)}")
    }
  }

  private def getSchedulingModeValue(
      poolNode: Node,
      poolName: String,
      defaultValue: SchedulingMode,
      fileName: String): SchedulingMode = {

    val xmlSchedulingMode =
      (poolNode \ SCHEDULING_MODE_PROPERTY).text.trim.toUpperCase(Locale.ROOT)
    val warningMessage = log"Unsupported schedulingMode: " +
      log"${MDC(XML_SCHEDULING_MODE, xmlSchedulingMode)} found in " +
      log"Fair Scheduler configuration file: ${MDC(FILE_NAME, fileName)}, using " +
      log"the default schedulingMode: " +
      log"${MDC(LogKeys.SCHEDULING_MODE, defaultValue)} for pool: " +
      log"${MDC(POOL_NAME, poolName)}"
    try {
      if (SchedulingMode.withName(xmlSchedulingMode) != SchedulingMode.NONE) {
        SchedulingMode.withName(xmlSchedulingMode)
      } else {
        logWarning(warningMessage)
        defaultValue
      }
    } catch {
      case _: NoSuchElementException =>
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
      case _: NumberFormatException =>
        logWarning(log"Error while loading fair scheduler configuration from " +
          log"${MDC(FILE_NAME, fileName)}: " +
          log"${MDC(PROPERTY_NAME, propertyName)} is blank or invalid: ${MDC(DATA, data)}, " +
          log"using the default ${MDC(DEFAULT_NAME, propertyName)}: " +
          log"${MDC(DEFAULT_VALUE, defaultValue)} for pool: ${MDC(POOL_NAME, poolName)}")
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
      logWarning(log"A job was submitted with scheduler pool " +
        log"${MDC(SCHEDULER_POOL_NAME, poolName)}, which has not been " +
        log"configured. This can happen when the file that pools are read from isn't set, or " +
        log"when that file doesn't contain ${MDC(POOL_NAME, poolName)}. " +
        log"Created ${MDC(CREATED_POOL_NAME, poolName)} with default " +
        log"configuration (schedulingMode: " +
        log"${MDC(LogKeys.SCHEDULING_MODE, DEFAULT_SCHEDULING_MODE)}, " +
        log"minShare: ${MDC(MIN_SHARE, DEFAULT_MINIMUM_SHARE)}, " +
        log"weight: ${MDC(WEIGHT, DEFAULT_WEIGHT)}")
    }
    parentPool.addSchedulable(manager)
    logInfo(log"Added task set ${MDC(LogKeys.TASK_SET_MANAGER, manager.name)} tasks to pool " +
      log"${MDC(LogKeys.POOL_NAME, poolName)}")
  }
}
