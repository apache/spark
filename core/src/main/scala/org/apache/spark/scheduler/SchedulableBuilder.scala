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

import java.io.{FileInputStream, InputStream}
import java.util.{NoSuchElementException, Properties}

import scala.xml.XML

import org.apache.spark.SparkConf
import org.apache.spark.internal.Logging
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

  override def buildPools() {
    // nothing
  }

  override def addTaskSetManager(manager: Schedulable, properties: Properties) {
    rootPool.addSchedulable(manager)
  }
}

private[spark] class FairSchedulableBuilder(val rootPool: Pool, conf: SparkConf)
  extends SchedulableBuilder with Logging {

  val schedulerAllocFile = conf.getOption("spark.scheduler.allocation.file")
  val DEFAULT_SCHEDULER_FILE = "fairscheduler.xml"
  val FAIR_SCHEDULER_PROPERTIES = "spark.scheduler.pool"
  val DEFAULT_POOL_NAME = "default"
  val MINIMUM_SHARES_PROPERTY = "minShare"
  val MAXIMUM_RUNNING_TASKS_PROPERTY = "maxRunningTasks"
  val SCHEDULING_MODE_PROPERTY = "schedulingMode"
  val PARENT_PROPERTY = "parent"
  val WEIGHT_PROPERTY = "weight"
  val POOL_NAME_PROPERTY = "@name"
  val POOLS_PROPERTY = "pool"
  val DEFAULT_SCHEDULING_MODE = SchedulingMode.FIFO
  val DEFAULT_MINIMUM_SHARE = 0
  val DEFAULT_MAXIMUM_RUNNING_TASKS = Int.MaxValue
  val DEFAULT_WEIGHT = 1

  override def buildPools() {
    var is: Option[InputStream] = None
    try {
      is = Option {
        schedulerAllocFile.map { f =>
          new FileInputStream(f)
        }.getOrElse {
          Utils.getSparkClassLoader.getResourceAsStream(DEFAULT_SCHEDULER_FILE)
        }
      }

      is.foreach { i => buildFairSchedulerPool(i) }
    } finally {
      is.foreach(_.close())
    }

    // finally create "default" pool
    buildDefaultPool()
  }

  private def logPoolCreated(pool: Pool) =
      logInfo(s"""Created pool ${pool.name},
        | parent: ${Option(pool.parent).map(_.name).getOrElse("(none)")},
        | schedulingMode: ${pool.schedulingMode},
        | minShare: ${pool.minShare},
        | maxRunningTasks: ${pool.maxRunningTasks},
        | weight: ${pool.weight}""".stripMargin)

  private def buildDefaultPool() {
    if (rootPool.getSchedulableByName(DEFAULT_POOL_NAME) == null) {
      val pool = new Pool(DEFAULT_POOL_NAME, DEFAULT_SCHEDULING_MODE,
        DEFAULT_MINIMUM_SHARE, DEFAULT_MAXIMUM_RUNNING_TASKS, DEFAULT_WEIGHT)
      rootPool.addSchedulable(pool)
      logPoolCreated(pool)
    }
  }

  private def buildFairSchedulerPool(is: InputStream) {
    val xml = XML.load(is)
    for (poolNode <- (xml \\ POOLS_PROPERTY)) {

      val poolName = (poolNode \ POOL_NAME_PROPERTY).text
      var schedulingMode = DEFAULT_SCHEDULING_MODE
      var minShare = DEFAULT_MINIMUM_SHARE
      var maxRunningTasks = DEFAULT_MAXIMUM_RUNNING_TASKS
      var weight = DEFAULT_WEIGHT
      var parent = DEFAULT_POOL_NAME

      val xmlSchedulingMode = (poolNode \ SCHEDULING_MODE_PROPERTY).text
      if (xmlSchedulingMode != "") {
        try {
          schedulingMode = SchedulingMode.withName(xmlSchedulingMode)
        } catch {
          case e: NoSuchElementException =>
            logWarning(s"Unsupported schedulingMode: $xmlSchedulingMode, " +
              s"using the default schedulingMode: $schedulingMode")
        }
      }

      val xmlMinShare = (poolNode \ MINIMUM_SHARES_PROPERTY).text
      if (xmlMinShare != "") {
        minShare = xmlMinShare.toInt
      }

      val xmlMaxShare = (poolNode \ MAXIMUM_RUNNING_TASKS_PROPERTY).text
      if (xmlMaxShare != "") {
        maxRunningTasks = xmlMaxShare.toInt
      }

      val xmlWeight = (poolNode \ WEIGHT_PROPERTY).text
      if (xmlWeight != "") {
        weight = xmlWeight.toInt
      }

      val xmlParent = (poolNode \ PARENT_PROPERTY).text
      if (xmlParent != "") {
        parent = xmlParent
      }

      val pool = new Pool(poolName, schedulingMode, minShare, maxRunningTasks, weight)
      val parentPool = rootPool.getSchedulableByName(parent)
      if (parentPool == null) {
        logWarning(s"couldn't find parent pool $parent, adding pool to the root")
        rootPool.addSchedulable(pool)
      } else {
        parentPool.addSchedulable(pool)
      }
      logPoolCreated(pool)
    }
  }

  override def addTaskSetManager(manager: Schedulable, properties: Properties) {
    var poolName = DEFAULT_POOL_NAME
    var parentPool = rootPool.getSchedulableByName(poolName)
    if (properties != null) {
      poolName = properties.getProperty(FAIR_SCHEDULER_PROPERTIES, DEFAULT_POOL_NAME)
      parentPool = rootPool.getSchedulableByName(poolName)
      if (parentPool == null) {
        // we will create a new pool that user has configured in app
        // instead of being defined in xml file
        val pool = new Pool(poolName, DEFAULT_SCHEDULING_MODE,
          DEFAULT_MINIMUM_SHARE, DEFAULT_MAXIMUM_RUNNING_TASKS, DEFAULT_WEIGHT)
        rootPool.addSchedulable(pool)
        logPoolCreated(pool)
        parentPool = pool
      }
    }
    parentPool.addSchedulable(manager)
    logInfo("Added task set " + manager.name + " tasks to pool " + poolName)
  }
}
