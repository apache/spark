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

package spark.scheduler.cluster

import java.io.{File, FileInputStream, FileOutputStream}

import scala.collection.mutable.ArrayBuffer
import scala.collection.mutable.ArrayBuffer
import scala.collection.mutable.HashMap
import scala.collection.mutable.HashSet
import scala.util.control.Breaks._
import scala.xml._

import spark.Logging
import spark.scheduler.cluster.SchedulingMode.SchedulingMode

import java.util.Properties

/**
 * An interface to build Schedulable tree
 * buildPools: build the tree nodes(pools)
 * addTaskSetManager: build the leaf nodes(TaskSetManagers)
 */
private[spark] trait SchedulableBuilder {
  def buildPools()
  def addTaskSetManager(manager: Schedulable, properties: Properties)
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

private[spark] class FairSchedulableBuilder(val rootPool: Pool)
  extends SchedulableBuilder with Logging {

  val schedulerAllocFile = System.getProperty("spark.fairscheduler.allocation.file","unspecified")
  val FAIR_SCHEDULER_PROPERTIES = "spark.scheduler.cluster.fair.pool"
  val DEFAULT_POOL_NAME = "default"
  val MINIMUM_SHARES_PROPERTY = "minShare"
  val SCHEDULING_MODE_PROPERTY = "schedulingMode"
  val WEIGHT_PROPERTY = "weight"
  val POOL_NAME_PROPERTY = "@name"
  val POOLS_PROPERTY = "pool"
  val DEFAULT_SCHEDULING_MODE = SchedulingMode.FIFO
  val DEFAULT_MINIMUM_SHARE = 2
  val DEFAULT_WEIGHT = 1

  override def buildPools() {
    val file = new File(schedulerAllocFile)
    if (file.exists()) {
      val xml = XML.loadFile(file)
      for (poolNode <- (xml \\ POOLS_PROPERTY)) {

        val poolName = (poolNode \ POOL_NAME_PROPERTY).text
        var schedulingMode = DEFAULT_SCHEDULING_MODE
        var minShare = DEFAULT_MINIMUM_SHARE
        var weight = DEFAULT_WEIGHT

        val xmlSchedulingMode = (poolNode \ SCHEDULING_MODE_PROPERTY).text
        if (xmlSchedulingMode != "") {
          try {
            schedulingMode = SchedulingMode.withName(xmlSchedulingMode)
          } catch {
            case e: Exception => logInfo("Error xml schedulingMode, using default schedulingMode")
          }
        }

        val xmlMinShare = (poolNode \ MINIMUM_SHARES_PROPERTY).text
        if (xmlMinShare != "") {
          minShare = xmlMinShare.toInt
        }

        val xmlWeight = (poolNode \ WEIGHT_PROPERTY).text
        if (xmlWeight != "") {
          weight = xmlWeight.toInt
        }

        val pool = new Pool(poolName, schedulingMode, minShare, weight)
        rootPool.addSchedulable(pool)
        logInfo("Create new pool with name:%s,schedulingMode:%s,minShare:%d,weight:%d".format(
          poolName, schedulingMode, minShare, weight))
      }
    }

    // finally create "default" pool
    if (rootPool.getSchedulableByName(DEFAULT_POOL_NAME) == null) {
      val pool = new Pool(DEFAULT_POOL_NAME, DEFAULT_SCHEDULING_MODE,
        DEFAULT_MINIMUM_SHARE, DEFAULT_WEIGHT)
      rootPool.addSchedulable(pool)
      logInfo("Create default pool with name:%s,schedulingMode:%s,minShare:%d,weight:%d".format(
        DEFAULT_POOL_NAME, DEFAULT_SCHEDULING_MODE, DEFAULT_MINIMUM_SHARE, DEFAULT_WEIGHT))
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
        parentPool = new Pool(poolName, DEFAULT_SCHEDULING_MODE,
          DEFAULT_MINIMUM_SHARE, DEFAULT_WEIGHT)
        rootPool.addSchedulable(parentPool)
        logInfo("Create pool with name:%s,schedulingMode:%s,minShare:%d,weight:%d".format(
          poolName, DEFAULT_SCHEDULING_MODE, DEFAULT_MINIMUM_SHARE, DEFAULT_WEIGHT))
      }
    }
    parentPool.addSchedulable(manager)
    logInfo("Added task set " + manager.name + " tasks to pool "+poolName)
  }
}
