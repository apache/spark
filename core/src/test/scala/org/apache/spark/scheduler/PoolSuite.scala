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

import java.util.Properties

import org.apache.spark.{LocalSparkContext, SparkConf, SparkContext, SparkFunSuite}
import org.apache.spark.scheduler.SchedulingMode._

/**
 * Tests that pools and the associated scheduling algorithms for FIFO and fair scheduling work
 * correctly.
 */
class PoolSuite extends SparkFunSuite with LocalSparkContext {

  val LOCAL = "local"
  val APP_NAME = "PoolSuite"
  val SCHEDULER_ALLOCATION_FILE_PROPERTY = "spark.scheduler.allocation.file"

  def createTaskSetManager(stageId: Int, numTasks: Int, taskScheduler: TaskSchedulerImpl)
    : TaskSetManager = {
    val tasks = Array.tabulate[Task[_]](numTasks) { i =>
      new FakeTask(stageId, i, Nil)
    }
    new TaskSetManager(taskScheduler, new TaskSet(tasks, stageId, 0, 0, null), 0)
  }

  def scheduleTaskAndVerifyId(taskId: Int, rootPool: Pool, expectedStageId: Int) {
    val taskSetQueue = rootPool.getSortedTaskSetQueue
    val nextTaskSetToSchedule =
      taskSetQueue.find(t => (t.runningTasks + t.tasksSuccessful) < t.numTasks)
    assert(nextTaskSetToSchedule.isDefined)
    nextTaskSetToSchedule.get.addRunningTask(taskId)
    assert(nextTaskSetToSchedule.get.stageId === expectedStageId)
  }

  test("FIFO Scheduler Test") {
    sc = new SparkContext(LOCAL, APP_NAME)
    val taskScheduler = new TaskSchedulerImpl(sc)

    val rootPool = new Pool("", FIFO, 0, 0)
    val schedulableBuilder = new FIFOSchedulableBuilder(rootPool)

    val taskSetManager0 = createTaskSetManager(0, 2, taskScheduler)
    val taskSetManager1 = createTaskSetManager(1, 2, taskScheduler)
    val taskSetManager2 = createTaskSetManager(2, 2, taskScheduler)
    schedulableBuilder.addTaskSetManager(taskSetManager0, null)
    schedulableBuilder.addTaskSetManager(taskSetManager1, null)
    schedulableBuilder.addTaskSetManager(taskSetManager2, null)

    scheduleTaskAndVerifyId(0, rootPool, 0)
    scheduleTaskAndVerifyId(1, rootPool, 0)
    scheduleTaskAndVerifyId(2, rootPool, 1)
    scheduleTaskAndVerifyId(3, rootPool, 1)
    scheduleTaskAndVerifyId(4, rootPool, 2)
    scheduleTaskAndVerifyId(5, rootPool, 2)
  }

  /**
   * This test creates three scheduling pools, and creates task set managers in the first
   * two scheduling pools. The test verifies that as tasks are scheduled, the fair scheduling
   * algorithm properly orders the two scheduling pools.
   */
  test("Fair Scheduler Test") {
    val xmlPath = getClass.getClassLoader.getResource("fairscheduler.xml").getFile()
    val conf = new SparkConf().set(SCHEDULER_ALLOCATION_FILE_PROPERTY, xmlPath)
    sc = new SparkContext(LOCAL, APP_NAME, conf)
    val taskScheduler = new TaskSchedulerImpl(sc)

    val rootPool = new Pool("", FAIR, 0, 0)
    val schedulableBuilder = new FairSchedulableBuilder(rootPool, sc.conf)
    schedulableBuilder.buildPools()

    // Ensure that the XML file was read in correctly.
    verifyPool(rootPool, schedulableBuilder.DEFAULT_POOL_NAME, 0, 1, FIFO)
    verifyPool(rootPool, "1", 2, 1, FIFO)
    verifyPool(rootPool, "2", 3, 1, FIFO)
    verifyPool(rootPool, "3", 0, 1, FIFO)

    val properties1 = new Properties()
    properties1.setProperty(schedulableBuilder.FAIR_SCHEDULER_PROPERTIES, "1")
    val properties2 = new Properties()
    properties2.setProperty(schedulableBuilder.FAIR_SCHEDULER_PROPERTIES, "2")

    val taskSetManager10 = createTaskSetManager(0, 1, taskScheduler)
    val taskSetManager11 = createTaskSetManager(1, 1, taskScheduler)
    val taskSetManager12 = createTaskSetManager(2, 2, taskScheduler)
    schedulableBuilder.addTaskSetManager(taskSetManager10, properties1)
    schedulableBuilder.addTaskSetManager(taskSetManager11, properties1)
    schedulableBuilder.addTaskSetManager(taskSetManager12, properties1)

    val taskSetManager23 = createTaskSetManager(3, 2, taskScheduler)
    val taskSetManager24 = createTaskSetManager(4, 2, taskScheduler)
    schedulableBuilder.addTaskSetManager(taskSetManager23, properties2)
    schedulableBuilder.addTaskSetManager(taskSetManager24, properties2)

    // Pool 1 share ratio: 0. Pool 2 share ratio: 0. 1 gets scheduled based on ordering of names.
    scheduleTaskAndVerifyId(0, rootPool, 0)
    // Pool 1 share ratio: 1/2. Pool 2 share ratio: 0. 2 gets scheduled because ratio is lower.
    scheduleTaskAndVerifyId(1, rootPool, 3)
    // Pool 1 share ratio: 1/2. Pool 2 share ratio: 1/3. 2 gets scheduled because ratio is lower.
    scheduleTaskAndVerifyId(2, rootPool, 3)
    // Pool 1 share ratio: 1/2. Pool 2 share ratio: 2/3. 1 gets scheduled because ratio is lower.
    scheduleTaskAndVerifyId(3, rootPool, 1)
    // Pool 1 share ratio: 1. Pool 2 share ratio: 2/3. 2 gets scheduled because ratio is lower.
    scheduleTaskAndVerifyId(4, rootPool, 4)
    // Neither pool is needy so ordering is based on number of running tasks.
    // Pool 1 running tasks: 2, Pool 2 running tasks: 3. 1 gets scheduled because fewer running
    // tasks.
    scheduleTaskAndVerifyId(5, rootPool, 2)
    // Pool 1 running tasks: 3, Pool 2 running tasks: 3. 1 gets scheduled because of naming
    // ordering.
    scheduleTaskAndVerifyId(6, rootPool, 2)
    // Pool 1 running tasks: 4, Pool 2 running tasks: 3. 2 gets scheduled because fewer running
    // tasks.
    scheduleTaskAndVerifyId(7, rootPool, 4)
  }

  test("Nested Pool Test") {
    sc = new SparkContext(LOCAL, APP_NAME)
    val taskScheduler = new TaskSchedulerImpl(sc)

    val rootPool = new Pool("", FAIR, 0, 0)
    val pool0 = new Pool("0", FAIR, 3, 1)
    val pool1 = new Pool("1", FAIR, 4, 1)
    rootPool.addSchedulable(pool0)
    rootPool.addSchedulable(pool1)

    val pool00 = new Pool("00", FAIR, 2, 2)
    val pool01 = new Pool("01", FAIR, 1, 1)
    pool0.addSchedulable(pool00)
    pool0.addSchedulable(pool01)

    val pool10 = new Pool("10", FAIR, 2, 2)
    val pool11 = new Pool("11", FAIR, 2, 1)
    pool1.addSchedulable(pool10)
    pool1.addSchedulable(pool11)

    val taskSetManager000 = createTaskSetManager(0, 5, taskScheduler)
    val taskSetManager001 = createTaskSetManager(1, 5, taskScheduler)
    pool00.addSchedulable(taskSetManager000)
    pool00.addSchedulable(taskSetManager001)

    val taskSetManager010 = createTaskSetManager(2, 5, taskScheduler)
    val taskSetManager011 = createTaskSetManager(3, 5, taskScheduler)
    pool01.addSchedulable(taskSetManager010)
    pool01.addSchedulable(taskSetManager011)

    val taskSetManager100 = createTaskSetManager(4, 5, taskScheduler)
    val taskSetManager101 = createTaskSetManager(5, 5, taskScheduler)
    pool10.addSchedulable(taskSetManager100)
    pool10.addSchedulable(taskSetManager101)

    val taskSetManager110 = createTaskSetManager(6, 5, taskScheduler)
    val taskSetManager111 = createTaskSetManager(7, 5, taskScheduler)
    pool11.addSchedulable(taskSetManager110)
    pool11.addSchedulable(taskSetManager111)

    scheduleTaskAndVerifyId(0, rootPool, 0)
    scheduleTaskAndVerifyId(1, rootPool, 4)
    scheduleTaskAndVerifyId(2, rootPool, 6)
    scheduleTaskAndVerifyId(3, rootPool, 2)
  }

  test("SPARK-17663: FairSchedulableBuilder sets default values for blank or invalid datas") {
    val xmlPath = getClass.getClassLoader.getResource("fairscheduler-with-invalid-data.xml")
      .getFile()
    val conf = new SparkConf().set(SCHEDULER_ALLOCATION_FILE_PROPERTY, xmlPath)

    val rootPool = new Pool("", FAIR, 0, 0)
    val schedulableBuilder = new FairSchedulableBuilder(rootPool, conf)
    schedulableBuilder.buildPools()

    verifyPool(rootPool, schedulableBuilder.DEFAULT_POOL_NAME, 0, 1, FIFO)
    verifyPool(rootPool, "pool_with_invalid_min_share", 0, 2, FAIR)
    verifyPool(rootPool, "pool_with_invalid_weight", 1, 1, FAIR)
    verifyPool(rootPool, "pool_with_invalid_scheduling_mode", 3, 2, FIFO)
    verifyPool(rootPool, "pool_with_non_uppercase_scheduling_mode", 2, 1, FAIR)
    verifyPool(rootPool, "pool_with_NONE_scheduling_mode", 1, 2, FIFO)
    verifyPool(rootPool, "pool_with_whitespace_min_share", 0, 2, FAIR)
    verifyPool(rootPool, "pool_with_whitespace_weight", 1, 1, FAIR)
    verifyPool(rootPool, "pool_with_whitespace_scheduling_mode", 3, 2, FIFO)
    verifyPool(rootPool, "pool_with_empty_min_share", 0, 3, FAIR)
    verifyPool(rootPool, "pool_with_empty_weight", 2, 1, FAIR)
    verifyPool(rootPool, "pool_with_empty_scheduling_mode", 2, 2, FIFO)
    verifyPool(rootPool, "pool_with_surrounded_whitespace", 3, 2, FAIR)
  }

  private def verifyPool(rootPool: Pool, poolName: String, expectedInitMinShare: Int,
                         expectedInitWeight: Int, expectedSchedulingMode: SchedulingMode): Unit = {
    assert(rootPool.getSchedulableByName(poolName) != null)
    assert(rootPool.getSchedulableByName(poolName).minShare === expectedInitMinShare)
    assert(rootPool.getSchedulableByName(poolName).weight === expectedInitWeight)
    assert(rootPool.getSchedulableByName(poolName).schedulingMode === expectedSchedulingMode)
  }

}
