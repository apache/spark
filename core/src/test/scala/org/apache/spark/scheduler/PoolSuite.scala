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

/**
 * Tests that pools and the associated scheduling algorithms for FIFO and fair scheduling work
 * correctly.
 */
class PoolSuite extends SparkFunSuite with LocalSparkContext {

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
    sc = new SparkContext("local", "TaskSchedulerImplSuite")
    val taskScheduler = new TaskSchedulerImpl(sc)

    val rootPool = new Pool("", SchedulingMode.FIFO, 0, Int.MaxValue, 0)
    val schedulableBuilder = new FIFOSchedulableBuilder(rootPool)
    schedulableBuilder.buildPools()

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
    val conf = new SparkConf().set("spark.scheduler.allocation.file", xmlPath)
    sc = new SparkContext("local", "TaskSchedulerImplSuite", conf)
    val taskScheduler = new TaskSchedulerImpl(sc)

    val rootPool = new Pool("", SchedulingMode.FAIR, 0, Int.MaxValue, 0)
    val schedulableBuilder = new FairSchedulableBuilder(rootPool, sc.conf)
    schedulableBuilder.buildPools()

    // Ensure that the XML file was read in correctly.
    assert(rootPool.getSchedulableByName("default") != null)
    assert(rootPool.getSchedulableByName("1") != null)
    assert(rootPool.getSchedulableByName("2") != null)
    assert(rootPool.getSchedulableByName("3") != null)
    assert(rootPool.getSchedulableByName("1").minShare === 2)
    assert(rootPool.getSchedulableByName("1").maxRunningTasks === 512)
    assert(rootPool.getSchedulableByName("1").weight === 1)
    assert(rootPool.getSchedulableByName("2").minShare === 3)
    assert(rootPool.getSchedulableByName("2").maxRunningTasks === 256)
    assert(rootPool.getSchedulableByName("2").weight === 1)
    assert(rootPool.getSchedulableByName("3").minShare === 0)
    assert(rootPool.getSchedulableByName("3").maxRunningTasks === Int.MaxValue)
    assert(rootPool.getSchedulableByName("3").weight === 1)

    val properties1 = new Properties()
    properties1.setProperty("spark.scheduler.pool", "1")
    val properties2 = new Properties()
    properties2.setProperty("spark.scheduler.pool", "2")

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
    val xmlPath = getClass.getClassLoader.getResource("nestedpool.xml").getFile()
    val conf = new SparkConf().set("spark.scheduler.allocation.file", xmlPath)
    sc = new SparkContext("local", "TaskSchedulerImplSuite", conf)
    val taskScheduler = new TaskSchedulerImpl(sc)

    val rootPool = new Pool("", SchedulingMode.FAIR, 0, Int.MaxValue, 0)
    val schedulableBuilder = new FairSchedulableBuilder(rootPool, sc.conf)
    schedulableBuilder.buildPools()

    val pool0 = rootPool.getSchedulableByName("0")
    val pool1 = rootPool.getSchedulableByName("1")
    val pool00 = rootPool.getSchedulableByName("00")
    val pool01 = rootPool.getSchedulableByName("01")
    val pool10 = rootPool.getSchedulableByName("10")
    val pool11 = rootPool.getSchedulableByName("11")

    // Ensure that the XML file was read in correctly.
    assert(pool0 != null)
    assert(pool1 != null)
    assert(pool00 != null)
    assert(pool01 != null)
    assert(pool10 != null)
    assert(pool11 != null)

    assert(pool0.minShare === 3)
    assert(pool0.maxRunningTasks === 1024)
    assert(pool0.weight === 1)

    assert(pool1.minShare === 4)
    assert(pool1.maxRunningTasks === 128)
    assert(pool1.weight === 1)

    assert(pool00.minShare === 2)
    assert(pool00.maxRunningTasks === 512)
    assert(pool00.weight === 2)
    assert(pool00.parent === pool0)

    assert(pool01.minShare === 1)
    assert(pool01.maxRunningTasks === 128)
    assert(pool01.weight === 1)
    assert(pool01.parent === pool0)

    assert(pool10.minShare === 2)
    assert(pool10.maxRunningTasks === 128) // not 256 due to parent
    assert(pool10.weight === 2)
    assert(pool10.parent === pool1)

    assert(pool11.minShare === 2)
    assert(pool11.maxRunningTasks === 64)
    assert(pool11.weight === 1)
    assert(pool11.parent === pool1)

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
}
