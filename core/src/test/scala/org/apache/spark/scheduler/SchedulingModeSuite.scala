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

import scala.collection.mutable.ArrayBuffer

import org.scalatest.{BeforeAndAfter, FunSuite}
import org.scalatest.mock.EasyMockSugar

import org.apache.spark.{SparkContext, Logging, LocalSparkContext}


class SchedulingModeSuite extends FunSuite with BeforeAndAfter with EasyMockSugar
  with LocalSparkContext with Logging {

  private var taskScheduler: TaskSchedulerImpl = _

  before {
    sc = new SparkContext("local", "TaskSchedulerImplSuite")
    taskScheduler = sc.taskScheduler.asInstanceOf[TaskSchedulerImpl]
  }

  def createDummyTaskSetManager(priority: Int, stage: Int, numTasks: Int, cs: TaskSchedulerImpl,
                                taskSet: TaskSet): FakeTaskSetManager = {
    new FakeTaskSetManager(priority, stage, numTasks, cs , taskSet)
  }

  def checkTaskSetIds(workerOffers: Seq[WorkerOffer], expectedTaskSetIds: Seq[String]) {
    assert(workerOffers.size == expectedTaskSetIds.size)
    for (i <- 0 until workerOffers.size) {
      val scheduledTask = taskScheduler.resourceOffers(Seq[WorkerOffer](workerOffers(i))).flatten
      assert(getTasksetIdFromTaskDescription(scheduledTask(0)) === expectedTaskSetIds(i))
    }
  }

  private def getTasksetIdFromTaskDescription(td: TaskDescription) = {
    """task (\d+\.\d+):\d+""".r.findFirstMatchIn(td.name) match {
      case None => null
      case Some(matchedString) => matchedString.group(1)
    }
  }

  test("FIFO Scheduler Test") {
    val taskSet1 = FakeTask.createTaskSet(1, 0, 0)
    val taskSet2 = FakeTask.createTaskSet(1, 1, 0)
    val taskSet3 = FakeTask.createTaskSet(1, 2, 0)

    val rootPool = new Pool("", SchedulingMode.FIFO, 0, 0)
    val schedulableBuilder = new FIFOSchedulableBuilder(rootPool)
    schedulableBuilder.buildPools()
    taskScheduler.rootPool = rootPool
    taskScheduler.schedulableBuilder = schedulableBuilder

    val taskSetManager0 = createDummyTaskSetManager(0, 0, 1, taskScheduler, taskSet1)
    val taskSetManager1 = createDummyTaskSetManager(0, 1, 1, taskScheduler, taskSet2)
    val taskSetManager2 = createDummyTaskSetManager(0, 2, 1, taskScheduler, taskSet3)
    schedulableBuilder.addTaskSetManager(taskSetManager0, null)
    schedulableBuilder.addTaskSetManager(taskSetManager1, null)
    schedulableBuilder.addTaskSetManager(taskSetManager2, null)

    checkTaskSetIds(generateWorkerOffers(3), Seq[String]("0.0", "1.0", "2.0"))
  }

  def generateWorkerOffers(offerNum: Int): Seq[WorkerOffer] = {
    var s = new ArrayBuffer[WorkerOffer]
    for (i <- 0 until offerNum) {
      s += new WorkerOffer("execId_%d".format(i), "hostname_%s".format(i), 1)
    }
    s
  }

  test("Fair Scheduler Test") {
    val taskSet10 = FakeTask.createTaskSet(1, 0, 0)
    val taskSet11 = FakeTask.createTaskSet(1, 1, 0)
    val taskSet12 = FakeTask.createTaskSet(1, 2, 0)
    val taskSet23 = FakeTask.createTaskSet(1, 3, 0)
    val taskSet24 = FakeTask.createTaskSet(1, 4, 0)

    val xmlPath = getClass.getClassLoader.getResource("fairscheduler.xml").getFile()
    System.setProperty("spark.scheduler.allocation.file", xmlPath)
    val rootPool = new Pool("", SchedulingMode.FAIR, 0, 0)
    val schedulableBuilder = new FairSchedulableBuilder(rootPool, sc.conf)
    schedulableBuilder.buildPools()
    taskScheduler.rootPool = rootPool
    taskScheduler.schedulableBuilder = schedulableBuilder

    assert(rootPool.getSchedulableByName("default") != null)
    assert(rootPool.getSchedulableByName("1") != null)
    assert(rootPool.getSchedulableByName("2") != null)
    assert(rootPool.getSchedulableByName("3") != null)
    assert(rootPool.getSchedulableByName("1").minShare === 2)
    assert(rootPool.getSchedulableByName("1").weight === 1)
    assert(rootPool.getSchedulableByName("2").minShare === 3)
    assert(rootPool.getSchedulableByName("2").weight === 1)
    assert(rootPool.getSchedulableByName("3").minShare === 0)
    assert(rootPool.getSchedulableByName("3").weight === 1)

    val properties1 = new Properties()
    properties1.setProperty("spark.scheduler.pool","1")
    val properties2 = new Properties()
    properties2.setProperty("spark.scheduler.pool","2")

    val taskSetManager10 = createDummyTaskSetManager(1, 0, 1, taskScheduler, taskSet10)
    val taskSetManager11 = createDummyTaskSetManager(1, 1, 1, taskScheduler, taskSet11)
    val taskSetManager12 = createDummyTaskSetManager(1, 2, 2, taskScheduler, taskSet12)
    schedulableBuilder.addTaskSetManager(taskSetManager10, properties1)
    schedulableBuilder.addTaskSetManager(taskSetManager11, properties1)
    schedulableBuilder.addTaskSetManager(taskSetManager12, properties1)

    val taskSetManager23 = createDummyTaskSetManager(2, 3, 2, taskScheduler, taskSet23)
    val taskSetManager24 = createDummyTaskSetManager(2, 4, 2, taskScheduler, taskSet24)
    schedulableBuilder.addTaskSetManager(taskSetManager23, properties2)
    schedulableBuilder.addTaskSetManager(taskSetManager24, properties2)

    val workerOffers = generateWorkerOffers(8)

    checkTaskSetIds(workerOffers,
      Seq[String]("0.0", "3.0", "3.0", "1.0", "4.0", "2.0", "2.0", "4.0"))

    taskSetManager12.taskFinished()
    assert(rootPool.getSchedulableByName("1").runningTasks === 3)
    taskSetManager24.abort()
    assert(rootPool.getSchedulableByName("2").runningTasks === 2)
  }

  test("Nested Pool Test") {
    val taskSet0 = FakeTask.createTaskSet(5, 0, 0)
    val taskSet1 = FakeTask.createTaskSet(5, 1, 0)
    val taskSet2 = FakeTask.createTaskSet(5, 2, 0)
    val taskSet3 = FakeTask.createTaskSet(5, 3, 0)
    val taskSet4 = FakeTask.createTaskSet(5, 4, 0)
    val taskSet5 = FakeTask.createTaskSet(5, 5, 0)
    val taskSet6 = FakeTask.createTaskSet(5, 6, 0)
    val taskSet7 = FakeTask.createTaskSet(5, 7, 0)

    val rootPool = new Pool("", SchedulingMode.FAIR, 0, 0)
    val pool0 = new Pool("0", SchedulingMode.FAIR, 3, 1)
    val pool1 = new Pool("1", SchedulingMode.FAIR, 4, 1)
    rootPool.addSchedulable(pool0)
    rootPool.addSchedulable(pool1)

    val pool00 = new Pool("00", SchedulingMode.FAIR, 2, 2)
    val pool01 = new Pool("01", SchedulingMode.FAIR, 1, 1)
    pool0.addSchedulable(pool00)
    pool0.addSchedulable(pool01)

    val pool10 = new Pool("10", SchedulingMode.FAIR, 2, 2)
    val pool11 = new Pool("11", SchedulingMode.FAIR, 2, 1)
    pool1.addSchedulable(pool10)
    pool1.addSchedulable(pool11)

    val taskSetManager000 = createDummyTaskSetManager(0, 0, 5, taskScheduler, taskSet0)
    val taskSetManager001 = createDummyTaskSetManager(0, 1, 5, taskScheduler, taskSet1)
    pool00.addSchedulable(taskSetManager000)
    pool00.addSchedulable(taskSetManager001)

    val taskSetManager010 = createDummyTaskSetManager(1, 2, 5, taskScheduler, taskSet2)
    val taskSetManager011 = createDummyTaskSetManager(1, 3, 5, taskScheduler, taskSet3)
    pool01.addSchedulable(taskSetManager010)
    pool01.addSchedulable(taskSetManager011)

    val taskSetManager100 = createDummyTaskSetManager(2, 4, 5, taskScheduler, taskSet4)
    val taskSetManager101 = createDummyTaskSetManager(2, 5, 5, taskScheduler, taskSet5)
    pool10.addSchedulable(taskSetManager100)
    pool10.addSchedulable(taskSetManager101)

    val taskSetManager110 = createDummyTaskSetManager(3, 6, 5, taskScheduler, taskSet6)
    val taskSetManager111 = createDummyTaskSetManager(3, 7, 5, taskScheduler, taskSet7)
    pool11.addSchedulable(taskSetManager110)
    pool11.addSchedulable(taskSetManager111)

    taskScheduler.rootPool = rootPool

    val workerOffers = generateWorkerOffers(4)
    checkTaskSetIds(workerOffers, Seq[String]("0.0", "4.0", "6.0", "2.0"))
  }
}
