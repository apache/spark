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

import org.scalatest.FunSuite
import org.scalatest.BeforeAndAfter

import org.apache.spark._
import scala.collection.mutable.ArrayBuffer

import java.util.Properties

class FakeTaskSetManager(
    initPriority: Int,
    initStageId: Int,
    initNumTasks: Int,
    clusterScheduler: TaskSchedulerImpl,
    taskSet: TaskSet)
  extends TaskSetManager(clusterScheduler, taskSet, 0) {

  parent = null
  weight = 1
  minShare = 2
  runningTasks = 0
  priority = initPriority
  stageId = initStageId
  name = "TaskSet_"+stageId
  override val numTasks = initNumTasks
  tasksSuccessful = 0

  def increaseRunningTasks(taskNum: Int) {
    runningTasks += taskNum
    if (parent != null) {
      parent.increaseRunningTasks(taskNum)
    }
  }

  def decreaseRunningTasks(taskNum: Int) {
    runningTasks -= taskNum
    if (parent != null) {
      parent.decreaseRunningTasks(taskNum)
    }
  }

  override def addSchedulable(schedulable: Schedulable) {
  }

  override def removeSchedulable(schedulable: Schedulable) {
  }

  override def getSchedulableByName(name: String): Schedulable = {
    null
  }

  override def executorLost(executorId: String, host: String): Unit = {
  }

  override def resourceOffer(
      execId: String,
      host: String,
      availableCpus: Int,
      maxLocality: TaskLocality.TaskLocality)
    : Option[TaskDescription] =
  {
    if (tasksSuccessful + runningTasks < numTasks) {
      increaseRunningTasks(1)
      Some(new TaskDescription(0, execId, "task 0:0", 0, null))
    } else {
      None
    }
  }

  override def checkSpeculatableTasks(): Boolean = {
    true
  }

  def taskFinished() {
    decreaseRunningTasks(1)
    tasksSuccessful +=1
    if (tasksSuccessful == numTasks) {
      parent.removeSchedulable(this)
    }
  }

  def abort() {
    decreaseRunningTasks(runningTasks)
    parent.removeSchedulable(this)
  }
}

class ClusterSchedulerSuite extends FunSuite with LocalSparkContext with Logging {

  def createDummyTaskSetManager(priority: Int, stage: Int, numTasks: Int, cs: TaskSchedulerImpl, taskSet: TaskSet): FakeTaskSetManager = {
    new FakeTaskSetManager(priority, stage, numTasks, cs , taskSet)
  }

  def resourceOffer(rootPool: Pool): Int = {
    val taskSetQueue = rootPool.getSortedTaskSetQueue()
    /* Just for Test*/
    for (manager <- taskSetQueue) {
       logInfo("parentName:%s, parent running tasks:%d, name:%s,runningTasks:%d".format(
         manager.parent.name, manager.parent.runningTasks, manager.name, manager.runningTasks))
    }
    for (taskSet <- taskSetQueue) {
      taskSet.resourceOffer("execId_1", "hostname_1", 1, TaskLocality.ANY) match {
        case Some(task) =>
          return taskSet.stageId
        case None => {}
      }
    }
    -1
  }

  def checkTaskSetId(rootPool: Pool, expectedTaskSetId: Int) {
    assert(resourceOffer(rootPool) === expectedTaskSetId)
  }

  test("FIFO Scheduler Test") {
    sc = new SparkContext("local", "ClusterSchedulerSuite")
    val clusterScheduler = new TaskSchedulerImpl(sc)
    var tasks = ArrayBuffer[Task[_]]()
    val task = new FakeTask(0)
    tasks += task
    val taskSet = new TaskSet(tasks.toArray,0,0,0,null)

    val rootPool = new Pool("", SchedulingMode.FIFO, 0, 0)
    val schedulableBuilder = new FIFOSchedulableBuilder(rootPool)
    schedulableBuilder.buildPools()

    val taskSetManager0 = createDummyTaskSetManager(0, 0, 2, clusterScheduler, taskSet)
    val taskSetManager1 = createDummyTaskSetManager(0, 1, 2, clusterScheduler, taskSet)
    val taskSetManager2 = createDummyTaskSetManager(0, 2, 2, clusterScheduler, taskSet)
    schedulableBuilder.addTaskSetManager(taskSetManager0, null)
    schedulableBuilder.addTaskSetManager(taskSetManager1, null)
    schedulableBuilder.addTaskSetManager(taskSetManager2, null)

    checkTaskSetId(rootPool, 0)
    resourceOffer(rootPool)
    checkTaskSetId(rootPool, 1)
    resourceOffer(rootPool)
    taskSetManager1.abort()
    checkTaskSetId(rootPool, 2)
  }

  test("Fair Scheduler Test") {
    sc = new SparkContext("local", "ClusterSchedulerSuite")
    val clusterScheduler = new TaskSchedulerImpl(sc)
    var tasks = ArrayBuffer[Task[_]]()
    val task = new FakeTask(0)
    tasks += task
    val taskSet = new TaskSet(tasks.toArray,0,0,0,null)

    val xmlPath = getClass.getClassLoader.getResource("fairscheduler.xml").getFile()
    System.setProperty("spark.scheduler.allocation.file", xmlPath)
    val rootPool = new Pool("", SchedulingMode.FAIR, 0, 0)
    val schedulableBuilder = new FairSchedulableBuilder(rootPool, sc.conf)
    schedulableBuilder.buildPools()

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

    val taskSetManager10 = createDummyTaskSetManager(1, 0, 1, clusterScheduler, taskSet)
    val taskSetManager11 = createDummyTaskSetManager(1, 1, 1, clusterScheduler, taskSet)
    val taskSetManager12 = createDummyTaskSetManager(1, 2, 2, clusterScheduler, taskSet)
    schedulableBuilder.addTaskSetManager(taskSetManager10, properties1)
    schedulableBuilder.addTaskSetManager(taskSetManager11, properties1)
    schedulableBuilder.addTaskSetManager(taskSetManager12, properties1)

    val taskSetManager23 = createDummyTaskSetManager(2, 3, 2, clusterScheduler, taskSet)
    val taskSetManager24 = createDummyTaskSetManager(2, 4, 2, clusterScheduler, taskSet)
    schedulableBuilder.addTaskSetManager(taskSetManager23, properties2)
    schedulableBuilder.addTaskSetManager(taskSetManager24, properties2)

    checkTaskSetId(rootPool, 0)
    checkTaskSetId(rootPool, 3)
    checkTaskSetId(rootPool, 3)
    checkTaskSetId(rootPool, 1)
    checkTaskSetId(rootPool, 4)
    checkTaskSetId(rootPool, 2)
    checkTaskSetId(rootPool, 2)
    checkTaskSetId(rootPool, 4)

    taskSetManager12.taskFinished()
    assert(rootPool.getSchedulableByName("1").runningTasks === 3)
    taskSetManager24.abort()
    assert(rootPool.getSchedulableByName("2").runningTasks === 2)
  }

  test("Nested Pool Test") {
    sc = new SparkContext("local", "ClusterSchedulerSuite")
    val clusterScheduler = new TaskSchedulerImpl(sc)
    var tasks = ArrayBuffer[Task[_]]()
    val task = new FakeTask(0)
    tasks += task
    val taskSet = new TaskSet(tasks.toArray,0,0,0,null)

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

    val taskSetManager000 = createDummyTaskSetManager(0, 0, 5, clusterScheduler, taskSet)
    val taskSetManager001 = createDummyTaskSetManager(0, 1, 5, clusterScheduler, taskSet)
    pool00.addSchedulable(taskSetManager000)
    pool00.addSchedulable(taskSetManager001)

    val taskSetManager010 = createDummyTaskSetManager(1, 2, 5, clusterScheduler, taskSet)
    val taskSetManager011 = createDummyTaskSetManager(1, 3, 5, clusterScheduler, taskSet)
    pool01.addSchedulable(taskSetManager010)
    pool01.addSchedulable(taskSetManager011)

    val taskSetManager100 = createDummyTaskSetManager(2, 4, 5, clusterScheduler, taskSet)
    val taskSetManager101 = createDummyTaskSetManager(2, 5, 5, clusterScheduler, taskSet)
    pool10.addSchedulable(taskSetManager100)
    pool10.addSchedulable(taskSetManager101)

    val taskSetManager110 = createDummyTaskSetManager(3, 6, 5, clusterScheduler, taskSet)
    val taskSetManager111 = createDummyTaskSetManager(3, 7, 5, clusterScheduler, taskSet)
    pool11.addSchedulable(taskSetManager110)
    pool11.addSchedulable(taskSetManager111)

    checkTaskSetId(rootPool, 0)
    checkTaskSetId(rootPool, 4)
    checkTaskSetId(rootPool, 6)
    checkTaskSetId(rootPool, 2)
  }
}
