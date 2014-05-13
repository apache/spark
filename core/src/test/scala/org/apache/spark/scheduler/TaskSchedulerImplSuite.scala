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

import scala.collection.mutable.{ArrayBuffer, HashSet}

import org.scalatest.{BeforeAndAfter, FunSuite}
import org.scalatest.mock.EasyMockSugar

import org.apache.spark._

class FakeSchedulerBackend extends SchedulerBackend {

  def start() {}
  def stop() {}
  def reviveOffers() {

  }
  def defaultParallelism() = 1
}

class TaskSchedulerImplSuite extends FunSuite with BeforeAndAfter with EasyMockSugar
  with LocalSparkContext with Logging {

  private var taskScheduler: TaskSchedulerImpl = _

  before {
    sc = new SparkContext("local", "TaskSchedulerImplSuite")
    taskScheduler = sc.taskScheduler.asInstanceOf[TaskSchedulerImpl]
  }

  test("ClusterSchedulerBackend is started and stopped properly") {
    taskScheduler = new TaskSchedulerImpl(sc)
    val mockSchedulerBackend = mock[SchedulerBackend]
    taskScheduler.initialize(mockSchedulerBackend)
    expecting {
      mockSchedulerBackend.start()
      mockSchedulerBackend.stop()
    }
    whenExecuting(mockSchedulerBackend) {
      taskScheduler.start()
      taskScheduler.stop()
    }
  }

  test("speculative execution task is started and stopped properly") {
    sc.conf.set("spark.speculation", "true")
    taskScheduler = new TaskSchedulerImpl(sc)
    taskScheduler.backend = new FakeSchedulerBackend
    taskScheduler.start()
    assert(taskScheduler.speculativeExecTask != null)
    taskScheduler.stop()
    assert(taskScheduler.speculativeExecTask.isCancelled === true)
  }

  test("tasks are successfully submitted and reviveOffer is called") {
    taskScheduler = new TaskSchedulerImpl(sc)
    val mockSchedulerBackend = mock[SchedulerBackend]
    taskScheduler.initialize(mockSchedulerBackend)
    val taskSet1 = FakeTask.createTaskSet(1, 0, 0)
    expecting {
      mockSchedulerBackend.reviveOffers()
    }
    whenExecuting(mockSchedulerBackend) {
      taskScheduler.submitTasks(taskSet1)
    }
    assert(taskScheduler.activeTaskSets(taskSet1.id) != null)
  }

  test("running tasks are cancelled and the involved stage is aborted when calling cancelTasks") {
    taskScheduler = new TaskSchedulerImpl(sc)
    taskScheduler.setDAGScheduler(sc.dagScheduler)
    val taskSet1 = FakeTask.createTaskSet(1, 0, 0)
    val tm =  new TaskSetManager(taskScheduler, taskSet1, 1)
    val mockSchedulerBackend = mock[SchedulerBackend]
    taskScheduler.initialize(mockSchedulerBackend)
    tm.runningTasksSet += 0
    taskScheduler.activeTaskSets += taskSet1.id -> tm
    taskScheduler.schedulableBuilder.addTaskSetManager(tm, tm.taskSet.properties)
    val dummyTaskId = taskScheduler.newTaskId()
    taskScheduler.taskIdToExecutorId += dummyTaskId -> "exec1"
    expecting {
      mockSchedulerBackend.killTask(dummyTaskId, "exec1", false)
    }
    whenExecuting(mockSchedulerBackend) {
      taskScheduler.cancelTasks(0, false)
    }
  }

  test("the data structures are updated properly when the task state is updated") {
    val taskSet1 = FakeTask.createTaskSet(1, 0, 0)
    val tm = new TaskSetManager(taskScheduler, taskSet1, 1)
    taskScheduler.schedulableBuilder.addTaskSetManager(tm, tm.taskSet.properties)
    tm.taskInfos += 0.toLong -> new TaskInfo(0, 0, 0, 0, "exec1", "host1", TaskLocality.ANY, false)
    taskScheduler.activeExecutorIds += "exec1"
    taskScheduler.executorIdToHost += "exec1" -> "host1"
    taskScheduler.executorsByHost += "host1" -> HashSet[String]("exec1")
    taskScheduler.taskIdToExecutorId += 0.toLong -> "exec1"
    taskScheduler.taskIdToTaskSetId += 0.toLong -> taskSet1.id
    taskScheduler.activeTaskSets += taskSet1.id -> tm
    assert(taskScheduler.activeExecutorIds.contains("exec1") === true)
    assert(taskScheduler.executorIdToHost.contains("exec1") === true)
    assert(taskScheduler.executorsByHost.contains("host1") === true)
    assert(taskScheduler.taskIdToTaskSetId.contains(0) === true)
    taskScheduler.statusUpdate(0, TaskState.LOST, null)
    assert(taskScheduler.activeExecutorIds.contains("exec1") === false)
    assert(taskScheduler.executorIdToHost.contains("exec1") === false)
    assert(taskScheduler.executorsByHost.contains("host1") === false)
    assert(taskScheduler.taskIdToTaskSetId.contains(0) === false)
  }

  test("task result is correctly enqueued according to the updated state") {
    taskScheduler = new TaskSchedulerImpl(sc)
    taskScheduler.setDAGScheduler(sc.dagScheduler)
    val taskSet1 = FakeTask.createTaskSet(2, 0, 0)
    val tm = new TaskSetManager(taskScheduler, taskSet1, 1)
    tm.taskInfos += 0.toLong -> new TaskInfo(0, 0, 0, 0, "exec1", "host1", TaskLocality.ANY, false)
    tm.taskInfos += 1.toLong -> new TaskInfo(0, 1, 0, 0, "exec1", "host1", TaskLocality.ANY, false)
    taskScheduler.activeExecutorIds += "exec1"
    taskScheduler.executorIdToHost += "exec1" -> "host1"
    taskScheduler.executorsByHost += "host1" -> HashSet[String]("exec1")
    taskScheduler.taskIdToExecutorId += 0.toLong -> "exec1"
    taskScheduler.taskIdToExecutorId += 1.toLong -> "exec1"
    taskScheduler.taskIdToTaskSetId += 0.toLong -> taskSet1.id
    taskScheduler.taskIdToTaskSetId += 1.toLong -> taskSet1.id
    taskScheduler.activeTaskSets += taskSet1.id -> tm
    taskScheduler.taskResultGetter = mock[TaskResultGetter]
    expecting {
      taskScheduler.taskResultGetter.enqueueFailedTask(tm, 0, TaskState.FAILED, null)
      taskScheduler.taskResultGetter.enqueueSuccessfulTask(tm, 1, null)
    }
    whenExecuting(taskScheduler.taskResultGetter) {
      taskScheduler.statusUpdate(0, TaskState.FAILED, null)
      taskScheduler.statusUpdate(1, TaskState.FINISHED, null)
    }
  }

  test("Scheduler does not always schedule tasks on the same workers") {
    taskScheduler.initialize(new FakeSchedulerBackend)

    val numFreeCores = 1
    val workerOffers = Seq(new WorkerOffer("executor0", "host0", numFreeCores),
      new WorkerOffer("executor1", "host1", numFreeCores))
    // Repeatedly try to schedule a 1-task job, and make sure that it doesn't always
    // get scheduled on the same executor. While there is a chance this test will fail
    // because the task randomly gets placed on the first executor all 1000 times, the
    // probability of that happening is 2^-1000 (so sufficiently small to be considered
    // negligible).
    val numTrials = 1000
    val selectedExecutorIds = 1.to(numTrials).map { _ =>
      val taskSet = FakeTask.createTaskSet(1)
      taskScheduler.submitTasks(taskSet)
      val taskDescriptions = taskScheduler.resourceOffers(workerOffers).flatten
      assert(1 === taskDescriptions.length)
      taskDescriptions(0).executorId
    }
    val count = selectedExecutorIds.count(_ == workerOffers(0).executorId)
    assert(count > 0)
    assert(count < numTrials)
  }

  test("Scheduler correctly accounts for multiple CPUs per task") {
    sc = new SparkContext("local", "TaskSchedulerImplSuite")
    val taskCpus = 2

    sc.conf.set("spark.task.cpus", taskCpus.toString)
    taskScheduler = new TaskSchedulerImpl(sc)
    taskScheduler.initialize(new FakeSchedulerBackend)
    // Need to initialize a DAGScheduler for the taskScheduler to use for callbacks.
    val dagScheduler = new DAGScheduler(sc, taskScheduler) {
      override def taskStarted(task: Task[_], taskInfo: TaskInfo) {}

      override def executorAdded(execId: String, host: String) {}
    }
    taskScheduler.setDAGScheduler(dagScheduler)
    // Give zero core offers. Should not generate any tasks
    val zeroCoreWorkerOffers = Seq(new WorkerOffer("executor0", "host0", 0),
      new WorkerOffer("executor1", "host1", 0))
    val taskSet = FakeTask.createTaskSet(1)
    taskScheduler.submitTasks(taskSet)
    var taskDescriptions = taskScheduler.resourceOffers(zeroCoreWorkerOffers).flatten
    assert(0 === taskDescriptions.length)

    // No tasks should run as we only have 1 core free.
    val numFreeCores = 1
    val singleCoreWorkerOffers = Seq(new WorkerOffer("executor0", "host0", numFreeCores),
      new WorkerOffer("executor1", "host1", numFreeCores))
    taskScheduler.submitTasks(taskSet)
    taskDescriptions = taskScheduler.resourceOffers(singleCoreWorkerOffers).flatten
    assert(0 === taskDescriptions.length)

    // Now change the offers to have 2 cores in one executor and verify if it
    // is chosen.
    val multiCoreWorkerOffers = Seq(new WorkerOffer("executor0", "host0", taskCpus),
      new WorkerOffer("executor1", "host1", numFreeCores))
    taskScheduler.submitTasks(taskSet)
    taskDescriptions = taskScheduler.resourceOffers(multiCoreWorkerOffers).flatten
    assert(1 === taskDescriptions.length)
    assert("executor0" === taskDescriptions(0).executorId)
  }
}
