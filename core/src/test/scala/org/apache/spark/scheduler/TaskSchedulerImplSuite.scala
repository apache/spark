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

import org.scalatest.BeforeAndAfterEach

import org.apache.spark._
import org.apache.spark.internal.Logging

class FakeSchedulerBackend extends SchedulerBackend {
  def start() {}
  def stop() {}
  def reviveOffers() {}
  def defaultParallelism(): Int = 1
}

class TaskSchedulerImplSuite extends SparkFunSuite with LocalSparkContext with BeforeAndAfterEach
    with Logging {


  var failedTaskSetException: Option[Throwable] = None
  var failedTaskSetReason: String = null
  var failedTaskSet = false

  var taskScheduler: TaskSchedulerImpl = null
  var dagScheduler: DAGScheduler = null

  override def beforeEach(): Unit = {
    super.beforeEach()
    failedTaskSet = false
    failedTaskSetException = None
    failedTaskSetReason = null
  }

  override def afterEach(): Unit = {
    super.afterEach()
    if (taskScheduler != null) {
      taskScheduler.stop()
      taskScheduler = null
    }
    if (dagScheduler != null) {
      dagScheduler.stop()
      dagScheduler = null
    }
  }

  def setupScheduler(confs: (String, String)*): TaskSchedulerImpl = {
    sc = new SparkContext("local", "TaskSchedulerImplSuite")
    confs.foreach { case (k, v) =>
      sc.conf.set(k, v)
    }
    taskScheduler = new TaskSchedulerImpl(sc)
    taskScheduler.initialize(new FakeSchedulerBackend)
    // Need to initialize a DAGScheduler for the taskScheduler to use for callbacks.
    dagScheduler = new DAGScheduler(sc, taskScheduler) {
      override def taskStarted(task: Task[_], taskInfo: TaskInfo): Unit = {}
      override def executorAdded(execId: String, host: String): Unit = {}
      override def taskSetFailed(
          taskSet: TaskSet,
          reason: String,
          exception: Option[Throwable]): Unit = {
        // Normally the DAGScheduler puts this in the event loop, which will eventually fail
        // dependent jobs
        failedTaskSet = true
        failedTaskSetReason = reason
        failedTaskSetException = exception
      }
    }
    taskScheduler
  }

  test("Scheduler does not always schedule tasks on the same workers") {
    val taskScheduler = setupScheduler()
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
    assert(!failedTaskSet)
  }

  test("Scheduler correctly accounts for multiple CPUs per task") {
    val taskCpus = 2
    val taskScheduler = setupScheduler("spark.task.cpus" -> taskCpus.toString)
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
    assert(!failedTaskSet)
  }

  test("Scheduler does not crash when tasks are not serializable") {
    val taskCpus = 2
    val taskScheduler = setupScheduler("spark.task.cpus" -> taskCpus.toString)
    val numFreeCores = 1
    val taskSet = new TaskSet(
      Array(new NotSerializableFakeTask(1, 0), new NotSerializableFakeTask(0, 1)), 0, 0, 0, null)
    val multiCoreWorkerOffers = Seq(new WorkerOffer("executor0", "host0", taskCpus),
      new WorkerOffer("executor1", "host1", numFreeCores))
    taskScheduler.submitTasks(taskSet)
    var taskDescriptions = taskScheduler.resourceOffers(multiCoreWorkerOffers).flatten
    assert(0 === taskDescriptions.length)

    // Now check that we can still submit tasks
    // Even if one of the task sets has not-serializable tasks, the other task set should
    // still be processed without error
    taskScheduler.submitTasks(FakeTask.createTaskSet(1))
    taskScheduler.submitTasks(taskSet)
    taskDescriptions = taskScheduler.resourceOffers(multiCoreWorkerOffers).flatten
    assert(taskDescriptions.map(_.executorId) === Seq("executor0"))
  }

  test("refuse to schedule concurrent attempts for the same stage (SPARK-8103)") {
    val taskScheduler = setupScheduler()
    val attempt1 = FakeTask.createTaskSet(1, 0)
    val attempt2 = FakeTask.createTaskSet(1, 1)
    taskScheduler.submitTasks(attempt1)
    intercept[IllegalStateException] { taskScheduler.submitTasks(attempt2) }

    // OK to submit multiple if previous attempts are all zombie
    taskScheduler.taskSetManagerForAttempt(attempt1.stageId, attempt1.stageAttemptId)
      .get.isZombie = true
    taskScheduler.submitTasks(attempt2)
    val attempt3 = FakeTask.createTaskSet(1, 2)
    intercept[IllegalStateException] { taskScheduler.submitTasks(attempt3) }
    taskScheduler.taskSetManagerForAttempt(attempt2.stageId, attempt2.stageAttemptId)
      .get.isZombie = true
    taskScheduler.submitTasks(attempt3)
    assert(!failedTaskSet)
  }

  test("don't schedule more tasks after a taskset is zombie") {
    val taskScheduler = setupScheduler()

    val numFreeCores = 1
    val workerOffers = Seq(new WorkerOffer("executor0", "host0", numFreeCores))
    val attempt1 = FakeTask.createTaskSet(10)

    // submit attempt 1, offer some resources, some tasks get scheduled
    taskScheduler.submitTasks(attempt1)
    val taskDescriptions = taskScheduler.resourceOffers(workerOffers).flatten
    assert(1 === taskDescriptions.length)

    // now mark attempt 1 as a zombie
    taskScheduler.taskSetManagerForAttempt(attempt1.stageId, attempt1.stageAttemptId)
      .get.isZombie = true

    // don't schedule anything on another resource offer
    val taskDescriptions2 = taskScheduler.resourceOffers(workerOffers).flatten
    assert(0 === taskDescriptions2.length)

    // if we schedule another attempt for the same stage, it should get scheduled
    val attempt2 = FakeTask.createTaskSet(10, 1)

    // submit attempt 2, offer some resources, some tasks get scheduled
    taskScheduler.submitTasks(attempt2)
    val taskDescriptions3 = taskScheduler.resourceOffers(workerOffers).flatten
    assert(1 === taskDescriptions3.length)
    val mgr = taskScheduler.taskIdToTaskSetManager.get(taskDescriptions3(0).taskId).get
    assert(mgr.taskSet.stageAttemptId === 1)
    assert(!failedTaskSet)
  }

  test("if a zombie attempt finishes, continue scheduling tasks for non-zombie attempts") {
    val taskScheduler = setupScheduler()

    val numFreeCores = 10
    val workerOffers = Seq(new WorkerOffer("executor0", "host0", numFreeCores))
    val attempt1 = FakeTask.createTaskSet(10)

    // submit attempt 1, offer some resources, some tasks get scheduled
    taskScheduler.submitTasks(attempt1)
    val taskDescriptions = taskScheduler.resourceOffers(workerOffers).flatten
    assert(10 === taskDescriptions.length)

    // now mark attempt 1 as a zombie
    val mgr1 = taskScheduler.taskSetManagerForAttempt(attempt1.stageId, attempt1.stageAttemptId).get
    mgr1.isZombie = true

    // don't schedule anything on another resource offer
    val taskDescriptions2 = taskScheduler.resourceOffers(workerOffers).flatten
    assert(0 === taskDescriptions2.length)

    // submit attempt 2
    val attempt2 = FakeTask.createTaskSet(10, 1)
    taskScheduler.submitTasks(attempt2)

    // attempt 1 finished (this can happen even if it was marked zombie earlier -- all tasks were
    // already submitted, and then they finish)
    taskScheduler.taskSetFinished(mgr1)

    // now with another resource offer, we should still schedule all the tasks in attempt2
    val taskDescriptions3 = taskScheduler.resourceOffers(workerOffers).flatten
    assert(10 === taskDescriptions3.length)

    taskDescriptions3.foreach { task =>
      val mgr = taskScheduler.taskIdToTaskSetManager.get(task.taskId).get
      assert(mgr.taskSet.stageAttemptId === 1)
    }
    assert(!failedTaskSet)
  }

  test("tasks are not re-scheduled while executor loss reason is pending") {
    val taskScheduler = setupScheduler()

    val e0Offers = Seq(new WorkerOffer("executor0", "host0", 1))
    val e1Offers = Seq(new WorkerOffer("executor1", "host0", 1))
    val attempt1 = FakeTask.createTaskSet(1)

    // submit attempt 1, offer resources, task gets scheduled
    taskScheduler.submitTasks(attempt1)
    val taskDescriptions = taskScheduler.resourceOffers(e0Offers).flatten
    assert(1 === taskDescriptions.length)

    // mark executor0 as dead but pending fail reason
    taskScheduler.executorLost("executor0", LossReasonPending)

    // offer some more resources on a different executor, nothing should change
    val taskDescriptions2 = taskScheduler.resourceOffers(e1Offers).flatten
    assert(0 === taskDescriptions2.length)

    // provide the actual loss reason for executor0
    taskScheduler.executorLost("executor0", SlaveLost("oops"))

    // executor0's tasks should have failed now that the loss reason is known, so offering more
    // resources should make them be scheduled on the new executor.
    val taskDescriptions3 = taskScheduler.resourceOffers(e1Offers).flatten
    assert(1 === taskDescriptions3.length)
    assert("executor1" === taskDescriptions3(0).executorId)
    assert(!failedTaskSet)
  }

  test("abort stage if executor loss results in unschedulability from previously failed tasks") {
    // Make sure we can detect when a taskset becomes unschedulability from a blacklisting.  This
    // test explores a particular corner case -- you may have one task fail, but still be
    // schedulable on another executor.  However, that executor may fail later on, leaving the
    // first task with no place to run.
    val taskScheduler = setupScheduler(
      // set this to something much longer than the test duration
      "spark.scheduler.executorTaskBlacklistTime" -> "10000000"
    )

    val taskSet = FakeTask.createTaskSet(2)
    taskScheduler.submitTasks(taskSet)
    val tsm = taskScheduler.taskSetManagerForAttempt(taskSet.stageId, taskSet.stageAttemptId).get

    val firstTaskAttempts = taskScheduler.resourceOffers(Seq(
      new WorkerOffer("executor0", "host0", 1),
      new WorkerOffer("executor1", "host1", 1)
    )).flatten
    assert(Set("executor0", "executor1") === firstTaskAttempts.map(_.executorId).toSet)

    // fail one of the tasks, but leave the other running
    val failedTask = firstTaskAttempts.find(_.executorId == "executor0").get
    taskScheduler.handleFailedTask(tsm, failedTask.taskId, TaskState.FAILED, TaskResultLost)
    // at this point, our failed task could run on the other executor, so don't give up the task
    // set yet.
    assert(!failedTaskSet)

    // Now we fail our second executor.  The other task can still run on executor1, so make an offer
    // on that executor, and make sure that the other task (not the failed one) is assigned there
    taskScheduler.executorLost("executor1", SlaveLost("oops"))
    val nextTaskAttempts =
      taskScheduler.resourceOffers(Seq(new WorkerOffer("executor0", "host0", 1))).flatten
    // Note: Its OK if some future change makes this already realize the taskset has become
    // unschedulable at this point (though in the current implementation, we're sure it will not)
    assert(nextTaskAttempts.size === 1)
    assert(nextTaskAttempts.head.executorId === "executor0")
    assert(nextTaskAttempts.head.attemptNumber === 1)
    assert(nextTaskAttempts.head.index != failedTask.index)

    // now we should definitely realize that our task set is unschedulable, because the only
    // task left can't be scheduled on any executors due to the blacklist
    taskScheduler.resourceOffers(Seq(new WorkerOffer("executor0", "host0", 1)))
    sc.listenerBus.waitUntilEmpty(100000)
    assert(tsm.isZombie)
    assert(failedTaskSet)
    assert(failedTaskSetReason.contains(s"Aborting TaskSet 0.0 because Task ${failedTask.index} " +
      s"cannot be scheduled on any executor due to blacklists."))
  }

}
