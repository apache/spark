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

import java.nio.ByteBuffer

import scala.collection.mutable.HashMap

import org.mockito.Matchers.{anyInt, anyObject, anyString, eq => meq}
import org.mockito.Mockito.{atLeast, atMost, never, spy, times, verify, when}
import org.scalatest.BeforeAndAfterEach
import org.scalatest.mock.MockitoSugar

import org.apache.spark._
import org.apache.spark.internal.config
import org.apache.spark.internal.Logging
import org.apache.spark.util.ManualClock

class FakeSchedulerBackend extends SchedulerBackend {
  def start() {}
  def stop() {}
  def reviveOffers() {}
  def defaultParallelism(): Int = 1
}

class TaskSchedulerImplSuite extends SparkFunSuite with LocalSparkContext with BeforeAndAfterEach
    with Logging with MockitoSugar {

  var failedTaskSetException: Option[Throwable] = None
  var failedTaskSetReason: String = null
  var failedTaskSet = false

  var blacklist: BlacklistTracker = null
  var taskScheduler: TaskSchedulerImpl = null
  var dagScheduler: DAGScheduler = null

  val stageToMockTaskSetBlacklist = new HashMap[Int, TaskSetBlacklist]()
  val stageToMockTaskSetManager = new HashMap[Int, TaskSetManager]()

  override def beforeEach(): Unit = {
    super.beforeEach()
    failedTaskSet = false
    failedTaskSetException = None
    failedTaskSetReason = null
    stageToMockTaskSetBlacklist.clear()
    stageToMockTaskSetManager.clear()
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
    val conf = new SparkConf().setMaster("local").setAppName("TaskSchedulerImplSuite")
    confs.foreach { case (k, v) => conf.set(k, v) }
    sc = new SparkContext(conf)
    taskScheduler = new TaskSchedulerImpl(sc)
    setupHelper()
  }

  def setupSchedulerWithMockTaskSetBlacklist(): TaskSchedulerImpl = {
    blacklist = mock[BlacklistTracker]
    val conf = new SparkConf().setMaster("local").setAppName("TaskSchedulerImplSuite")
    conf.set(config.BLACKLIST_ENABLED, true)
    sc = new SparkContext(conf)
    taskScheduler =
      new TaskSchedulerImpl(sc, sc.conf.getInt("spark.task.maxFailures", 4), Some(blacklist)) {
        override def createTaskSetManager(taskSet: TaskSet, maxFailures: Int): TaskSetManager = {
          val tsm = super.createTaskSetManager(taskSet, maxFailures)
          // we need to create a spied tsm just so we can set the TaskSetBlacklist
          val tsmSpy = spy(tsm)
          val taskSetBlacklist = mock[TaskSetBlacklist]
          when(tsmSpy.taskSetBlacklistHelperOpt).thenReturn(Some(taskSetBlacklist))
          stageToMockTaskSetManager(taskSet.stageId) = tsmSpy
          stageToMockTaskSetBlacklist(taskSet.stageId) = taskSetBlacklist
          tsmSpy
        }
      }
    setupHelper()
  }

  def setupHelper(): TaskSchedulerImpl = {
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
    val workerOffers = IndexedSeq(new WorkerOffer("executor0", "host0", numFreeCores),
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
    val zeroCoreWorkerOffers = IndexedSeq(new WorkerOffer("executor0", "host0", 0),
      new WorkerOffer("executor1", "host1", 0))
    val taskSet = FakeTask.createTaskSet(1)
    taskScheduler.submitTasks(taskSet)
    var taskDescriptions = taskScheduler.resourceOffers(zeroCoreWorkerOffers).flatten
    assert(0 === taskDescriptions.length)

    // No tasks should run as we only have 1 core free.
    val numFreeCores = 1
    val singleCoreWorkerOffers = IndexedSeq(new WorkerOffer("executor0", "host0", numFreeCores),
      new WorkerOffer("executor1", "host1", numFreeCores))
    taskScheduler.submitTasks(taskSet)
    taskDescriptions = taskScheduler.resourceOffers(singleCoreWorkerOffers).flatten
    assert(0 === taskDescriptions.length)

    // Now change the offers to have 2 cores in one executor and verify if it
    // is chosen.
    val multiCoreWorkerOffers = IndexedSeq(new WorkerOffer("executor0", "host0", taskCpus),
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
    val multiCoreWorkerOffers = IndexedSeq(new WorkerOffer("executor0", "host0", taskCpus),
      new WorkerOffer("executor1", "host1", numFreeCores))
    taskScheduler.submitTasks(taskSet)
    var taskDescriptions = taskScheduler.resourceOffers(multiCoreWorkerOffers).flatten
    assert(0 === taskDescriptions.length)
    assert(failedTaskSet)
    assert(failedTaskSetReason.contains("Failed to serialize task"))

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
    val workerOffers = IndexedSeq(new WorkerOffer("executor0", "host0", numFreeCores))
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
    val workerOffers = IndexedSeq(new WorkerOffer("executor0", "host0", numFreeCores))
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

    val e0Offers = IndexedSeq(new WorkerOffer("executor0", "host0", 1))
    val e1Offers = IndexedSeq(new WorkerOffer("executor1", "host0", 1))
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

  test("scheduled tasks obey task and stage blacklists") {
    taskScheduler = setupSchedulerWithMockTaskSetBlacklist()
    (0 to 2).foreach {stageId =>
      val taskSet = FakeTask.createTaskSet(numTasks = 2, stageId = stageId, stageAttemptId = 0)
      taskScheduler.submitTasks(taskSet)
    }

    // Setup our mock blacklist:
    // * stage 0 is blacklisted on node "host1"
    // * stage 1 is blacklisted on executor "executor3"
    // * stage 0, partition 0 is blacklisted on executor 0
    // (mocked methods default to returning false, ie. no blacklisting)
    when(stageToMockTaskSetBlacklist(0).isNodeBlacklistedForTaskSet("host1")).thenReturn(true)
    when(stageToMockTaskSetBlacklist(1).isExecutorBlacklistedForTaskSet("executor3"))
      .thenReturn(true)
    when(stageToMockTaskSetBlacklist(0).isExecutorBlacklistedForTask("executor0", 0))
      .thenReturn(true)

    val offers = IndexedSeq(
      new WorkerOffer("executor0", "host0", 1),
      new WorkerOffer("executor1", "host1", 1),
      new WorkerOffer("executor2", "host1", 1),
      new WorkerOffer("executor3", "host2", 10)
    )
    val firstTaskAttempts = taskScheduler.resourceOffers(offers).flatten
    // We should schedule all tasks.
    assert(firstTaskAttempts.size === 6)
    // Whenever we schedule a task, we must consult the node and executor blacklist.  (The test
    // doesn't check exactly what checks are made because the offers get shuffled.)
    (0 to 2).foreach { stageId =>
      verify(stageToMockTaskSetBlacklist(stageId), atLeast(1))
        .isNodeBlacklistedForTaskSet(anyString())
      verify(stageToMockTaskSetBlacklist(stageId), atLeast(1))
        .isExecutorBlacklistedForTaskSet(anyString())
    }

    def tasksForStage(stageId: Int): Seq[TaskDescription] = {
      firstTaskAttempts.filter{_.name.contains(s"stage $stageId")}
    }
    tasksForStage(0).foreach { task =>
      // executors 1 & 2 blacklisted for node
      // executor 0 blacklisted just for partition 0
      if (task.index == 0) {
        assert(task.executorId === "executor3")
      } else {
        assert(Set("executor0", "executor3").contains(task.executorId))
      }
    }
    tasksForStage(1).foreach { task =>
      // executor 3 blacklisted
      assert("executor3" != task.executorId)
    }
    // no restrictions on stage 2

    // Finally, just make sure that we can still complete tasks as usual with blacklisting
    // in effect.  Finish each of the tasksets -- taskset 0 & 1 complete successfully, taskset 2
    // fails.
    (0 to 2).foreach { stageId =>
      val tasks = tasksForStage(stageId)
      val tsm = taskScheduler.taskSetManagerForAttempt(stageId, 0).get
      val valueSer = SparkEnv.get.serializer.newInstance()
      if (stageId == 2) {
        // Just need to make one task fail 4 times.
        var task = tasks(0)
        val taskIndex = task.index
        (0 until 4).foreach { attempt =>
          assert(task.attemptNumber === attempt)
          tsm.handleFailedTask(task.taskId, TaskState.FAILED, TaskResultLost)
          val nextAttempts =
            taskScheduler.resourceOffers(IndexedSeq(WorkerOffer("executor4", "host4", 1))).flatten
          if (attempt < 3) {
            assert(nextAttempts.size === 1)
            task = nextAttempts(0)
            assert(task.index === taskIndex)
          } else {
            assert(nextAttempts.size === 0)
          }
        }
        // End the other task of the taskset, doesn't matter whether it succeeds or fails.
        val otherTask = tasks(1)
        val result = new DirectTaskResult[Int](valueSer.serialize(otherTask.taskId), Seq())
        tsm.handleSuccessfulTask(otherTask.taskId, result)
      } else {
        tasks.foreach { task =>
          val result = new DirectTaskResult[Int](valueSer.serialize(task.taskId), Seq())
          tsm.handleSuccessfulTask(task.taskId, result)
        }
      }
      assert(tsm.isZombie)
    }

    // the tasksSets complete, so the tracker should be notified of the successful ones
    verify(blacklist, times(1)).updateBlacklistForSuccessfulTaskSet(
      stageId = 0,
      stageAttemptId = 0,
      failuresByExec = stageToMockTaskSetBlacklist(0).execToFailures)
    verify(blacklist, times(1)).updateBlacklistForSuccessfulTaskSet(
      stageId = 1,
      stageAttemptId = 0,
      failuresByExec = stageToMockTaskSetBlacklist(1).execToFailures)
    // but we shouldn't update for the failed taskset
    verify(blacklist, never).updateBlacklistForSuccessfulTaskSet(
      stageId = meq(2),
      stageAttemptId = anyInt(),
      failuresByExec = anyObject())
  }

  test("scheduled tasks obey node and executor blacklists") {
    taskScheduler = setupSchedulerWithMockTaskSetBlacklist()
    (0 to 2).foreach { stageId =>
      val taskSet = FakeTask.createTaskSet(numTasks = 2, stageId = stageId, stageAttemptId = 0)
      taskScheduler.submitTasks(taskSet)
    }

    val offers = IndexedSeq(
      new WorkerOffer("executor0", "host0", 1),
      new WorkerOffer("executor1", "host1", 1),
      new WorkerOffer("executor2", "host1", 1),
      new WorkerOffer("executor3", "host2", 10),
      new WorkerOffer("executor4", "host3", 1)
    )

    // setup our mock blacklist:
    // host1, executor0 & executor3 are completely blacklisted
    // This covers everything *except* one core on executor4 / host3, so that everything is still
    // schedulable.
    when(blacklist.isNodeBlacklisted("host1")).thenReturn(true)
    when(blacklist.isExecutorBlacklisted("executor0")).thenReturn(true)
    when(blacklist.isExecutorBlacklisted("executor3")).thenReturn(true)

    val stageToTsm = (0 to 2).map { stageId =>
      val tsm = taskScheduler.taskSetManagerForAttempt(stageId, 0).get
      stageId -> tsm
    }.toMap

    val firstTaskAttempts = taskScheduler.resourceOffers(offers).flatten
    firstTaskAttempts.foreach { task => logInfo(s"scheduled $task on ${task.executorId}") }
    assert(firstTaskAttempts.size === 1)
    assert(firstTaskAttempts.head.executorId === "executor4")
    ('0' until '2').foreach { hostNum =>
      verify(blacklist, atLeast(1)).isNodeBlacklisted("host" + hostNum)
    }
  }

  test("abort stage when all executors are blacklisted") {
    taskScheduler = setupSchedulerWithMockTaskSetBlacklist()
    val taskSet = FakeTask.createTaskSet(numTasks = 10, stageAttemptId = 0)
    taskScheduler.submitTasks(taskSet)
    val tsm = stageToMockTaskSetManager(0)

    // first just submit some offers so the scheduler knows about all the executors
    taskScheduler.resourceOffers(IndexedSeq(
      WorkerOffer("executor0", "host0", 2),
      WorkerOffer("executor1", "host0", 2),
      WorkerOffer("executor2", "host0", 2),
      WorkerOffer("executor3", "host1", 2)
    ))

    // now say our blacklist updates to blacklist a bunch of resources, but *not* everything
    when(blacklist.isNodeBlacklisted("host1")).thenReturn(true)
    when(blacklist.isExecutorBlacklisted("executor0")).thenReturn(true)

    // make an offer on the blacklisted resources.  We won't schedule anything, but also won't
    // abort yet, since we know of other resources that work
    assert(taskScheduler.resourceOffers(IndexedSeq(
      WorkerOffer("executor0", "host0", 2),
      WorkerOffer("executor3", "host1", 2)
    )).flatten.size === 0)
    assert(!tsm.isZombie)

    // now update the blacklist so that everything really is blacklisted
    when(blacklist.isExecutorBlacklisted("executor1")).thenReturn(true)
    when(blacklist.isExecutorBlacklisted("executor2")).thenReturn(true)
    assert(taskScheduler.resourceOffers(IndexedSeq(
      WorkerOffer("executor0", "host0", 2),
      WorkerOffer("executor3", "host1", 2)
    )).flatten.size === 0)
    assert(tsm.isZombie)
    verify(tsm).abort(anyString(), anyObject())
  }

  /**
   * Helper for performance tests.  Takes the explicitly blacklisted nodes and executors; verifies
   * that the blacklists are used efficiently to ensure scheduling is not O(numPendingTasks).
   * Creates 1 offer on executor[1-3].  Executor1 & 2 are on host1, executor3 is on host2.  Passed
   * in nodes and executors should be on that list.
   */
  private def testBlacklistPerformance(
      testName: String,
      nodeBlacklist: Seq[String],
      execBlacklist: Seq[String]): Unit = {
    // Because scheduling involves shuffling the order of offers around, we run this test a few
    // times to cover more possibilities.  There are only 3 offers, which means 6 permutations,
    // so 10 iterations is pretty good.
    (0 until 10).foreach { testItr =>
      test(s"$testName: iteration $testItr") {
        // When an executor or node is blacklisted, we want to make sure that we don't try
        // scheduling each pending task, one by one, to discover they are all blacklisted.  This is
        // important for performance -- if we did check each task one-by-one, then responding to a
        // resource offer (which is usually O(1)-ish) would become O(numPendingTasks), which would
        // slow down scheduler throughput and slow down scheduling even on healthy executors.
        // Here, we check a proxy for the runtime -- we make sure the scheduling is short-circuited
        // at the node or executor blacklist, so we never check the per-task blacklist.  We also
        // make sure we don't check the node & executor blacklist for the entire taskset
        // O(numPendingTasks) times.

        taskScheduler = setupSchedulerWithMockTaskSetBlacklist()
        // we schedule 500 tasks so we can clearly distinguish anything that is O(numPendingTasks)
        val taskSet = FakeTask.createTaskSet(numTasks = 500, stageId = 0, stageAttemptId = 0)
        taskScheduler.submitTasks(taskSet)

        val offers = IndexedSeq(
          new WorkerOffer("executor1", "host1", 1),
          new WorkerOffer("executor2", "host1", 1),
          new WorkerOffer("executor3", "host2", 1)
        )
        // We should check the node & exec blacklists, but only O(numOffers), not O(numPendingTasks)
        // times.  In the worst case, after shuffling, we offer our blacklisted resource first, and
        // then offer other resources which do get used.  The taskset blacklist is consulted
        // repeatedly as we offer resources to the taskset -- each iteration either schedules
        // something, or it terminates that locality level, so the maximum number of checks is
        // numCores + numLocalityLevels
        val numCoresOnAllOffers = offers.map(_.cores).sum
        val numLocalityLevels = TaskLocality.values.size
        val maxBlacklistChecks = numCoresOnAllOffers + numLocalityLevels

        // Setup the blacklist
        nodeBlacklist.foreach { node =>
          when(stageToMockTaskSetBlacklist(0).isNodeBlacklistedForTaskSet(node)).thenReturn(true)
        }
        execBlacklist.foreach { exec =>
          when(stageToMockTaskSetBlacklist(0).isExecutorBlacklistedForTaskSet(exec))
            .thenReturn(true)
        }

        // Figure out which nodes have any effective blacklisting on them.  This means all nodes
        // that are explicitly blacklisted, plus those that have *any* executors blacklisted.
        val nodesForBlacklistedExecutors = offers.filter { offer =>
          execBlacklist.contains(offer.executorId)
        }.map(_.host).toSet.toSeq
        val nodesWithAnyBlacklisting = (nodeBlacklist ++ nodesForBlacklistedExecutors).toSet
        // Similarly, figure out which executors have any blacklisting.  This means all executors
        // that are explicitly blacklisted, plus all executors on nodes that are blacklisted.
        val execsForBlacklistedNodes = offers.filter { offer =>
          nodeBlacklist.contains(offer.host)
        }.map(_.executorId).toSeq
        val executorsWithAnyBlacklisting = (execBlacklist ++ execsForBlacklistedNodes).toSet

        // Schedule a taskset, and make sure our test setup is correct -- we are able to schedule
        // a task on all executors that aren't blacklisted (whether that executor is a explicitly
        // blacklisted, or implicitly blacklisted via the node blacklist).
        val firstTaskAttempts = taskScheduler.resourceOffers(offers).flatten
        assert(firstTaskAttempts.size === offers.size - executorsWithAnyBlacklisting.size)

        // Now check that we haven't made too many calls to any of the blacklist methods.
        // We should be checking our node blacklist, but it should be within the bound we defined
        // above.
        verify(stageToMockTaskSetBlacklist(0), atMost(maxBlacklistChecks))
          .isNodeBlacklistedForTaskSet(anyString())
        // We shouldn't ever consult the per-task blacklist for the nodes that have been blacklisted
        // for the entire taskset, since the taskset level blacklisting should prevent scheduling
        // from ever looking at specific tasks.
        nodesWithAnyBlacklisting.foreach { node =>
          verify(stageToMockTaskSetBlacklist(0), never)
            .isNodeBlacklistedForTask(meq(node), anyInt())
        }
        executorsWithAnyBlacklisting.foreach { exec =>
          // We should be checking our executor blacklist, but it should be within the bound defined
          // above.  Its possible that this will be significantly fewer calls, maybe even 0, if
          // there is also a node-blacklist which takes effect first.  But this assert is all we
          // need to avoid an O(numPendingTask) slowdown.
          verify(stageToMockTaskSetBlacklist(0), atMost(maxBlacklistChecks))
            .isExecutorBlacklistedForTaskSet(exec)
          // We shouldn't ever consult the per-task blacklist for executors that have been
          // blacklisted for the entire taskset, since the taskset level blacklisting should prevent
          // scheduling from ever looking at specific tasks.
          verify(stageToMockTaskSetBlacklist(0), never)
            .isExecutorBlacklistedForTask(meq(exec), anyInt())
        }
      }
    }
  }

  testBlacklistPerformance(
    testName = "Blacklisted node for entire task set prevents per-task blacklist checks",
    nodeBlacklist = Seq("host1"),
    execBlacklist = Seq())

  testBlacklistPerformance(
    testName = "Blacklisted executor for entire task set prevents per-task blacklist checks",
    nodeBlacklist = Seq(),
    execBlacklist = Seq("executor3")
  )

  test("abort stage if executor loss results in unschedulability from previously failed tasks") {
    // Make sure we can detect when a taskset becomes unschedulable from a blacklisting.  This
    // test explores a particular corner case -- you may have one task fail, but still be
    // schedulable on another executor.  However, that executor may fail later on, leaving the
    // first task with no place to run.
    val taskScheduler = setupScheduler(
      config.BLACKLIST_ENABLED.key -> "true"
    )

    val taskSet = FakeTask.createTaskSet(2)
    taskScheduler.submitTasks(taskSet)
    val tsm = taskScheduler.taskSetManagerForAttempt(taskSet.stageId, taskSet.stageAttemptId).get

    val firstTaskAttempts = taskScheduler.resourceOffers(IndexedSeq(
      new WorkerOffer("executor0", "host0", 1),
      new WorkerOffer("executor1", "host1", 1)
    )).flatten
    assert(Set("executor0", "executor1") === firstTaskAttempts.map(_.executorId).toSet)

    // Fail one of the tasks, but leave the other running.
    val failedTask = firstTaskAttempts.find(_.executorId == "executor0").get
    taskScheduler.handleFailedTask(tsm, failedTask.taskId, TaskState.FAILED, TaskResultLost)
    // At this point, our failed task could run on the other executor, so don't give up the task
    // set yet.
    assert(!failedTaskSet)

    // Now we fail our second executor.  The other task can still run on executor1, so make an offer
    // on that executor, and make sure that the other task (not the failed one) is assigned there.
    taskScheduler.executorLost("executor1", SlaveLost("oops"))
    val nextTaskAttempts =
      taskScheduler.resourceOffers(IndexedSeq(new WorkerOffer("executor0", "host0", 1))).flatten
    // Note: Its OK if some future change makes this already realize the taskset has become
    // unschedulable at this point (though in the current implementation, we're sure it will not).
    assert(nextTaskAttempts.size === 1)
    assert(nextTaskAttempts.head.executorId === "executor0")
    assert(nextTaskAttempts.head.attemptNumber === 1)
    assert(nextTaskAttempts.head.index != failedTask.index)

    // Now we should definitely realize that our task set is unschedulable, because the only
    // task left can't be scheduled on any executors due to the blacklist.
    taskScheduler.resourceOffers(IndexedSeq(new WorkerOffer("executor0", "host0", 1)))
    sc.listenerBus.waitUntilEmpty(100000)
    assert(tsm.isZombie)
    assert(failedTaskSet)
    val idx = failedTask.index
    assert(failedTaskSetReason === s"Aborting TaskSet 0.0 because task $idx (partition $idx) " +
      s"cannot run anywhere due to node and executor blacklist.  Blacklisting behavior can be " +
      s"configured via spark.blacklist.*.")
  }

  test("don't abort if there is an executor available, though it hasn't had scheduled tasks yet") {
    // interaction of SPARK-15865 & SPARK-16106
    // if we have a small number of tasks, we might be able to schedule them all on the first
    // executor.  But if those tasks fail, we should still realize there is another executor
    // available and not bail on the job

    val taskScheduler = setupScheduler(
      config.BLACKLIST_ENABLED.key -> "true"
    )

    val taskSet = FakeTask.createTaskSet(2, (0 until 2).map { _ => Seq(TaskLocation("host0")) }: _*)
    taskScheduler.submitTasks(taskSet)
    val tsm = taskScheduler.taskSetManagerForAttempt(taskSet.stageId, taskSet.stageAttemptId).get

    val offers = IndexedSeq(
      // each offer has more than enough free cores for the entire task set, so when combined
      // with the locality preferences, we schedule all tasks on one executor
      new WorkerOffer("executor0", "host0", 4),
      new WorkerOffer("executor1", "host1", 4)
    )
    val firstTaskAttempts = taskScheduler.resourceOffers(offers).flatten
    assert(firstTaskAttempts.size == 2)
    firstTaskAttempts.foreach { taskAttempt => assert("executor0" === taskAttempt.executorId) }

    // fail all the tasks on the bad executor
    firstTaskAttempts.foreach { taskAttempt =>
      taskScheduler.handleFailedTask(tsm, taskAttempt.taskId, TaskState.FAILED, TaskResultLost)
    }

    // Here is the main check of this test -- we have the same offers again, and we schedule it
    // successfully.  Because the scheduler first tries to schedule with locality in mind, at first
    // it won't schedule anything on executor1.  But despite that, we don't abort the job.  Then the
    // scheduler tries for ANY locality, and successfully schedules tasks on executor1.
    val secondTaskAttempts = taskScheduler.resourceOffers(offers).flatten
    assert(secondTaskAttempts.size == 2)
    secondTaskAttempts.foreach { taskAttempt => assert("executor1" === taskAttempt.executorId) }
    assert(!failedTaskSet)
  }

  test("SPARK-16106 locality levels updated if executor added to existing host") {
    val taskScheduler = setupScheduler()

    taskScheduler.submitTasks(FakeTask.createTaskSet(2, 0,
      (0 until 2).map { _ => Seq(TaskLocation("host0", "executor2")) }: _*
    ))

    val taskDescs = taskScheduler.resourceOffers(IndexedSeq(
      new WorkerOffer("executor0", "host0", 1),
      new WorkerOffer("executor1", "host1", 1)
    )).flatten
    // only schedule one task because of locality
    assert(taskDescs.size === 1)

    val mgr = taskScheduler.taskIdToTaskSetManager.get(taskDescs(0).taskId).get
    assert(mgr.myLocalityLevels.toSet === Set(TaskLocality.NODE_LOCAL, TaskLocality.ANY))
    // we should know about both executors, even though we only scheduled tasks on one of them
    assert(taskScheduler.getExecutorsAliveOnHost("host0") === Some(Set("executor0")))
    assert(taskScheduler.getExecutorsAliveOnHost("host1") === Some(Set("executor1")))

    // when executor2 is added, we should realize that we can run process-local tasks.
    // And we should know its alive on the host.
    val secondTaskDescs = taskScheduler.resourceOffers(
      IndexedSeq(new WorkerOffer("executor2", "host0", 1))).flatten
    assert(secondTaskDescs.size === 1)
    assert(mgr.myLocalityLevels.toSet ===
      Set(TaskLocality.PROCESS_LOCAL, TaskLocality.NODE_LOCAL, TaskLocality.ANY))
    assert(taskScheduler.getExecutorsAliveOnHost("host0") === Some(Set("executor0", "executor2")))
    assert(taskScheduler.getExecutorsAliveOnHost("host1") === Some(Set("executor1")))

    // And even if we don't have anything left to schedule, another resource offer on yet another
    // executor should also update the set of live executors
    val thirdTaskDescs = taskScheduler.resourceOffers(
      IndexedSeq(new WorkerOffer("executor3", "host1", 1))).flatten
    assert(thirdTaskDescs.size === 0)
    assert(taskScheduler.getExecutorsAliveOnHost("host1") === Some(Set("executor1", "executor3")))
  }

  test("scheduler checks for executors that can be expired from blacklist") {
    taskScheduler = setupScheduler()

    taskScheduler.submitTasks(FakeTask.createTaskSet(1, 0))
    taskScheduler.resourceOffers(IndexedSeq(
      new WorkerOffer("executor0", "host0", 1)
    )).flatten

    verify(blacklist).applyBlacklistTimeout()
  }

  test("if an executor is lost then the state for its running tasks is cleaned up (SPARK-18553)") {
    sc = new SparkContext("local", "TaskSchedulerImplSuite")
    val taskScheduler = new TaskSchedulerImpl(sc)
    taskScheduler.initialize(new FakeSchedulerBackend)
    // Need to initialize a DAGScheduler for the taskScheduler to use for callbacks.
    new DAGScheduler(sc, taskScheduler) {
      override def taskStarted(task: Task[_], taskInfo: TaskInfo) {}
      override def executorAdded(execId: String, host: String) {}
    }

    val e0Offers = IndexedSeq(WorkerOffer("executor0", "host0", 1))
    val attempt1 = FakeTask.createTaskSet(1)

    // submit attempt 1, offer resources, task gets scheduled
    taskScheduler.submitTasks(attempt1)
    val taskDescriptions = taskScheduler.resourceOffers(e0Offers).flatten
    assert(1 === taskDescriptions.length)

    // mark executor0 as dead
    taskScheduler.executorLost("executor0", SlaveLost())
    assert(!taskScheduler.isExecutorAlive("executor0"))
    assert(!taskScheduler.hasExecutorsAliveOnHost("host0"))
    assert(taskScheduler.getExecutorsAliveOnHost("host0").isEmpty)


    // Check that state associated with the lost task attempt is cleaned up:
    assert(taskScheduler.taskIdToExecutorId.isEmpty)
    assert(taskScheduler.taskIdToTaskSetManager.isEmpty)
    assert(taskScheduler.runningTasksByExecutors.get("executor0").isEmpty)
  }

  test("if a task finishes with TaskState.LOST its executor is marked as dead") {
    sc = new SparkContext("local", "TaskSchedulerImplSuite")
    val taskScheduler = new TaskSchedulerImpl(sc)
    taskScheduler.initialize(new FakeSchedulerBackend)
    // Need to initialize a DAGScheduler for the taskScheduler to use for callbacks.
    new DAGScheduler(sc, taskScheduler) {
      override def taskStarted(task: Task[_], taskInfo: TaskInfo) {}
      override def executorAdded(execId: String, host: String) {}
    }

    val e0Offers = IndexedSeq(WorkerOffer("executor0", "host0", 1))
    val attempt1 = FakeTask.createTaskSet(1)

    // submit attempt 1, offer resources, task gets scheduled
    taskScheduler.submitTasks(attempt1)
    val taskDescriptions = taskScheduler.resourceOffers(e0Offers).flatten
    assert(1 === taskDescriptions.length)

    // Report the task as failed with TaskState.LOST
    taskScheduler.statusUpdate(
      tid = taskDescriptions.head.taskId,
      state = TaskState.LOST,
      serializedData = ByteBuffer.allocate(0)
    )

    // Check that state associated with the lost task attempt is cleaned up:
    assert(taskScheduler.taskIdToExecutorId.isEmpty)
    assert(taskScheduler.taskIdToTaskSetManager.isEmpty)
    assert(taskScheduler.runningTasksByExecutors.get("executor0").isEmpty)

    // Check that the executor has been marked as dead
    assert(!taskScheduler.isExecutorAlive("executor0"))
    assert(!taskScheduler.hasExecutorsAliveOnHost("host0"))
    assert(taskScheduler.getExecutorsAliveOnHost("host0").isEmpty)
  }

  test("Locality should be used for bulk offers even with delay scheduling off") {
    val conf = new SparkConf()
      .set("spark.locality.wait", "0")
    sc = new SparkContext("local", "TaskSchedulerImplSuite", conf)
    // we create a manual clock just so we can be sure the clock doesn't advance at all in this test
    val clock = new ManualClock()

    // We customize the task scheduler just to let us control the way offers are shuffled, so we
    // can be sure we try both permutations, and to control the clock on the tasksetmanager.
    val taskScheduler = new TaskSchedulerImpl(sc) {
      override def shuffleOffers(offers: IndexedSeq[WorkerOffer]): IndexedSeq[WorkerOffer] = {
        // Don't shuffle the offers around for this test.  Instead, we'll just pass in all
        // the permutations we care about directly.
        offers
      }
      override def createTaskSetManager(taskSet: TaskSet, maxTaskFailures: Int): TaskSetManager = {
        new TaskSetManager(this, taskSet, maxTaskFailures, blacklistTrackerOpt, clock)
      }
    }
    // Need to initialize a DAGScheduler for the taskScheduler to use for callbacks.
    new DAGScheduler(sc, taskScheduler) {
      override def taskStarted(task: Task[_], taskInfo: TaskInfo) {}
      override def executorAdded(execId: String, host: String) {}
    }
    taskScheduler.initialize(new FakeSchedulerBackend)

    // Make two different offers -- one in the preferred location, one that is not.
    val offers = IndexedSeq(
      WorkerOffer("exec1", "host1", 1),
      WorkerOffer("exec2", "host2", 1)
    )
    Seq(false, true).foreach { swapOrder =>
      // Submit a taskset with locality preferences.
      val taskSet = FakeTask.createTaskSet(
        1, stageId = 1, stageAttemptId = 0, Seq(TaskLocation("host1", "exec1")))
      taskScheduler.submitTasks(taskSet)
      val shuffledOffers = if (swapOrder) offers.reverse else offers
      // Regardless of the order of the offers (after the task scheduler shuffles them), we should
      // always take advantage of the local offer.
      val taskDescs = taskScheduler.resourceOffers(shuffledOffers).flatten
      withClue(s"swapOrder = $swapOrder") {
        assert(taskDescs.size === 1)
        assert(taskDescs.head.executorId === "exec1")
      }
    }
  }

  test("With delay scheduling off, tasks can be run at any locality level immediately") {
    val conf = new SparkConf()
      .set("spark.locality.wait", "0")
    sc = new SparkContext("local", "TaskSchedulerImplSuite", conf)

    // we create a manual clock just so we can be sure the clock doesn't advance at all in this test
    val clock = new ManualClock()
    val taskScheduler = new TaskSchedulerImpl(sc) {
      override def createTaskSetManager(taskSet: TaskSet, maxTaskFailures: Int): TaskSetManager = {
        new TaskSetManager(this, taskSet, maxTaskFailures, blacklistTrackerOpt, clock)
      }
    }
    // Need to initialize a DAGScheduler for the taskScheduler to use for callbacks.
    new DAGScheduler(sc, taskScheduler) {
      override def taskStarted(task: Task[_], taskInfo: TaskInfo) {}
      override def executorAdded(execId: String, host: String) {}
    }
    taskScheduler.initialize(new FakeSchedulerBackend)
    // make an offer on the preferred host so the scheduler knows its alive.  This is necessary
    // so that the taskset knows that it *could* take advantage of locality.
    taskScheduler.resourceOffers(IndexedSeq(WorkerOffer("exec1", "host1", 1)))

    // Submit a taskset with locality preferences.
    val taskSet = FakeTask.createTaskSet(
      1, stageId = 1, stageAttemptId = 0, Seq(TaskLocation("host1", "exec1")))
    taskScheduler.submitTasks(taskSet)
    val tsm = taskScheduler.taskSetManagerForAttempt(1, 0).get
    // make sure we've setup our test correctly, so that the taskset knows it *could* use local
    // offers.
    assert(tsm.myLocalityLevels.contains(TaskLocality.NODE_LOCAL))
    // make an offer on a non-preferred location.  Since the delay is 0, we should still schedule
    // immediately.
    val taskDescs =
      taskScheduler.resourceOffers(IndexedSeq(WorkerOffer("exec2", "host2", 1))).flatten
    assert(taskDescs.size === 1)
    assert(taskDescs.head.executorId === "exec2")
  }

  test("TaskScheduler should throw IllegalArgumentException when schedulingMode is not supported") {
    intercept[IllegalArgumentException] {
      val taskScheduler = setupScheduler(
        TaskSchedulerImpl.SCHEDULER_MODE_PROPERTY -> SchedulingMode.NONE.toString)
      taskScheduler.initialize(new FakeSchedulerBackend)
    }
  }
}
