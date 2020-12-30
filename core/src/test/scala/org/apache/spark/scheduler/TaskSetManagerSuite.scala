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

import java.util.{Properties, Random}

import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer
import scala.concurrent.duration._

import org.apache.hadoop.fs.FileAlreadyExistsException
import org.mockito.ArgumentMatchers.{any, anyBoolean, anyInt, anyString}
import org.mockito.Mockito._
import org.mockito.invocation.InvocationOnMock
import org.scalatest.Assertions._
import org.scalatest.PrivateMethodTester
import org.scalatest.concurrent.Eventually

import org.apache.spark.{FakeSchedulerBackend => _, _}
import org.apache.spark.internal.Logging
import org.apache.spark.internal.config
import org.apache.spark.internal.config.Tests.SKIP_VALIDATE_CORES_TESTING
import org.apache.spark.resource.{ResourceInformation, ResourceProfile}
import org.apache.spark.resource.ResourceUtils._
import org.apache.spark.resource.TestResourceIDs._
import org.apache.spark.scheduler.cluster.CoarseGrainedSchedulerBackend
import org.apache.spark.serializer.SerializerInstance
import org.apache.spark.storage.BlockManagerId
import org.apache.spark.util.{AccumulatorV2, Clock, ManualClock, SystemClock}

class FakeDAGScheduler(sc: SparkContext, taskScheduler: FakeTaskScheduler)
  extends DAGScheduler(sc) {

  override def taskStarted(task: Task[_], taskInfo: TaskInfo): Unit = {
    taskScheduler.startedTasks += taskInfo.index
  }

  override def taskEnded(
      task: Task[_],
      reason: TaskEndReason,
      result: Any,
      accumUpdates: Seq[AccumulatorV2[_, _]],
      metricPeaks: Array[Long],
      taskInfo: TaskInfo): Unit = {
    taskScheduler.endedTasks(taskInfo.index) = reason
  }

  override def executorAdded(execId: String, host: String): Unit = {}

  override def executorLost(execId: String, reason: ExecutorLossReason): Unit = {}

  override def taskSetFailed(
      taskSet: TaskSet,
      reason: String,
      exception: Option[Throwable]): Unit = {
    taskScheduler.taskSetsFailed += taskSet.id
  }

  override def speculativeTaskSubmitted(task: Task[_]): Unit = {
    taskScheduler.speculativeTasks += task.partitionId
  }
}

// Get the rack for a given host
object FakeRackUtil {
  private val hostToRack = new mutable.HashMap[String, String]()
  var numBatchInvocation = 0
  var numSingleHostInvocation = 0

  def cleanUp(): Unit = {
    hostToRack.clear()
    numBatchInvocation = 0
    numSingleHostInvocation = 0
  }

  def assignHostToRack(host: String, rack: String): Unit = {
    hostToRack(host) = rack
  }

  def getRacksForHosts(hosts: Seq[String]): Seq[Option[String]] = {
    assert(hosts.toSet.size == hosts.size) // no dups in hosts
    if (hosts.nonEmpty && hosts.length != 1) {
      numBatchInvocation += 1
    } else if (hosts.length == 1) {
      numSingleHostInvocation += 1
    }
    hosts.map(hostToRack.get(_))
  }
}

/**
 * A mock TaskSchedulerImpl implementation that just remembers information about tasks started and
 * feedback received from the TaskSetManagers. Note that it's important to initialize this with
 * a list of "live" executors and their hostnames for isExecutorAlive and hasExecutorsAliveOnHost
 * to work, and these are required for locality in TaskSetManager.
 */
class FakeTaskScheduler(
    sc: SparkContext,
    clock: Clock,
    liveExecutors: (String, String)* /* execId, host */)
  extends TaskSchedulerImpl(sc, sc.conf.get(config.TASK_MAX_FAILURES), clock = clock)
{
  val startedTasks = new ArrayBuffer[Long]
  val endedTasks = new mutable.HashMap[Long, TaskEndReason]
  val finishedManagers = new ArrayBuffer[TaskSetManager]
  val taskSetsFailed = new ArrayBuffer[String]
  val speculativeTasks = new ArrayBuffer[Int]

  val executors = new mutable.HashMap[String, String]

  def this(sc: SparkContext, liveExecutors: (String, String)*) = {
    this(sc, new SystemClock, liveExecutors: _*)
  }

  // this must be initialized before addExecutor
  override val defaultRackValue: Option[String] = Some("default")
  for ((execId, host) <- liveExecutors) {
    addExecutor(execId, host)
  }

  for ((execId, host) <- liveExecutors; rack <- getRackForHost(host)) {
    hostsByRack.getOrElseUpdate(rack, new mutable.HashSet[String]()) += host
  }

  dagScheduler = new FakeDAGScheduler(sc, this)

  def removeExecutor(execId: String): Unit = {
    executors -= execId
    val host = executorIdToHost.get(execId)
    assert(host.isDefined)
    val hostId = host.get
    val executorsOnHost = hostToExecutors(hostId)
    executorsOnHost -= execId
    for (rack <- getRackForHost(hostId); hosts <- hostsByRack.get(rack)) {
      hosts -= hostId
      if (hosts.isEmpty) {
        hostsByRack -= rack
      }
    }
  }

  override def taskSetFinished(manager: TaskSetManager): Unit = finishedManagers += manager

  override def isExecutorAlive(execId: String): Boolean =
    executors.contains(execId) && !isExecutorDecommissioned(execId)

  override def hasExecutorsAliveOnHost(host: String): Boolean =
    !isHostDecommissioned(host) && executors
      .exists { case (e, h) => h == host && !isExecutorDecommissioned(e) }

  def addExecutor(execId: String, host: String): Unit = {
    executors.put(execId, host)
    val executorsOnHost = hostToExecutors.getOrElseUpdate(host, new mutable.HashSet[String])
    executorsOnHost += execId
    executorIdToHost += execId -> host
    for (rack <- getRackForHost(host)) {
      hostsByRack.getOrElseUpdate(rack, new mutable.HashSet[String]()) += host
    }
  }

  override def getRacksForHosts(hosts: Seq[String]): Seq[Option[String]] = {
    FakeRackUtil.getRacksForHosts(hosts)
  }
}

/**
 * A Task implementation that results in a large serialized task.
 */
class LargeTask(stageId: Int) extends Task[Array[Byte]](stageId, 0, 0) {

  val randomBuffer = new Array[Byte](TaskSetManager.TASK_SIZE_TO_WARN_KIB * 1024)
  val random = new Random(0)
  random.nextBytes(randomBuffer)

  override def runTask(context: TaskContext): Array[Byte] = randomBuffer
  override def preferredLocations: Seq[TaskLocation] = Seq[TaskLocation]()
}

class TaskSetManagerSuite
  extends SparkFunSuite
  with LocalSparkContext
  with PrivateMethodTester
  with Eventually
  with Logging {
  import TaskLocality.{ANY, PROCESS_LOCAL, NO_PREF, NODE_LOCAL, RACK_LOCAL}

  private val conf = new SparkConf

  val LOCALITY_WAIT_MS = conf.get(config.LOCALITY_WAIT)
  val MAX_TASK_FAILURES = 4

  var sched: FakeTaskScheduler = null

  override def beforeEach(): Unit = {
    super.beforeEach()
    FakeRackUtil.cleanUp()
    sched = null
  }

  override def afterEach(): Unit = {
    if (sched != null) {
      sched.dagScheduler.stop()
      sched.stop()
      sched = null
    }
    super.afterEach()
  }

  test("TaskSet with no preferences") {
    sc = new SparkContext("local", "test")
    sched = new FakeTaskScheduler(sc, ("exec1", "host1"))
    val taskSet = FakeTask.createTaskSet(1)
    val clock = new ManualClock
    val manager = new TaskSetManager(sched, taskSet, MAX_TASK_FAILURES, clock = clock)
    val accumUpdates = taskSet.tasks.head.metrics.internalAccums

    // Offer a host with NO_PREF as the constraint,
    // we should get a nopref task immediately since that's what we only have
    val taskOption = manager.resourceOffer("exec1", "host1", NO_PREF)._1
    assert(taskOption.isDefined)

    clock.advance(1)
    // Tell it the task has finished
    manager.handleSuccessfulTask(0, createTaskResult(0, accumUpdates))
    assert(sched.endedTasks(0) === Success)
    assert(sched.finishedManagers.contains(manager))
  }

  test("multiple offers with no preferences") {
    sc = new SparkContext("local", "test")
    sched = new FakeTaskScheduler(sc, ("exec1", "host1"))
    val taskSet = FakeTask.createTaskSet(3)
    val manager = new TaskSetManager(sched, taskSet, MAX_TASK_FAILURES)
    val accumUpdatesByTask: Array[Seq[AccumulatorV2[_, _]]] = taskSet.tasks.map { task =>
      task.metrics.internalAccums
    }

    // First three offers should all find tasks
    for (i <- 0 until 3) {
      val taskOption = manager.resourceOffer("exec1", "host1", NO_PREF)._1
      assert(taskOption.isDefined)
      val task = taskOption.get
      assert(task.executorId === "exec1")
    }
    assert(sched.startedTasks.toSet === Set(0, 1, 2))

    // Re-offer the host -- now we should get no more tasks
    assert(manager.resourceOffer("exec1", "host1", NO_PREF)._1 === None)

    // Finish the first two tasks
    manager.handleSuccessfulTask(0, createTaskResult(0, accumUpdatesByTask(0)))
    manager.handleSuccessfulTask(1, createTaskResult(1, accumUpdatesByTask(1)))
    assert(sched.endedTasks(0) === Success)
    assert(sched.endedTasks(1) === Success)
    assert(!sched.finishedManagers.contains(manager))

    // Finish the last task
    manager.handleSuccessfulTask(2, createTaskResult(2, accumUpdatesByTask(2)))
    assert(sched.endedTasks(2) === Success)
    assert(sched.finishedManagers.contains(manager))
  }

  test("skip unsatisfiable locality levels") {
    sc = new SparkContext("local", "test")
    sched = new FakeTaskScheduler(sc, ("execA", "host1"), ("execC", "host2"))
    val taskSet = FakeTask.createTaskSet(1, Seq(TaskLocation("host1", "execB")))
    val clock = new ManualClock
    val manager = new TaskSetManager(sched, taskSet, MAX_TASK_FAILURES, clock = clock)

    // An executor that is not NODE_LOCAL should be rejected.
    assert(manager.resourceOffer("execC", "host2", ANY)._1 === None)

    // Because there are no alive PROCESS_LOCAL executors, the base locality level should be
    // NODE_LOCAL. So, we should schedule the task on this offered NODE_LOCAL executor before
    // any of the locality wait timers expire.
    assert(manager.resourceOffer("execA", "host1", ANY)._1.get.index === 0)
  }

  test("basic delay scheduling") {
    sc = new SparkContext("local", "test")
    sched = new FakeTaskScheduler(sc, ("exec1", "host1"), ("exec2", "host2"))
    val taskSet = FakeTask.createTaskSet(4,
      Seq(TaskLocation("host1", "exec1")),
      Seq(TaskLocation("host2", "exec2")),
      Seq(TaskLocation("host1"), TaskLocation("host2", "exec2")),
      Seq()   // Last task has no locality prefs
    )
    val clock = new ManualClock
    val manager = new TaskSetManager(sched, taskSet, MAX_TASK_FAILURES, clock = clock)
    // First offer host1, exec1: first task should be chosen
    assert(manager.resourceOffer("exec1", "host1", ANY)._1.get.index === 0)
    assert(manager.resourceOffer("exec1", "host1", PROCESS_LOCAL)._1 === None)

    clock.advance(LOCALITY_WAIT_MS)
    // Offer host1, exec1 again, at NODE_LOCAL level: the node local (task 3) should
    // get chosen before the noPref task
    assert(manager.resourceOffer("exec1", "host1", NODE_LOCAL)._1.get.index == 2)

    // Offer host2, exec2, at NODE_LOCAL level: we should choose task 2
    assert(manager.resourceOffer("exec2", "host2", NODE_LOCAL)._1.get.index == 1)

    // Offer host2, exec2 again, at NODE_LOCAL level: we should get noPref task
    // after failing to find a node_Local task
    assert(manager.resourceOffer("exec2", "host2", NODE_LOCAL)._1 === None)
    clock.advance(LOCALITY_WAIT_MS)
    assert(manager.resourceOffer("exec2", "host2", NO_PREF)._1.get.index == 3)
  }

  test("we do not need to delay scheduling when we only have noPref tasks in the queue") {
    sc = new SparkContext("local", "test")
    sched = new FakeTaskScheduler(sc, ("exec1", "host1"), ("exec3", "host2"))
    val taskSet = FakeTask.createTaskSet(3,
      Seq(TaskLocation("host1", "exec1")),
      Seq(TaskLocation("host2", "exec3")),
      Seq()   // Last task has no locality prefs
    )
    val clock = new ManualClock
    val manager = new TaskSetManager(sched, taskSet, MAX_TASK_FAILURES, clock = clock)
    // First offer host1, exec1: first task should be chosen
    assert(manager.resourceOffer("exec1", "host1", PROCESS_LOCAL)._1.get.index === 0)
    assert(manager.resourceOffer("exec3", "host2", PROCESS_LOCAL)._1.get.index === 1)
    assert(manager.resourceOffer("exec3", "host2", NODE_LOCAL)._1 === None)
    assert(manager.resourceOffer("exec3", "host2", NO_PREF)._1.get.index === 2)
  }

  test("delay scheduling with fallback") {
    sc = new SparkContext("local", "test")
    sched = new FakeTaskScheduler(sc,
      ("exec1", "host1"), ("exec2", "host2"), ("exec3", "host3"))
    val taskSet = FakeTask.createTaskSet(5,
      Seq(TaskLocation("host1")),
      Seq(TaskLocation("host2")),
      Seq(TaskLocation("host2")),
      Seq(TaskLocation("host3")),
      Seq(TaskLocation("host2"))
    )
    sc.conf.set(config.LEGACY_LOCALITY_WAIT_RESET, true)
    val clock = new ManualClock
    val manager = new TaskSetManager(sched, taskSet, MAX_TASK_FAILURES, clock = clock)

    // First offer host1: first task should be chosen
    assert(manager.resourceOffer("exec1", "host1", ANY)._1.get.index === 0)

    // Offer host1 again: nothing should get chosen
    assert(manager.resourceOffer("exec1", "host1", ANY)._1 === None)

    clock.advance(LOCALITY_WAIT_MS)

    // Offer host1 again: second task (on host2) should get chosen
    assert(manager.resourceOffer("exec1", "host1", ANY)._1.get.index === 1)

    // Offer host1 again: third task (on host2) should get chosen
    assert(manager.resourceOffer("exec1", "host1", ANY)._1.get.index === 2)

    // Offer host2: fifth task (also on host2) should get chosen
    assert(manager.resourceOffer("exec2", "host2", ANY)._1.get.index === 4)

    // Now that we've launched a local task, we should no longer launch the task for host3
    assert(manager.resourceOffer("exec2", "host2", ANY)._1 === None)

    clock.advance(LOCALITY_WAIT_MS)

    // offers not accepted due to task set zombies are not delay schedule rejects
    manager.isZombie = true
    val (taskDescription, delayReject) = manager.resourceOffer("exec2", "host2", ANY)
    assert(taskDescription.isEmpty)
    assert(delayReject === false)
    manager.isZombie = false

    // offers not accepted due to excludelist are not delay schedule rejects
    val tsmSpy = spy(manager)
    val excludelist = mock(classOf[TaskSetExcludelist])
    when(tsmSpy.taskSetExcludelistHelperOpt).thenReturn(Some(excludelist))
    when(excludelist.isNodeExcludedForTaskSet(any())).thenReturn(true)
    val (task, taskReject) = tsmSpy.resourceOffer("exec2", "host2", ANY)
    assert(task.isEmpty)
    assert(taskReject === false)

    // After another delay, we can go ahead and launch that task non-locally
    assert(manager.resourceOffer("exec2", "host2", ANY)._1.get.index === 3)

    // offers not accepted due to no pending tasks are not delay schedule rejects
    val (noPendingTask, noPendingReject) = manager.resourceOffer("exec2", "host2", ANY)
    assert(noPendingTask.isEmpty)
    assert(noPendingReject === false)
  }

  test("delay scheduling with failed hosts") {
    sc = new SparkContext("local", "test")
    sched = new FakeTaskScheduler(sc, ("exec1", "host1"), ("exec2", "host2"),
      ("exec3", "host3"))
    val taskSet = FakeTask.createTaskSet(3,
      Seq(TaskLocation("host1")),
      Seq(TaskLocation("host2")),
      Seq(TaskLocation("host3"))
    )
    val clock = new ManualClock
    val manager = new TaskSetManager(sched, taskSet, MAX_TASK_FAILURES, clock = clock)

    // First offer host1: first task should be chosen
    assert(manager.resourceOffer("exec1", "host1", ANY)._1.get.index === 0)

    // After this, nothing should get chosen, because we have separated tasks with unavailable
    // preference from the noPrefPendingTasks
    assert(manager.resourceOffer("exec1", "host1", ANY)._1 === None)

    // Now mark host2 as dead
    sched.removeExecutor("exec2")
    manager.executorLost("exec2", "host2", ExecutorProcessLost())

    // nothing should be chosen
    assert(manager.resourceOffer("exec1", "host1", ANY)._1 === None)

    clock.advance(LOCALITY_WAIT_MS * 2)

    // task 1 and 2 would be scheduled as nonLocal task
    assert(manager.resourceOffer("exec1", "host1", ANY)._1.get.index === 1)
    assert(manager.resourceOffer("exec1", "host1", ANY)._1.get.index === 2)

    // all finished
    assert(manager.resourceOffer("exec1", "host1", ANY)._1 === None)
    assert(manager.resourceOffer("exec2", "host2", ANY)._1 === None)
  }

  test("task result lost") {
    sc = new SparkContext("local", "test")
    sched = new FakeTaskScheduler(sc, ("exec1", "host1"))
    val taskSet = FakeTask.createTaskSet(1)
    val clock = new ManualClock
    clock.advance(1)
    val manager = new TaskSetManager(sched, taskSet, MAX_TASK_FAILURES, clock = clock)

    assert(manager.resourceOffer("exec1", "host1", ANY)._1.get.index === 0)

    // Tell it the task has finished but the result was lost.
    manager.handleFailedTask(0, TaskState.FINISHED, TaskResultLost)
    assert(sched.endedTasks(0) === TaskResultLost)

    // Re-offer the host -- now we should get task 0 again.
    assert(manager.resourceOffer("exec1", "host1", ANY)._1.get.index === 0)
  }

  test("repeated failures lead to task set abortion") {
    sc = new SparkContext("local", "test")
    sched = new FakeTaskScheduler(sc, ("exec1", "host1"))
    val taskSet = FakeTask.createTaskSet(1)
    val clock = new ManualClock
    clock.advance(1)
    val manager = new TaskSetManager(sched, taskSet, MAX_TASK_FAILURES, clock = clock)

    // Fail the task MAX_TASK_FAILURES times, and check that the task set is aborted
    // after the last failure.
    (1 to manager.maxTaskFailures).foreach { index =>
      val offerResult = manager.resourceOffer("exec1", "host1", ANY)._1
      assert(offerResult.isDefined,
        "Expect resource offer on iteration %s to return a task".format(index))
      assert(offerResult.get.index === 0)
      manager.handleFailedTask(offerResult.get.taskId, TaskState.FINISHED, TaskResultLost)
      if (index < MAX_TASK_FAILURES) {
        assert(!sched.taskSetsFailed.contains(taskSet.id))
      } else {
        assert(sched.taskSetsFailed.contains(taskSet.id))
      }
    }
  }

  test("executors should be excluded after task failure, in spite of locality preferences") {
    val rescheduleDelay = 300L
    val conf = new SparkConf().
      set(config.EXCLUDE_ON_FAILURE_ENABLED, true).
      set(config.EXCLUDE_ON_FAILURE_TIMEOUT_CONF, rescheduleDelay).
      // don't wait to jump locality levels in this test
      set(config.LOCALITY_WAIT.key, "0")

    sc = new SparkContext("local", "test", conf)
    // two executors on same host, one on different.
    sched = new FakeTaskScheduler(sc, ("exec1", "host1"),
      ("exec1.1", "host1"), ("exec2", "host2"))
    // affinity to exec1 on host1 - which we will fail.
    val taskSet = FakeTask.createTaskSet(1, Seq(TaskLocation("host1", "exec1")))
    val clock = new ManualClock
    clock.advance(1)
    // We don't directly use the application excludelist, but its presence triggers exclusion
    // within the taskset.
    val mockListenerBus = mock(classOf[LiveListenerBus])
    val healthTrackerOpt = Some(new HealthTracker(mockListenerBus, conf, None, clock))
    val manager = new TaskSetManager(sched, taskSet, 4, healthTrackerOpt, clock)

    {
      val offerResult = manager.resourceOffer("exec1", "host1", PROCESS_LOCAL)._1
      assert(offerResult.isDefined, "Expect resource offer to return a task")

      assert(offerResult.get.index === 0)
      assert(offerResult.get.executorId === "exec1")

      // Cause exec1 to fail : failure 1
      manager.handleFailedTask(offerResult.get.taskId, TaskState.FINISHED, TaskResultLost)
      assert(!sched.taskSetsFailed.contains(taskSet.id))

      // Ensure scheduling on exec1 fails after failure 1 due to executor being excluded
      assert(manager.resourceOffer("exec1", "host1", PROCESS_LOCAL)._1.isEmpty)
      assert(manager.resourceOffer("exec1", "host1", NODE_LOCAL)._1.isEmpty)
      assert(manager.resourceOffer("exec1", "host1", RACK_LOCAL)._1.isEmpty)
      assert(manager.resourceOffer("exec1", "host1", ANY)._1.isEmpty)
    }

    // Run the task on exec1.1 - should work, and then fail it on exec1.1
    {
      val offerResult = manager.resourceOffer("exec1.1", "host1", NODE_LOCAL)._1
      assert(offerResult.isDefined,
        "Expect resource offer to return a task for exec1.1, offerResult = " + offerResult)

      assert(offerResult.get.index === 0)
      assert(offerResult.get.executorId === "exec1.1")

      // Cause exec1.1 to fail : failure 2
      manager.handleFailedTask(offerResult.get.taskId, TaskState.FINISHED, TaskResultLost)
      assert(!sched.taskSetsFailed.contains(taskSet.id))

      // Ensure scheduling on exec1.1 fails after failure 2 due to executor being excluded
      assert(manager.resourceOffer("exec1.1", "host1", NODE_LOCAL)._1.isEmpty)
    }

    // Run the task on exec2 - should work, and then fail it on exec2
    {
      val offerResult = manager.resourceOffer("exec2", "host2", ANY)._1
      assert(offerResult.isDefined, "Expect resource offer to return a task")

      assert(offerResult.get.index === 0)
      assert(offerResult.get.executorId === "exec2")

      // Cause exec2 to fail : failure 3
      manager.handleFailedTask(offerResult.get.taskId, TaskState.FINISHED, TaskResultLost)
      assert(!sched.taskSetsFailed.contains(taskSet.id))

      // Ensure scheduling on exec2 fails after failure 3 due to executor being excluded
      assert(manager.resourceOffer("exec2", "host2", ANY)._1.isEmpty)
    }

    // Despite advancing beyond the time for expiring executors from within the excludelist,
    // we *never* expire from *within* the stage excludelist
    clock.advance(rescheduleDelay)

    {
      val offerResult = manager.resourceOffer("exec1", "host1", PROCESS_LOCAL)._1
      assert(offerResult.isEmpty)
    }

    {
      val offerResult = manager.resourceOffer("exec3", "host3", ANY)._1
      assert(offerResult.isDefined)
      assert(offerResult.get.index === 0)
      assert(offerResult.get.executorId === "exec3")

      assert(manager.resourceOffer("exec3", "host3", ANY)._1.isEmpty)

      // Cause exec3 to fail : failure 4
      manager.handleFailedTask(offerResult.get.taskId, TaskState.FINISHED, TaskResultLost)
    }

    // we have failed the same task 4 times now : task id should now be in taskSetsFailed
    assert(sched.taskSetsFailed.contains(taskSet.id))
  }

  test("new executors get added and lost") {
    // Assign host2 to rack2
    FakeRackUtil.assignHostToRack("host2", "rack2")
    sc = new SparkContext("local", "test")
    sched = new FakeTaskScheduler(sc)
    val taskSet = FakeTask.createTaskSet(4,
      Seq(TaskLocation("host1", "execA")),
      Seq(TaskLocation("host1", "execB")),
      Seq(TaskLocation("host2", "execC")),
      Seq())
    val clock = new ManualClock
    val manager = new TaskSetManager(sched, taskSet, MAX_TASK_FAILURES, clock = clock)
    // Only ANY is valid
    assert(manager.myLocalityLevels.sameElements(Array(NO_PREF, ANY)))
    // Add a new executor
    sched.addExecutor("execD", "host1")
    manager.executorAdded()
    // Valid locality should contain NODE_LOCAL and ANY
    assert(manager.myLocalityLevels.sameElements(Array(NODE_LOCAL, NO_PREF, ANY)))
    // Add another executor
    sched.addExecutor("execC", "host2")
    manager.executorAdded()
    // Valid locality should contain PROCESS_LOCAL, NODE_LOCAL, RACK_LOCAL and ANY
    assert(manager.myLocalityLevels.sameElements(
      Array(PROCESS_LOCAL, NODE_LOCAL, NO_PREF, RACK_LOCAL, ANY)))
    // test if the valid locality is recomputed when the executor is lost
    sched.removeExecutor("execC")
    manager.executorLost("execC", "host2", ExecutorProcessLost())
    assert(manager.myLocalityLevels.sameElements(Array(NODE_LOCAL, NO_PREF, ANY)))
    sched.removeExecutor("execD")
    manager.executorLost("execD", "host1", ExecutorProcessLost())
    assert(manager.myLocalityLevels.sameElements(Array(NO_PREF, ANY)))
  }

  test("Executors exit for reason unrelated to currently running tasks") {
    sc = new SparkContext("local", "test")
    sched = new FakeTaskScheduler(sc)
    val taskSet = FakeTask.createTaskSet(4,
      Seq(TaskLocation("host1", "execA")),
      Seq(TaskLocation("host1", "execB")),
      Seq(TaskLocation("host2", "execC")),
      Seq())
    val clock = new ManualClock()
    clock.advance(1)
    val manager = new TaskSetManager(sched, taskSet, 1, clock = clock)
    sched.addExecutor("execA", "host1")
    manager.executorAdded()
    sched.addExecutor("execC", "host2")
    manager.executorAdded()
    assert(manager.resourceOffer("execB", "host1", ANY)._1.isDefined)
    sched.removeExecutor("execA")
    manager.executorLost(
      "execA",
      "host1",
      ExecutorExited(143, false, "Terminated for reason unrelated to running tasks"))
    assert(!sched.taskSetsFailed.contains(taskSet.id))
    assert(manager.resourceOffer("execC", "host2", ANY)._1.isDefined)
    sched.removeExecutor("execC")
    manager.executorLost(
      "execC", "host2", ExecutorExited(1, true, "Terminated due to issue with running tasks"))
    assert(sched.taskSetsFailed.contains(taskSet.id))
  }

  test("SPARK-31837: Shift to the new highest locality level if there is when recomputeLocality") {
    sc = new SparkContext("local", "test")
    sched = new FakeTaskScheduler(sc)
    val taskSet = FakeTask.createTaskSet(2,
      Seq(TaskLocation("host1", "execA")),
      Seq(TaskLocation("host1", "execA")))
    val clock = new ManualClock()
    val manager = new TaskSetManager(sched, taskSet, 1, clock = clock)
    // before any executors are added to TaskScheduler, the manager's
    // locality level only has ANY, so tasks can be scheduled anyway.
    assert(manager.resourceOffer("execB", "host2", ANY)._1.isDefined)
    sched.addExecutor("execA", "host1")
    manager.executorAdded()
    // after adding a new executor, the manager locality has PROCESS_LOCAL, NODE_LOCAL, ANY.
    // And we'll shift to the new highest locality level, which is PROCESS_LOCAL in this case.
    assert(manager.resourceOffer("execC", "host3", ANY)._1.isEmpty)
    assert(manager.resourceOffer("execA", "host1", ANY)._1.isDefined)
  }

  test("SPARK-32653: Decommissioned host should not be used to calculate locality levels") {
    sc = new SparkContext("local", "test")
    sched = new FakeTaskScheduler(sc)
    val backend = mock(classOf[SchedulerBackend])
    doNothing().when(backend).reviveOffers()
    sched.initialize(backend)

    val exec0 = "exec0"
    val exec1 = "exec1"
    val host0 = "host0"
    sched.addExecutor(exec0, host0)
    sched.addExecutor(exec1, host0)

    val taskSet = FakeTask.createTaskSet(2,
      Seq(ExecutorCacheTaskLocation(host0, exec0)),
      Seq(ExecutorCacheTaskLocation(host0, exec1)))
    sched.submitTasks(taskSet)
    val manager = sched.taskSetManagerForAttempt(0, 0).get

    assert(manager.myLocalityLevels === Array(PROCESS_LOCAL, NODE_LOCAL, ANY))

    // Decommission all executors on host0, to mimic CoarseGrainedSchedulerBackend.
    sched.executorDecommission(exec0, ExecutorDecommissionInfo("test", Some(host0)))
    sched.executorDecommission(exec1, ExecutorDecommissionInfo("test", Some(host0)))

    assert(manager.myLocalityLevels === Array(ANY))
  }

  test("SPARK-32653: Decommissioned executor should not be used to calculate locality levels") {
    sc = new SparkContext("local", "test")
    sched = new FakeTaskScheduler(sc)
    val backend = mock(classOf[SchedulerBackend])
    doNothing().when(backend).reviveOffers()
    sched.initialize(backend)

    val exec0 = "exec0"
    val exec1 = "exec1"
    val host0 = "host0"
    sched.addExecutor(exec0, host0)
    sched.addExecutor(exec1, host0)

    val taskSet = FakeTask.createTaskSet(1, Seq(ExecutorCacheTaskLocation(host0, exec0)))
    sched.submitTasks(taskSet)
    val manager = sched.taskSetManagerForAttempt(0, 0).get

    assert(manager.myLocalityLevels === Array(PROCESS_LOCAL, NODE_LOCAL, ANY))

    // Decommission the only executor (without the host) that the task is interested in running on.
    sched.executorDecommission(exec0, ExecutorDecommissionInfo("test", None))

    assert(manager.myLocalityLevels === Array(NODE_LOCAL, ANY))
  }

  test("test RACK_LOCAL tasks") {
    // Assign host1 to rack1
    FakeRackUtil.assignHostToRack("host1", "rack1")
    // Assign host2 to rack1
    FakeRackUtil.assignHostToRack("host2", "rack1")
    // Assign host3 to rack2
    FakeRackUtil.assignHostToRack("host3", "rack2")
    sc = new SparkContext("local", "test")
    sched = new FakeTaskScheduler(sc,
      ("execA", "host1"), ("execB", "host2"), ("execC", "host3"))
    val taskSet = FakeTask.createTaskSet(2,
      Seq(TaskLocation("host1", "execA")),
      Seq(TaskLocation("host1", "execA")))
    val clock = new ManualClock
    val manager = new TaskSetManager(sched, taskSet, MAX_TASK_FAILURES, clock = clock)

    assert(manager.myLocalityLevels.sameElements(Array(PROCESS_LOCAL, NODE_LOCAL, RACK_LOCAL, ANY)))
    // Set allowed locality to ANY
    clock.advance(LOCALITY_WAIT_MS * 3)
    // Offer host3
    // No task is scheduled if we restrict locality to RACK_LOCAL
    assert(manager.resourceOffer("execC", "host3", RACK_LOCAL)._1 === None)
    // Task 0 can be scheduled with ANY
    assert(manager.resourceOffer("execC", "host3", ANY)._1.get.index === 0)
    // Offer host2
    // Task 1 can be scheduled with RACK_LOCAL
    assert(manager.resourceOffer("execB", "host2", RACK_LOCAL)._1.get.index === 1)
  }

  test("do not emit warning when serialized task is small") {
    sc = new SparkContext("local", "test")
    sched = new FakeTaskScheduler(sc, ("exec1", "host1"))
    val taskSet = FakeTask.createTaskSet(1)
    val manager = new TaskSetManager(sched, taskSet, MAX_TASK_FAILURES)

    assert(!manager.emittedTaskSizeWarning)

    assert(manager.resourceOffer("exec1", "host1", ANY)._1.get.index === 0)

    assert(!manager.emittedTaskSizeWarning)
  }

  test("emit warning when serialized task is large") {
    sc = new SparkContext("local", "test")
    sched = new FakeTaskScheduler(sc, ("exec1", "host1"))

    val taskSet = new TaskSet(Array(new LargeTask(0)), 0, 0, 0,
      null, ResourceProfile.DEFAULT_RESOURCE_PROFILE_ID)
    val manager = new TaskSetManager(sched, taskSet, MAX_TASK_FAILURES)

    assert(!manager.emittedTaskSizeWarning)

    assert(manager.resourceOffer("exec1", "host1", ANY)._1.get.index === 0)

    assert(manager.emittedTaskSizeWarning)
  }

  test("Not serializable exception thrown if the task cannot be serialized") {
    sc = new SparkContext("local", "test")
    sched = new FakeTaskScheduler(sc, ("exec1", "host1"))

    val taskSet = new TaskSet(
      Array(new NotSerializableFakeTask(1, 0), new NotSerializableFakeTask(0, 1)),
      0, 0, 0, null, ResourceProfile.DEFAULT_RESOURCE_PROFILE_ID)
    val manager = new TaskSetManager(sched, taskSet, MAX_TASK_FAILURES)

    intercept[TaskNotSerializableException] {
      manager.resourceOffer("exec1", "host1", ANY)
    }
    assert(manager.isZombie)
  }

  test("abort the job if total size of results is too large") {
    val conf = new SparkConf().set(config.MAX_RESULT_SIZE.key, "2m")
    sc = new SparkContext("local", "test", conf)

    def genBytes(size: Int): (Int) => Array[Byte] = { (x: Int) =>
      val bytes = Array.ofDim[Byte](size)
      scala.util.Random.nextBytes(bytes)
      bytes
    }

    // multiple 1k result
    val r = sc.makeRDD(0 until 10, 10).map(genBytes(1024)).collect()
    assert(10 === r.size)

    // single 10M result
    val thrown = intercept[SparkException] {sc.makeRDD(genBytes(10 << 20)(0), 1).collect()}
    assert(thrown.getMessage().contains("bigger than spark.driver.maxResultSize"))

    // multiple 1M results
    val thrown2 = intercept[SparkException] {
      sc.makeRDD(0 until 10, 10).map(genBytes(1 << 20)).collect()
    }
    assert(thrown2.getMessage().contains("bigger than spark.driver.maxResultSize"))
  }

  test("SPARK-32470: do not check total size of intermediate stages") {
    val conf = new SparkConf().set(config.MAX_RESULT_SIZE.key, "20k")
    sc = new SparkContext("local", "test", conf)
    // final result is below limit.
    val r = sc.makeRDD(0 until 2000, 2000).distinct(10).filter(_ == 0).collect()
    assert(1 === r.size)
  }

  test("[SPARK-13931] taskSetManager should not send Resubmitted tasks after being a zombie") {
    val conf = new SparkConf().set(config.SPECULATION_ENABLED, true)
    sc = new SparkContext("local", "test", conf)

    sched = new FakeTaskScheduler(sc, ("execA", "host1"), ("execB", "host2"))
    sched.initialize(new FakeSchedulerBackend() {
      override def killTask(
        taskId: Long,
        executorId: String,
        interruptThread: Boolean,
        reason: String): Unit = {}
    })

    // Keep track of the number of tasks that are resubmitted,
    // so that the test can check that no tasks were resubmitted.
    var resubmittedTasks = 0
    val dagScheduler = new FakeDAGScheduler(sc, sched) {
      override def taskEnded(
          task: Task[_],
          reason: TaskEndReason,
          result: Any,
          accumUpdates: Seq[AccumulatorV2[_, _]],
          metricPeaks: Array[Long],
          taskInfo: TaskInfo): Unit = {
        super.taskEnded(task, reason, result, accumUpdates, metricPeaks, taskInfo)
        reason match {
          case Resubmitted => resubmittedTasks += 1
          case _ =>
        }
      }
    }
    sched.dagScheduler.stop()
    sched.setDAGScheduler(dagScheduler)

    val singleTask = new ShuffleMapTask(0, 0, null, new Partition {
        override def index: Int = 0
      }, Seq(TaskLocation("host1", "execA")), new Properties, null)
    val taskSet = new TaskSet(Array(singleTask), 0, 0, 0,
      null, ResourceProfile.DEFAULT_RESOURCE_PROFILE_ID)
    val manager = new TaskSetManager(sched, taskSet, MAX_TASK_FAILURES)

    // Offer host1, which should be accepted as a PROCESS_LOCAL location
    // by the one task in the task set
    val task1 = manager.resourceOffer("execA", "host1", TaskLocality.PROCESS_LOCAL)._1.get

    // Mark the task as available for speculation, and then offer another resource,
    // which should be used to launch a speculative copy of the task.
    manager.speculatableTasks += singleTask.partitionId
    manager.addPendingTask(singleTask.partitionId, speculatable = true)
    val task2 = manager.resourceOffer("execB", "host2", TaskLocality.ANY)._1.get

    assert(manager.runningTasks === 2)
    assert(manager.isZombie === false)

    val directTaskResult = new DirectTaskResult[String](null, Seq(), Array()) {
      override def value(resultSer: SerializerInstance): String = ""
    }
    // Complete one copy of the task, which should result in the task set manager
    // being marked as a zombie, because at least one copy of its only task has completed.
    manager.handleSuccessfulTask(task1.taskId, directTaskResult)
    assert(manager.isZombie)
    assert(resubmittedTasks === 0)
    assert(manager.runningTasks === 1)

    manager.executorLost("execB", "host2", new ExecutorProcessLost())
    assert(manager.runningTasks === 0)
    assert(resubmittedTasks === 0)
  }


  test("[SPARK-22074] Task killed by other attempt task should not be resubmitted") {
    val conf = new SparkConf().set(config.SPECULATION_ENABLED, true)
    sc = new SparkContext("local", "test", conf)
    // Set the speculation multiplier to be 0 so speculative tasks are launched immediately
    sc.conf.set(config.SPECULATION_MULTIPLIER, 0.0)
    sc.conf.set(config.SPECULATION_QUANTILE, 0.5)
    sc.conf.set(config.SPECULATION_ENABLED, true)

    var killTaskCalled = false
    sched = new FakeTaskScheduler(sc, ("exec1", "host1"),
      ("exec2", "host2"), ("exec3", "host3"))
    sched.initialize(new FakeSchedulerBackend() {
      override def killTask(
          taskId: Long,
          executorId: String,
          interruptThread: Boolean,
          reason: String): Unit = {
        // Check the only one killTask event in this case, which triggered by
        // task 2.1 completed.
        assert(taskId === 2)
        assert(executorId === "exec3")
        assert(interruptThread)
        assert(reason === "another attempt succeeded")
        killTaskCalled = true
      }
    })

    // Keep track of the number of tasks that are resubmitted,
    // so that the test can check that no tasks were resubmitted.
    var resubmittedTasks = 0
    val dagScheduler = new FakeDAGScheduler(sc, sched) {
      override def taskEnded(
          task: Task[_],
          reason: TaskEndReason,
          result: Any,
          accumUpdates: Seq[AccumulatorV2[_, _]],
          metricPeaks: Array[Long],
          taskInfo: TaskInfo): Unit = {
        super.taskEnded(task, reason, result, accumUpdates, metricPeaks, taskInfo)
        reason match {
          case Resubmitted => resubmittedTasks += 1
          case _ =>
        }
      }
    }
    sched.dagScheduler.stop()
    sched.setDAGScheduler(dagScheduler)

    val taskSet = FakeTask.createShuffleMapTaskSet(4, 0, 0,
      Seq(TaskLocation("host1", "exec1")),
      Seq(TaskLocation("host1", "exec1")),
      Seq(TaskLocation("host3", "exec3")),
      Seq(TaskLocation("host2", "exec2")))

    val clock = new ManualClock()
    val manager = new TaskSetManager(sched, taskSet, MAX_TASK_FAILURES, clock = clock)
    val accumUpdatesByTask: Array[Seq[AccumulatorV2[_, _]]] = taskSet.tasks.map { task =>
      task.metrics.internalAccums
    }
    // Offer resources for 4 tasks to start
    for ((exec, host) <- Seq(
      "exec1" -> "host1",
      "exec1" -> "host1",
      "exec3" -> "host3",
      "exec2" -> "host2")) {
      val taskOption = manager.resourceOffer(exec, host, NO_PREF)._1
      assert(taskOption.isDefined)
      val task = taskOption.get
      assert(task.executorId === exec)
      // Add an extra assert to make sure task 2.0 is running on exec3
      if (task.index == 2) {
        assert(task.attemptNumber === 0)
        assert(task.executorId === "exec3")
      }
    }
    assert(sched.startedTasks.toSet === Set(0, 1, 2, 3))
    clock.advance(1)
    // Complete the 2 tasks and leave 2 task in running
    for (id <- Set(0, 1)) {
      manager.handleSuccessfulTask(id, createTaskResult(id, accumUpdatesByTask(id)))
      assert(sched.endedTasks(id) === Success)
    }

    // checkSpeculatableTasks checks that the task runtime is greater than the threshold for
    // speculating. Since we use a threshold of 0 for speculation, tasks need to be running for
    // > 0ms, so advance the clock by 1ms here.
    clock.advance(1)
    assert(manager.checkSpeculatableTasks(0))
    assert(sched.speculativeTasks.toSet === Set(2, 3))

    // Offer resource to start the speculative attempt for the running task 2.0
    val taskOption = manager.resourceOffer("exec2", "host2", ANY)._1
    assert(taskOption.isDefined)
    val task4 = taskOption.get
    assert(task4.index === 2)
    assert(task4.taskId === 4)
    assert(task4.executorId === "exec2")
    assert(task4.attemptNumber === 1)
    // Complete the speculative attempt for the running task
    manager.handleSuccessfulTask(4, createTaskResult(2, accumUpdatesByTask(2)))
    // Make sure schedBackend.killTask(2, "exec3", true, "another attempt succeeded") gets called
    assert(killTaskCalled)
    // Host 3 Losts, there's only task 2.0 on it, which killed by task 2.1
    manager.executorLost("exec3", "host3", ExecutorProcessLost())
    // Check the resubmittedTasks
    assert(resubmittedTasks === 0)
  }

  test("speculative and noPref task should be scheduled after node-local") {
    sc = new SparkContext("local", "test")
    sched = new FakeTaskScheduler(
      sc, ("execA", "host1"), ("execB", "host2"), ("execC", "host3"))
    val taskSet = FakeTask.createTaskSet(4,
      Seq(TaskLocation("host1", "execA")),
      Seq(TaskLocation("host2"), TaskLocation("host1")),
      Seq(),
      Seq(TaskLocation("host3", "execC")))
    val clock = new ManualClock
    val manager = new TaskSetManager(sched, taskSet, MAX_TASK_FAILURES, clock = clock)

    assert(manager.resourceOffer("execA", "host1", PROCESS_LOCAL)._1.get.index === 0)
    assert(manager.resourceOffer("execA", "host1", NODE_LOCAL)._1 === None)
    assert(manager.resourceOffer("execA", "host1", NO_PREF)._1.get.index == 1)

    manager.speculatableTasks += 1
    manager.addPendingTask(1, speculatable = true)
    clock.advance(LOCALITY_WAIT_MS)
    // schedule the nonPref task
    assert(manager.resourceOffer("execA", "host1", NO_PREF)._1.get.index === 2)
    // schedule the speculative task
    assert(manager.resourceOffer("execB", "host2", NO_PREF)._1.get.index === 1)
    clock.advance(LOCALITY_WAIT_MS * 3)
    // schedule non-local tasks
    assert(manager.resourceOffer("execB", "host2", ANY)._1.get.index === 3)
  }

  test("node-local tasks should be scheduled right away " +
    "when there are only node-local and no-preference tasks") {
    sc = new SparkContext("local", "test")
    sched = new FakeTaskScheduler(
      sc, ("execA", "host1"), ("execB", "host2"), ("execC", "host3"))
    val taskSet = FakeTask.createTaskSet(4,
      Seq(TaskLocation("host1")),
      Seq(TaskLocation("host2")),
      Seq(),
      Seq(TaskLocation("host3")))
    val clock = new ManualClock
    val manager = new TaskSetManager(sched, taskSet, MAX_TASK_FAILURES, clock = clock)

    // node-local tasks are scheduled without delay
    assert(manager.resourceOffer("execA", "host1", NODE_LOCAL)._1.get.index === 0)
    assert(manager.resourceOffer("execA", "host2", NODE_LOCAL)._1.get.index === 1)
    assert(manager.resourceOffer("execA", "host3", NODE_LOCAL)._1.get.index === 3)
    assert(manager.resourceOffer("execA", "host3", NODE_LOCAL)._1 === None)

    // schedule no-preference after node local ones
    assert(manager.resourceOffer("execA", "host3", NO_PREF)._1.get.index === 2)
  }

  test("SPARK-4939: node-local tasks should be scheduled right after process-local tasks finished")
  {
    sc = new SparkContext("local", "test")
    sched = new FakeTaskScheduler(sc, ("execA", "host1"), ("execB", "host2"))
    val taskSet = FakeTask.createTaskSet(4,
      Seq(TaskLocation("host1")),
      Seq(TaskLocation("host2")),
      Seq(ExecutorCacheTaskLocation("host1", "execA")),
      Seq(ExecutorCacheTaskLocation("host2", "execB")))
    val clock = new ManualClock
    val manager = new TaskSetManager(sched, taskSet, MAX_TASK_FAILURES, clock = clock)

    // process-local tasks are scheduled first
    assert(manager.resourceOffer("execA", "host1", NODE_LOCAL)._1.get.index === 2)
    assert(manager.resourceOffer("execB", "host2", NODE_LOCAL)._1.get.index === 3)
    // node-local tasks are scheduled without delay
    assert(manager.resourceOffer("execA", "host1", NODE_LOCAL)._1.get.index === 0)
    assert(manager.resourceOffer("execB", "host2", NODE_LOCAL)._1.get.index === 1)
    assert(manager.resourceOffer("execA", "host1", NODE_LOCAL)._1 === None)
    assert(manager.resourceOffer("execB", "host2", NODE_LOCAL)._1 === None)
  }

  test("SPARK-4939: no-pref tasks should be scheduled after process-local tasks finished") {
    sc = new SparkContext("local", "test")
    sched = new FakeTaskScheduler(sc, ("execA", "host1"), ("execB", "host2"))
    val taskSet = FakeTask.createTaskSet(3,
      Seq(),
      Seq(ExecutorCacheTaskLocation("host1", "execA")),
      Seq(ExecutorCacheTaskLocation("host2", "execB")))
    val clock = new ManualClock
    val manager = new TaskSetManager(sched, taskSet, MAX_TASK_FAILURES, clock = clock)

    // process-local tasks are scheduled first
    assert(manager.resourceOffer("execA", "host1", PROCESS_LOCAL)._1.get.index === 1)
    assert(manager.resourceOffer("execB", "host2", PROCESS_LOCAL)._1.get.index === 2)
    // no-pref tasks are scheduled without delay
    assert(manager.resourceOffer("execA", "host1", PROCESS_LOCAL)._1 === None)
    assert(manager.resourceOffer("execA", "host1", NODE_LOCAL)._1 === None)
    assert(manager.resourceOffer("execA", "host1", NO_PREF)._1.get.index === 0)
    assert(manager.resourceOffer("execA", "host1", ANY)._1 === None)
  }

  test("Ensure TaskSetManager is usable after addition of levels") {
    // Regression test for SPARK-2931
    sc = new SparkContext("local", "test")
    sched = new FakeTaskScheduler(sc)
    val taskSet = FakeTask.createTaskSet(2,
      Seq(TaskLocation("host1", "execA")),
      Seq(TaskLocation("host2", "execB.1")))
    val clock = new ManualClock
    val manager = new TaskSetManager(sched, taskSet, MAX_TASK_FAILURES, clock = clock)
    // Only ANY is valid
    assert(manager.myLocalityLevels.sameElements(Array(ANY)))
    // Add a new executor
    sched.addExecutor("execA", "host1")
    sched.addExecutor("execB.2", "host2")
    manager.executorAdded()
    assert(manager.pendingTasks.noPrefs.size === 0)
    // Valid locality should contain PROCESS_LOCAL, NODE_LOCAL and ANY
    assert(manager.myLocalityLevels.sameElements(Array(PROCESS_LOCAL, NODE_LOCAL, ANY)))
    assert(manager.resourceOffer("execA", "host1", ANY) !== None)
    clock.advance(LOCALITY_WAIT_MS * 4)
    assert(manager.resourceOffer("execB.2", "host2", ANY) !== None)
    sched.removeExecutor("execA")
    sched.removeExecutor("execB.2")
    manager.executorLost("execA", "host1", ExecutorProcessLost())
    manager.executorLost("execB.2", "host2", ExecutorProcessLost())
    clock.advance(LOCALITY_WAIT_MS * 4)
    sched.addExecutor("execC", "host3")
    manager.executorAdded()
    // Prior to the fix, this line resulted in an ArrayIndexOutOfBoundsException:
    assert(manager.resourceOffer("execC", "host3", ANY) !== None)
  }

  test("Test that locations with HDFSCacheTaskLocation are treated as PROCESS_LOCAL.") {
    // Regression test for SPARK-2931
    sc = new SparkContext("local", "test")
    sched = new FakeTaskScheduler(sc,
      ("execA", "host1"), ("execB", "host2"), ("execC", "host3"))
    val taskSet = FakeTask.createTaskSet(3,
      Seq(TaskLocation("host1")),
      Seq(TaskLocation("host2")),
      Seq(TaskLocation("hdfs_cache_host3")))
    val clock = new ManualClock
    val manager = new TaskSetManager(sched, taskSet, MAX_TASK_FAILURES, clock = clock)
    assert(manager.myLocalityLevels.sameElements(Array(PROCESS_LOCAL, NODE_LOCAL, ANY)))
    sched.removeExecutor("execA")
    manager.executorAdded()
    assert(manager.myLocalityLevels.sameElements(Array(PROCESS_LOCAL, NODE_LOCAL, ANY)))
    sched.removeExecutor("execB")
    manager.executorAdded()
    assert(manager.myLocalityLevels.sameElements(Array(PROCESS_LOCAL, NODE_LOCAL, ANY)))
    sched.removeExecutor("execC")
    manager.executorAdded()
    assert(manager.myLocalityLevels.sameElements(Array(ANY)))
  }

  test("Test TaskLocation for different host type.") {
    assert(TaskLocation("host1") === HostTaskLocation("host1"))
    assert(TaskLocation("hdfs_cache_host1") === HDFSCacheTaskLocation("host1"))
    assert(TaskLocation("executor_host1_3") === ExecutorCacheTaskLocation("host1", "3"))
    assert(TaskLocation("executor_some.host1_executor_task_3") ===
      ExecutorCacheTaskLocation("some.host1", "executor_task_3"))
  }

  test("Kill other task attempts when one attempt belonging to the same task succeeds") {
    sc = new SparkContext("local", "test")
    sched = new FakeTaskScheduler(sc, ("exec1", "host1"), ("exec2", "host2"))
    val taskSet = FakeTask.createTaskSet(4)
    // Set the speculation multiplier to be 0 so speculative tasks are launched immediately
    sc.conf.set(config.SPECULATION_MULTIPLIER, 0.0)
    sc.conf.set(config.SPECULATION_ENABLED, true)
    val clock = new ManualClock()
    val manager = new TaskSetManager(sched, taskSet, MAX_TASK_FAILURES, clock = clock)
    val accumUpdatesByTask: Array[Seq[AccumulatorV2[_, _]]] = taskSet.tasks.map { task =>
      task.metrics.internalAccums
    }
    // Offer resources for 4 tasks to start
    for ((k, v) <- List(
      "exec1" -> "host1",
      "exec1" -> "host1",
      "exec2" -> "host2",
      "exec2" -> "host2")) {
      val taskOption = manager.resourceOffer(k, v, NO_PREF)._1
      assert(taskOption.isDefined)
      val task = taskOption.get
      assert(task.executorId === k)
    }
    assert(sched.startedTasks.toSet === Set(0, 1, 2, 3))
    clock.advance(1)
    // Complete the 3 tasks and leave 1 task in running
    for (id <- Set(0, 1, 2)) {
      manager.handleSuccessfulTask(id, createTaskResult(id, accumUpdatesByTask(id)))
      assert(sched.endedTasks(id) === Success)
    }

    // checkSpeculatableTasks checks that the task runtime is greater than the threshold for
    // speculating. Since we use a threshold of 0 for speculation, tasks need to be running for
    // > 0ms, so advance the clock by 1ms here.
    clock.advance(1)
    assert(manager.checkSpeculatableTasks(0))
    assert(sched.speculativeTasks.toSet === Set(3))

    // Offer resource to start the speculative attempt for the running task
    val taskOption5 = manager.resourceOffer("exec1", "host1", NO_PREF)._1
    assert(taskOption5.isDefined)
    val task5 = taskOption5.get
    assert(task5.index === 3)
    assert(task5.taskId === 4)
    assert(task5.executorId === "exec1")
    assert(task5.attemptNumber === 1)
    sched.backend = mock(classOf[SchedulerBackend])
    // Complete the speculative attempt for the running task
    manager.handleSuccessfulTask(4, createTaskResult(3, accumUpdatesByTask(3)))
    // Verify that it kills other running attempt
    verify(sched.backend).killTask(3, "exec2", true, "another attempt succeeded")
    // Because the SchedulerBackend was a mock, the 2nd copy of the task won't actually be
    // killed, so the FakeTaskScheduler is only told about the successful completion
    // of the speculated task.
    assert(sched.endedTasks(3) === Success)
  }

  test("Killing speculative tasks does not count towards aborting the taskset") {
    sc = new SparkContext("local", "test")
    sched = new FakeTaskScheduler(sc, ("exec1", "host1"), ("exec2", "host2"))
    val taskSet = FakeTask.createTaskSet(5)
    // Set the speculation multiplier to be 0 so speculative tasks are launched immediately
    sc.conf.set(config.SPECULATION_MULTIPLIER, 0.0)
    sc.conf.set(config.SPECULATION_QUANTILE, 0.6)
    sc.conf.set(config.SPECULATION_ENABLED, true)
    val clock = new ManualClock()
    val manager = new TaskSetManager(sched, taskSet, MAX_TASK_FAILURES, clock = clock)
    val accumUpdatesByTask: Array[Seq[AccumulatorV2[_, _]]] = taskSet.tasks.map { task =>
      task.metrics.internalAccums
    }
    // Offer resources for 5 tasks to start
    val tasks = new ArrayBuffer[TaskDescription]()
    for ((k, v) <- List(
      "exec1" -> "host1",
      "exec1" -> "host1",
      "exec1" -> "host1",
      "exec2" -> "host2",
      "exec2" -> "host2")) {
      val taskOption = manager.resourceOffer(k, v, NO_PREF)._1
      assert(taskOption.isDefined)
      val task = taskOption.get
      assert(task.executorId === k)
      tasks += task
    }
    assert(sched.startedTasks.toSet === (0 until 5).toSet)
    clock.advance(1)
    // Complete 3 tasks and leave 2 tasks in running
    for (id <- Set(0, 1, 2)) {
      manager.handleSuccessfulTask(id, createTaskResult(id, accumUpdatesByTask(id)))
      assert(sched.endedTasks(id) === Success)
    }

    def runningTaskForIndex(index: Int): TaskDescription = {
      tasks.find { task =>
        task.index == index && !sched.endedTasks.contains(task.taskId)
      }.getOrElse {
        throw new RuntimeException(s"couldn't find index $index in " +
          s"tasks: ${tasks.map { t => t.index -> t.taskId }} with endedTasks:" +
          s" ${sched.endedTasks.keys}")
      }
    }

    // have each of the running tasks fail 3 times (not enough to abort the stage)
    (0 until 3).foreach { attempt =>
      Seq(3, 4).foreach { index =>
        val task = runningTaskForIndex(index)
        logInfo(s"failing task $task")
        val endReason = ExceptionFailure("a", "b", Array(), "c", None)
        manager.handleFailedTask(task.taskId, TaskState.FAILED, endReason)
        sched.endedTasks(task.taskId) = endReason
        assert(!manager.isZombie)
        val nextTask = manager.resourceOffer(s"exec2", s"host2", NO_PREF)._1
        assert(nextTask.isDefined, s"no offer for attempt $attempt of $index")
        tasks += nextTask.get
      }
    }

    // we can't be sure which one of our running tasks will get another speculative copy
    val originalTasks = Seq(3, 4).map { index => index -> runningTaskForIndex(index) }.toMap

    // checkSpeculatableTasks checks that the task runtime is greater than the threshold for
    // speculating. Since we use a threshold of 0 for speculation, tasks need to be running for
    // > 0ms, so advance the clock by 1ms here.
    clock.advance(1)
    assert(manager.checkSpeculatableTasks(0))
    assert(sched.speculativeTasks.toSet === Set(3, 4))
    // Offer resource to start the speculative attempt for the running task
    val taskOption5 = manager.resourceOffer("exec1", "host1", NO_PREF)._1
    assert(taskOption5.isDefined)
    val speculativeTask = taskOption5.get
    assert(speculativeTask.index === 3 || speculativeTask.index === 4)
    assert(speculativeTask.taskId === 11)
    assert(speculativeTask.executorId === "exec1")
    assert(speculativeTask.attemptNumber === 4)
    sched.backend = mock(classOf[SchedulerBackend])
    // Complete the speculative attempt for the running task
    manager.handleSuccessfulTask(speculativeTask.taskId, createTaskResult(3, accumUpdatesByTask(3)))
    // Verify that it kills other running attempt
    val origTask = originalTasks(speculativeTask.index)
    verify(sched.backend).killTask(origTask.taskId, "exec2", true, "another attempt succeeded")
    // Because the SchedulerBackend was a mock, the 2nd copy of the task won't actually be
    // killed, so the FakeTaskScheduler is only told about the successful completion
    // of the speculated task.
    assert(sched.endedTasks(4) === Success)
    // also because the scheduler is a mock, our manager isn't notified about the task killed event,
    // so we do that manually
    manager.handleFailedTask(origTask.taskId, TaskState.KILLED, TaskKilled("test"))
    // this task has "failed" 4 times, but one of them doesn't count, so keep running the stage
    assert(manager.tasksSuccessful === 4)
    assert(!manager.isZombie)

    // now run another speculative task
    val taskOpt6 = manager.resourceOffer("exec1", "host1", NO_PREF)._1
    assert(taskOpt6.isDefined)
    val speculativeTask2 = taskOpt6.get
    assert(speculativeTask2.index === 3 || speculativeTask2.index === 4)
    assert(speculativeTask2.index !== speculativeTask.index)
    assert(speculativeTask2.attemptNumber === 4)
    // Complete the speculative attempt for the running task
    manager.handleSuccessfulTask(speculativeTask2.taskId,
      createTaskResult(3, accumUpdatesByTask(3)))
    // Verify that it kills other running attempt
    val origTask2 = originalTasks(speculativeTask2.index)
    verify(sched.backend).killTask(origTask2.taskId, "exec2", true, "another attempt succeeded")
    assert(manager.tasksSuccessful === 5)
    assert(manager.isZombie)
  }


  test("SPARK-19868: DagScheduler only notified of taskEnd when state is ready") {
    // dagScheduler.taskEnded() is async, so it may *seem* ok to call it before we've set all
    // appropriate state, e.g. isZombie.   However, this sets up a race that could go the wrong way.
    // This is a super-focused regression test which checks the zombie state as soon as
    // dagScheduler.taskEnded() is called, to ensure we haven't introduced a race.
    sc = new SparkContext("local", "test")
    sched = new FakeTaskScheduler(sc, ("exec1", "host1"))
    val mockDAGScheduler = mock(classOf[DAGScheduler])
    sched.dagScheduler.stop()
    sched.dagScheduler = mockDAGScheduler
    val taskSet = FakeTask.createTaskSet(numTasks = 1, stageId = 0, stageAttemptId = 0)
    val manager = new TaskSetManager(sched, taskSet, MAX_TASK_FAILURES, clock = new ManualClock(1))
    when(mockDAGScheduler.taskEnded(any(), any(), any(), any(), any(), any())).thenAnswer(
      (invocationOnMock: InvocationOnMock) => assert(manager.isZombie))
    val taskOption = manager.resourceOffer("exec1", "host1", NO_PREF)._1
    assert(taskOption.isDefined)
    // this would fail, inside our mock dag scheduler, if it calls dagScheduler.taskEnded() too soon
    manager.handleSuccessfulTask(0, createTaskResult(0))
  }

  test("SPARK-17894: Verify TaskSetManagers for different stage attempts have unique names") {
    sc = new SparkContext("local", "test")
    sched = new FakeTaskScheduler(sc, ("exec1", "host1"))
    val taskSet = FakeTask.createTaskSet(numTasks = 1, stageId = 0, stageAttemptId = 0)
    val manager = new TaskSetManager(sched, taskSet, MAX_TASK_FAILURES, clock = new ManualClock)
    assert(manager.name === "TaskSet_0.0")

    // Make sure a task set with the same stage ID but different attempt ID has a unique name
    val taskSet2 = FakeTask.createTaskSet(numTasks = 1, stageId = 0, stageAttemptId = 1)
    val manager2 = new TaskSetManager(sched, taskSet2, MAX_TASK_FAILURES, clock = new ManualClock)
    assert(manager2.name === "TaskSet_0.1")

    // Make sure a task set with the same attempt ID but different stage ID also has a unique name
    val taskSet3 = FakeTask.createTaskSet(numTasks = 1, stageId = 1, stageAttemptId = 1)
    val manager3 = new TaskSetManager(sched, taskSet3, MAX_TASK_FAILURES, clock = new ManualClock)
    assert(manager3.name === "TaskSet_1.1")
  }

  test("don't update excludelist for shuffle-fetch failures, preemption, denied commits, " +
      "or killed tasks") {
    // Setup a taskset, and fail some tasks for a fetch failure, preemption, denied commit,
    // and killed task.
    val conf = new SparkConf().
      set(config.EXCLUDE_ON_FAILURE_ENABLED, true)
    sc = new SparkContext("local", "test", conf)
    sched = new FakeTaskScheduler(sc, ("exec1", "host1"), ("exec2", "host2"))
    val taskSet = FakeTask.createTaskSet(4)
    val tsm = new TaskSetManager(sched, taskSet, 4)
    // we need a spy so we can attach our mock excludelist
    val tsmSpy = spy(tsm)
    val excludelist = mock(classOf[TaskSetExcludelist])
    when(tsmSpy.taskSetExcludelistHelperOpt).thenReturn(Some(excludelist))

    // make some offers to our taskset, to get tasks we will fail
    val taskDescs = Seq(
      "exec1" -> "host1",
      "exec2" -> "host1"
    ).flatMap { case (exec, host) =>
      // offer each executor twice (simulating 2 cores per executor)
      (0 until 2).flatMap{ _ => tsmSpy.resourceOffer(exec, host, TaskLocality.ANY)._1}
    }
    assert(taskDescs.size === 4)

    // now fail those tasks
    tsmSpy.handleFailedTask(taskDescs(0).taskId, TaskState.FAILED,
      FetchFailed(BlockManagerId(taskDescs(0).executorId, "host1", 12345), 0, 0L, 0, 0, "ignored"))
    tsmSpy.handleFailedTask(taskDescs(1).taskId, TaskState.FAILED,
      ExecutorLostFailure(taskDescs(1).executorId, exitCausedByApp = false, reason = None))
    tsmSpy.handleFailedTask(taskDescs(2).taskId, TaskState.FAILED,
      TaskCommitDenied(0, 2, 0))
    tsmSpy.handleFailedTask(taskDescs(3).taskId, TaskState.KILLED, TaskKilled("test"))

    // Make sure that the excludelist ignored all of the task failures above, since they aren't
    // the fault of the executor where the task was running.
    verify(excludelist, never())
      .updateExcludedForFailedTask(anyString(), anyString(), anyInt(), anyString())
  }

  test("update application healthTracker for shuffle-fetch") {
    // Setup a taskset, and fail some one task for fetch failure.
    val conf = new SparkConf()
      .set(config.EXCLUDE_ON_FAILURE_ENABLED, true)
      .set(config.SHUFFLE_SERVICE_ENABLED, true)
      .set(config.EXCLUDE_ON_FAILURE_FETCH_FAILURE_ENABLED, true)
    sc = new SparkContext("local", "test", conf)
    sched = new FakeTaskScheduler(sc, ("exec1", "host1"), ("exec2", "host2"))
    val taskSet = FakeTask.createTaskSet(4)
    val healthTracker = new HealthTracker(sc, None)
    val tsm = new TaskSetManager(sched, taskSet, 4, Some(healthTracker))

    // make some offers to our taskset, to get tasks we will fail
    val taskDescs = Seq(
      "exec1" -> "host1",
      "exec2" -> "host2"
    ).flatMap { case (exec, host) =>
      // offer each executor twice (simulating 2 cores per executor)
      (0 until 2).flatMap{ _ => tsm.resourceOffer(exec, host, TaskLocality.ANY)._1}
    }
    assert(taskDescs.size === 4)

    assert(!healthTracker.isExecutorExcluded(taskDescs(0).executorId))
    assert(!healthTracker.isNodeExcluded("host1"))

    // Fail the task with fetch failure
    tsm.handleFailedTask(taskDescs(0).taskId, TaskState.FAILED,
      FetchFailed(BlockManagerId(taskDescs(0).executorId, "host1", 12345), 0, 0L, 0, 0, "ignored"))

    assert(healthTracker.isNodeExcluded("host1"))
  }

  test("update healthTracker before adding pending task to avoid race condition") {
    // When a task fails, it should apply the excludeOnFailure policy prior to
    // retrying the task otherwise there's a race condition where run on
    // the same executor that it was intended to be black listed from.
    val conf = new SparkConf().
      set(config.EXCLUDE_ON_FAILURE_ENABLED, true)

    // Create a task with two executors.
    sc = new SparkContext("local", "test", conf)
    val exec = "executor1"
    val host = "host1"
    val exec2 = "executor2"
    val host2 = "host2"
    sched = new FakeTaskScheduler(sc, (exec, host), (exec2, host2))
    val taskSet = FakeTask.createTaskSet(1)

    val clock = new ManualClock
    val mockListenerBus = mock(classOf[LiveListenerBus])
    val healthTracker = new HealthTracker(mockListenerBus, conf, None, clock)
    val taskSetManager = new TaskSetManager(sched, taskSet, 1, Some(healthTracker))
    val taskSetManagerSpy = spy(taskSetManager)

    val taskDesc = taskSetManagerSpy.resourceOffer(exec, host, TaskLocality.ANY)._1

    // Assert the task has been black listed on the executor it was last executed on.
    when(taskSetManagerSpy.addPendingTask(anyInt(), anyBoolean(), anyBoolean())).thenAnswer(
      (invocationOnMock: InvocationOnMock) => {
        val task: Int = invocationOnMock.getArgument(0)
        assert(taskSetManager.taskSetExcludelistHelperOpt.get.
          isExecutorExcludedForTask(exec, task))
      }
    )

    // Simulate a fake exception
    val e = new ExceptionFailure("a", "b", Array(), "c", None)
    taskSetManagerSpy.handleFailedTask(taskDesc.get.taskId, TaskState.FAILED, e)

    verify(taskSetManagerSpy, times(1)).addPendingTask(0, false, false)
  }

  test("SPARK-21563 context's added jars shouldn't change mid-TaskSet") {
    sc = new SparkContext("local", "test")
    val addedJarsPreTaskSet = Map[String, Long](sc.addedJars.toSeq: _*)
    assert(addedJarsPreTaskSet.size === 0)

    sched = new FakeTaskScheduler(sc, ("exec1", "host1"))
    val taskSet1 = FakeTask.createTaskSet(3)
    val manager1 = new TaskSetManager(sched, taskSet1, MAX_TASK_FAILURES, clock = new ManualClock)

    // all tasks from the first taskset have the same jars
    val taskOption1 = manager1.resourceOffer("exec1", "host1", NO_PREF)._1
    assert(taskOption1.get.addedJars === addedJarsPreTaskSet)
    val taskOption2 = manager1.resourceOffer("exec1", "host1", NO_PREF)._1
    assert(taskOption2.get.addedJars === addedJarsPreTaskSet)

    // even with a jar added mid-TaskSet
    val jarPath = Thread.currentThread().getContextClassLoader.getResource("TestUDTF.jar")
    sc.addJar(jarPath.toString)
    val addedJarsMidTaskSet = Map[String, Long](sc.addedJars.toSeq: _*)
    assert(addedJarsPreTaskSet !== addedJarsMidTaskSet)
    val taskOption3 = manager1.resourceOffer("exec1", "host1", NO_PREF)._1
    // which should have the old version of the jars list
    assert(taskOption3.get.addedJars === addedJarsPreTaskSet)

    // and then the jar does appear in the next TaskSet
    val taskSet2 = FakeTask.createTaskSet(1)
    val manager2 = new TaskSetManager(sched, taskSet2, MAX_TASK_FAILURES, clock = new ManualClock)

    val taskOption4 = manager2.resourceOffer("exec1", "host1", NO_PREF)._1
    assert(taskOption4.get.addedJars === addedJarsMidTaskSet)
  }

  test("SPARK-24677: Avoid NoSuchElementException from MedianHeap") {
    val conf = new SparkConf().set(config.SPECULATION_ENABLED, true)
    sc = new SparkContext("local", "test", conf)
    // Set the speculation multiplier to be 0 so speculative tasks are launched immediately
    sc.conf.set(config.SPECULATION_MULTIPLIER, 0.0)
    sc.conf.set(config.SPECULATION_QUANTILE, 0.1)
    sc.conf.set(config.SPECULATION_ENABLED, true)

    sched = new FakeTaskScheduler(sc)
    sched.initialize(new FakeSchedulerBackend())

    val dagScheduler = new FakeDAGScheduler(sc, sched)
    sched.setDAGScheduler(dagScheduler)

    val taskSet = FakeTask.createTaskSet(10)

    sched.submitTasks(taskSet)
    sched.resourceOffers(
      (0 until 8).map { idx => WorkerOffer(s"exec-$idx", s"host-$idx", 1) })

    val taskSetManager = sched.taskSetManagerForAttempt(0, 0).get
    assert(taskSetManager.runningTasks === 8)
    taskSetManager.markPartitionCompleted(8)
    assert(taskSetManager.successfulTaskDurations.isEmpty())
    taskSetManager.checkSpeculatableTasks(0)
  }


  test("SPARK-24755 Executor loss can cause task to not be resubmitted") {
    val conf = new SparkConf().set(config.SPECULATION_ENABLED, true)
    sc = new SparkContext("local", "test", conf)
    // Set the speculation multiplier to be 0 so speculative tasks are launched immediately
    sc.conf.set(config.SPECULATION_MULTIPLIER, 0.0)

    sc.conf.set(config.SPECULATION_QUANTILE, 0.5)
    sc.conf.set(config.SPECULATION_ENABLED, true)

    var killTaskCalled = false
    sched = new FakeTaskScheduler(sc, ("exec1", "host1"),
      ("exec2", "host2"), ("exec3", "host3"))
    sched.initialize(new FakeSchedulerBackend() {
      override def killTask(
          taskId: Long,
          executorId: String,
          interruptThread: Boolean,
          reason: String): Unit = {
        // Check the only one killTask event in this case, which triggered by
        // task 2.1 completed.
        assert(taskId === 2)
        assert(executorId === "exec3")
        assert(interruptThread)
        assert(reason === "another attempt succeeded")
        killTaskCalled = true
      }
    })

    // Keep track of the index of tasks that are resubmitted,
    // so that the test can check that task is resubmitted correctly
    var resubmittedTasks = new mutable.HashSet[Int]
    val dagScheduler = new FakeDAGScheduler(sc, sched) {
      override def taskEnded(
          task: Task[_],
          reason: TaskEndReason,
          result: Any,
          accumUpdates: Seq[AccumulatorV2[_, _]],
          metricPeaks: Array[Long],
          taskInfo: TaskInfo): Unit = {
        super.taskEnded(task, reason, result, accumUpdates, metricPeaks, taskInfo)
        reason match {
          case Resubmitted => resubmittedTasks += taskInfo.index
          case _ =>
        }
      }
    }
    sched.dagScheduler.stop()
    sched.setDAGScheduler(dagScheduler)

    val taskSet = FakeTask.createShuffleMapTaskSet(4, 0, 0,
      Seq(TaskLocation("host1", "exec1")),
      Seq(TaskLocation("host1", "exec1")),
      Seq(TaskLocation("host3", "exec3")),
      Seq(TaskLocation("host2", "exec2")))

    val clock = new ManualClock()
    val manager = new TaskSetManager(sched, taskSet, MAX_TASK_FAILURES, clock = clock)
    val accumUpdatesByTask: Array[Seq[AccumulatorV2[_, _]]] = taskSet.tasks.map { task =>
      task.metrics.internalAccums
    }
    // Offer resources for 4 tasks to start
    for ((exec, host) <- Seq(
      "exec1" -> "host1",
      "exec1" -> "host1",
      "exec3" -> "host3",
      "exec2" -> "host2")) {
      val taskOption = manager.resourceOffer(exec, host, NO_PREF)._1
      assert(taskOption.isDefined)
      val task = taskOption.get
      assert(task.executorId === exec)
      // Add an extra assert to make sure task 2.0 is running on exec3
      if (task.index == 2) {
        assert(task.attemptNumber === 0)
        assert(task.executorId === "exec3")
      }
    }
    assert(sched.startedTasks.toSet === Set(0, 1, 2, 3))
    clock.advance(1)
    // Complete the 2 tasks and leave 2 task in running
    for (id <- Set(0, 1)) {
      manager.handleSuccessfulTask(id, createTaskResult(id, accumUpdatesByTask(id)))
      assert(sched.endedTasks(id) === Success)
    }

    // checkSpeculatableTasks checks that the task runtime is greater than the threshold for
    // speculating. Since we use a threshold of 0 for speculation, tasks need to be running for
    // > 0ms, so advance the clock by 1ms here.
    clock.advance(1)
    assert(manager.checkSpeculatableTasks(0))
    assert(sched.speculativeTasks.toSet === Set(2, 3))

    // Offer resource to start the speculative attempt for the running task 2.0
    val taskOption = manager.resourceOffer("exec2", "host2", ANY)._1
    assert(taskOption.isDefined)
    val task4 = taskOption.get
    assert(task4.index === 2)
    assert(task4.taskId === 4)
    assert(task4.executorId === "exec2")
    assert(task4.attemptNumber === 1)
    // Complete the speculative attempt for the running task
    manager.handleSuccessfulTask(4, createTaskResult(2, accumUpdatesByTask(2)))
    // Make sure schedBackend.killTask(2, "exec3", true, "another attempt succeeded") gets called
    assert(killTaskCalled)

    assert(resubmittedTasks.isEmpty)
    // Host 2 Losts, meaning we lost the map output task4
    manager.executorLost("exec2", "host2", ExecutorProcessLost())
    // Make sure that task with index 2 is re-submitted
    assert(resubmittedTasks.contains(2))

  }

  private def createTaskResult(
      id: Int,
      accumUpdates: Seq[AccumulatorV2[_, _]] = Seq.empty,
      metricPeaks: Array[Long] = Array.empty): DirectTaskResult[Int] = {
    val valueSer = SparkEnv.get.serializer.newInstance()
    new DirectTaskResult[Int](valueSer.serialize(id), accumUpdates, metricPeaks)
  }

  test("SPARK-13343 speculative tasks that didn't commit shouldn't be marked as success") {
    sc = new SparkContext("local", "test")
    sched = new FakeTaskScheduler(sc, ("exec1", "host1"), ("exec2", "host2"))
    val taskSet = FakeTask.createTaskSet(4)
    // Set the speculation multiplier to be 0 so speculative tasks are launched immediately
    sc.conf.set(config.SPECULATION_MULTIPLIER, 0.0)
    sc.conf.set(config.SPECULATION_ENABLED, true)
    val clock = new ManualClock()
    val manager = new TaskSetManager(sched, taskSet, MAX_TASK_FAILURES, clock = clock)
    val accumUpdatesByTask: Array[Seq[AccumulatorV2[_, _]]] = taskSet.tasks.map { task =>
      task.metrics.internalAccums
    }
    // Offer resources for 4 tasks to start
    for ((k, v) <- List(
      "exec1" -> "host1",
      "exec1" -> "host1",
      "exec2" -> "host2",
      "exec2" -> "host2")) {
      val taskOption = manager.resourceOffer(k, v, NO_PREF)._1
      assert(taskOption.isDefined)
      val task = taskOption.get
      assert(task.executorId === k)
    }
    assert(sched.startedTasks.toSet === Set(0, 1, 2, 3))
    clock.advance(1)
    // Complete the 3 tasks and leave 1 task in running
    for (id <- Set(0, 1, 2)) {
      manager.handleSuccessfulTask(id, createTaskResult(id, accumUpdatesByTask(id)))
      assert(sched.endedTasks(id) === Success)
    }
    // checkSpeculatableTasks checks that the task runtime is greater than the threshold for
    // speculating. Since we use a threshold of 0 for speculation, tasks need to be running for
    // > 0ms, so advance the clock by 1ms here.
    clock.advance(1)
    assert(manager.checkSpeculatableTasks(0))
    assert(sched.speculativeTasks.toSet === Set(3))

    // Offer resource to start the speculative attempt for the running task
    val taskOption5 = manager.resourceOffer("exec1", "host1", NO_PREF)._1
    assert(taskOption5.isDefined)
    val task5 = taskOption5.get
    assert(task5.index === 3)
    assert(task5.taskId === 4)
    assert(task5.executorId === "exec1")
    assert(task5.attemptNumber === 1)
    sched.backend = mock(classOf[SchedulerBackend])
    sched.dagScheduler.stop()
    sched.dagScheduler = mock(classOf[DAGScheduler])
    // Complete one attempt for the running task
    val result = createTaskResult(3, accumUpdatesByTask(3))
    manager.handleSuccessfulTask(3, result)
    // There is a race between the scheduler asking to kill the other task, and that task
    // actually finishing. We simulate what happens if the other task finishes before we kill it.
    verify(sched.backend).killTask(4, "exec1", true, "another attempt succeeded")
    manager.handleSuccessfulTask(4, result)

    val info3 = manager.taskInfos(3)
    val info4 = manager.taskInfos(4)
    assert(info3.successful)
    assert(info4.killed)
    verify(sched.dagScheduler).taskEnded(
      manager.tasks(3),
      TaskKilled("Finish but did not commit due to another attempt succeeded"),
      null,
      Seq.empty,
      Array.empty,
      info4)
    verify(sched.dagScheduler).taskEnded(manager.tasks(3), Success, result.value(),
      result.accumUpdates, Array.empty, info3)
  }

  test("SPARK-13704 Rack Resolution is done with a batch of de-duped hosts") {
    val conf = new SparkConf()
      .set(config.LOCALITY_WAIT, 0L)
      .set(config.LOCALITY_WAIT_RACK, 1L)
    sc = new SparkContext("local", "test", conf)
    // Create a cluster with 20 racks, with hosts spread out among them
    val execAndHost = (0 to 199).map { i =>
      FakeRackUtil.assignHostToRack("host" + i, "rack" + (i % 20))
      ("exec" + i, "host" + i)
    }
    sched = new FakeTaskScheduler(sc, execAndHost: _*)
    // make a taskset with preferred locations on the first 100 hosts in our cluster
    val locations = new ArrayBuffer[Seq[TaskLocation]]()
    for (i <- 0 to 99) {
      locations += Seq(TaskLocation("host" + i))
    }
    val taskSet = FakeTask.createTaskSet(100, locations.toSeq: _*)
    val clock = new ManualClock
    // make sure we only do one rack resolution call, for the entire batch of hosts, as this
    // can be expensive.  The FakeTaskScheduler calls rack resolution more than the real one
    // -- that is outside of the scope of this test, we just want to check the task set manager.
    FakeRackUtil.numBatchInvocation = 0
    FakeRackUtil.numSingleHostInvocation = 0
    val manager = new TaskSetManager(sched, taskSet, MAX_TASK_FAILURES, clock = clock)
    assert(FakeRackUtil.numBatchInvocation === 1)
    assert(FakeRackUtil.numSingleHostInvocation === 0)
    // with rack locality, reject an offer on a host with an unknown rack
    assert(manager.resourceOffer("otherExec", "otherHost", TaskLocality.RACK_LOCAL)._1.isEmpty)
    (0 until 20).foreach { rackIdx =>
      (0 until 5).foreach { offerIdx =>
        // if we offer hosts which are not in preferred locations,
        // we'll reject them at NODE_LOCAL level,
        // but accept them at RACK_LOCAL level if they're on OK racks
        val hostIdx = 100 + rackIdx
        assert(manager.resourceOffer("exec" + hostIdx, "host" + hostIdx, TaskLocality.NODE_LOCAL)
          ._1.isEmpty)
        assert(manager.resourceOffer("exec" + hostIdx, "host" + hostIdx, TaskLocality.RACK_LOCAL)
          ._1.isDefined)
      }
    }
    // check no more expensive calls to the rack resolution.  manager.resourceOffer() will call
    // the single-host resolution, but the real rack resolution would have cached all hosts
    // by that point.
    assert(FakeRackUtil.numBatchInvocation === 1)
  }

  test("TaskSetManager passes task resource along") {

    sc = new SparkContext("local", "test")
    sc.conf.set(TASK_GPU_ID.amountConf, "2")
    sched = new FakeTaskScheduler(sc, ("exec1", "host1"))
    val taskSet = FakeTask.createTaskSet(1)
    val manager = new TaskSetManager(sched, taskSet, MAX_TASK_FAILURES)

    val taskResourceAssignments = Map(GPU -> new ResourceInformation(GPU, Array("0", "1")))
    val taskOption =
      manager.resourceOffer("exec1", "host1", NO_PREF, taskResourceAssignments)._1
    assert(taskOption.isDefined)
    val allocatedResources = taskOption.get.resources
    assert(allocatedResources.size == 1)
    assert(allocatedResources(GPU).addresses sameElements Array("0", "1"))
  }

  test("SPARK-26755 Ensure that a speculative task is submitted only once for execution") {
    sc = new SparkContext("local", "test")
    sched = new FakeTaskScheduler(sc, ("exec1", "host1"), ("exec2", "host2"))
    val taskSet = FakeTask.createTaskSet(4)
    // Set the speculation multiplier to be 0 so speculative tasks are launched immediately
    sc.conf.set(config.SPECULATION_MULTIPLIER, 0.0)
    sc.conf.set(config.SPECULATION_ENABLED, true)
    sc.conf.set(config.SPECULATION_QUANTILE, 0.5)
    val clock = new ManualClock()
    val manager = new TaskSetManager(sched, taskSet, MAX_TASK_FAILURES, clock = clock)
    val accumUpdatesByTask: Array[Seq[AccumulatorV2[_, _]]] = taskSet.tasks.map { task =>
      task.metrics.internalAccums
    }
    // Offer resources for 4 tasks to start, 2 on each exec
    Seq("exec1" -> "host1", "exec2" -> "host2").foreach { case (exec, host) =>
      (0 until 2).foreach { _ =>
        val taskOption = manager.resourceOffer(exec, host, NO_PREF)._1
        assert(taskOption.isDefined)
        assert(taskOption.get.executorId === exec)
      }
    }
    assert(sched.startedTasks.toSet === Set(0, 1, 2, 3))
    clock.advance(1)
    // Complete the first 2 tasks and leave the other 2 tasks in running
    for (id <- Set(0, 2)) {
      manager.handleSuccessfulTask(id, createTaskResult(id, accumUpdatesByTask(id)))
      assert(sched.endedTasks(id) === Success)
    }
    // checkSpeculatableTasks checks that the task runtime is greater than the threshold for
    // speculating. Since we use a threshold of 0 for speculation, tasks need to be running for
    // > 0ms, so advance the clock by 1ms here.
    clock.advance(1)
    assert(manager.checkSpeculatableTasks(0))
    assert(sched.speculativeTasks.toSet === Set(1, 3))
    assert(manager.copiesRunning(1) === 1)
    assert(manager.copiesRunning(3) === 1)

    // Offer resource to start the speculative attempt for the running task. We offer more
    // resources, and ensure that speculative tasks get scheduled appropriately -- only one extra
    // copy per speculatable task
    val taskOption2 = manager.resourceOffer("exec1", "host1", NO_PREF)._1
    val taskOption3 = manager.resourceOffer("exec2", "host2", NO_PREF)._1
    assert(taskOption2.isDefined)
    val task2 = taskOption2.get
    // Ensure that task index 3 is launched on host1 and task index 4 on host2
    assert(task2.index === 3)
    assert(task2.taskId === 4)
    assert(task2.executorId === "exec1")
    assert(task2.attemptNumber === 1)
    assert(taskOption3.isDefined)
    val task3 = taskOption3.get
    assert(task3.index === 1)
    assert(task3.taskId === 5)
    assert(task3.executorId === "exec2")
    assert(task3.attemptNumber === 1)
    clock.advance(1)
    // Running checkSpeculatableTasks again should return false
    assert(!manager.checkSpeculatableTasks(0))
    assert(manager.copiesRunning(1) === 2)
    assert(manager.copiesRunning(3) === 2)
    // Offering additional resources should not lead to any speculative tasks being respawned
    assert(manager.resourceOffer("exec1", "host1", ANY)._1.isEmpty)
    assert(manager.resourceOffer("exec2", "host2", ANY)._1.isEmpty)
    assert(manager.resourceOffer("exec3", "host3", ANY)._1.isEmpty)
  }

  test("SPARK-26755 Ensure that a speculative task obeys original locality preferences") {
    sc = new SparkContext("local", "test")
    // Set the speculation multiplier to be 0 so speculative tasks are launched immediately
    sc.conf.set(config.SPECULATION_MULTIPLIER, 0.0)
    sc.conf.set(config.SPECULATION_ENABLED, true)
    sc.conf.set(config.SPECULATION_QUANTILE, 0.5)
    // Launch a new set of tasks with locality preferences
    sched = new FakeTaskScheduler(sc, ("exec1", "host1"),
      ("exec2", "host2"), ("exec3", "host3"), ("exec4", "host4"))
    val taskSet = FakeTask.createTaskSet(3,
      Seq(TaskLocation("host1"), TaskLocation("host3")),
      Seq(TaskLocation("host2")),
      Seq(TaskLocation("host3")))
    val clock = new ManualClock()
    val manager = new TaskSetManager(sched, taskSet, MAX_TASK_FAILURES, clock = clock)
    val accumUpdatesByTask2: Array[Seq[AccumulatorV2[_, _]]] = taskSet.tasks.map { task =>
      task.metrics.internalAccums
    }
    // Offer resources for 3 tasks to start
    Seq("exec1" -> "host1", "exec2" -> "host2", "exec3" -> "host3").foreach { case (exec, host) =>
      val taskOption = manager.resourceOffer(exec, host, NO_PREF)._1
      assert(taskOption.isDefined)
      assert(taskOption.get.executorId === exec)
    }
    assert(sched.startedTasks.toSet === Set(0, 1, 2))
    clock.advance(1)
    // Finish one task and mark the others as speculatable
    manager.handleSuccessfulTask(2, createTaskResult(2, accumUpdatesByTask2(2)))
    assert(sched.endedTasks(2) === Success)
    clock.advance(1)
    assert(manager.checkSpeculatableTasks(0))
    assert(sched.speculativeTasks.toSet === Set(0, 1))
    // Ensure that the speculatable tasks obey the original locality preferences
    assert(manager.resourceOffer("exec4", "host4", NODE_LOCAL)._1.isEmpty)
    // task 1 does have a node-local preference for host2 -- but we've already got a regular
    // task running there, so we should not schedule a speculative there as well.
    assert(manager.resourceOffer("exec2", "host2", NODE_LOCAL)._1.isEmpty)
    assert(manager.resourceOffer("exec3", "host3", NODE_LOCAL)._1.isDefined)
    assert(manager.resourceOffer("exec4", "host4", ANY)._1.isDefined)
    // Since, all speculatable tasks have been launched, making another offer
    // should not schedule any more tasks
    assert(manager.resourceOffer("exec1", "host1", ANY)._1.isEmpty)
    assert(!manager.checkSpeculatableTasks(0))
    assert(manager.resourceOffer("exec1", "host1", ANY)._1.isEmpty)
  }

  private def testSpeculationDurationSetup(
      speculationThresholdOpt: Option[String],
      speculationQuantile: Double,
      numTasks: Int,
      numExecutorCores: Int,
      numCoresPerTask: Int): (TaskSetManager, ManualClock) = {
    val conf = new SparkConf()
    conf.set(config.SPECULATION_ENABLED, true)
    conf.set(config.SPECULATION_QUANTILE.key, speculationQuantile.toString)
    // Set the number of slots per executor
    conf.set(config.EXECUTOR_CORES.key, numExecutorCores.toString)
    conf.set(config.CPUS_PER_TASK.key, numCoresPerTask.toString)
    if (speculationThresholdOpt.isDefined) {
      conf.set(config.SPECULATION_TASK_DURATION_THRESHOLD.key, speculationThresholdOpt.get)
    }
    sc = new SparkContext("local", "test", conf)
    sched = new FakeTaskScheduler(sc, ("exec1", "host1"), ("exec2", "host2"))
    // Create a task set with the given number of tasks
    val taskSet = FakeTask.createTaskSet(numTasks)
    val clock = new ManualClock()
    val manager = new TaskSetManager(sched, taskSet, MAX_TASK_FAILURES, clock = clock)
    manager.isZombie = false

    // Offer resources for the task to start
    for (i <- 1 to numTasks) {
      manager.resourceOffer(s"exec$i", s"host$i", NO_PREF)
    }
    (manager, clock)
  }

  private def testSpeculationDurationThreshold(
      speculationThresholdProvided: Boolean,
      numTasks: Int,
      numSlots: Int): Unit = {
    val (manager, clock) = testSpeculationDurationSetup(
      // Set the threshold to be 60 minutes
      if (speculationThresholdProvided) Some("60min") else None,
      // Set the quantile to be 1.0 so that regular speculation would not be triggered
      speculationQuantile = 1.0,
      numTasks,
      numSlots,
      numCoresPerTask = 1
    )

    // if the time threshold has not been exceeded, no speculative run should be triggered
    clock.advance(1000*60*60)
    assert(!manager.checkSpeculatableTasks(0))
    assert(sched.speculativeTasks.size == 0)

    // Now the task should have been running for 60 minutes and 1 second
    clock.advance(1000)
    if (speculationThresholdProvided && numSlots >= numTasks) {
      assert(manager.checkSpeculatableTasks(0))
      assert(sched.speculativeTasks.size == numTasks)
      // Should not submit duplicated tasks
      assert(!manager.checkSpeculatableTasks(0))
      assert(sched.speculativeTasks.size == numTasks)
    } else {
      // If the feature flag is turned off, or the stage contains too many tasks
      assert(!manager.checkSpeculatableTasks(0))
      assert(sched.speculativeTasks.size == 0)
    }
  }

  Seq(1, 2).foreach { numTasks =>
    test("SPARK-29976 when a speculation time threshold is provided, should speculative " +
      s"run the task even if there are not enough successful runs, total tasks: $numTasks") {
      testSpeculationDurationThreshold(true, numTasks, numTasks)
    }

    test("SPARK-29976: when the speculation time threshold is not provided," +
      s"don't speculative run if there are not enough successful runs, total tasks: $numTasks") {
      testSpeculationDurationThreshold(false, numTasks, numTasks)
    }
  }

  test("SPARK-29976 when a speculation time threshold is provided, should not speculative " +
      "if there are too many tasks in the stage even though time threshold is provided") {
    testSpeculationDurationThreshold(true, 2, 1)
  }

  test("SPARK-21040: Check speculative tasks are launched when an executor is decommissioned" +
    " and the tasks running on it cannot finish within EXECUTOR_DECOMMISSION_KILL_INTERVAL") {
    sc = new SparkContext("local", "test")
    val clock = new ManualClock()
    sched = new FakeTaskScheduler(sc, clock,
      ("exec1", "host1"), ("exec2", "host2"), ("exec3", "host3"))
    sched.backend = mock(classOf[SchedulerBackend])
    val taskSet = FakeTask.createTaskSet(4)
    sc.conf.set(config.SPECULATION_ENABLED, true)
    sc.conf.set(config.SPECULATION_MULTIPLIER, 1.5)
    sc.conf.set(config.SPECULATION_QUANTILE, 0.5)
    sc.conf.set(config.EXECUTOR_DECOMMISSION_KILL_INTERVAL.key, "5s")
    val manager = sched.createTaskSetManager(taskSet, MAX_TASK_FAILURES)
    val accumUpdatesByTask: Array[Seq[AccumulatorV2[_, _]]] = taskSet.tasks.map { task =>
      task.metrics.internalAccums
    }

    // Start TASK 0,1 on exec1, TASK 2 on exec2
    (0 until 2).foreach { _ =>
      val taskOption = manager.resourceOffer("exec1", "host1", NO_PREF)._1
      assert(taskOption.isDefined)
      assert(taskOption.get.executorId === "exec1")
    }
    val taskOption2 = manager.resourceOffer("exec2", "host2", NO_PREF)._1
    assert(taskOption2.isDefined)
    assert(taskOption2.get.executorId === "exec2")

    clock.advance(6*1000) // time = 6s
    // Start TASK 3 on exec2 after some delay
    val taskOption3 = manager.resourceOffer("exec2", "host2", NO_PREF)._1
    assert(taskOption3.isDefined)
    assert(taskOption3.get.executorId === "exec2")

    assert(sched.startedTasks.toSet === Set(0, 1, 2, 3))

    clock.advance(4*1000) // time = 10s
    // Complete the first 2 tasks and leave the other 2 tasks in running
    for (id <- Set(0, 1)) {
      manager.handleSuccessfulTask(id, createTaskResult(id, accumUpdatesByTask(id)))
      assert(sched.endedTasks(id) === Success)
    }

    // checkSpeculatableTasks checks that the task runtime is greater than the threshold for
    // speculating. Since we use a SPECULATION_MULTIPLIER of 1.5, So tasks need to be running for
    // > 15s for speculation
    assert(!manager.checkSpeculatableTasks(0))
    assert(sched.speculativeTasks.toSet === Set())

    // decommission exec-2. All tasks running on exec-2 (i.e. TASK 2,3) will be now
    // checked if they should be speculated.
    // (TASK 2 -> 15, TASK 3 -> 15)
    sched.executorDecommission("exec2", ExecutorDecommissionInfo("decom", None))
    assert(sched.getExecutorDecommissionState("exec2").map(_.startTime) ===
      Some(clock.getTimeMillis()))

    assert(manager.checkSpeculatableTasks(0))
    // TASK 2 started at t=0s, so it can still finish before t=15s (Median task runtime = 10s)
    // TASK 3 started at t=6s, so it might not finish before t=15s. So TASK 3 should be part
    // of speculativeTasks
    assert(sched.speculativeTasks.toSet === Set(3))
    assert(manager.copiesRunning(3) === 1)

    // Offer resource to start the speculative attempt for the running task
    val taskOption3New = manager.resourceOffer("exec3", "host3", NO_PREF)._1
    // Offer more resources. Nothing should get scheduled now.
    assert(manager.resourceOffer("exec3", "host3", NO_PREF)._1.isEmpty)
    assert(taskOption3New.isDefined)

    // Assert info about the newly launched speculative task
    val speculativeTask3 = taskOption3New.get
    assert(speculativeTask3.index === 3)
    assert(speculativeTask3.taskId === 4)
    assert(speculativeTask3.executorId === "exec3")
    assert(speculativeTask3.attemptNumber === 1)

    clock.advance(1*1000) // time = 11s
    // Running checkSpeculatableTasks again should return false
    assert(!manager.checkSpeculatableTasks(0))
    assert(manager.copiesRunning(2) === 1)
    assert(manager.copiesRunning(3) === 2)

    clock.advance(5*1000) // time = 16s
    // At t=16s, TASK 2 has been running for 16s. It is more than the
    // SPECULATION_MULTIPLIER * medianRuntime = 1.5 * 10 = 15s. So now TASK 2 will
    // be selected for speculation. Here we are verifying that regular speculation configs
    // should still take effect even when a EXECUTOR_DECOMMISSION_KILL_INTERVAL is provided and
    // corresponding executor is decommissioned
    assert(manager.checkSpeculatableTasks(0))
    assert(sched.speculativeTasks.toSet === Set(2, 3))
    assert(manager.copiesRunning(2) === 1)
    assert(manager.copiesRunning(3) === 2)
    val taskOption2New = manager.resourceOffer("exec3", "host3", NO_PREF)._1
    assert(taskOption2New.isDefined)
    val speculativeTask2 = taskOption2New.get
    // Ensure that TASK 2 is re-launched on exec3, host3
    assert(speculativeTask2.index === 2)
    assert(speculativeTask2.taskId === 5)
    assert(speculativeTask2.executorId === "exec3")
    assert(speculativeTask2.attemptNumber === 1)

    assert(manager.copiesRunning(2) === 2)
    assert(manager.copiesRunning(3) === 2)

    // Offering additional resources should not lead to any speculative tasks being respawned
    assert(manager.resourceOffer("exec1", "host1", ANY)._1.isEmpty)
  }

  test("SPARK-29976 Regular speculation configs should still take effect even when a " +
      "threshold is provided") {
    val (manager, clock) = testSpeculationDurationSetup(
      Some("60min"),
      speculationQuantile = 0.5,
      numTasks = 2,
      numExecutorCores = 2,
      numCoresPerTask = 1
    )

    // Task duration can't be 0, advance 1 sec
    clock.advance(1000)
    // Mark one of the task succeeded, which should satisfy the quantile
    manager.handleSuccessfulTask(0, createTaskResult(0))
    // Advance 1 more second so the remaining task takes longer than medium but doesn't satisfy the
    // duration threshold yet
    clock.advance(1000)
    assert(manager.checkSpeculatableTasks(0))
    assert(sched.speculativeTasks.size == 1)
  }

  test("SPARK-30417 when spark.task.cpus is greater than spark.executor.cores due to " +
    "standalone settings, speculate if there is only one task in the stage") {
    val numTasks = 1
    val numCoresPerTask = 2
    val conf = new SparkConf()
    // skip throwing exception when cores per task > cores per executor to emulate standalone mode
    conf.set(SKIP_VALIDATE_CORES_TESTING, true)
    conf.set(config.SPECULATION_ENABLED, true)
    conf.set(config.SPECULATION_QUANTILE.key, "1.0")
    // Skip setting cores per executor to emulate standalone default mode
    conf.set(config.CPUS_PER_TASK.key, numCoresPerTask.toString)
    conf.set(config.SPECULATION_TASK_DURATION_THRESHOLD.key, "60min")
    sc = new SparkContext("local", "test", conf)
    sched = new FakeTaskScheduler(sc, ("exec1", "host1"), ("exec2", "host2"))
    // Create a task set with the given number of tasks
    val taskSet = FakeTask.createTaskSet(numTasks)
    val clock = new ManualClock()
    val manager = new TaskSetManager(sched, taskSet, MAX_TASK_FAILURES, clock = clock)
    manager.isZombie = false

    // Offer resources for the task to start
    for (i <- 1 to numTasks) {
      manager.resourceOffer(s"exec$i", s"host$i", NO_PREF)
    }
    clock.advance(1000*60*60)
    assert(!manager.checkSpeculatableTasks(0))
    assert(sched.speculativeTasks.size == 0)
    // Now the task should have been running for 60 minutes and 1 second
    clock.advance(1000)
    assert(manager.checkSpeculatableTasks(0))
    assert(sched.speculativeTasks.size == 1)
  }

  test("TaskOutputFileAlreadyExistException lead to task set abortion") {
    sc = new SparkContext("local", "test")
    sched = new FakeTaskScheduler(sc, ("exec1", "host1"))
    val taskSet = FakeTask.createTaskSet(1)
    val manager = new TaskSetManager(sched, taskSet, MAX_TASK_FAILURES)
    assert(sched.taskSetsFailed.isEmpty)

    val offerResult = manager.resourceOffer("exec1", "host1", ANY)._1
    assert(offerResult.isDefined,
      "Expect resource offer on iteration 0 to return a task")
    assert(offerResult.get.index === 0)
    val reason = new ExceptionFailure(
      new TaskOutputFileAlreadyExistException(
        new FileAlreadyExistsException("file already exists")),
      Seq.empty[AccumulableInfo])
    manager.handleFailedTask(offerResult.get.taskId, TaskState.FAILED, reason)
    assert(sched.taskSetsFailed.contains(taskSet.id))
  }

  test("SPARK-30359: don't clean executorsPendingToRemove " +
    "at the beginning of CoarseGrainedSchedulerBackend.reset") {
    val conf = new SparkConf()
      // use local-cluster mode in order to get CoarseGrainedSchedulerBackend
      .setMaster("local-cluster[2, 1, 2048]")
      // allow to set up at most two executors
      .set("spark.cores.max", "2")
      .setAppName("CoarseGrainedSchedulerBackend.reset")
    sc = new SparkContext(conf)
    val sched = sc.taskScheduler
    val backend = sc.schedulerBackend.asInstanceOf[CoarseGrainedSchedulerBackend]

    TestUtils.waitUntilExecutorsUp(sc, 2, 60000)

    val tasks = Array.tabulate[Task[_]](2)(partition => new FakeLongTasks(stageId = 0, partition))
    val taskSet: TaskSet = new TaskSet(tasks, stageId = 0, stageAttemptId = 0, priority = 0, null,
      ResourceProfile.DEFAULT_RESOURCE_PROFILE_ID)
    val stageId = taskSet.stageId
    val stageAttemptId = taskSet.stageAttemptId
    sched.submitTasks(taskSet)
    val taskSetManagers =
      PrivateMethod[mutable.HashMap[Int, mutable.HashMap[Int, TaskSetManager]]](
        Symbol("taskSetsByStageIdAndAttempt"))
    // get the TaskSetManager
    val manager = sched.invokePrivate(taskSetManagers()).get(stageId).get(stageAttemptId)

    val (task0, task1) = eventually(timeout(10.seconds), interval(100.milliseconds)) {
      (manager.taskInfos(0), manager.taskInfos(1))
    }

    val (taskId0, index0, exec0) = (task0.taskId, task0.index, task0.executorId)
    val (taskId1, index1, exec1) = (task1.taskId, task1.index, task1.executorId)
    // set up two running tasks
    assert(manager.taskInfos(taskId0).running)
    assert(manager.taskInfos(taskId1).running)

    val numFailures = PrivateMethod[Array[Int]](Symbol("numFailures"))
    // no task failures yet
    assert(manager.invokePrivate(numFailures())(index0) === 0)
    assert(manager.invokePrivate(numFailures())(index1) === 0)

    // let exec1 count task failures but exec0 doesn't
    backend.executorsPendingToRemove(exec0) = true
    backend.executorsPendingToRemove(exec1) = false

    backend.reset()

    eventually(timeout(10.seconds), interval(100.milliseconds)) {
      // executorsPendingToRemove should eventually be empty after reset()
      assert(backend.executorsPendingToRemove.isEmpty)
      assert(manager.invokePrivate(numFailures())(index0) === 0)
      assert(manager.invokePrivate(numFailures())(index1) === 1)
    }
  }
}

class FakeLongTasks(stageId: Int, partitionId: Int) extends FakeTask(stageId, partitionId) {

  override def runTask(context: TaskContext): Int = {
    while (true) {
      Thread.sleep(10000)
    }
    0
  }
}
