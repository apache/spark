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

import java.util.Random

import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer

import org.mockito.Mockito.{mock, verify}

import org.apache.spark._
import org.apache.spark.internal.config
import org.apache.spark.internal.Logging
import org.apache.spark.util.{AccumulatorV2, ManualClock}

class FakeDAGScheduler(sc: SparkContext, taskScheduler: FakeTaskScheduler)
  extends DAGScheduler(sc) {

  override def taskStarted(task: Task[_], taskInfo: TaskInfo) {
    taskScheduler.startedTasks += taskInfo.index
  }

  override def taskEnded(
      task: Task[_],
      reason: TaskEndReason,
      result: Any,
      accumUpdates: Seq[AccumulatorV2[_, _]],
      taskInfo: TaskInfo) {
    taskScheduler.endedTasks(taskInfo.index) = reason
  }

  override def executorAdded(execId: String, host: String) {}

  override def executorLost(execId: String, reason: ExecutorLossReason) {}

  override def taskSetFailed(
      taskSet: TaskSet,
      reason: String,
      exception: Option[Throwable]): Unit = {
    taskScheduler.taskSetsFailed += taskSet.id
  }
}

// Get the rack for a given host
object FakeRackUtil {
  private val hostToRack = new mutable.HashMap[String, String]()

  def cleanUp() {
    hostToRack.clear()
  }

  def assignHostToRack(host: String, rack: String) {
    hostToRack(host) = rack
  }

  def getRackForHost(host: String): Option[String] = {
    hostToRack.get(host)
  }
}

/**
 * A mock TaskSchedulerImpl implementation that just remembers information about tasks started and
 * feedback received from the TaskSetManagers. Note that it's important to initialize this with
 * a list of "live" executors and their hostnames for isExecutorAlive and hasExecutorsAliveOnHost
 * to work, and these are required for locality in TaskSetManager.
 */
class FakeTaskScheduler(sc: SparkContext, liveExecutors: (String, String)* /* execId, host */)
  extends TaskSchedulerImpl(sc)
{
  val startedTasks = new ArrayBuffer[Long]
  val endedTasks = new mutable.HashMap[Long, TaskEndReason]
  val finishedManagers = new ArrayBuffer[TaskSetManager]
  val taskSetsFailed = new ArrayBuffer[String]

  val executors = new mutable.HashMap[String, String]
  for ((execId, host) <- liveExecutors) {
    addExecutor(execId, host)
  }

  for ((execId, host) <- liveExecutors; rack <- getRackForHost(host)) {
    hostsByRack.getOrElseUpdate(rack, new mutable.HashSet[String]()) += host
  }

  dagScheduler = new FakeDAGScheduler(sc, this)

  def removeExecutor(execId: String) {
    executors -= execId
    val host = executorIdToHost.get(execId)
    assert(host != None)
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

  override def isExecutorAlive(execId: String): Boolean = executors.contains(execId)

  override def hasExecutorsAliveOnHost(host: String): Boolean = executors.values.exists(_ == host)

  override def hasHostAliveOnRack(rack: String): Boolean = {
    hostsByRack.get(rack) != None
  }

  def addExecutor(execId: String, host: String) {
    executors.put(execId, host)
    val executorsOnHost = hostToExecutors.getOrElseUpdate(host, new mutable.HashSet[String])
    executorsOnHost += execId
    executorIdToHost += execId -> host
    for (rack <- getRackForHost(host)) {
      hostsByRack.getOrElseUpdate(rack, new mutable.HashSet[String]()) += host
    }
  }

  override def getRackForHost(value: String): Option[String] = FakeRackUtil.getRackForHost(value)
}

/**
 * A Task implementation that results in a large serialized task.
 */
class LargeTask(stageId: Int) extends Task[Array[Byte]](stageId, 0, 0) {

  val randomBuffer = new Array[Byte](TaskSetManager.TASK_SIZE_TO_WARN_KB * 1024)
  val random = new Random(0)
  random.nextBytes(randomBuffer)

  override def runTask(context: TaskContext): Array[Byte] = randomBuffer
  override def preferredLocations: Seq[TaskLocation] = Seq[TaskLocation]()
}

class TaskSetManagerSuite extends SparkFunSuite with LocalSparkContext with Logging {
  import TaskLocality.{ANY, PROCESS_LOCAL, NO_PREF, NODE_LOCAL, RACK_LOCAL}

  private val conf = new SparkConf

  val LOCALITY_WAIT_MS = conf.getTimeAsMs("spark.locality.wait", "3s")
  val MAX_TASK_FAILURES = 4

  var sched: FakeTaskScheduler = null

  override def beforeEach(): Unit = {
    super.beforeEach()
    FakeRackUtil.cleanUp()
    sched = null
  }

  override def afterEach(): Unit = {
    super.afterEach()
    if (sched != null) {
      sched.dagScheduler.stop()
      sched.stop()
      sched = null
    }
  }


  test("TaskSet with no preferences") {
    sc = new SparkContext("local", "test")
    sched = new FakeTaskScheduler(sc, ("exec1", "host1"))
    val taskSet = FakeTask.createTaskSet(1)
    val clock = new ManualClock
    val manager = new TaskSetManager(sched, taskSet, MAX_TASK_FAILURES, clock)
    val accumUpdates = taskSet.tasks.head.metrics.internalAccums

    // Offer a host with NO_PREF as the constraint,
    // we should get a nopref task immediately since that's what we only have
    val taskOption = manager.resourceOffer("exec1", "host1", NO_PREF)
    assert(taskOption.isDefined)

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
      val taskOption = manager.resourceOffer("exec1", "host1", NO_PREF)
      assert(taskOption.isDefined)
      val task = taskOption.get
      assert(task.executorId === "exec1")
    }
    assert(sched.startedTasks.toSet === Set(0, 1, 2))

    // Re-offer the host -- now we should get no more tasks
    assert(manager.resourceOffer("exec1", "host1", NO_PREF) === None)

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
    val manager = new TaskSetManager(sched, taskSet, MAX_TASK_FAILURES, clock)

    // An executor that is not NODE_LOCAL should be rejected.
    assert(manager.resourceOffer("execC", "host2", ANY) === None)

    // Because there are no alive PROCESS_LOCAL executors, the base locality level should be
    // NODE_LOCAL. So, we should schedule the task on this offered NODE_LOCAL executor before
    // any of the locality wait timers expire.
    assert(manager.resourceOffer("execA", "host1", ANY).get.index === 0)
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
    val manager = new TaskSetManager(sched, taskSet, MAX_TASK_FAILURES, clock)
    // First offer host1, exec1: first task should be chosen
    assert(manager.resourceOffer("exec1", "host1", ANY).get.index === 0)
    assert(manager.resourceOffer("exec1", "host1", PROCESS_LOCAL) == None)

    clock.advance(LOCALITY_WAIT_MS)
    // Offer host1, exec1 again, at NODE_LOCAL level: the node local (task 3) should
    // get chosen before the noPref task
    assert(manager.resourceOffer("exec1", "host1", NODE_LOCAL).get.index == 2)

    // Offer host2, exec2, at NODE_LOCAL level: we should choose task 2
    assert(manager.resourceOffer("exec2", "host2", NODE_LOCAL).get.index == 1)

    // Offer host2, exec2 again, at NODE_LOCAL level: we should get noPref task
    // after failing to find a node_Local task
    assert(manager.resourceOffer("exec2", "host2", NODE_LOCAL) == None)
    clock.advance(LOCALITY_WAIT_MS)
    assert(manager.resourceOffer("exec2", "host2", NO_PREF).get.index == 3)
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
    val manager = new TaskSetManager(sched, taskSet, MAX_TASK_FAILURES, clock)
    // First offer host1, exec1: first task should be chosen
    assert(manager.resourceOffer("exec1", "host1", PROCESS_LOCAL).get.index === 0)
    assert(manager.resourceOffer("exec3", "host2", PROCESS_LOCAL).get.index === 1)
    assert(manager.resourceOffer("exec3", "host2", NODE_LOCAL) == None)
    assert(manager.resourceOffer("exec3", "host2", NO_PREF).get.index === 2)
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
    val clock = new ManualClock
    val manager = new TaskSetManager(sched, taskSet, MAX_TASK_FAILURES, clock)

    // First offer host1: first task should be chosen
    assert(manager.resourceOffer("exec1", "host1", ANY).get.index === 0)

    // Offer host1 again: nothing should get chosen
    assert(manager.resourceOffer("exec1", "host1", ANY) === None)

    clock.advance(LOCALITY_WAIT_MS)

    // Offer host1 again: second task (on host2) should get chosen
    assert(manager.resourceOffer("exec1", "host1", ANY).get.index === 1)

    // Offer host1 again: third task (on host2) should get chosen
    assert(manager.resourceOffer("exec1", "host1", ANY).get.index === 2)

    // Offer host2: fifth task (also on host2) should get chosen
    assert(manager.resourceOffer("exec2", "host2", ANY).get.index === 4)

    // Now that we've launched a local task, we should no longer launch the task for host3
    assert(manager.resourceOffer("exec2", "host2", ANY) === None)

    clock.advance(LOCALITY_WAIT_MS)

    // After another delay, we can go ahead and launch that task non-locally
    assert(manager.resourceOffer("exec2", "host2", ANY).get.index === 3)
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
    val manager = new TaskSetManager(sched, taskSet, MAX_TASK_FAILURES, clock)

    // First offer host1: first task should be chosen
    assert(manager.resourceOffer("exec1", "host1", ANY).get.index === 0)

    // After this, nothing should get chosen, because we have separated tasks with unavailable
    // preference from the noPrefPendingTasks
    assert(manager.resourceOffer("exec1", "host1", ANY) === None)

    // Now mark host2 as dead
    sched.removeExecutor("exec2")
    manager.executorLost("exec2", "host2", SlaveLost())

    // nothing should be chosen
    assert(manager.resourceOffer("exec1", "host1", ANY) === None)

    clock.advance(LOCALITY_WAIT_MS * 2)

    // task 1 and 2 would be scheduled as nonLocal task
    assert(manager.resourceOffer("exec1", "host1", ANY).get.index === 1)
    assert(manager.resourceOffer("exec1", "host1", ANY).get.index === 2)

    // all finished
    assert(manager.resourceOffer("exec1", "host1", ANY) === None)
    assert(manager.resourceOffer("exec2", "host2", ANY) === None)
  }

  test("task result lost") {
    sc = new SparkContext("local", "test")
    sched = new FakeTaskScheduler(sc, ("exec1", "host1"))
    val taskSet = FakeTask.createTaskSet(1)
    val clock = new ManualClock
    val manager = new TaskSetManager(sched, taskSet, MAX_TASK_FAILURES, clock)

    assert(manager.resourceOffer("exec1", "host1", ANY).get.index === 0)

    // Tell it the task has finished but the result was lost.
    manager.handleFailedTask(0, TaskState.FINISHED, TaskResultLost)
    assert(sched.endedTasks(0) === TaskResultLost)

    // Re-offer the host -- now we should get task 0 again.
    assert(manager.resourceOffer("exec1", "host1", ANY).get.index === 0)
  }

  test("repeated failures lead to task set abortion") {
    sc = new SparkContext("local", "test")
    sched = new FakeTaskScheduler(sc, ("exec1", "host1"))
    val taskSet = FakeTask.createTaskSet(1)
    val clock = new ManualClock
    val manager = new TaskSetManager(sched, taskSet, MAX_TASK_FAILURES, clock)

    // Fail the task MAX_TASK_FAILURES times, and check that the task set is aborted
    // after the last failure.
    (1 to manager.maxTaskFailures).foreach { index =>
      val offerResult = manager.resourceOffer("exec1", "host1", ANY)
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

  test("executors should be blacklisted after task failure, in spite of locality preferences") {
    val rescheduleDelay = 300L
    val conf = new SparkConf().
      set(config.BLACKLIST_ENABLED, true).
      set(config.BLACKLIST_TIMEOUT_CONF, rescheduleDelay).
      // don't wait to jump locality levels in this test
      set("spark.locality.wait", "0")

    sc = new SparkContext("local", "test", conf)
    // two executors on same host, one on different.
    sched = new FakeTaskScheduler(sc, ("exec1", "host1"),
      ("exec1.1", "host1"), ("exec2", "host2"))
    // affinity to exec1 on host1 - which we will fail.
    val taskSet = FakeTask.createTaskSet(1, Seq(TaskLocation("host1", "exec1")))
    val clock = new ManualClock
    val manager = new TaskSetManager(sched, taskSet, 4, clock)

    {
      val offerResult = manager.resourceOffer("exec1", "host1", PROCESS_LOCAL)
      assert(offerResult.isDefined, "Expect resource offer to return a task")

      assert(offerResult.get.index === 0)
      assert(offerResult.get.executorId === "exec1")

      // Cause exec1 to fail : failure 1
      manager.handleFailedTask(offerResult.get.taskId, TaskState.FINISHED, TaskResultLost)
      assert(!sched.taskSetsFailed.contains(taskSet.id))

      // Ensure scheduling on exec1 fails after failure 1 due to blacklist
      assert(manager.resourceOffer("exec1", "host1", PROCESS_LOCAL).isEmpty)
      assert(manager.resourceOffer("exec1", "host1", NODE_LOCAL).isEmpty)
      assert(manager.resourceOffer("exec1", "host1", RACK_LOCAL).isEmpty)
      assert(manager.resourceOffer("exec1", "host1", ANY).isEmpty)
    }

    // Run the task on exec1.1 - should work, and then fail it on exec1.1
    {
      val offerResult = manager.resourceOffer("exec1.1", "host1", NODE_LOCAL)
      assert(offerResult.isDefined,
        "Expect resource offer to return a task for exec1.1, offerResult = " + offerResult)

      assert(offerResult.get.index === 0)
      assert(offerResult.get.executorId === "exec1.1")

      // Cause exec1.1 to fail : failure 2
      manager.handleFailedTask(offerResult.get.taskId, TaskState.FINISHED, TaskResultLost)
      assert(!sched.taskSetsFailed.contains(taskSet.id))

      // Ensure scheduling on exec1.1 fails after failure 2 due to blacklist
      assert(manager.resourceOffer("exec1.1", "host1", NODE_LOCAL).isEmpty)
    }

    // Run the task on exec2 - should work, and then fail it on exec2
    {
      val offerResult = manager.resourceOffer("exec2", "host2", ANY)
      assert(offerResult.isDefined, "Expect resource offer to return a task")

      assert(offerResult.get.index === 0)
      assert(offerResult.get.executorId === "exec2")

      // Cause exec2 to fail : failure 3
      manager.handleFailedTask(offerResult.get.taskId, TaskState.FINISHED, TaskResultLost)
      assert(!sched.taskSetsFailed.contains(taskSet.id))

      // Ensure scheduling on exec2 fails after failure 3 due to blacklist
      assert(manager.resourceOffer("exec2", "host2", ANY).isEmpty)
    }

    // Despite advancing beyond the time for expiring executors from within the blacklist,
    // we *never* expire from *within* the stage blacklist
    clock.advance(rescheduleDelay)

    {
      val offerResult = manager.resourceOffer("exec1", "host1", PROCESS_LOCAL)
      assert(offerResult.isEmpty)
    }

    {
      val offerResult = manager.resourceOffer("exec3", "host3", ANY)
      assert(offerResult.isDefined)
      assert(offerResult.get.index === 0)
      assert(offerResult.get.executorId === "exec3")

      assert(manager.resourceOffer("exec3", "host3", ANY).isEmpty)

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
    val manager = new TaskSetManager(sched, taskSet, MAX_TASK_FAILURES, clock)
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
    manager.executorLost("execC", "host2", SlaveLost())
    assert(manager.myLocalityLevels.sameElements(Array(NODE_LOCAL, NO_PREF, ANY)))
    sched.removeExecutor("execD")
    manager.executorLost("execD", "host1", SlaveLost())
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
    val manager = new TaskSetManager(sched, taskSet, 1, new ManualClock)
    sched.addExecutor("execA", "host1")
    manager.executorAdded()
    sched.addExecutor("execC", "host2")
    manager.executorAdded()
    assert(manager.resourceOffer("exec1", "host1", ANY).isDefined)
    sched.removeExecutor("execA")
    manager.executorLost(
      "execA",
      "host1",
      ExecutorExited(143, false, "Terminated for reason unrelated to running tasks"))
    assert(!sched.taskSetsFailed.contains(taskSet.id))
    assert(manager.resourceOffer("execC", "host2", ANY).isDefined)
    sched.removeExecutor("execC")
    manager.executorLost(
      "execC", "host2", ExecutorExited(1, true, "Terminated due to issue with running tasks"))
    assert(sched.taskSetsFailed.contains(taskSet.id))
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
    val manager = new TaskSetManager(sched, taskSet, MAX_TASK_FAILURES, clock)

    assert(manager.myLocalityLevels.sameElements(Array(PROCESS_LOCAL, NODE_LOCAL, RACK_LOCAL, ANY)))
    // Set allowed locality to ANY
    clock.advance(LOCALITY_WAIT_MS * 3)
    // Offer host3
    // No task is scheduled if we restrict locality to RACK_LOCAL
    assert(manager.resourceOffer("execC", "host3", RACK_LOCAL) === None)
    // Task 0 can be scheduled with ANY
    assert(manager.resourceOffer("execC", "host3", ANY).get.index === 0)
    // Offer host2
    // Task 1 can be scheduled with RACK_LOCAL
    assert(manager.resourceOffer("execB", "host2", RACK_LOCAL).get.index === 1)
  }

  test("do not emit warning when serialized task is small") {
    sc = new SparkContext("local", "test")
    sched = new FakeTaskScheduler(sc, ("exec1", "host1"))
    val taskSet = FakeTask.createTaskSet(1)
    val manager = new TaskSetManager(sched, taskSet, MAX_TASK_FAILURES)

    assert(!manager.emittedTaskSizeWarning)

    assert(manager.resourceOffer("exec1", "host1", ANY).get.index === 0)

    assert(!manager.emittedTaskSizeWarning)
  }

  test("emit warning when serialized task is large") {
    sc = new SparkContext("local", "test")
    sched = new FakeTaskScheduler(sc, ("exec1", "host1"))

    val taskSet = new TaskSet(Array(new LargeTask(0)), 0, 0, 0, null)
    val manager = new TaskSetManager(sched, taskSet, MAX_TASK_FAILURES)

    assert(!manager.emittedTaskSizeWarning)

    assert(manager.resourceOffer("exec1", "host1", ANY).get.index === 0)

    assert(manager.emittedTaskSizeWarning)
  }

  test("Not serializable exception thrown if the task cannot be serialized") {
    sc = new SparkContext("local", "test")
    sched = new FakeTaskScheduler(sc, ("exec1", "host1"))

    val taskSet = new TaskSet(
      Array(new NotSerializableFakeTask(1, 0), new NotSerializableFakeTask(0, 1)), 0, 0, 0, null)
    val manager = new TaskSetManager(sched, taskSet, MAX_TASK_FAILURES)

    intercept[TaskNotSerializableException] {
      manager.resourceOffer("exec1", "host1", ANY)
    }
    assert(manager.isZombie)
  }

  test("abort the job if total size of results is too large") {
    val conf = new SparkConf().set("spark.driver.maxResultSize", "2m")
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
    val manager = new TaskSetManager(sched, taskSet, MAX_TASK_FAILURES, clock)

    assert(manager.resourceOffer("execA", "host1", PROCESS_LOCAL).get.index === 0)
    assert(manager.resourceOffer("execA", "host1", NODE_LOCAL) == None)
    assert(manager.resourceOffer("execA", "host1", NO_PREF).get.index == 1)

    manager.speculatableTasks += 1
    clock.advance(LOCALITY_WAIT_MS)
    // schedule the nonPref task
    assert(manager.resourceOffer("execA", "host1", NO_PREF).get.index === 2)
    // schedule the speculative task
    assert(manager.resourceOffer("execB", "host2", NO_PREF).get.index === 1)
    clock.advance(LOCALITY_WAIT_MS * 3)
    // schedule non-local tasks
    assert(manager.resourceOffer("execB", "host2", ANY).get.index === 3)
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
    val manager = new TaskSetManager(sched, taskSet, MAX_TASK_FAILURES, clock)

    // node-local tasks are scheduled without delay
    assert(manager.resourceOffer("execA", "host1", NODE_LOCAL).get.index === 0)
    assert(manager.resourceOffer("execA", "host2", NODE_LOCAL).get.index === 1)
    assert(manager.resourceOffer("execA", "host3", NODE_LOCAL).get.index === 3)
    assert(manager.resourceOffer("execA", "host3", NODE_LOCAL) === None)

    // schedule no-preference after node local ones
    assert(manager.resourceOffer("execA", "host3", NO_PREF).get.index === 2)
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
    val manager = new TaskSetManager(sched, taskSet, MAX_TASK_FAILURES, clock)

    // process-local tasks are scheduled first
    assert(manager.resourceOffer("execA", "host1", NODE_LOCAL).get.index === 2)
    assert(manager.resourceOffer("execB", "host2", NODE_LOCAL).get.index === 3)
    // node-local tasks are scheduled without delay
    assert(manager.resourceOffer("execA", "host1", NODE_LOCAL).get.index === 0)
    assert(manager.resourceOffer("execB", "host2", NODE_LOCAL).get.index === 1)
    assert(manager.resourceOffer("execA", "host1", NODE_LOCAL) == None)
    assert(manager.resourceOffer("execB", "host2", NODE_LOCAL) == None)
  }

  test("SPARK-4939: no-pref tasks should be scheduled after process-local tasks finished") {
    sc = new SparkContext("local", "test")
    sched = new FakeTaskScheduler(sc, ("execA", "host1"), ("execB", "host2"))
    val taskSet = FakeTask.createTaskSet(3,
      Seq(),
      Seq(ExecutorCacheTaskLocation("host1", "execA")),
      Seq(ExecutorCacheTaskLocation("host2", "execB")))
    val clock = new ManualClock
    val manager = new TaskSetManager(sched, taskSet, MAX_TASK_FAILURES, clock)

    // process-local tasks are scheduled first
    assert(manager.resourceOffer("execA", "host1", PROCESS_LOCAL).get.index === 1)
    assert(manager.resourceOffer("execB", "host2", PROCESS_LOCAL).get.index === 2)
    // no-pref tasks are scheduled without delay
    assert(manager.resourceOffer("execA", "host1", PROCESS_LOCAL) == None)
    assert(manager.resourceOffer("execA", "host1", NODE_LOCAL) == None)
    assert(manager.resourceOffer("execA", "host1", NO_PREF).get.index === 0)
    assert(manager.resourceOffer("execA", "host1", ANY) == None)
  }

  test("Ensure TaskSetManager is usable after addition of levels") {
    // Regression test for SPARK-2931
    sc = new SparkContext("local", "test")
    sched = new FakeTaskScheduler(sc)
    val taskSet = FakeTask.createTaskSet(2,
      Seq(TaskLocation("host1", "execA")),
      Seq(TaskLocation("host2", "execB.1")))
    val clock = new ManualClock
    val manager = new TaskSetManager(sched, taskSet, MAX_TASK_FAILURES, clock)
    // Only ANY is valid
    assert(manager.myLocalityLevels.sameElements(Array(ANY)))
    // Add a new executor
    sched.addExecutor("execA", "host1")
    sched.addExecutor("execB.2", "host2")
    manager.executorAdded()
    assert(manager.pendingTasksWithNoPrefs.size === 0)
    // Valid locality should contain PROCESS_LOCAL, NODE_LOCAL and ANY
    assert(manager.myLocalityLevels.sameElements(Array(PROCESS_LOCAL, NODE_LOCAL, ANY)))
    assert(manager.resourceOffer("execA", "host1", ANY) !== None)
    clock.advance(LOCALITY_WAIT_MS * 4)
    assert(manager.resourceOffer("execB.2", "host2", ANY) !== None)
    sched.removeExecutor("execA")
    sched.removeExecutor("execB.2")
    manager.executorLost("execA", "host1", SlaveLost())
    manager.executorLost("execB.2", "host2", SlaveLost())
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
    val manager = new TaskSetManager(sched, taskSet, MAX_TASK_FAILURES, clock)
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
    sc.conf.set("spark.speculation.multiplier", "0.0")
    val clock = new ManualClock()
    val manager = new TaskSetManager(sched, taskSet, MAX_TASK_FAILURES, clock)
    val accumUpdatesByTask: Array[Seq[AccumulatorV2[_, _]]] = taskSet.tasks.map { task =>
      task.metrics.internalAccums
    }
    // Offer resources for 4 tasks to start
    for ((k, v) <- List(
        "exec1" -> "host1",
        "exec1" -> "host1",
        "exec2" -> "host2",
        "exec2" -> "host2")) {
      val taskOption = manager.resourceOffer(k, v, NO_PREF)
      assert(taskOption.isDefined)
      val task = taskOption.get
      assert(task.executorId === k)
    }
    assert(sched.startedTasks.toSet === Set(0, 1, 2, 3))
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
    // Offer resource to start the speculative attempt for the running task
    val taskOption5 = manager.resourceOffer("exec1", "host1", NO_PREF)
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
    verify(sched.backend).killTask(3, "exec2", true)
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
    sc.conf.set("spark.speculation.multiplier", "0.0")
    sc.conf.set("spark.speculation.quantile", "0.6")
    val clock = new ManualClock()
    val manager = new TaskSetManager(sched, taskSet, MAX_TASK_FAILURES, clock)
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
      val taskOption = manager.resourceOffer(k, v, NO_PREF)
      assert(taskOption.isDefined)
      val task = taskOption.get
      assert(task.executorId === k)
      tasks += task
    }
    assert(sched.startedTasks.toSet === (0 until 5).toSet)
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
        val nextTask = manager.resourceOffer(s"exec2", s"host2", NO_PREF)
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
    // Offer resource to start the speculative attempt for the running task
    val taskOption5 = manager.resourceOffer("exec1", "host1", NO_PREF)
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
    verify(sched.backend).killTask(origTask.taskId, "exec2", true)
    // Because the SchedulerBackend was a mock, the 2nd copy of the task won't actually be
    // killed, so the FakeTaskScheduler is only told about the successful completion
    // of the speculated task.
    assert(sched.endedTasks(3) === Success)
    // also because the scheduler is a mock, our manager isn't notified about the task killed event,
    // so we do that manually
    manager.handleFailedTask(origTask.taskId, TaskState.KILLED, TaskKilled)
    // this task has "failed" 4 times, but one of them doesn't count, so keep running the stage
    assert(manager.tasksSuccessful === 4)
    assert(!manager.isZombie)

    // now run another speculative task
    val taskOpt6 = manager.resourceOffer("exec1", "host1", NO_PREF)
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
    verify(sched.backend).killTask(origTask2.taskId, "exec2", true)
    assert(manager.tasksSuccessful === 5)
    assert(manager.isZombie)
  }

  test("SPARK-17894: Verify TaskSetManagers for different stage attempts have unique names") {
    sc = new SparkContext("local", "test")
    sched = new FakeTaskScheduler(sc, ("exec1", "host1"))
    val taskSet = FakeTask.createTaskSet(numTasks = 1, stageId = 0, stageAttemptId = 0)
    val manager = new TaskSetManager(sched, taskSet, MAX_TASK_FAILURES, new ManualClock)
    assert(manager.name === "TaskSet_0.0")

    // Make sure a task set with the same stage ID but different attempt ID has a unique name
    val taskSet2 = FakeTask.createTaskSet(numTasks = 1, stageId = 0, stageAttemptId = 1)
    val manager2 = new TaskSetManager(sched, taskSet2, MAX_TASK_FAILURES, new ManualClock)
    assert(manager2.name === "TaskSet_0.1")

    // Make sure a task set with the same attempt ID but different stage ID also has a unique name
    val taskSet3 = FakeTask.createTaskSet(numTasks = 1, stageId = 1, stageAttemptId = 1)
    val manager3 = new TaskSetManager(sched, taskSet3, MAX_TASK_FAILURES, new ManualClock)
    assert(manager3.name === "TaskSet_1.1")
  }

  private def createTaskResult(
      id: Int,
      accumUpdates: Seq[AccumulatorV2[_, _]] = Seq.empty): DirectTaskResult[Int] = {
    val valueSer = SparkEnv.get.serializer.newInstance()
    new DirectTaskResult[Int](valueSer.serialize(id), accumUpdates)
  }
}
