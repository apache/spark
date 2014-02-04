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

import scala.collection.mutable.ArrayBuffer
import scala.collection.mutable

import org.scalatest.FunSuite

import org.apache.spark._
import org.apache.spark.executor.TaskMetrics
import java.nio.ByteBuffer
import org.apache.spark.util.{Utils, FakeClock}

class FakeDAGScheduler(taskScheduler: FakeClusterScheduler) extends DAGScheduler(taskScheduler) {
  override def taskStarted(task: Task[_], taskInfo: TaskInfo) {
    taskScheduler.startedTasks += taskInfo.index
  }

  override def taskEnded(
      task: Task[_],
      reason: TaskEndReason,
      result: Any,
      accumUpdates: mutable.Map[Long, Any],
      taskInfo: TaskInfo,
      taskMetrics: TaskMetrics) {
    taskScheduler.endedTasks(taskInfo.index) = reason
  }

  override def executorGained(execId: String, host: String) {}

  override def executorLost(execId: String) {}

  override def taskSetFailed(taskSet: TaskSet, reason: String) {
    taskScheduler.taskSetsFailed += taskSet.id
  }
}

/**
 * A mock ClusterScheduler implementation that just remembers information about tasks started and
 * feedback received from the TaskSetManagers. Note that it's important to initialize this with
 * a list of "live" executors and their hostnames for isExecutorAlive and hasExecutorsAliveOnHost
 * to work, and these are required for locality in TaskSetManager.
 */
class FakeClusterScheduler(sc: SparkContext, liveExecutors: (String, String)* /* execId, host */)
  extends TaskSchedulerImpl(sc)
{
  val startedTasks = new ArrayBuffer[Long]
  val endedTasks = new mutable.HashMap[Long, TaskEndReason]
  val finishedManagers = new ArrayBuffer[TaskSetManager]
  val taskSetsFailed = new ArrayBuffer[String]

  val executors = new mutable.HashMap[String, String] ++ liveExecutors

  dagScheduler = new FakeDAGScheduler(this)

  def removeExecutor(execId: String): Unit = executors -= execId

  override def taskSetFinished(manager: TaskSetManager): Unit = finishedManagers += manager

  override def isExecutorAlive(execId: String): Boolean = executors.contains(execId)

  override def hasExecutorsAliveOnHost(host: String): Boolean = executors.values.exists(_ == host)
}

class TaskSetManagerSuite extends FunSuite with LocalSparkContext with Logging {
  import TaskLocality.{ANY, PROCESS_LOCAL, NODE_LOCAL, RACK_LOCAL}

  private val conf = new SparkConf

  val LOCALITY_WAIT = conf.getLong("spark.locality.wait", 3000)
  val MAX_TASK_FAILURES = 4

  test("TaskSet with no preferences") {
    sc = new SparkContext("local", "test")
    val sched = new FakeClusterScheduler(sc, ("exec1", "host1"))
    val taskSet = createTaskSet(1)
    val manager = new TaskSetManager(sched, taskSet, MAX_TASK_FAILURES)

    // Offer a host with no CPUs
    assert(manager.resourceOffer("exec1", "host1", 0, ANY) === None)

    // Offer a host with process-local as the constraint; this should work because the TaskSet
    // above won't have any locality preferences
    val taskOption = manager.resourceOffer("exec1", "host1", 2, TaskLocality.PROCESS_LOCAL)
    assert(taskOption.isDefined)
    val task = taskOption.get
    assert(task.executorId === "exec1")
    assert(sched.startedTasks.contains(0))

    // Re-offer the host -- now we should get no more tasks
    assert(manager.resourceOffer("exec1", "host1", 2, PROCESS_LOCAL) === None)

    // Tell it the task has finished
    manager.handleSuccessfulTask(0, createTaskResult(0))
    assert(sched.endedTasks(0) === Success)
    assert(sched.finishedManagers.contains(manager))
  }

  test("multiple offers with no preferences") {
    sc = new SparkContext("local", "test")
    val sched = new FakeClusterScheduler(sc, ("exec1", "host1"))
    val taskSet = createTaskSet(3)
    val manager = new TaskSetManager(sched, taskSet, MAX_TASK_FAILURES)

    // First three offers should all find tasks
    for (i <- 0 until 3) {
      val taskOption = manager.resourceOffer("exec1", "host1", 1, PROCESS_LOCAL)
      assert(taskOption.isDefined)
      val task = taskOption.get
      assert(task.executorId === "exec1")
    }
    assert(sched.startedTasks.toSet === Set(0, 1, 2))

    // Re-offer the host -- now we should get no more tasks
    assert(manager.resourceOffer("exec1", "host1", 1, PROCESS_LOCAL) === None)

    // Finish the first two tasks
    manager.handleSuccessfulTask(0, createTaskResult(0))
    manager.handleSuccessfulTask(1, createTaskResult(1))
    assert(sched.endedTasks(0) === Success)
    assert(sched.endedTasks(1) === Success)
    assert(!sched.finishedManagers.contains(manager))

    // Finish the last task
    manager.handleSuccessfulTask(2, createTaskResult(2))
    assert(sched.endedTasks(2) === Success)
    assert(sched.finishedManagers.contains(manager))
  }

  test("basic delay scheduling") {
    sc = new SparkContext("local", "test")
    val sched = new FakeClusterScheduler(sc, ("exec1", "host1"), ("exec2", "host2"))
    val taskSet = createTaskSet(4,
      Seq(TaskLocation("host1", "exec1")),
      Seq(TaskLocation("host2", "exec2")),
      Seq(TaskLocation("host1"), TaskLocation("host2", "exec2")),
      Seq()   // Last task has no locality prefs
    )
    val clock = new FakeClock
    val manager = new TaskSetManager(sched, taskSet, MAX_TASK_FAILURES, clock)

    // First offer host1, exec1: first task should be chosen
    assert(manager.resourceOffer("exec1", "host1", 1, ANY).get.index === 0)

    // Offer host1, exec1 again: the last task, which has no prefs, should be chosen
    assert(manager.resourceOffer("exec1", "host1", 1, ANY).get.index === 3)

    // Offer host1, exec1 again, at PROCESS_LOCAL level: nothing should get chosen
    assert(manager.resourceOffer("exec1", "host1", 1, PROCESS_LOCAL) === None)

    clock.advance(LOCALITY_WAIT)

    // Offer host1, exec1 again, at PROCESS_LOCAL level: nothing should get chosen
    assert(manager.resourceOffer("exec1", "host1", 1, PROCESS_LOCAL) === None)

    // Offer host1, exec1 again, at NODE_LOCAL level: we should choose task 2
    assert(manager.resourceOffer("exec1", "host1", 1, NODE_LOCAL).get.index == 2)

    // Offer host1, exec1 again, at NODE_LOCAL level: nothing should get chosen
    assert(manager.resourceOffer("exec1", "host1", 1, NODE_LOCAL) === None)

    // Offer host1, exec1 again, at ANY level: nothing should get chosen
    assert(manager.resourceOffer("exec1", "host1", 1, ANY) === None)

    clock.advance(LOCALITY_WAIT)

    // Offer host1, exec1 again, at ANY level: task 1 should get chosen
    assert(manager.resourceOffer("exec1", "host1", 1, ANY).get.index === 1)

    // Offer host1, exec1 again, at ANY level: nothing should be chosen as we've launched all tasks
    assert(manager.resourceOffer("exec1", "host1", 1, ANY) === None)
  }

  test("delay scheduling with fallback") {
    sc = new SparkContext("local", "test")
    val sched = new FakeClusterScheduler(sc,
      ("exec1", "host1"), ("exec2", "host2"), ("exec3", "host3"))
    val taskSet = createTaskSet(5,
      Seq(TaskLocation("host1")),
      Seq(TaskLocation("host2")),
      Seq(TaskLocation("host2")),
      Seq(TaskLocation("host3")),
      Seq(TaskLocation("host2"))
    )
    val clock = new FakeClock
    val manager = new TaskSetManager(sched, taskSet, MAX_TASK_FAILURES, clock)

    // First offer host1: first task should be chosen
    assert(manager.resourceOffer("exec1", "host1", 1, ANY).get.index === 0)

    // Offer host1 again: nothing should get chosen
    assert(manager.resourceOffer("exec1", "host1", 1, ANY) === None)

    clock.advance(LOCALITY_WAIT)

    // Offer host1 again: second task (on host2) should get chosen
    assert(manager.resourceOffer("exec1", "host1", 1, ANY).get.index === 1)

    // Offer host1 again: third task (on host2) should get chosen
    assert(manager.resourceOffer("exec1", "host1", 1, ANY).get.index === 2)

    // Offer host2: fifth task (also on host2) should get chosen
    assert(manager.resourceOffer("exec2", "host2", 1, ANY).get.index === 4)

    // Now that we've launched a local task, we should no longer launch the task for host3
    assert(manager.resourceOffer("exec2", "host2", 1, ANY) === None)

    clock.advance(LOCALITY_WAIT)

    // After another delay, we can go ahead and launch that task non-locally
    assert(manager.resourceOffer("exec2", "host2", 1, ANY).get.index === 3)
  }

  test("delay scheduling with failed hosts") {
    sc = new SparkContext("local", "test")
    val sched = new FakeClusterScheduler(sc, ("exec1", "host1"), ("exec2", "host2"))
    val taskSet = createTaskSet(3,
      Seq(TaskLocation("host1")),
      Seq(TaskLocation("host2")),
      Seq(TaskLocation("host3"))
    )
    val clock = new FakeClock
    val manager = new TaskSetManager(sched, taskSet, MAX_TASK_FAILURES, clock)

    // First offer host1: first task should be chosen
    assert(manager.resourceOffer("exec1", "host1", 1, ANY).get.index === 0)

    // Offer host1 again: third task should be chosen immediately because host3 is not up
    assert(manager.resourceOffer("exec1", "host1", 1, ANY).get.index === 2)

    // After this, nothing should get chosen
    assert(manager.resourceOffer("exec1", "host1", 1, ANY) === None)

    // Now mark host2 as dead
    sched.removeExecutor("exec2")
    manager.executorLost("exec2", "host2")

    // Task 1 should immediately be launched on host1 because its original host is gone
    assert(manager.resourceOffer("exec1", "host1", 1, ANY).get.index === 1)

    // Now that all tasks have launched, nothing new should be launched anywhere else
    assert(manager.resourceOffer("exec1", "host1", 1, ANY) === None)
    assert(manager.resourceOffer("exec2", "host2", 1, ANY) === None)
  }

  test("task result lost") {
    sc = new SparkContext("local", "test")
    val sched = new FakeClusterScheduler(sc, ("exec1", "host1"))
    val taskSet = createTaskSet(1)
    val clock = new FakeClock
    val manager = new TaskSetManager(sched, taskSet, MAX_TASK_FAILURES, clock)

    assert(manager.resourceOffer("exec1", "host1", 1, ANY).get.index === 0)

    // Tell it the task has finished but the result was lost.
    manager.handleFailedTask(0, TaskState.FINISHED, Some(TaskResultLost))
    assert(sched.endedTasks(0) === TaskResultLost)

    // Re-offer the host -- now we should get task 0 again.
    assert(manager.resourceOffer("exec1", "host1", 1, ANY).get.index === 0)
  }

  test("repeated failures lead to task set abortion") {
    sc = new SparkContext("local", "test")
    val sched = new FakeClusterScheduler(sc, ("exec1", "host1"))
    val taskSet = createTaskSet(1)
    val clock = new FakeClock
    val manager = new TaskSetManager(sched, taskSet, MAX_TASK_FAILURES, clock)

    // Fail the task MAX_TASK_FAILURES times, and check that the task set is aborted
    // after the last failure.
    (1 to manager.maxTaskFailures).foreach { index =>
      val offerResult = manager.resourceOffer("exec1", "host1", 1, ANY)
      assert(offerResult.isDefined,
        "Expect resource offer on iteration %s to return a task".format(index))
      assert(offerResult.get.index === 0)
      manager.handleFailedTask(offerResult.get.taskId, TaskState.FINISHED, Some(TaskResultLost))
      if (index < MAX_TASK_FAILURES) {
        assert(!sched.taskSetsFailed.contains(taskSet.id))
      } else {
        assert(sched.taskSetsFailed.contains(taskSet.id))
      }
    }
  }


  /**
   * Utility method to create a TaskSet, potentially setting a particular sequence of preferred
   * locations for each task (given as varargs) if this sequence is not empty.
   */
  def createTaskSet(numTasks: Int, prefLocs: Seq[TaskLocation]*): TaskSet = {
    if (prefLocs.size != 0 && prefLocs.size != numTasks) {
      throw new IllegalArgumentException("Wrong number of task locations")
    }
    val tasks = Array.tabulate[Task[_]](numTasks) { i =>
      new FakeTask(i, if (prefLocs.size != 0) prefLocs(i) else Nil)
    }
    new TaskSet(tasks, 0, 0, 0, null)
  }

  def createTaskResult(id: Int): DirectTaskResult[Int] = {
    val valueSer = SparkEnv.get.serializer.newInstance()
    new DirectTaskResult[Int](valueSer.serialize(id), mutable.Map.empty, new TaskMetrics)
  }
}
