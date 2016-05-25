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

import java.util.concurrent.atomic.AtomicBoolean

import scala.concurrent.duration.Duration

import org.apache.spark.util.Utils

class SchedulerPerformanceSuite extends SchedulerIntegrationSuite[MultiExecutorMockBackend] {

  def simpleWorkload(N: Int): MockRDD = {
    // relatively simple job with 5 stages, so scheduling includes some aspects of submitting stages
    // in addition to tasks
    val a = new MockRDD(sc, N, Nil)
    val b = shuffle(N, a)
    val c = shuffle(N, a)
    join(N, b, c)
  }

  def goodBackend(N: Int): Unit = {
    val taskDescription = backend.beginTask()
    val host = backend.executorIdToExecutor(taskDescription.executorId).host
    val taskSet = taskScheduler.taskIdToTaskSetManager(taskDescription.taskId).taskSet
    val task = taskSet.tasks(taskDescription.index)

    // every 5th stage is a ResultStage -- the rest are ShuffleMapStages
    (task.stageId, task.partitionId) match {
      case (stage, _) if stage % 5 != 4 =>
        backend.taskSuccess(taskDescription,
          DAGSchedulerSuite.makeMapStatus(host, N))
      case (_, _) =>
        backend.taskSuccess(taskDescription, 42)
    }
  }

  def runJobWithBackend(N: Int, backendFunc: () => Unit): Unit = {
    runJobWithCustomBackend(N, new SimpleWrappedBackend(backend, backendFunc))
  }

  def runJobWithCustomBackend(N: Int, backendWrapper: WrappedBackend): Unit = {
    // Try to run as many jobs as we can in 10 seconds, get the time per job.  The idea here is to
    // balance:
    // 1) have a big enough job that we're not effected by delays just from waiting for job
    //   completion to propagate to the user thread (probably minor)
    // 2) run enough iterations to get some reliable data
    // 3) not wait toooooo long
    var itrs = 0
    val totalMs = backendWrapper.withBackend {
      val start = System.currentTimeMillis()
      while (System.currentTimeMillis() - start < 10000 ) {
//        while (System.currentTimeMillis() - start < 10000  && itrs == 0) {
        withClue(s"failure in iteration = $itrs") {
          val itrStart = System.currentTimeMillis()
          val jobFuture = submit(simpleWorkload(N), (0 until N).toArray)
          // Note: Do not call Await.ready(future) because that calls `scala.concurrent.blocking`,
          // which causes concurrent SQL executions to fail if a fork-join pool is used. Note that
          // due to idiosyncrasies in Scala, `awaitPermission` is not actually used anywhere so it's
          // safe to pass in null here. For more detail, see SPARK-13747.
          val awaitPermission = null.asInstanceOf[scala.concurrent.CanAwait]
          jobFuture.ready(Duration.Inf)(awaitPermission)
          // scalastyle:off println
          println(s"Iteration $itrs finished in" +
            s" ${Utils.msDurationToString(System.currentTimeMillis() - itrStart)}")
          // scalastyle:on println
          assertDataStructuresEmpty(noFailure = true)
          itrs += 1
        }
      }
      (System.currentTimeMillis() - start)
    }

    val msPerItr = Utils.msDurationToString((totalMs.toDouble / itrs).toLong)
    // scalastyle:off println
    println(s"ran $itrs iterations in ${Utils.msDurationToString(totalMs)} ($msPerItr per itr)")
    // scalastyle:on println
  }

  def runSuccessfulJob(N: Int): Unit = {
    runJobWithBackend(N, () => goodBackend(N))
  }

  testScheduler("Scheduling speed -- small job on a small cluster") {
    runSuccessfulJob(40)
  }

  testScheduler("COMPARE C Scheduling speed -- large job on a small cluster") {
    runSuccessfulJob(3000)
  }

  testScheduler(
    "COMPARE C Scheduling speed -- large job on a small cluster with advanced blacklist",
    extraConfs = Seq(
      "spark.scheduler.executorTaskBlacklistTime" -> "10000000",
      "spark.scheduler.blacklist.advancedStrategy" -> "true"
    )
  ) {
    runSuccessfulJob(3000)
  }

  testScheduler(
    "COMPARE A Scheduling speed -- large job on a super node",
    extraConfs = Seq(
      "spark.testing.nHosts" -> "1",
      "spark.testing.nExecutorsPerHost" -> "1",
      "spark.testing.nCoresPerExecutor" -> "20000"
    )
  ) {
    runSuccessfulJob(3000)
  }

  testScheduler(
    // 4 execs per node, 2 cores per exec, so 400 cores
    "COMPARE A Scheduling speed -- large job on 50 node cluster",
    extraConfs = Seq(
      "spark.testing.nHosts" -> "50"
    )
  ) {
    runSuccessfulJob(3000)
  }

  testScheduler(
    // 4 execs per node, 2 cores per exec, so 800 cores
    "COMPARE A Scheduling speed -- large job on 100 node cluster",
    extraConfs = Seq(
      "spark.testing.nHosts" -> "100"
    )
  ) {
    runSuccessfulJob(3000)
  }

  Seq(200, 300, 400, 450, 500, 550).foreach { nodes =>
    /*
  ran 1 iterations in 12.9 s (12.9 s per itr)
  [info] - COMPARE A: Scheduling speed -- large job on 200 node cluster (13 seconds, 861
   milliseconds)
  ran 1 iterations in 25.0 s (25.0 s per itr)
  [info] - COMPARE A: Scheduling speed -- large job on 300 node cluster (25 seconds, 50
   milliseconds)
  ran 1 iterations in 34.6 s (34.6 s per itr)
  [info] - COMPARE A: Scheduling speed -- large job on 400 node cluster (34 seconds,
   668 milliseconds)
  ran 1 iterations in 54.0 s (54.0 s per itr)
  [info] - COMPARE A: Scheduling speed -- large job on 450 node cluster (53 seconds,
   991 milliseconds)
  ran 1 iterations in 1.8 m (1.8 m per itr)
  [info] - COMPARE A: Scheduling speed -- large job on 500 node cluster (1 minute, 48 seconds)
  ran 1 iterations in 2.3 m (2.3 m per itr)
  [info] - COMPARE A: Scheduling speed -- large job on 550 node cluster (2 minutes, 19 seconds)
     */
    testScheduler(
      s"COMPARE A: Scheduling speed -- large job on ${nodes} node cluster",
      extraConfs = Seq(
        "spark.testing.nHosts" -> s"$nodes"
      )
    ) {
      runSuccessfulJob(3000)
    }
  }

  /*
  nHosts = 400; nExecutorsPerHost = 1; nCores = 800
  ran 2 iterations in 11.7 s (5.9 s per itr)
  [info] - COMPARE B: Lots of nodes (12 seconds, 679 milliseconds)
  nHosts = 1; nExecutorsPerHost = 400; nCores = 800
  ran 3 iterations in 14.2 s (4.7 s per itr)
  [info] - COMPARE B: Lots of executors, one node (14 seconds, 290 milliseconds)
  nHosts = 1; nExecutorsPerHost = 1; nCores = 800
  ran 3 iterations in 11.0 s (3.7 s per itr)
  [info] - COMPARE B: Super executor (11 seconds, 6 milliseconds)
   */
  testScheduler(
    s"COMPARE B: Lots of nodes",
    extraConfs = Seq(
      "spark.testing.nHosts" -> "400",
      "spark.testing.nExecutorsPerHost" -> "1"
    )
  ) {
    runSuccessfulJob(3000)
  }

  testScheduler(
    s"COMPARE B: Lots of executors, one node",
    extraConfs = Seq(
      "spark.testing.nHosts" -> "1",
      "spark.testing.nExecutorsPerHost" -> "400"
    )
  ) {
    runSuccessfulJob(3000)
  }

  testScheduler(
    s"COMPARE B: Super executor",
    extraConfs = Seq(
      "spark.testing.nHosts" -> "1",
      "spark.testing.nExecutorsPerHost" -> "1",
      "spark.testing.nCoresPerExecutor" -> "800"
    )
  ) {
    runSuccessfulJob(3000)
  }

  def backendWithBadExecs(
      continue: AtomicBoolean,
      N: Int,
      badExecs: Set[String],
      badHosts: Set[String]): Unit = {
    var tasksToFail = List[TaskDescription]()
    var tasksToSucceed = List[TaskDescription]()
    val FAILURES_TILL_SUCCESS = 100 // that is, we get a task failure 100 times as fast as success
    val waitForSuccess = 100
    var failuresSinceLastSuccess = 0
    while (continue.get()) {
      // don't *just* keep failing tasks on the same executor.  While there are tasks to fail,
      // we fail them more often, but we fail across all executors.  Furthermore, after X failures,
      // we do have a task success

      // first, queue up all the tasks needing to run
      while (backend.hasTasksWaitingToRun) {
        val taskDescription = backend.beginTask()
        val host = backend.executorIdToExecutor(taskDescription.executorId).host
        val taskSet = taskScheduler.taskIdToTaskSetManager(taskDescription.taskId).taskSet
        val task = taskSet.tasks(taskDescription.index)
        if (badExecs(taskDescription.executorId) || badHosts(host)) {
          tasksToFail :+= taskDescription
        } else {
          tasksToSucceed :+= taskDescription
        }
      }

      // send a task result.  Failure if there are any and we haven't had too many failures in a row
      def failTask(): Unit = {
        failuresSinceLastSuccess += 1
        val toFail = tasksToFail.head
        tasksToFail = tasksToFail.tail
        val host = backend.executorIdToExecutor(toFail.executorId).host
        if (badExecs(toFail.executorId)) {
          val exc = new RuntimeException(s"bad exec ${toFail.executorId}")
          backend.taskFailed(toFail, exc)
        } else if (badHosts(host)) {
          val exc = new RuntimeException(s"bad host ${host}")
          backend.taskFailed(toFail, exc)
        }
      }
      if (tasksToFail.nonEmpty && failuresSinceLastSuccess < FAILURES_TILL_SUCCESS) {
        failTask()
      } else if (tasksToSucceed.nonEmpty) {
        // we might get here just by some chance of thread-scheduling in this mock.  Tasks fail,
        // but the dag scheduler thread hasn't processed those before this thread tries to find
        // another task to respond to.
//        Thread.sleep(waitForSuccess)
        if (tasksToFail.nonEmpty && failuresSinceLastSuccess < FAILURES_TILL_SUCCESS) {
          failTask()
        } else {
          logInfo(s"tasksToFail.size = ${tasksToFail.size}; " +
            s"tasksToSucceed.size = ${tasksToSucceed.size}; " +
            s"failuresSinceLastSuccess = ${failuresSinceLastSuccess}")
          failuresSinceLastSuccess = 0
          val taskDescription = tasksToSucceed.head
          tasksToSucceed = tasksToSucceed.tail
          val host = backend.executorIdToExecutor(taskDescription.executorId).host
          val taskSet = taskScheduler.taskIdToTaskSetManager(taskDescription.taskId).taskSet
          val task = taskSet.tasks(taskDescription.index)
          // every 5th stage is a ResultStage -- the rest are ShuffleMapStages
          (task.stageId, task.partitionId) match {
            case (stage, _) if stage % 5 != 4 =>
              backend.taskSuccess(taskDescription,
                DAGSchedulerSuite.makeMapStatus(host, N))
            case (_, _) =>
              backend.taskSuccess(taskDescription, 42)
          }
        }
      } else {
        Thread.sleep(10)  // wait till we've got work to do
      }
    }
  }

  def runBadExecJob(N: Int, badExecs: Set[String], badHosts: Set[String]): Unit = {
    val backendWrapper = new WrappedBackend(backend) {
      override def runBackend(continue: AtomicBoolean): Unit = {
        backendWithBadExecs(continue, N, badExecs, badHosts)
      }
    }
    runJobWithCustomBackend(N, backendWrapper)
  }

  val badExecs = (0 until 2).map{_.toString}.toSet

  // note this is *very* unlikely to succeed without blacklisting, even though its only
  // one bad executor out of 20.  When a task fails, it gets requeued immediately -- and guess
  // which is the only executor which has a free slot?  Bingo, the one it just failed on
  Seq(
    ("bad execs with simple blacklist", "false", Set[String]()),
    ("bad execs with advanced blacklist", "true", Set[String]()),
    ("bad hosts with advanced blacklist", "true", Set[String]("host-0"))
  ).foreach { case (name, strategy, badHosts) =>
    testScheduler(
      s"COMPARE D $name",
      extraConfs = Seq(
        "spark.scheduler.executorTaskBlacklistTime" -> "10000000",
        "spark.scheduler.blacklist.advancedStrategy" -> strategy
      )
    ) {
      // scalastyle:off println
      println(s"Bad execs = ${badExecs}")
      // scalastyle:on println

      // because offers get shuffled, its a crapshoot whether or not the "bad" executor will finish
      // tasks first.  (A more complicated mock backend could make sure it fails the first executor
      // it gets assigned)
      runBadExecJob(3000, badExecs, badHosts)
    }
  }


  // scalastyle:off line.size.limit

  /*
  Here's how you can get into really slow scheduling, even with the simple blacklist.  Say there
  is just one bad executor.  You've got a bunch of tasks to run, and you schedule all available
  slots.  Then one task fails on your bad executor.  You don't re-schedule that task on the bad
  executor, but you do think you've got one open slot, so you try to find the next task you can
  schedule.  Since you've got a massive backlog of tasks, you just take the next task and schedule
  it on your bad executor.  The task fails again.

  This repeats a while, and now you've gone through and failed a bunch of tasks on this one bad
  executor.  But each time, you clear the cache of invalid executors, so you do a bunch of work
  to recompute the set of OK executors.  This is *really* expensive, and doesn't help you at all
  anyway.


16/05/23 20:53:57.871 dag-scheduler-event-loop INFO BlacklistTracker: Blacklisting executors Set() for task StageAndPartition(8,38)
16/05/23 20:53:57.871 dag-scheduler-event-loop INFO TaskSetManager: Starting task 38.0 in stage 8.0 (TID 21056, host-2, partition 38, PROCESS_LOCAL, 5112 bytes)
16/05/23 20:53:57.871 dag-scheduler-event-loop INFO BlacklistTracker: Blacklisting nodes Set() for stage 8
16/05/23 20:53:57.871 dag-scheduler-event-loop INFO BlacklistTracker: Blacklisting executors Set() for task StageAndPartition(8,39)
16/05/23 20:53:57.871 dag-scheduler-event-loop INFO TaskSetManager: Starting task 39.0 in stage 8.0 (TID 21057, host-0, partition 39, PROCESS_LOCAL, 5112 bytes)
16/05/23 20:53:57.871 mock backend thread INFO BlacklistTracker: Blacklisting nodes Set() for stage 8
16/05/23 20:53:57.871 mock backend thread INFO BlacklistTracker: Blacklisting executors Set() for task StageAndPartition(8,40)
16/05/23 20:53:57.871 dag-scheduler-event-loop INFO DAGScheduler: ShuffleMapStage 5 (RDD at SchedulerIntegrationSuite.scala:360) finished in 1.731 s
16/05/23 20:53:57.871 dag-scheduler-event-loop INFO DAGScheduler: looking for newly runnable stages
16/05/23 20:53:57.871 dag-scheduler-event-loop INFO DAGScheduler: running: Set(ShuffleMapStage 8)
16/05/23 20:53:57.871 dag-scheduler-event-loop INFO DAGScheduler: waiting: Set(ResultStage 9, ShuffleMapStage 6)
16/05/23 20:53:57.871 dag-scheduler-event-loop INFO DAGScheduler: failed: Set()
16/05/23 20:53:57.872 mock backend thread INFO TaskSetManager: Starting task 40.0 in stage 8.0 (TID 21058, host-0, partition 40, PROCESS_LOCAL, 5112 bytes)
16/05/23 20:53:57.872 task-result-getter-2 WARN TaskSetManager: Lost task 39.0 in stage 8.0 (TID 21057, host-0): java.lang.RuntimeException: bad exec 1
        at org.apache.spark.scheduler.SchedulerPerformanceSuite.backendWithBadExecs(SchedulerPerformanceSuite.scala:218)
        at org.apache.spark.scheduler.SchedulerPerformanceSuite$$anonfun$runBadExecJob$1.apply$mcV$sp(SchedulerPerformanceSuite.scala:236)
        at org.apache.spark.scheduler.SchedulerIntegrationSuite$$anon$2.run(SchedulerIntegrationSuite.scala:194)

16/05/23 20:53:57.872 task-result-getter-2 INFO BlacklistTracker: invalidating blacklist cache
16/05/23 20:53:57.872 dag-scheduler-event-loop INFO DAGScheduler: Submitting ShuffleMapStage 6 (MockRDD 5), which has no missing parents
16/05/23 20:53:57.872 mock backend thread INFO BlacklistTracker: Blacklisting nodes Set() for stage 8
16/05/23 20:53:57.872 mock backend thread INFO BlacklistTracker: Blacklisting executors Set(1) for task StageAndPartition(8,39)
16/05/23 20:53:57.872 mock backend thread INFO BlacklistTracker: Blacklisting nodes Set() for stage 8
16/05/23 20:53:57.872 mock backend thread INFO BlacklistTracker: Blacklisting executors Set() for task StageAndPartition(8,41)
16/05/23 20:53:57.872 mock backend thread INFO TaskSetManager: Starting task 41.0 in stage 8.0 (TID 21059, host-0, partition 41, PROCESS_LOCAL, 5112 bytes)
16/05/23 20:53:57.872 task-result-getter-3 INFO TaskSetManager: Lost task 40.0 in stage 8.0 (TID 21058) on executor host-0: java.lang.RuntimeException (bad exec 1) [duplicate 1]
16/05/23 20:53:57.872 task-result-getter-3 INFO BlacklistTracker: invalidating blacklist cache
16/05/23 20:53:57.873 mock backend thread INFO BlacklistTracker: Blacklisting nodes Set() for stage 8
16/05/23 20:53:57.873 mock backend thread INFO BlacklistTracker: Blacklisting executors Set(1) for task StageAndPartition(8,40)
16/05/23 20:53:57.873 mock backend thread INFO BlacklistTracker: Blacklisting nodes Set() for stage 8
16/05/23 20:53:57.873 mock backend thread INFO BlacklistTracker: Blacklisting executors Set(1) for task StageAndPartition(8,39)
16/05/23 20:53:57.873 mock backend thread INFO BlacklistTracker: Blacklisting nodes Set() for stage 8
16/05/23 20:53:57.873 mock backend thread INFO BlacklistTracker: Blacklisting executors Set() for task StageAndPartition(8,42)
16/05/23 20:53:57.873 mock backend thread INFO TaskSetManager: Starting task 42.0 in stage 8.0 (TID 21060, host-0, partition 42, PROCESS_LOCAL, 5112 bytes)
16/05/23 20:53:57.873 task-result-getter-1 INFO TaskSetManager: Lost task 41.0 in stage 8.0 (TID 21059) on executor host-0: java.lang.RuntimeException (bad exec 1) [duplicate 2]
16/05/23 20:53:57.873 task-result-getter-1 INFO BlacklistTracker: invalidating blacklist cache
16/05/23 20:53:57.873 mock backend thread INFO BlacklistTracker: Blacklisting nodes Set() for stage 8
16/05/23 20:53:57.873 mock backend thread INFO BlacklistTracker: Blacklisting executors Set(1) for task StageAndPartition(8,41)
16/05/23 20:53:57.873 mock backend thread INFO BlacklistTracker: Blacklisting nodes Set() for stage 8
16/05/23 20:53:57.873 mock backend thread INFO BlacklistTracker: Blacklisting executors Set(1) for task StageAndPartition(8,40)
16/05/23 20:53:57.873 mock backend thread INFO BlacklistTracker: Blacklisting nodes Set() for stage 8
16/05/23 20:53:57.873 mock backend thread INFO BlacklistTracker: Blacklisting executors Set(1) for task StageAndPartition(8,39)
16/05/23 20:53:57.873 mock backend thread INFO BlacklistTracker: Blacklisting nodes Set() for stage 8
16/05/23 20:53:57.873 mock backend thread INFO BlacklistTracker: Blacklisting executors Set() for task StageAndPartition(8,43)

   */

  // scalastyle:on line.size.limit

  abstract class WrappedBackend(backend: MockBackend) {
    val backendContinue = new AtomicBoolean(true)
    def runBackend(continue: AtomicBoolean): Unit
    val backendThread = new Thread("mock backend thread") {
      override def run(): Unit = {
        runBackend(backendContinue)
      }
    }

    def withBackend[T](testBody: => T): T = {
      try {
        backendThread.start()
        testBody
      } finally {
        backendContinue.set(false)
        backendThread.join()
      }
    }
  }

  class SimpleWrappedBackend(backend: MockBackend, backendFunc: () => Unit)
      extends WrappedBackend(backend) {
    override def runBackend(continue: AtomicBoolean): Unit = {
      while (continue.get()) {
        if (backend.hasTasksWaitingToRun) {
          backendFunc()
        } else {
          Thread.sleep(10)
        }
      }
    }
  }
}
