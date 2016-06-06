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

  def runJobWithCustomBackend(N: Int, backendWrapper: FifoBackend): Unit = {
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
    runJobWithCustomBackend(N, new FifoBackend(backend) {
      override def handleTask(taskDesc: TaskDescription, task: Task[_], host: String): Unit = {
        // every 5th stage is a ResultStage -- the rest are ShuffleMapStages
        (task.stageId, task.partitionId) match {
          case (stage, _) if stage % 5 != 4 =>
            queueSuccess(taskDesc, DAGSchedulerSuite.makeMapStatus(host, N))
          case (_, _) =>
            queueSuccess(taskDesc, 42)
        }
      }
    })
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
    testScheduler(
      s"COMPARE A: Scheduling speed -- large job on ${nodes} node cluster",
      extraConfs = Seq(
        "spark.testing.nHosts" -> s"$nodes"
      )
    ) {
      runSuccessfulJob(3000)
    }
  }

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

  def runBadExecJob(N: Int, badExecs: Set[String], badHosts: Set[String]): Unit = {
    val backendWrapper = new FifoBackend(backend) {
      override def handleTask(taskDesc: TaskDescription, task: Task[_], host: String): Unit = {
        if (badExecs(taskDesc.executorId)) {
          val exc = new RuntimeException(s"bad exec ${taskDesc.executorId}")
          queueFailure(taskDesc, exc)
        } else if (badHosts(host)) {
          val exc = new RuntimeException(s"bad host ${host}")
          queueFailure(taskDesc, exc)
        } else {
          // every 5th stage is a ResultStage -- the rest are ShuffleMapStages
          (task.stageId, task.partitionId) match {
            case (stage, _) if stage % 5 != 4 =>
              queueSuccess(taskDesc, DAGSchedulerSuite.makeMapStatus(host, N))
            case (_, _) =>
              queueSuccess(taskDesc, 42)
          }
        }
      }
    }
    runJobWithCustomBackend(N, backendWrapper)
  }

  val oneBadExec = Set("0")
  // intentionally on different nodes, so they don't trigger node blacklist
  val twoBadExecs = Set("0", "15")


  // note this is *very* unlikely to succeed without blacklisting, even with only
  // one bad executor out of 20.  When a task fails, it gets requeued immediately -- and guess
  // which is the only executor which has a free slot?  Bingo, the one it just failed on
  Seq(
    ("bad exec with simple blacklist", false, oneBadExec, Set[String]()),
    ("two bad execs with simple blacklist", false, twoBadExecs, Set[String]()),
    ("bad exec with advanced blacklist", true, oneBadExec, Set[String]()),
    ("bad host with advanced blacklist", true, Set[String](), Set[String]("host-0")),
    ("bad exec and host with advanced blacklist", true, oneBadExec, Set[String]("host-3"))
  ).foreach { case (name, strategy, badExecs, badHosts) =>
    testScheduler(
      s"COMPARE D $name",
      extraConfs = Seq(
        "spark.scheduler.executorTaskBlacklistTime" -> "10000000",
        "spark.scheduler.blacklist.advancedStrategy" -> strategy.toString
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


  /*
  RESULTS

  On a happy cluster, speed is about the same in all modes, ~5s per iteration

  On a bad cluster, slow in most versions, about 2m per iteration (original code, and new code with
  various strategies).  the reason is that we waste soooooo long looping all tasks through
  the bad nodes, and that has one n^2 penalty.

  The one case where it is *fast* is with the new code, and the node blacklist, since that
  specifically has an optimization to avoid the n^2 penalty
   */

  /**
   * Helper to make sure the backend processes tasks in FIFO(-ish) order.  This is important
   * because if it were LIFO your mock backend will just keep running tasks on one executor --
   * whatever task gets run first, sends back its result, and the scheduler immediately schedules
   * another task on the same executor, which keeps repeating.
   *
   * The exception to FIFO order is that we send back failures first, to simulate the case where
   * tasks fail much more quickly then tasks succeed.  If there are any bad executors, this leads
   * to all tasks getting scheduled there.  Without effective blacklisting, this leads to a lot
   * of task failures.
   *
   * Just implement [[handleTask]]
   */
  abstract class FifoBackend(private val backend: MockBackend) {
    private val backendContinue = new AtomicBoolean(true)
    private val backendThread = new Thread("mock backend thread") {
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

    private var tasksToFail = List[(TaskDescription, Exception)]()
    private var tasksToSucceed = List[(TaskDescription, Any)]()
    private val FAILURES_TILL_SUCCESS = 100
    private val waitForSuccess = 100
    private var failuresSinceLastSuccess = 0

    /**
     *  Mark a task as either a success or failure (by calling [[queueSuccess]] or [[queueFailure]]
     *  respectively).  This will take of care of sending back the result in the correct
     *  order.  You *must* call one of those functions for the purposes of this mock (a more
     *  complex might simulate other conditions.)
     */
    def handleTask(taskDesc: TaskDescription, task: Task[_], host: String): Unit

    def queueSuccess(taskDesc: TaskDescription, result: Any): Unit = {
      tasksToSucceed :+= taskDesc -> result
    }

    def queueFailure(taskDesc: TaskDescription, exc: Exception): Unit = {
      tasksToFail :+= taskDesc -> exc
    }

    private def runBackend(continue: AtomicBoolean): Unit = {
      while (continue.get()) {
        // don't *just* keep failing tasks on the same executor.  While there are tasks to fail,
        // we fail them more often, but we fail across all executors.  Furthermore, after X failures
        // we do have a task success

        // first, queue up all the tasks needing to run
        while (backend.hasTasksWaitingToRun) {
          val (taskDescription, task) = backend.beginTask()
          val host = backend.executorIdToExecutor(taskDescription.executorId).host
          handleTask(taskDescription, task, host)
        }

        // send a task result.  Prioritize failures, if we haven't had too many failures in a row
        if (tasksToFail.nonEmpty && failuresSinceLastSuccess < FAILURES_TILL_SUCCESS) {
          failuresSinceLastSuccess += 1
          val (toFail, exc) = tasksToFail.head
          tasksToFail = tasksToFail.tail
          backend.taskFailed(toFail, exc)
        } else if (tasksToSucceed.nonEmpty) {
          // we might get here just by some chance of thread-scheduling in this mock.  Tasks fail,
          // but the scheduler thread hasn't processed those before this thread tries to find
          // another task to respond to.  Really this shouldn't be a huge problem, it just means
          // a few more interspersed successes.
          logInfo(s"tasksToFail.size = ${tasksToFail.size}; " +
            s"tasksToSucceed.size = ${tasksToSucceed.size}; " +
            s"failuresSinceLastSuccess = ${failuresSinceLastSuccess}")
          failuresSinceLastSuccess = 0
          val (taskDescription, result) = tasksToSucceed.head
          tasksToSucceed = tasksToSucceed.tail
          val host = backend.executorIdToExecutor(taskDescription.executorId).host
          val taskSet = taskScheduler.taskIdToTaskSetManager(taskDescription.taskId).taskSet
          val task = taskSet.tasks(taskDescription.index)
          backend.taskSuccess(taskDescription, result)
        } else {
          Thread.sleep(10) // wait till we've got work to do
        }
      }
    }
  }
}
