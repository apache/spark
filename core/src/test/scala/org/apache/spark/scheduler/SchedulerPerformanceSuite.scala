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

  def runJobWithBackend(N: Int, backend: () => Unit): Unit = {
    // Try to run as many jobs as we can in 10 seconds, get the time per job.  The idea here is to
    // balance:
    // 1) have a big enough job that we're not effected by delays just from waiting for job
    //   completion to propagate to the user thread (probably minor)
    // 2) run enough iterations to get some reliable data
    // 3) not wait toooooo long
    var itrs = 0
    val totalMs = withBackend(backend) {
      val start = System.currentTimeMillis()
      while (System.currentTimeMillis() - start < 30000 ) {
        withClue(s"failure in iteration = $itrs") {
          val jobFuture = submit(simpleWorkload(N), (0 until N).toArray)
          // Note: Do not call Await.ready(future) because that calls `scala.concurrent.blocking`,
          // which causes concurrent SQL executions to fail if a fork-join pool is used. Note that
          // due to idiosyncrasies in Scala, `awaitPermission` is not actually used anywhere so it's
          // safe to pass in null here. For more detail, see SPARK-13747.
          val awaitPermission = null.asInstanceOf[scala.concurrent.CanAwait]
          jobFuture.ready(Duration.Inf)(awaitPermission)
          assertDataStructuresEmpty(noFailure = true)
          itrs += 1
        }
      }
      System.currentTimeMillis() - start
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

  def backendWithBadExecs(N: Int, badExecs: Set[String], badHosts: Set[String]): Unit = {
    val taskDescription = backend.beginTask()
    val host = backend.executorIdToExecutor(taskDescription.executorId).host
    val taskSet = taskScheduler.taskIdToTaskSetManager(taskDescription.taskId).taskSet
    val task = taskSet.tasks(taskDescription.index)
    if (badExecs(taskDescription.executorId)) {
      val exc = new RuntimeException(s"bad exec ${taskDescription.executorId}")
      backend.taskFailed(taskDescription, exc)
    } else if (badHosts(host)) {
      val exc = new RuntimeException(s"bad host ${host}")
      backend.taskFailed(taskDescription, exc)
    } else {
      // every 5th stage is a ResultStage -- the rest are ShuffleMapStages
      (task.stageId, task.partitionId) match {
        case (stage, _) if stage % 5 != 4 =>
          backend.taskSuccess(taskDescription,
            DAGSchedulerSuite.makeMapStatus(host, N))
        case (_, _) =>
          backend.taskSuccess(taskDescription, 42)
      }
    }
  }

  def runBadExecJob(N: Int, badExecs: Set[String], badHosts: Set[String]): Unit = {
    runJobWithBackend(N, () => backendWithBadExecs(N, badExecs, badHosts))
  }

  val badExecs = (0 until 2).map{_.toString}.toSet
  val badHosts = Set[String]()

  // note this is *very* unlikely to succeed without blacklisting, even though its only
  // one bad executor out of 20.  When a task fails, it gets requeued immediately -- and guess
  // which is the only executor which has a free slot?  Bingo, the one it just failed on
  testScheduler(
    "COMPARE D bad execs with simple blacklist",
    extraConfs = Seq(
      "spark.scheduler.executorTaskBlacklistTime" -> "10000000",
      "spark.scheduler.blacklist.advancedStrategy" -> "false",
      "spark.testing.nHosts" -> "2",
      "spark.testing.nExecutorsPerHost" -> "2"
    )
  ) {
    runBadExecJob(100, badExecs, badHosts)
  }

  testScheduler(
    "COMPARE D bad execs with advanced blacklist",
    extraConfs = Seq(
      "spark.scheduler.executorTaskBlacklistTime" -> "10000000",
      "spark.scheduler.blacklist.advancedStrategy" -> "true",
      "spark.testing.nHosts" -> "2",
      "spark.testing.nExecutorsPerHost" -> "2"
    )
  ) {
    runBadExecJob(100, badExecs, badHosts)
  }

  testScheduler(
    "COMPARE D bad host with advanced blacklist",
    extraConfs = Seq(
      "spark.scheduler.executorTaskBlacklistTime" -> "10000000",
      "spark.scheduler.blacklist.advancedStrategy" -> "true",
      "spark.testing.nHosts" -> "2",
      "spark.testing.nExecutorsPerHost" -> "2"
    )
  ) {
    runBadExecJob(100, badExecs, Set("host-0"))
  }

}
