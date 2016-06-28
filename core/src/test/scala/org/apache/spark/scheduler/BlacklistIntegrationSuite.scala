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

import scala.concurrent.Await
import scala.concurrent.duration._

import org.apache.spark._

class BlacklistIntegrationSuite extends SchedulerIntegrationSuite[MultiExecutorMockBackend]{

  val badHost = "host-0"
  val duration = Duration(10, SECONDS)

  /**
   * This backend just always fails if the task is executed on a bad host, but otherwise succeeds
   * all tasks.
   */
  def badHostBackend(): Unit = {
    val (taskDescription, _) = backend.beginTask()
    val host = backend.executorIdToExecutor(taskDescription.executorId).host
    if (host == badHost) {
      backend.taskFailed(taskDescription, new RuntimeException("I'm a bad host!"))
    } else {
      backend.taskSuccess(taskDescription, 42)
    }
  }

  // Test demonstrating the issue -- without a config change, the scheduler keeps scheduling
  // according to locality preferences, and so the job fails
  testScheduler("If preferred node is bad, without blacklist job will fail",
    extraConfs = Seq(
      "spark.scheduler.blacklist.enabled" -> "false"
  )) {
    val rdd = new MockRDDWithLocalityPrefs(sc, 10, Nil, badHost)
    withBackend(badHostBackend _) {
      val jobFuture = submit(rdd, (0 until 10).toArray)
      awaitJobTermination(jobFuture, duration)
    }
    assertDataStructuresEmpty(noFailure = false)
  }

  // even with the blacklist turned on, bad configs can lead to job failure.  To survive one
  // bad node, you need to make sure that
  // maxTaskFailures > spark.blacklist.maxFailedTasksPerExecutorStage *
  //                    spark.blacklist.maxFailedExecutorsPerNodeStage
  testScheduler(
    "With blacklist on, job will still fail if there are too many bad executors on bad host",
    extraConfs = Seq(
      "spark.scheduler.blacklist.enabled" -> "true",
      // just set this to something much longer than the test duration
      "spark.scheduler.executorTaskBlacklistTime" -> "10000000",
      "spark.testing.nHosts" -> "2",
      "spark.testing.nExecutorsPerHost" -> "5",
      "spark.testing.nCoresPerExecutor" -> "10",
      "spark.task.maxFailures" -> "4",
      "spark.blacklist.maxFailedTasksPerExecutorStage" -> "1",
      "spark.blacklist.maxFailedExecutorsPerNodeStage" -> "5"
    )
  ) {
    // to reliably reproduce the failure, we have to use 1 task.  That way, we ensure this
    // 1 task gets rotated through enough bad executors on the host to fail the taskSet,
    // before we have a bunch of different tasks fail in the executors so we blacklist them.
    // But the point here is -- we never try scheduling tasks on the good host-1, since we
    // hit too many failures trying our preferred host-0.
    val rdd = new MockRDDWithLocalityPrefs(sc, 1, Nil, badHost)
    withBackend(badHostBackend _) {
      val jobFuture = submit(rdd, (0 until 1).toArray)
      awaitJobTermination(jobFuture, duration)
    }
    assertDataStructuresEmpty(noFailure = false)
  }

  // Here we run with the blacklist on, and the default config takes care of having this
  // robust to one bad node.
  testScheduler(
    "Bad node with multiple executors, job will still succeed with the right confs",
    extraConfs = Seq(
      "spark.scheduler.blacklist.enabled" -> "true",
      // just set this to something much longer than the test duration
      "spark.scheduler.executorTaskBlacklistTime" -> "10000000",
      // just to avoid this test taking too long
      "spark.locality.wait" -> "10ms"
    )
  ) {
    val rdd = new MockRDDWithLocalityPrefs(sc, 10, Nil, badHost)
    withBackend(badHostBackend _) {
      val jobFuture = submit(rdd, (0 until 10).toArray)
      awaitJobTermination(jobFuture, duration)
    }
    assert(results === (0 until 10).map { _ -> 42 }.toMap)
    assertDataStructuresEmpty(noFailure = true)
  }

  // Make sure that is we've failed on all executors, but haven't hit task.maxFailures yet, the job
  // doesn't hang
  testScheduler(
    "Progress with fewer executors than maxTaskFailures",
    extraConfs = Seq(
      "spark.scheduler.blacklist.enabled" -> "true",
      // just set this to something much longer than the test duration
      "spark.scheduler.executorTaskBlacklistTime" -> "10000000",
      "spark.testing.nHosts" -> "2",
      "spark.testing.nExecutorsPerHost" -> "1",
      "spark.testing.nCoresPerExecutor" -> "1"
    )
  ) {
    def runBackend(): Unit = {
      val (taskDescription, _) = backend.beginTask()
      backend.taskFailed(taskDescription, new RuntimeException("test task failure"))
    }
    withBackend(runBackend _) {
      val jobFuture = submit(new MockRDD(sc, 10, Nil), (0 until 10).toArray)
      Await.ready(jobFuture, duration)
      assert(failure.getMessage.contains("Aborting TaskSet 0.0 because it has a task which " +
        "cannot be scheduled on any executor due to blacklists"))
    }
    assertDataStructuresEmpty(noFailure = false)
  }

  test("executors can be blacklisted with only a few failures per stage") {
    pending
  }

  test("executors aren't blacklisted if task sets fail") {
    pending
  }

  test("can blacklist executor for just one stage") {
    pending
  }

}

class MultiExecutorMockBackend(
    conf: SparkConf,
    taskScheduler: TaskSchedulerImpl) extends MockBackend(conf, taskScheduler) {

  val nHosts = conf.getInt("spark.testing.nHosts", 5)
  val nExecutorsPerHost = conf.getInt("spark.testing.nExecutorsPerHost", 4)
  val nCoresPerExecutor = conf.getInt("spark.testing.nCoresPerExecutor", 2)

  override val executorIdToExecutor: Map[String, ExecutorTaskStatus] = {
    (0 until nHosts).flatMap { hostIdx =>
      val hostName = "host-" + hostIdx
      (0 until nExecutorsPerHost).map { subIdx =>
        val executorId = (hostIdx * nExecutorsPerHost + subIdx).toString
        executorId ->
          ExecutorTaskStatus(host = hostName, executorId = executorId, nCoresPerExecutor)
      }
    }.toMap
  }

  override def defaultParallelism(): Int = nHosts * nExecutorsPerHost * nCoresPerExecutor
}

class MockRDDWithLocalityPrefs(
    sc: SparkContext,
    numPartitions: Int,
    shuffleDeps: Seq[ShuffleDependency[Int, Int, Nothing]],
    val preferredLoc: String) extends MockRDD(sc, numPartitions, shuffleDeps) {
  override def getPreferredLocations(split: Partition): Seq[String] = {
    Seq(preferredLoc)
  }
}
