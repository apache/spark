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

import org.apache.spark._
import org.apache.spark.internal.config
import org.apache.spark.internal.config.Tests._

class HealthTrackerIntegrationSuite extends SchedulerIntegrationSuite[MultiExecutorMockBackend]{

  val badHost = "host-0"

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
  testScheduler("If preferred node is bad, without excludeOnFailure job will fail",
    extraConfs = Seq(
      config.EXCLUDE_ON_FAILURE_ENABLED.key -> "false"
  )) {
    val rdd = new MockRDDWithLocalityPrefs(sc, 10, Nil, badHost)
    withBackend(badHostBackend _) {
      val jobFuture = submit(rdd, (0 until 10).toArray)
      awaitJobTermination(jobFuture, duration)
    }
    assertDataStructuresEmpty(noFailure = false)
  }

  testScheduler(
    "With default settings, job can succeed despite multiple bad executors on node",
    extraConfs = Seq(
      config.EXCLUDE_ON_FAILURE_ENABLED.key -> "true",
      config.TASK_MAX_FAILURES.key -> "4",
      TEST_N_HOSTS.key -> "2",
      TEST_N_EXECUTORS_HOST.key -> "5",
      TEST_N_CORES_EXECUTOR.key -> "10"
    )
  ) {
    // To reliably reproduce the failure that would occur without exludeOnFailure, we have to use 1
    // task.  That way, we ensure this 1 task gets rotated through enough bad executors on the host
    // to fail the taskSet, before we have a bunch of different tasks fail in the executors so we
    // exclude them.
    // But the point here is -- without excludeOnFailure, we would never schedule anything on the
    // good host-1 before we hit too many failures trying our preferred host-0.
    val rdd = new MockRDDWithLocalityPrefs(sc, 1, Nil, badHost)
    withBackend(badHostBackend _) {
      val jobFuture = submit(rdd, (0 until 1).toArray)
      awaitJobTermination(jobFuture, duration)
    }
    assertDataStructuresEmpty(noFailure = true)
  }

  // Here we run with the excludeOnFailure on, and the default config takes care of having this
  // robust to one bad node.
  testScheduler(
    "Bad node with multiple executors, job will still succeed with the right confs",
    extraConfs = Seq(
       config.EXCLUDE_ON_FAILURE_ENABLED.key -> "true",
      // just to avoid this test taking too long
      config.LOCALITY_WAIT.key -> "10ms"
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

  // Make sure that if we've failed on all executors, but haven't hit task.maxFailures yet, we try
  // to acquire a new executor and if we aren't able to get one, the job doesn't hang and we abort
  testScheduler(
    "SPARK-15865 Progress with fewer executors than maxTaskFailures",
    extraConfs = Seq(
      config.EXCLUDE_ON_FAILURE_ENABLED.key -> "true",
      TEST_N_HOSTS.key -> "2",
      TEST_N_EXECUTORS_HOST.key -> "1",
      TEST_N_CORES_EXECUTOR.key -> "1",
      config.UNSCHEDULABLE_TASKSET_TIMEOUT.key -> "0s"
    )
  ) {
    def runBackend(): Unit = {
      val (taskDescription, _) = backend.beginTask()
      backend.taskFailed(taskDescription, new RuntimeException("test task failure"))
    }
    withBackend(runBackend _) {
      val jobFuture = submit(new MockRDD(sc, 10, Nil, Nil), (0 until 10).toArray)
      awaitJobTermination(jobFuture, duration)
      val pattern = (
        s"""|Aborting TaskSet 0.0 because task .*
            |cannot run anywhere due to node and executor excludeOnFailure""".stripMargin).r
      assert(pattern.findFirstIn(failure.getMessage).isDefined,
        s"Couldn't find $pattern in ${failure.getMessage()}")
    }
    assertDataStructuresEmpty(noFailure = false)
  }
}

class MultiExecutorMockBackend(
    conf: SparkConf,
    taskScheduler: TaskSchedulerImpl) extends MockBackend(conf, taskScheduler) {

  val nHosts = conf.get(TEST_N_HOSTS)
  val nExecutorsPerHost = conf.get(TEST_N_EXECUTORS_HOST)
  val nCoresPerExecutor = conf.get(TEST_N_CORES_EXECUTOR)

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
    val preferredLoc: String) extends MockRDD(sc, numPartitions, shuffleDeps, Nil) {
  override def getPreferredLocations(split: Partition): Seq[String] = {
    Seq(preferredLoc)
  }
}
