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

  /**
   * This backend just always fails if the task is executed on a bad host, but otherwise succeeds
   * all tasks.
   */
  def badHostBackend(): Unit = {
    val task = backend.beginTask()
    val host = backend.executorIdToExecutor(task.executorId).host
    if (host == badHost) {
      backend.taskFailed(task, new RuntimeException("I'm a bad host!"))
    } else {
      backend.taskSuccess(task, 42)
    }
  }

  // Test demonstrating the issue -- without a config change, the scheduler keeps scheduling
  // according to locality preferences, and so the job fails
  testScheduler("If preferred node is bad, without blacklist job will fail") {
    val rdd = new MockRDDWithLocalityPrefs(sc, 10, Nil, badHost)
    withBackend(badHostBackend _) {
      val jobFuture = submit(rdd, (0 until 10).toArray)
      val duration = Duration(1, SECONDS)
      Await.ready(jobFuture, duration)
    }
    assert(results.isEmpty)
    assertDataStructuresEmpty(noFailure = false)
  }

  // even with the blacklist turned on, if maxTaskFailures is not more than the number
  // of executors on the bad node, then locality preferences will lead to us cycling through
  // the executors on the bad node, and still failing the job
  testScheduler(
    "With blacklist on, job will still fail if there are too many bad executors on bad host",
    extraConfs = Seq(
      // just set this to something much longer than the test duration
      ("spark.scheduler.executorTaskBlacklistTime", "10000000")
    )
  ) {
    val rdd = new MockRDDWithLocalityPrefs(sc, 10, Nil, badHost)
    withBackend(badHostBackend _) {
      val jobFuture = submit(rdd, (0 until 10).toArray)
      val duration = Duration(3, SECONDS)
      Await.ready(jobFuture, duration)
    }
    assert(results.isEmpty)
    assertDataStructuresEmpty(noFailure = false)
  }

  // Here we run with the blacklist on, and maxTaskFailures high enough that we'll eventually
  // schedule on a good node and succeed the job
  testScheduler(
    "Bad node with multiple executors, job will still succeed with the right confs",
    extraConfs = Seq(
      // just set this to something much longer than the test duration
      ("spark.scheduler.executorTaskBlacklistTime", "10000000"),
      // this has to be higher than the number of executors on the bad host
      ("spark.task.maxFailures", "5"),
      // just to avoid this test taking too long
      ("spark.locality.wait", "10ms")
    )
  ) {
    val rdd = new MockRDDWithLocalityPrefs(sc, 10, Nil, badHost)
    withBackend(badHostBackend _) {
      val jobFuture = submit(rdd, (0 until 10).toArray)
      val duration = Duration(1, SECONDS)
      Await.ready(jobFuture, duration)
    }
    assert(results === (0 until 10).map { _ -> 42 }.toMap)
    assertDataStructuresEmpty(noFailure = true)
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
