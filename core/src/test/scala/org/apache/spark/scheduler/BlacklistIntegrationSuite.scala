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

import scala.concurrent.duration._

import org.apache.spark._
import org.apache.spark.internal.config

class BlacklistIntegrationSuite extends SchedulerIntegrationSuite[MultiExecutorMockBackend]{

  val badHost = "host-0"
  val duration = Duration(10, MINUTES)

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

  // Here we run with the blacklist on, and the default config takes care of having this
  // robust to one bad node.
  testScheduler(
    "Bad node with multiple executors, job will still succeed with the right confs",
    extraConfs = Seq(
       config.BLACKLIST_ENABLED.key -> "true",
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
