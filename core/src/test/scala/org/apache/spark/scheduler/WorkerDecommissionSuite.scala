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

import scala.concurrent.TimeoutException
import scala.concurrent.duration._

import org.apache.spark.{LocalSparkContext, SparkConf, SparkContext, SparkException, SparkFunSuite}
import org.apache.spark.internal.config
import org.apache.spark.scheduler.cluster.StandaloneSchedulerBackend
import org.apache.spark.util.{RpcUtils, SerializableBuffer, ThreadUtils}

class WorkerDecommissionSuite extends SparkFunSuite with LocalSparkContext {


  override def beforeEach(): Unit = {
    val conf = new SparkConf().setAppName("test").setMaster("local")
      .set(config.WORKER_DECOMMISSION_ENABLED.key, "true")

    sc = new SparkContext("local-cluster[2, 1, 1024]", "test", conf)
  }

  test("verify task with no decommissioning works as expected") {
    val input = sc.parallelize(1 to 10)
    input.count()
    val sleepyRdd = input.mapPartitions{ x =>
      Thread.sleep(100)
      x
    }
    assert(sleepyRdd.count() === 10)
  }

  test("verify a task with all workers decommissioned succeeds") {
    val input = sc.parallelize(1 to 10)
    // Do a count to wait for the executors to be registered.
    input.count()
    val sleepyRdd = input.mapPartitions{ x =>
      Thread.sleep(100)
      x
    }
    // Start the task
    val asyncCount = sleepyRdd.countAsync()
    Thread.sleep(10)
    // Decommission all the executors, this should not halt the current task.
    // The master passing message is tested with
    val sched = sc.schedulerBackend.asInstanceOf[StandaloneSchedulerBackend]
    val execs = sched.getExecutorIds()
    println("execs are " + execs)
    execs.foreach(execId => sched.decommissionExecutor(execId))
    assert(asyncCount.get() === 10)
    // Try and launch task after decommissioning, this should fail
    println("post decom execs are " + sched.getExecutorIds())
    val postDecommissioned = input.map(x => x)
    val postDecomAsyncCount = postDecommissioned.countAsync()
    val thrown = intercept[java.util.concurrent.TimeoutException]{
      val result = ThreadUtils.awaitResult(postDecomAsyncCount, 1.seconds)
    }
    assert(postDecomAsyncCount.isCompleted === false,
      "After exec decomission new task could not launch")
  }
}
