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

import org.scalatest.concurrent.Eventually.{eventually, interval, timeout}

import org.apache.spark.{LocalSparkContext, SparkContext, SparkFunSuite, TestUtils}
import org.apache.spark.LocalSparkContext.withSpark
import org.apache.spark.internal.config.{DECOMMISSION_ENABLED, DYN_ALLOCATION_ENABLED, DYN_ALLOCATION_EXECUTOR_IDLE_TIMEOUT, DYN_ALLOCATION_INITIAL_EXECUTORS, DYN_ALLOCATION_SHUFFLE_TRACKING_ENABLED}
import org.apache.spark.launcher.SparkLauncher.{EXECUTOR_MEMORY, SPARK_MASTER}
import org.apache.spark.scheduler.cluster.StandaloneSchedulerBackend

/** This test suite aims to test worker decommission with various configurations. */
class WorkerDecommissionExtendedSuite extends SparkFunSuite with LocalSparkContext {
  private val conf = new org.apache.spark.SparkConf()
    .setAppName(getClass.getName)
    .set(SPARK_MASTER, "local-cluster[3,1,384]")
    .set(EXECUTOR_MEMORY, "384m")
    .set(DYN_ALLOCATION_ENABLED, true)
    .set(DYN_ALLOCATION_SHUFFLE_TRACKING_ENABLED, true)
    .set(DYN_ALLOCATION_INITIAL_EXECUTORS, 3)
    .set(DECOMMISSION_ENABLED, true)

  test("Worker decommission and executor idle timeout") {
    sc = new SparkContext(conf.set(DYN_ALLOCATION_EXECUTOR_IDLE_TIMEOUT.key, "10s"))
    withSpark(sc) { sc =>
      TestUtils.waitUntilExecutorsUp(sc, 3, 80000)
      val rdd1 = sc.parallelize(1 to 10, 2)
      val rdd2 = rdd1.map(x => (1, x))
      val rdd3 = rdd2.reduceByKey(_ + _)
      val rdd4 = rdd3.sortByKey()
      assert(rdd4.count() === 1)
      eventually(timeout(20.seconds), interval(1.seconds)) {
        assert(sc.getExecutorIds().length < 5)
      }
    }
  }

  test("Decommission 2 executors from 3 executors in total") {
    sc = new SparkContext(conf)
    withSpark(sc) { sc =>
      TestUtils.waitUntilExecutorsUp(sc, 3, 80000)
      val rdd1 = sc.parallelize(1 to 100000, 200)
      val rdd2 = rdd1.map(x => (x % 100, x))
      val rdd3 = rdd2.reduceByKey(_ + _)
      assert(rdd3.count() === 100)

      val sched = sc.schedulerBackend.asInstanceOf[StandaloneSchedulerBackend]
      sc.getExecutorIds().tail.foreach { id =>
        sched.decommissionExecutor(id, ExecutorDecommissionInfo("", None),
          adjustTargetNumExecutors = false)
        assert(rdd3.sortByKey().collect().length === 100)
      }
    }
  }
}
