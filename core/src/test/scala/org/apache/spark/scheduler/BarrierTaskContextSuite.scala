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

import scala.util.Random

import org.apache.spark._

class BarrierTaskContextSuite extends SparkFunSuite with LocalSparkContext {

  test("global sync by barrier() call") {
    val conf = new SparkConf()
      // Init local cluster here so each barrier task runs in a separated process, thus `barrier()`
      // call is actually useful.
      .setMaster("local-cluster[4, 1, 1024]")
      .setAppName("test-cluster")
    sc = new SparkContext(conf)
    val rdd = sc.makeRDD(1 to 10, 4)
    val rdd2 = rdd.barrier().mapPartitions { it =>
      val context = BarrierTaskContext.get()
      // Sleep for a random time before global sync.
      Thread.sleep(Random.nextInt(1000))
      context.barrier()
      Seq(System.currentTimeMillis()).iterator
    }

    val times = rdd2.collect()
    // All the tasks shall finish global sync within a short time slot.
    assert(times.max - times.min <= 1000)
  }

  test("support multiple barrier() call within a single task") {
    val conf = new SparkConf()
      .setMaster("local-cluster[4, 1, 1024]")
      .setAppName("test-cluster")
    sc = new SparkContext(conf)
    val rdd = sc.makeRDD(1 to 10, 4)
    val rdd2 = rdd.barrier().mapPartitions { it =>
      val context = BarrierTaskContext.get()
      // Sleep for a random time before global sync.
      Thread.sleep(Random.nextInt(1000))
      context.barrier()
      val time1 = System.currentTimeMillis()
      // Sleep for a random time between two global syncs.
      Thread.sleep(Random.nextInt(1000))
      context.barrier()
      val time2 = System.currentTimeMillis()
      Seq((time1, time2)).iterator
    }

    val times = rdd2.collect()
    // All the tasks shall finish the first round of global sync within a short time slot.
    val times1 = times.map(_._1)
    assert(times1.max - times1.min <= 1000)

    // All the tasks shall finish the second round of global sync within a short time slot.
    val times2 = times.map(_._2)
    assert(times2.max - times2.min <= 1000)
  }

  test("throw exception on barrier() call timeout") {
    val conf = new SparkConf()
      .set("spark.barrier.sync.timeout", "1")
      .set("spark.test.noStageRetry", "true")
      .setMaster("local-cluster[4, 1, 1024]")
      .setAppName("test-cluster")
    sc = new SparkContext(conf)
    val rdd = sc.makeRDD(1 to 10, 4)
    val rdd2 = rdd.barrier().mapPartitions { it =>
      val context = BarrierTaskContext.get()
      // Task 3 shall sleep 2000ms to ensure barrier() call timeout
      if (context.taskAttemptId == 3) {
        Thread.sleep(2000)
      }
      context.barrier()
      it
    }

    val error = intercept[SparkException] {
      rdd2.collect()
    }.getMessage
    assert(error.contains("The coordinator didn't get all barrier sync requests"))
    assert(error.contains("within 1 second(s)"))
  }

  test("throw exception if barrier() call doesn't happen on every task") {
    val conf = new SparkConf()
      .set("spark.barrier.sync.timeout", "1")
      .set("spark.test.noStageRetry", "true")
      .setMaster("local-cluster[4, 1, 1024]")
      .setAppName("test-cluster")
    sc = new SparkContext(conf)
    val rdd = sc.makeRDD(1 to 10, 4)
    val rdd2 = rdd.barrier().mapPartitions { it =>
      val context = BarrierTaskContext.get()
      if (context.taskAttemptId != 0) {
        context.barrier()
      }
      it
    }

    val error = intercept[SparkException] {
      rdd2.collect()
    }.getMessage
    assert(error.contains("The coordinator didn't get all barrier sync requests"))
    assert(error.contains("within 1 second(s)"))
  }

  test("throw exception if the number of barrier() calls are not the same on every task") {
    val conf = new SparkConf()
      .set("spark.barrier.sync.timeout", "1")
      .set("spark.test.noStageRetry", "true")
      .setMaster("local-cluster[4, 1, 1024]")
      .setAppName("test-cluster")
    sc = new SparkContext(conf)
    val rdd = sc.makeRDD(1 to 10, 4)
    val rdd2 = rdd.barrier().mapPartitions { it =>
      val context = BarrierTaskContext.get()
      try {
        if (context.taskAttemptId == 0) {
          // Due to some non-obvious reason, the code can trigger an Exception and skip the
          // following statements within the try ... catch block, including the first barrier()
          // call.
          throw new SparkException("test")
        }
        context.barrier()
      } catch {
        case e: Exception => // Do nothing
      }
      context.barrier()
      it
    }

    val error = intercept[SparkException] {
      rdd2.collect()
    }.getMessage
    assert(error.contains("The coordinator didn't get all barrier sync requests"))
    assert(error.contains("within 1 second(s)"))
  }

  // Disabled as it is flaky in GitHub Actions.
  ignore("SPARK-31485: barrier stage should fail if only partial tasks are launched") {
    val conf = new SparkConf()
      .setMaster("local-cluster[2, 1, 1024]")
      .setAppName("test-cluster")
      .set("spark.test.noStageRetry", "true")
    sc = new SparkContext(conf)
    TestUtils.waitUntilExecutorsUp(sc, 2, 6000)
    val id = sc.getExecutorIds().head
    val rdd0 = sc.parallelize(Seq(0, 1, 2, 3), 2)
    val dep = new OneToOneDependency[Int](rdd0)
    // set up a barrier stage with 2 tasks and both tasks prefer the same executor (only 1 core) for
    // scheduling. So, one of tasks won't be scheduled in one round of resource offer.
    val rdd = new MyRDD(sc, 2, List(dep), Seq(Seq(s"executor_h_$id"), Seq(s"executor_h_$id")))
    val errorMsg = intercept[SparkException] {
      rdd.barrier().mapPartitions { iter =>
        BarrierTaskContext.get().barrier()
        iter
      }.collect()
    }.getMessage
    assert(errorMsg.contains("Fail resource offers for barrier stage"))
  }
}
