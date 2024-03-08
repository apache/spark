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

package org.apache.spark

import scala.concurrent.duration._

import org.apache.spark.TestUtils.createTempScriptWithExpectedOutput
import org.apache.spark.internal.config._
import org.apache.spark.rdd.{PartitionPruningRDD, RDD}
import org.apache.spark.resource.TestResourceIDs.{EXECUTOR_GPU_ID, TASK_GPU_ID, WORKER_GPU_ID}
import org.apache.spark.scheduler.BarrierJobAllocationFailed._
import org.apache.spark.scheduler.BarrierJobSlotsNumberCheckFailed
import org.apache.spark.util.ThreadUtils

/**
 * This test suite covers all the cases that shall fail fast on job submitted that contains one
 * of more barrier stages.
 */
class BarrierStageOnSubmittedSuite extends SparkFunSuite with LocalSparkContext {

  private def createSparkContext(conf: Option[SparkConf] = None): SparkContext = {
    new SparkContext(conf.getOrElse(
      new SparkConf()
        .setMaster("local[4]")
        .setAppName("test")))
  }

  private def testSubmitJob(
      sc: SparkContext,
      rdd: RDD[Int],
      partitions: Option[Seq[Int]] = None,
      message: String): Unit = {
    val futureAction = sc.submitJob(
      rdd,
      (iter: Iterator[Int]) => iter.toArray,
      partitions.getOrElse(rdd.partitions.indices),
      { case (_, _) => return }: (Int, Array[Int]) => Unit,
      { return }
    )

    val error = intercept[SparkException] {
      ThreadUtils.awaitResult(futureAction, 5.seconds)
    }.getCause.getMessage
    assert(error.contains(message))
  }

  test("submit a barrier ResultStage that contains PartitionPruningRDD") {
    sc = createSparkContext()
    val prunedRdd = new PartitionPruningRDD(sc.parallelize(1 to 10, 4), index => index > 1)
    val rdd = prunedRdd
      .barrier()
      .mapPartitions(iter => iter)
    testSubmitJob(sc, rdd,
      message = ERROR_MESSAGE_RUN_BARRIER_WITH_UNSUPPORTED_RDD_CHAIN_PATTERN)
  }

  test("submit a barrier ShuffleMapStage that contains PartitionPruningRDD") {
    sc = createSparkContext()
    val prunedRdd = new PartitionPruningRDD(sc.parallelize(1 to 10, 4), index => index > 1)
    val rdd = prunedRdd
      .barrier()
      .mapPartitions(iter => iter)
      .repartition(2)
      .map(x => x + 1)
    testSubmitJob(sc, rdd,
      message = ERROR_MESSAGE_RUN_BARRIER_WITH_UNSUPPORTED_RDD_CHAIN_PATTERN)
  }

  test("submit a barrier stage that doesn't contain PartitionPruningRDD") {
    sc = createSparkContext()
    val prunedRdd = new PartitionPruningRDD(sc.parallelize(1 to 10, 4), index => index > 1)
    val rdd = prunedRdd
      .repartition(2)
      .barrier()
      .mapPartitions(iter => iter)
    // Should be able to submit job and run successfully.
    val result = rdd.collect().sorted
    assert(result === Seq(6, 7, 8, 9, 10))
  }

  test("submit a barrier stage with partial partitions") {
    sc = createSparkContext()
    val rdd = sc.parallelize(1 to 10, 4)
      .barrier()
      .mapPartitions(iter => iter)
    testSubmitJob(sc, rdd, Some(Seq(1, 3)),
      message = ERROR_MESSAGE_RUN_BARRIER_WITH_UNSUPPORTED_RDD_CHAIN_PATTERN)
  }

  test("submit a barrier stage with union()") {
    sc = createSparkContext()
    val rdd1 = sc.parallelize(1 to 10, 2)
      .barrier()
      .mapPartitions(iter => iter)
    val rdd2 = sc.parallelize(1 to 20, 2)
    val rdd3 = rdd1
      .union(rdd2)
      .map(x => x * 2)
    // Fail the job on submit because the barrier RDD (rdd1) may be not assigned Task 0.
    testSubmitJob(sc, rdd3,
      message = ERROR_MESSAGE_RUN_BARRIER_WITH_UNSUPPORTED_RDD_CHAIN_PATTERN)
  }

  test("submit a barrier stage with coalesce()") {
    sc = createSparkContext()
    val rdd = sc.parallelize(1 to 10, 4)
      .barrier()
      .mapPartitions(iter => iter)
      .coalesce(1)
    // Fail the job on submit because the barrier RDD requires to run on 4 tasks, but the stage
    // only launches 1 task.
    testSubmitJob(sc, rdd,
      message = ERROR_MESSAGE_RUN_BARRIER_WITH_UNSUPPORTED_RDD_CHAIN_PATTERN)
  }

  test("submit a barrier stage that contains an RDD that depends on multiple barrier RDDs") {
    sc = createSparkContext()
    val rdd1 = sc.parallelize(1 to 10, 4)
      .barrier()
      .mapPartitions(iter => iter)
    val rdd2 = sc.parallelize(11 to 20, 4)
      .barrier()
      .mapPartitions(iter => iter)
    val rdd3 = rdd1
      .zip(rdd2)
      .map(x => x._1 + x._2)
    testSubmitJob(sc, rdd3,
      message = ERROR_MESSAGE_RUN_BARRIER_WITH_UNSUPPORTED_RDD_CHAIN_PATTERN)
  }

  test("submit a barrier stage with zip()") {
    sc = createSparkContext()
    val rdd1 = sc.parallelize(1 to 10, 4)
      .barrier()
      .mapPartitions(iter => iter)
    val rdd2 = sc.parallelize(11 to 20, 4)
    val rdd3 = rdd1
      .zip(rdd2)
      .map(x => x._1 + x._2)
    // Should be able to submit job and run successfully.
    val result = rdd3.collect().sorted
    assert(result === Seq(12, 14, 16, 18, 20, 22, 24, 26, 28, 30))
  }

  test("submit a barrier ResultStage with dynamic resource allocation enabled") {
    val conf = new SparkConf()
      .set(DYN_ALLOCATION_ENABLED, true)
      .set(DYN_ALLOCATION_TESTING, true)
      .setMaster("local[4]")
      .setAppName("test")
    sc = createSparkContext(Some(conf))

    val rdd = sc.parallelize(1 to 10, 4)
      .barrier()
      .mapPartitions(iter => iter)
    testSubmitJob(sc, rdd,
      message = ERROR_MESSAGE_RUN_BARRIER_WITH_DYN_ALLOCATION)
  }

  test("submit a barrier ShuffleMapStage with dynamic resource allocation enabled") {
    val conf = new SparkConf()
      .set(DYN_ALLOCATION_ENABLED, true)
      .set(DYN_ALLOCATION_TESTING, true)
      .setMaster("local[4]")
      .setAppName("test")
    sc = createSparkContext(Some(conf))

    val rdd = sc.parallelize(1 to 10, 4)
      .barrier()
      .mapPartitions(iter => iter)
      .repartition(2)
      .map(x => x + 1)
    testSubmitJob(sc, rdd,
      message = ERROR_MESSAGE_RUN_BARRIER_WITH_DYN_ALLOCATION)
  }

  test("submit a barrier ResultStage that requires more slots than current total under local " +
      "mode") {
    val conf = new SparkConf()
      // Shorten the time interval between two failed checks to make the test fail faster.
      .set(BARRIER_MAX_CONCURRENT_TASKS_CHECK_INTERVAL.key, "1s")
      // Reduce max check failures allowed to make the test fail faster.
      .set(BARRIER_MAX_CONCURRENT_TASKS_CHECK_MAX_FAILURES, 3)
      .setMaster("local[4]")
      .setAppName("test")
    sc = createSparkContext(Some(conf))
    val rdd = sc.parallelize(1 to 10, 5)
      .barrier()
      .mapPartitions(iter => iter)
    testSubmitJob(sc, rdd,
      message = ERROR_MESSAGE_BARRIER_REQUIRE_MORE_SLOTS_THAN_CURRENT_TOTAL_NUMBER)
  }

  test("submit a barrier ShuffleMapStage that requires more slots than current total under " +
    "local mode") {
    val conf = new SparkConf()
      // Shorten the time interval between two failed checks to make the test fail faster.
      .set(BARRIER_MAX_CONCURRENT_TASKS_CHECK_INTERVAL.key, "1s")
      // Reduce max check failures allowed to make the test fail faster.
      .set(BARRIER_MAX_CONCURRENT_TASKS_CHECK_MAX_FAILURES, 3)
      .setMaster("local[4]")
      .setAppName("test")
    sc = createSparkContext(Some(conf))
    val rdd = sc.parallelize(1 to 10, 5)
      .barrier()
      .mapPartitions(iter => iter)
      .repartition(2)
      .map(x => x + 1)
    testSubmitJob(sc, rdd,
      message = ERROR_MESSAGE_BARRIER_REQUIRE_MORE_SLOTS_THAN_CURRENT_TOTAL_NUMBER)
  }

  test("submit a barrier ResultStage that requires more slots than current total under " +
    "local-cluster mode") {
    val conf = new SparkConf()
      .set(CPUS_PER_TASK, 2)
      // Shorten the time interval between two failed checks to make the test fail faster.
      .set(BARRIER_MAX_CONCURRENT_TASKS_CHECK_INTERVAL.key, "1s")
      // Reduce max check failures allowed to make the test fail faster.
      .set(BARRIER_MAX_CONCURRENT_TASKS_CHECK_MAX_FAILURES, 3)
      .setMaster("local-cluster[4, 3, 1024]")
      .setAppName("test")
    sc = createSparkContext(Some(conf))
    val rdd = sc.parallelize(1 to 10, 5)
      .barrier()
      .mapPartitions(iter => iter)
    testSubmitJob(sc, rdd,
      message = ERROR_MESSAGE_BARRIER_REQUIRE_MORE_SLOTS_THAN_CURRENT_TOTAL_NUMBER)
  }

  test("submit a barrier ShuffleMapStage that requires more slots than current total under " +
    "local-cluster mode") {
    val conf = new SparkConf()
      .set(CPUS_PER_TASK, 2)
      // Shorten the time interval between two failed checks to make the test fail faster.
      .set(BARRIER_MAX_CONCURRENT_TASKS_CHECK_INTERVAL.key, "1s")
      // Reduce max check failures allowed to make the test fail faster.
      .set(BARRIER_MAX_CONCURRENT_TASKS_CHECK_MAX_FAILURES, 3)
      .setMaster("local-cluster[4, 3, 1024]")
      .setAppName("test")
    sc = createSparkContext(Some(conf))
    val rdd = sc.parallelize(1 to 10, 5)
      .barrier()
      .mapPartitions(iter => iter)
      .repartition(2)
      .map(x => x + 1)
    testSubmitJob(sc, rdd,
      message = ERROR_MESSAGE_BARRIER_REQUIRE_MORE_SLOTS_THAN_CURRENT_TOTAL_NUMBER)
  }

  test("SPARK-32518: CoarseGrainedSchedulerBackend.maxNumConcurrentTasks should " +
    "consider all kinds of resources for the barrier stage") {
    withTempDir { dir =>
      val discoveryScript = createTempScriptWithExpectedOutput(
        dir, "gpuDiscoveryScript", """{"name": "gpu","addresses":["0"]}""")

      val conf = new SparkConf()
        // Setup a local cluster which would only has one executor with 2 CPUs and 1 GPU.
        .setMaster("local-cluster[1, 2, 1024]")
        .setAppName("test-cluster")
        .set(WORKER_GPU_ID.amountConf, "1")
        .set(WORKER_GPU_ID.discoveryScriptConf, discoveryScript)
        .set(EXECUTOR_GPU_ID.amountConf, "1")
        .set(TASK_GPU_ID.amountConf, "1")
        // disable barrier stage retry to fail the application as soon as possible
        .set(BARRIER_MAX_CONCURRENT_TASKS_CHECK_MAX_FAILURES, 1)
      sc = new SparkContext(conf)
      TestUtils.waitUntilExecutorsUp(sc, 1, 60000)

      val exception = intercept[BarrierJobSlotsNumberCheckFailed] {
        // Setup a barrier stage which contains 2 tasks and each task requires 1 CPU and 1 GPU.
        // Therefore, the total resources requirement (2 CPUs and 2 GPUs) of this barrier stage
        // can not be satisfied since the cluster only has 2 CPUs and 1 GPU in total.
        sc.parallelize(Range(1, 10), 2)
          .barrier()
          .mapPartitions { iter => iter }
          .collect()
      }
      assert(exception.getMessage.contains("[SPARK-24819]: Barrier execution " +
        "mode does not allow run a barrier stage that requires more slots"))
    }
  }
}
