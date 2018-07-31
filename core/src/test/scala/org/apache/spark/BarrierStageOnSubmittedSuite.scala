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
import scala.language.postfixOps

import org.apache.spark.rdd.{PartitionPruningRDD, RDD}
import org.apache.spark.util.ThreadUtils

/**
 * This test suite covers all the cases that shall fail fast on job submitted that contains one
 * of more barrier stages.
 */
class BarrierStageOnSubmittedSuite extends SparkFunSuite with LocalSparkContext {

  private def testSubmitJob(sc: SparkContext, rdd: RDD[Int], message: String): Unit = {
    val futureAction = sc.submitJob(
      rdd,
      (iter: Iterator[Int]) => iter.toArray,
      0 until rdd.partitions.length,
      { case (_, _) => return }: (Int, Array[Int]) => Unit,
      { return }
    )

    val error = intercept[SparkException] {
      ThreadUtils.awaitResult(futureAction, 1 seconds)
    }.getCause.getMessage
    assert(error.contains(message))
  }

  test("submit a barrier ResultStage that contains PartitionPruningRDD") {
    val conf = new SparkConf()
      .setMaster("local[4]")
      .setAppName("test")
    sc = new SparkContext(conf)

    val prunedRdd = new PartitionPruningRDD(sc.parallelize(1 to 10, 4), index => index > 1)
    val rdd = prunedRdd
      .barrier()
      .mapPartitions((iter, context) => iter)
    testSubmitJob(sc, rdd,
      "Don't support run a barrier stage that contains PartitionPruningRDD")
  }

  test("submit a barrier ShuffleMapStage that contains PartitionPruningRDD") {
    val conf = new SparkConf()
      .setMaster("local[4]")
      .setAppName("test")
    sc = new SparkContext(conf)

    val prunedRdd = new PartitionPruningRDD(sc.parallelize(1 to 10, 4), index => index > 1)
    val rdd = prunedRdd
      .barrier()
      .mapPartitions((iter, context) => iter)
      .repartition(2)
      .map(x => x + 1)
    testSubmitJob(sc, rdd,
      "Don't support run a barrier stage that contains PartitionPruningRDD")
  }

  test("submit a barrier stage that doesn't contain PartitionPruningRDD") {
    val conf = new SparkConf()
      .setMaster("local[4]")
      .setAppName("test")
    sc = new SparkContext(conf)

    val prunedRdd = new PartitionPruningRDD(sc.parallelize(1 to 10, 4), index => index > 1)
    val rdd = prunedRdd
      .repartition(2)
      .barrier()
      .mapPartitions((iter, context) => iter)

    // Should be able to submit job and run successfully.
    val result = rdd.collect().sorted
    assert(result === Seq(6, 7, 8, 9, 10))
  }
}
