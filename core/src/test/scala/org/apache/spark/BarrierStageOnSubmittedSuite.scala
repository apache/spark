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

import org.apache.spark.rdd.RDD
import org.apache.spark.util.ThreadUtils

/**
 * This test suite covers all the cases that shall fail fast on job submitted that contains one
 * of more barrier stages.
 */
class BarrierStageOnSubmittedSuite extends SparkFunSuite with LocalSparkContext {

  private def testSubmitJob(
      sc: SparkContext,
      rdd: RDD[Int],
      partitions: Seq[Int]): Unit = {
    sc.submitJob(
      rdd,
      (iter: Iterator[Int]) => iter.toArray,
      partitions,
      { case (_, _) => return }: (Int, Array[Int]) => Unit,
      { return }
    )
  }

  test("submit job on a barrier RDD with partial partitions") {
    val conf = new SparkConf()
      .setMaster("local[4]")
      .setAppName("test")
    sc = new SparkContext(conf)

    val rdd = sc.parallelize(1 to 10, 4)
      .barrier()
      .mapPartitions((iter, context) => iter)
    val error = intercept[SparkException] {
      testSubmitJob(sc, rdd, Seq(1, 3))
    }.getMessage
    assert(error.contains("Don't support run a barrier stage on partial partitions"))
  }
}
