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

package org.apache.spark.mllib.rdd

import org.scalatest.FunSuite

import org.apache.spark.mllib.util.LocalSparkContext
import org.apache.spark.mllib.rdd.RDDFunctions._

class RDDFunctionsSuite extends FunSuite with LocalSparkContext {

  test("sliding") {
    val data = 0 until 6
    for (numPartitions <- 1 to 8) {
      val rdd = sc.parallelize(data, numPartitions)
      for (windowSize <- 1 to 6) {
        val sliding = rdd.sliding(windowSize).collect().map(_.toList).toList
        val expected = data.sliding(windowSize).map(_.toList).toList
        assert(sliding === expected)
      }
      assert(rdd.sliding(7).collect().isEmpty,
        "Should return an empty RDD if the window size is greater than the number of items.")
    }
  }

  test("sliding with empty partitions") {
    val data = Seq(Seq(1, 2, 3), Seq.empty[Int], Seq(4), Seq.empty[Int], Seq(5, 6, 7))
    val rdd = sc.parallelize(data, data.length).flatMap(s => s)
    assert(rdd.partitions.size === data.length)
    val sliding = rdd.sliding(3)
    val expected = data.flatMap(x => x).sliding(3).toList
    assert(sliding.collect().toList === expected)
  }

  test("treeAggregate") {
    val rdd = sc.makeRDD(-1000 until 1000, 10)
    def seqOp = (c: Long, x: Int) => c + x
    def combOp = (c1: Long, c2: Long) => c1 + c2
    for (depth <- 1 until 10) {
      val sum = rdd.treeAggregate(0L)(seqOp, combOp, depth)
      assert(sum === -1000L)
    }
  }

  test("treeReduce") {
    val rdd = sc.makeRDD(-1000 until 1000, 10)
    for (depth <- 1 until 10) {
      val sum = rdd.treeReduce(_ + _, depth)
      assert(sum === -1000)
    }
  }
}
