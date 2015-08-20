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

object AdaptiveSchedulingSuiteState {
  var tasksRun = 0

  def clear(): Unit = {
    tasksRun = 0
  }
}

class AdaptiveSchedulingSuite extends SparkFunSuite with LocalSparkContext {
  test("simple use of submitMapStage") {
    try {
      sc = new SparkContext("local", "test")
      val rdd = sc.parallelize(1 to 3, 3).map { x =>
        AdaptiveSchedulingSuiteState.tasksRun += 1
        (x, x)
      }
      val dep = new ShuffleDependency[Int, Int, Int](rdd, new HashPartitioner(2))
      val shuffled = new CustomShuffledRDD[Int, Int, Int](dep)
      sc.submitMapStage(dep).get()
      assert(AdaptiveSchedulingSuiteState.tasksRun == 3)
      assert(shuffled.collect().toSet == Set((1, 1), (2, 2), (3, 3)))
      assert(AdaptiveSchedulingSuiteState.tasksRun == 3)
    } finally {
      AdaptiveSchedulingSuiteState.clear()
    }
  }

  test("fetching multiple map output partitions per reduce") {
    sc = new SparkContext("local", "test")
    val rdd = sc.parallelize(0 to 2, 3).map(x => (x, x))
    val dep = new ShuffleDependency[Int, Int, Int](rdd, new HashPartitioner(3))
    val shuffled = new CustomShuffledRDD[Int, Int, Int](dep, Array(0, 2))
    assert(shuffled.partitions.length === 2)
    assert(shuffled.glom().map(_.toSet).collect().toSet == Set(Set((0, 0), (1, 1)), Set((2, 2))))
  }

  test("fetching all map output partitions in one reduce") {
    sc = new SparkContext("local", "test")
    val rdd = sc.parallelize(0 to 2, 3).map(x => (x, x))
    // Also create lots of hash partitions so that some of them are empty
    val dep = new ShuffleDependency[Int, Int, Int](rdd, new HashPartitioner(5))
    val shuffled = new CustomShuffledRDD[Int, Int, Int](dep, Array(0))
    assert(shuffled.partitions.length === 1)
    assert(shuffled.collect().toSet == Set((0, 0), (1, 1), (2, 2)))
  }

  test("more reduce tasks than map output partitions") {
    sc = new SparkContext("local", "test")
    val rdd = sc.parallelize(0 to 2, 3).map(x => (x, x))
    val dep = new ShuffleDependency[Int, Int, Int](rdd, new HashPartitioner(3))
    val shuffled = new CustomShuffledRDD[Int, Int, Int](dep, Array(0, 0, 0, 1, 1, 1, 2))
    assert(shuffled.partitions.length === 7)
    assert(shuffled.collect().toSet == Set((0, 0), (1, 1), (2, 2)))
  }
}
