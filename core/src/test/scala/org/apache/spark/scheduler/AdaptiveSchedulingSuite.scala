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

import org.apache.spark.rdd.{ShuffledRDDPartition, RDD, ShuffledRDD}
import org.apache.spark._

object AdaptiveSchedulingSuiteState {
  var tasksRun = 0

  def clear(): Unit = {
    tasksRun = 0
  }
}

/** A special ShuffledRDD where we can pass a ShuffleDependency object to use */
class CustomShuffledRDD[K, V, C](@transient dep: ShuffleDependency[K, V, C])
  extends RDD[(K, C)](dep.rdd.context, Seq(dep)) {

  override def compute(split: Partition, context: TaskContext): Iterator[(K, C)] = {
    val dep = dependencies.head.asInstanceOf[ShuffleDependency[K, V, C]]
    SparkEnv.get.shuffleManager.getReader(dep.shuffleHandle, split.index, split.index + 1, context)
      .read()
      .asInstanceOf[Iterator[(K, C)]]
  }

  override def getPartitions: Array[Partition] = {
    Array.tabulate[Partition](dep.partitioner.numPartitions)(i => new ShuffledRDDPartition(i))
  }
}

class AdaptiveSchedulingSuite extends SparkFunSuite with LocalSparkContext {
  test("simple use of submitMapStage") {
    try {
      sc = new SparkContext("local[1,2]", "test")
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
}
