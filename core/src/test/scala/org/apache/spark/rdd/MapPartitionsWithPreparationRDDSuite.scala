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

package org.apache.spark.rdd

import org.apache.spark._
import org.apache.spark.shuffle.ShuffleMemoryManager

class MapPartitionsWithPreparationRDDSuite extends SparkFunSuite with LocalSparkContext {

  test("prepare called before parent partition is computed") {
    val conf = new SparkConf().setMaster("local").setAppName("testing")
    val maxMemory = ShuffleMemoryManager.getMaxMemory(conf)
    sc = new SparkContext(conf)

    // Make sure we can in fact acquire all the memory once, but not twice.
    val shuffleMemoryManager = sc.env.shuffleMemoryManager
    assert(shuffleMemoryManager.tryToAcquire(maxMemory) > 0)
    assert(shuffleMemoryManager.tryToAcquire(maxMemory) === 0)
    shuffleMemoryManager.release(maxMemory)

    // Have the parent partition try to acquire all the shuffle memory
    // Return the acquire result so we can verify it later
    val parent = sc.parallelize(1 to 100, 1).mapPartitions { iter =>
      Seq(SparkEnv.get.shuffleMemoryManager.tryToAcquire(maxMemory)).toIterator
    }

    // ... and try to do the same ourselves
    val preparePartition = () => {
      SparkEnv.get.shuffleMemoryManager.tryToAcquire(maxMemory)
    }

    // Return the acquire results as a tuple (parent result, our result)
    val executePartition = (
        taskContext: TaskContext,
        partitionIndex: Int,
        ourAcquireResult: Long,
        parentIterator: Iterator[Long]) => {
      val parentAcquireResult = parentIterator.toSeq.head
      Seq((parentAcquireResult, ourAcquireResult)).toIterator
    }

    // Verify that that we are victorious!
    // More specifically: we acquired memory and our parent didn't,
    // meaning prepared is run before parent partition is computed.
    val result = {
      new MapPartitionsWithPreparationRDD[(Long, Long), Long, Long](
        parent, preparePartition, executePartition).collect()
    }
    assert(result.size === 1)
    val (parentAcquireResult, ourAcquireResult) = result.head
    assert(ourAcquireResult > 0)
    assert(parentAcquireResult === 0)
  }

}
