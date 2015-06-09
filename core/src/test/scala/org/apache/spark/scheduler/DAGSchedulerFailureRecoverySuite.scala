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

import java.util.Date

import scala.collection.mutable.{ArrayBuffer, HashMap}

import org.apache.spark.shuffle.FetchFailedException
import org.apache.spark.storage.BlockManagerId
import org.apache.spark._

class DAGSchedulerFailureRecoverySuite extends SparkFunSuite with Logging {

  // TODO we should run this with a matrix of configurations: different shufflers,
  // external shuffle service, etc.  But that is really pushing the question of how to run
  // such a long test ...

  ignore("no concurrent retries for stage attempts (SPARK-7308)") {
    // see SPARK-7308 for a detailed description of the conditions this is trying to recreate.
    // note that this is somewhat convoluted for a test case, but isn't actually very unusual
    // under a real workload.  We only fail the first attempt of stage 2, but that
    // could be enough to cause havoc.

    (0 until 100).foreach { idx =>
      println(new Date() + "\ttrial " + idx)
      logInfo(new Date() + "\ttrial " + idx)

      val conf = new SparkConf().set("spark.executor.memory", "100m")
      val clusterSc = new SparkContext("local-cluster[5,4,100]", "test-cluster", conf)
      val bms = ArrayBuffer[BlockManagerId]()
      val stageFailureCount = HashMap[Int, Int]()
      clusterSc.addSparkListener(new SparkListener {
        override def onBlockManagerAdded(bmAdded: SparkListenerBlockManagerAdded): Unit = {
          bms += bmAdded.blockManagerId
        }

        override def onStageCompleted(stageCompleted: SparkListenerStageCompleted): Unit = {
          if (stageCompleted.stageInfo.failureReason.isDefined) {
            val stage = stageCompleted.stageInfo.stageId
            stageFailureCount(stage) = stageFailureCount.getOrElse(stage, 0) + 1
            val reason = stageCompleted.stageInfo.failureReason.get
            println("stage " + stage + " failed: " + stageFailureCount(stage))
          }
        }
      })
      try {
        val rawData = clusterSc.parallelize(1 to 1e6.toInt, 20).map { x => (x % 100) -> x }.cache()
        rawData.count()

        // choose any executor block manager for the fetch failures.  Just can't be driver
        // to avoid broadcast failures
        val someBlockManager = bms.filter{!_.isDriver}(0)

        val shuffled = rawData.groupByKey(100).mapPartitionsWithIndex { case (idx, itr) =>
          // we want one failure quickly, and more failures after stage 0 has finished its
          // second attempt
          val stageAttemptId = TaskContext.get().asInstanceOf[TaskContextImpl].stageAttemptId
          if (stageAttemptId == 0) {
            if (idx == 0) {
              throw new FetchFailedException(someBlockManager, 0, 0, idx,
                cause = new RuntimeException("simulated fetch failure"))
            } else if (idx > 0 && math.random < 0.2) {
              Thread.sleep(5000)
              throw new FetchFailedException(someBlockManager, 0, 0, idx,
                cause = new RuntimeException("simulated fetch failure"))
            } else {
              // want to make sure plenty of these finish after task 0 fails, and some even finish
              // after the previous stage is retried and this stage retry is started
              Thread.sleep((500 + math.random * 5000).toLong)
            }
          }
          itr.map { x => ((x._1 + 5) % 100) -> x._2 }
        }
        val data = shuffled.mapPartitions { itr => itr.flatMap(_._2) }.collect()
        val count = data.size
        assert(count === 1e6.toInt)
        assert(data.toSet === (1 to 1e6.toInt).toSet)

        assert(stageFailureCount.getOrElse(1, 0) === 0)
        assert(stageFailureCount.getOrElse(2, 0) == 1)
        assert(stageFailureCount.getOrElse(3, 0) == 0)
      } finally {
        clusterSc.stop()
      }
    }
  }



}
