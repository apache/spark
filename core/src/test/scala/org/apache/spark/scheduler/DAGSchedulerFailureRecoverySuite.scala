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

  test("no concurrent retries for stage attempts (SPARK-8103)") {
    // make sure that if we get fetch failures after the retry has started, we ignore them,
    // and so don't end up submitting multiple concurrent attempts for the same stage

    (0 until 20).foreach { idx =>
      logInfo(new Date() + "\ttrial " + idx)

      val conf = new SparkConf().set("spark.executor.memory", "100m")
      val clusterSc = new SparkContext("local-cluster[2,2,100]", "test-cluster", conf)
      val bms = ArrayBuffer[BlockManagerId]()
      val stageFailureCount = HashMap[Int, Int]()
      val stageSubmissionCount = HashMap[Int, Int]()
      clusterSc.addSparkListener(new SparkListener {
        override def onBlockManagerAdded(bmAdded: SparkListenerBlockManagerAdded): Unit = {
          bms += bmAdded.blockManagerId
        }

        override def onStageSubmitted(stageSubmited: SparkListenerStageSubmitted): Unit = {
          val stage = stageSubmited.stageInfo.stageId
          stageSubmissionCount(stage) = stageSubmissionCount.getOrElse(stage, 0) + 1
        }


        override def onStageCompleted(stageCompleted: SparkListenerStageCompleted): Unit = {
          if (stageCompleted.stageInfo.failureReason.isDefined) {
            val stage = stageCompleted.stageInfo.stageId
            stageFailureCount(stage) = stageFailureCount.getOrElse(stage, 0) + 1
          }
        }
      })
      try {
        val rawData = clusterSc.parallelize(1 to 1e6.toInt, 20).map { x => (x % 100) -> x }.cache()
        rawData.count()

        // choose any executor block manager for the fetch failures.  Just can't be driver
        // to avoid broadcast failures
        val someBlockManager = bms.filter{!_.isDriver}(0)

        val shuffled = rawData.groupByKey(20).mapPartitionsWithIndex { case (idx, itr) =>
          // we want one failure quickly, and more failures after stage 0 has finished its
          // second attempt
          val stageAttemptId = TaskContext.get().asInstanceOf[TaskContextImpl].stageAttemptId
          if (stageAttemptId == 0) {
            if (idx == 0) {
              throw new FetchFailedException(someBlockManager, 0, 0, idx,
                cause = new RuntimeException("simulated fetch failure"))
            } else if (idx == 1) {
              Thread.sleep(2000)
              throw new FetchFailedException(someBlockManager, 0, 0, idx,
                cause = new RuntimeException("simulated fetch failure"))
            }
          } else {
            // just to make sure the second attempt doesn't finish before we trigger more failures
            // from the first attempt
            Thread.sleep(2000)
          }
          itr.map { x => ((x._1 + 5) % 100) -> x._2 }
        }
        val data = shuffled.mapPartitions { itr =>
          itr.flatMap(_._2)
        }.cache().collect()
        val count = data.size
        assert(count === 1e6.toInt)
        assert(data.toSet === (1 to 1e6.toInt).toSet)

        assert(stageFailureCount.getOrElse(1, 0) === 0)
        assert(stageFailureCount.getOrElse(2, 0) === 1)
        assert(stageSubmissionCount.getOrElse(1, 0) <= 2)
        assert(stageSubmissionCount.getOrElse(2, 0) === 2)
      } finally {
        clusterSc.stop()
      }
    }
  }



}
