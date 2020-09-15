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

import java.util.Properties

import scala.collection.mutable.Map

import org.apache.spark._
import org.apache.spark.internal.config
import org.apache.spark.scheduler.DAGSchedulerTestBase.makeBlockManagerId

class IndeterminateStageSuite extends DAGSchedulerTestBase {

  test("SPARK-25341: abort stage while using old fetch protocol") {
    conf.set(config.SHUFFLE_USE_OLD_FETCH_PROTOCOL.key, "true")
    // Construct the scenario of indeterminate stage fetch failed.
    constructIndeterminateStageFetchFailed()
    // The job should fail because Spark can't rollback the shuffle map stage while
    // using old protocol.
    assert(failure != null && failure.getMessage.contains(
      "Spark can only do this while using the new shuffle block fetching protocol"))
  }

  test("SPARK-25341: continuous indeterminate stage roll back") {
    // shuffleMapRdd1/2/3 are all indeterminate.
    val shuffleMapRdd1 = new MyRDD(sc, 2, Nil, indeterminate = true)
    val shuffleDep1 = new ShuffleDependency(shuffleMapRdd1, new HashPartitioner(2))
    val shuffleId1 = shuffleDep1.shuffleId

    val shuffleMapRdd2 = new MyRDD(
      sc, 2, List(shuffleDep1), tracker = mapOutputTracker, indeterminate = true)
    val shuffleDep2 = new ShuffleDependency(shuffleMapRdd2, new HashPartitioner(2))
    val shuffleId2 = shuffleDep2.shuffleId

    val shuffleMapRdd3 = new MyRDD(
      sc, 2, List(shuffleDep2), tracker = mapOutputTracker, indeterminate = true)
    val shuffleDep3 = new ShuffleDependency(shuffleMapRdd3, new HashPartitioner(2))
    val shuffleId3 = shuffleDep3.shuffleId
    val finalRdd = new MyRDD(sc, 2, List(shuffleDep3), tracker = mapOutputTracker)

    submit(finalRdd, Array(0, 1), properties = new Properties())

    // Finish the first 2 shuffle map stages.
    completeShuffleMapStageSuccessfully(0, 0, 2)
    assert(mapOutputTracker.findMissingPartitions(shuffleId1) === Some(Seq.empty))

    completeShuffleMapStageSuccessfully(1, 0, 2, Seq("hostB", "hostD"))
    assert(mapOutputTracker.findMissingPartitions(shuffleId2) === Some(Seq.empty))

    // Executor lost on hostB, both of stage 0 and 1 should be reran.
    runEvent(makeCompletionEvent(
      taskSets(2).tasks(0),
      FetchFailed(makeBlockManagerId("hostB"), shuffleId2, 0L, 0, 0, "ignored"),
      null))
    mapOutputTracker.removeOutputsOnHost("hostB")

    assert(scheduler.failedStages.toSeq.map(_.id) == Seq(1, 2))
    scheduler.resubmitFailedStages()

    def checkAndCompleteRetryStage(
        taskSetIndex: Int,
        stageId: Int,
        shuffleId: Int): Unit = {
      assert(taskSets(taskSetIndex).stageId == stageId)
      assert(taskSets(taskSetIndex).stageAttemptId == 1)
      assert(taskSets(taskSetIndex).tasks.length == 2)
      completeShuffleMapStageSuccessfully(stageId, 1, 2)
      assert(mapOutputTracker.findMissingPartitions(shuffleId) === Some(Seq.empty))
    }

    // Check all indeterminate stage roll back.
    checkAndCompleteRetryStage(3, 0, shuffleId1)
    checkAndCompleteRetryStage(4, 1, shuffleId2)
    checkAndCompleteRetryStage(5, 2, shuffleId3)

    // Result stage success, all job ended.
    complete(taskSets(6), Seq((Success, 11), (Success, 12)))
    assert(results === Map(0 -> 11, 1 -> 12))
    results.clear()
    assertDataStructuresEmpty()
  }

}
