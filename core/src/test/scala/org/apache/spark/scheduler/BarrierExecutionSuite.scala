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

class BarrierExecutionSuite extends DAGSchedulerTestHelper {

  test("Retry all the tasks on a resubmitted attempt of a barrier stage caused by FetchFailure") {
    val shuffleMapRdd = new MyRDD(sc, 2, Nil).barrier().mapPartitions(iter => iter)
    val shuffleDep = new ShuffleDependency(shuffleMapRdd, new HashPartitioner(2))
    val shuffleId = shuffleDep.shuffleId
    val reduceRdd = new MyRDD(sc, 2, List(shuffleDep), tracker = mapOutputTracker)
    submit(reduceRdd, Array(0, 1))
    completeShuffleMapStageSuccessfully(0, 0, reduceRdd.partitions.length)
    assert(mapOutputTracker.findMissingPartitions(shuffleId) === Some(Seq.empty))

    // The first result task fails, with a fetch failure for the output from the first mapper.
    runEvent(makeCompletionEvent(
      taskSets(1).tasks(0),
      FetchFailed(
        DAGSchedulerTestHelper.makeBlockManagerId("hostA"), shuffleId, 0L, 0, 0, "ignored"),
      null))
    assert(mapOutputTracker.findMissingPartitions(shuffleId) === Some(Seq(0, 1)))

    scheduler.resubmitFailedStages()
    // Complete the map stage.
    completeShuffleMapStageSuccessfully(0, 1, numShufflePartitions = 2)

    // Complete the result stage.
    completeNextResultStageWithSuccess(1, 1)

    sc.listenerBus.waitUntilEmpty()
    assertDataStructuresEmpty()
  }

  test("Retry all the tasks on a resubmitted attempt of a barrier stage caused by TaskKilled") {
    val shuffleMapRdd = new MyRDD(sc, 2, Nil).barrier().mapPartitions(iter => iter)
    val shuffleDep = new ShuffleDependency(shuffleMapRdd, new HashPartitioner(2))
    val shuffleId = shuffleDep.shuffleId
    val reduceRdd = new MyRDD(sc, 2, List(shuffleDep), tracker = mapOutputTracker)
    submit(reduceRdd, Array(0, 1))
    complete(taskSets(0), Seq(
      (Success, DAGSchedulerTestHelper.makeMapStatus("hostA", reduceRdd.partitions.length))))
    assert(mapOutputTracker.findMissingPartitions(shuffleId) === Some(Seq(1)))

    // The second map task fails with TaskKilled.
    runEvent(makeCompletionEvent(
      taskSets(0).tasks(1),
      TaskKilled("test"),
      null))
    assert(sparkListener.failedStages === Seq(0))
    assert(mapOutputTracker.findMissingPartitions(shuffleId) === Some(Seq(0, 1)))

    scheduler.resubmitFailedStages()
    // Complete the map stage.
    completeShuffleMapStageSuccessfully(0, 1, numShufflePartitions = 2)

    // Complete the result stage.
    completeNextResultStageWithSuccess(1, 0)

    sc.listenerBus.waitUntilEmpty()
    assertDataStructuresEmpty()
  }

  test("Fail the job if a barrier ResultTask failed") {
    val shuffleMapRdd = new MyRDD(sc, 2, Nil)
    val shuffleDep = new ShuffleDependency(shuffleMapRdd, new HashPartitioner(2))
    val shuffleId = shuffleDep.shuffleId
    val reduceRdd = new MyRDD(sc, 2, List(shuffleDep), tracker = mapOutputTracker)
      .barrier()
      .mapPartitions(iter => iter)
    submit(reduceRdd, Array(0, 1))

    // Complete the map stage.
    completeShuffleMapStageSuccessfully(0, 0, 2, hostNames = Seq("hostA", "hostA"))
    assert(mapOutputTracker.findMissingPartitions(shuffleId) === Some(Seq.empty))

    // The first ResultTask fails
    runEvent(makeCompletionEvent(
      taskSets(1).tasks(0),
      TaskKilled("test"),
      null))

    // Assert the stage has been cancelled.
    sc.listenerBus.waitUntilEmpty()
    assert(failure.getMessage.startsWith("Job aborted due to stage failure: Could not recover " +
      "from a failed barrier ResultStage."))
  }

  test("Barrier task failures from the same stage attempt don't trigger multiple stage retries") {
    val shuffleMapRdd = new MyRDD(sc, 2, Nil).barrier().mapPartitions(iter => iter)
    val shuffleDep = new ShuffleDependency(shuffleMapRdd, new HashPartitioner(2))
    val reduceRdd = new MyRDD(sc, 2, List(shuffleDep), tracker = mapOutputTracker)
    submit(reduceRdd, Array(0, 1))

    val mapStageId = 0

    def countSubmittedMapStageAttempts(): Int = {
      sparkListener.submittedStageInfos.count(_.stageId == mapStageId)
    }

    // The map stage should have been submitted.
    assert(countSubmittedMapStageAttempts() === 1)

    // The first map task fails with TaskKilled.
    runEvent(makeCompletionEvent(
      taskSets(0).tasks(0),
      TaskKilled("test"),
      null))
    assert(sparkListener.failedStages === Seq(0))

    // The second map task fails with TaskKilled.
    runEvent(makeCompletionEvent(
      taskSets(0).tasks(1),
      TaskKilled("test"),
      null))

    // Trigger resubmission of the failed map stage.
    runEvent(ResubmitFailedStages)

    // Another attempt for the map stage should have been submitted, resulting in 2 total attempts.
    assert(countSubmittedMapStageAttempts() === 2)
  }

  test("Barrier task failures from a previous stage attempt don't trigger stage retry") {
    val shuffleMapRdd = new MyRDD(sc, 2, Nil).barrier().mapPartitions(iter => iter)
    val shuffleDep = new ShuffleDependency(shuffleMapRdd, new HashPartitioner(2))
    val reduceRdd = new MyRDD(sc, 2, List(shuffleDep), tracker = mapOutputTracker)
    submit(reduceRdd, Array(0, 1))

    val mapStageId = 0

    def countSubmittedMapStageAttempts(): Int = {
      sparkListener.submittedStageInfos.count(_.stageId == mapStageId)
    }

    // The map stage should have been submitted.
    assert(countSubmittedMapStageAttempts() === 1)

    // The first map task fails with TaskKilled.
    runEvent(makeCompletionEvent(
      taskSets(0).tasks(0),
      TaskKilled("test"),
      null))
    assert(sparkListener.failedStages === Seq(0))

    // Trigger resubmission of the failed map stage.
    runEvent(ResubmitFailedStages)

    // Another attempt for the map stage should have been submitted, resulting in 2 total attempts.
    assert(countSubmittedMapStageAttempts() === 2)

    // The second map task fails with TaskKilled.
    runEvent(makeCompletionEvent(
      taskSets(0).tasks(1),
      TaskKilled("test"),
      null))

    // The second map task failure doesn't trigger stage retry.
    runEvent(ResubmitFailedStages)
    assert(countSubmittedMapStageAttempts() === 2)
  }

}
