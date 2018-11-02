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

import scala.collection.mutable.ArrayBuffer

import org.apache.spark.executor.TaskMetrics
import org.apache.spark.scheduler.AccumulableInfo
import org.apache.spark.shuffle.FetchFailedException
import org.apache.spark.util.{AccumulatorContext, AccumulatorV2}


class InternalAccumulatorSuite extends SparkFunSuite with LocalSparkContext {
  import InternalAccumulator._

  override def afterEach(): Unit = {
    try {
      AccumulatorContext.clear()
    } finally {
      super.afterEach()
    }
  }

  test("internal accumulators in TaskContext") {
    val taskContext = TaskContext.empty()
    val accumUpdates = taskContext.taskMetrics.accumulators()
    assert(accumUpdates.size > 0)
    val testAccum = taskContext.taskMetrics.testAccum.get
    assert(accumUpdates.exists(_.id == testAccum.id))
  }

  test("internal accumulators in a stage") {
    val listener = new SaveInfoListener
    val numPartitions = 10
    sc = new SparkContext("local", "test")
    sc.addSparkListener(listener)
    // Have each task add 1 to the internal accumulator
    val rdd = sc.parallelize(1 to 100, numPartitions).mapPartitions { iter =>
      TaskContext.get().taskMetrics().testAccum.get.add(1)
      iter
    }
    // Register asserts in job completion callback to avoid flakiness
    listener.registerJobCompletionCallback { () =>
      val stageInfos = listener.getCompletedStageInfos
      val taskInfos = listener.getCompletedTaskInfos
      assert(stageInfos.size === 1)
      assert(taskInfos.size === numPartitions)
      // The accumulator values should be merged in the stage
      val stageAccum = findTestAccum(stageInfos.head.accumulables.values)
      assert(stageAccum.value.get.asInstanceOf[Long] === numPartitions)
      // The accumulator should be updated locally on each task
      val taskAccumValues = taskInfos.map { taskInfo =>
        val taskAccum = findTestAccum(taskInfo.accumulables)
        assert(taskAccum.update.isDefined)
        assert(taskAccum.update.get.asInstanceOf[Long] === 1L)
        taskAccum.value.get.asInstanceOf[Long]
      }
      // Each task should keep track of the partial value on the way, i.e. 1, 2, ... numPartitions
      assert(taskAccumValues.sorted === (1L to numPartitions).toSeq)
    }
    rdd.count()
    listener.awaitNextJobCompletion()
  }

  test("internal accumulators in multiple stages") {
    val listener = new SaveInfoListener
    val numPartitions = 10
    sc = new SparkContext("local", "test")
    sc.addSparkListener(listener)
    // Each stage creates its own set of internal accumulators so the
    // values for the same metric should not be mixed up across stages
    val rdd = sc.parallelize(1 to 100, numPartitions)
      .map { i => (i, i) }
      .mapPartitions { iter =>
        TaskContext.get().taskMetrics().testAccum.get.add(1)
        iter
      }
      .reduceByKey { case (x, y) => x + y }
      .mapPartitions { iter =>
        TaskContext.get().taskMetrics().testAccum.get.add(10)
        iter
      }
      .repartition(numPartitions * 2)
      .mapPartitions { iter =>
        TaskContext.get().taskMetrics().testAccum.get.add(100)
        iter
      }
    // Register asserts in job completion callback to avoid flakiness
    listener.registerJobCompletionCallback { () =>
    // We ran 3 stages, and the accumulator values should be distinct
      val stageInfos = listener.getCompletedStageInfos
      assert(stageInfos.size === 3)
      val (firstStageAccum, secondStageAccum, thirdStageAccum) =
        (findTestAccum(stageInfos(0).accumulables.values),
        findTestAccum(stageInfos(1).accumulables.values),
        findTestAccum(stageInfos(2).accumulables.values))
      assert(firstStageAccum.value.get.asInstanceOf[Long] === numPartitions)
      assert(secondStageAccum.value.get.asInstanceOf[Long] === numPartitions * 10)
      assert(thirdStageAccum.value.get.asInstanceOf[Long] === numPartitions * 2 * 100)
    }
    rdd.count()
  }

  test("internal accumulators in resubmitted stages") {
    val listener = new SaveInfoListener
    val numPartitions = 10
    sc = new SparkContext("local", "test")
    sc.addSparkListener(listener)

    // Simulate fetch failures in order to trigger a stage retry. Here we run 1 job with
    // 2 stages. On the second stage, we trigger a fetch failure on the first stage attempt.
    // This should retry both stages in the scheduler. Note that we only want to fail the
    // first stage attempt because we want the stage to eventually succeed.
    val x = sc.parallelize(1 to 100, numPartitions)
      .mapPartitions { iter => TaskContext.get().taskMetrics().testAccum.get.add(1); iter }
      .groupBy(identity)
    val sid = x.dependencies.head.asInstanceOf[ShuffleDependency[_, _, _]].shuffleHandle.shuffleId
    val rdd = x.mapPartitionsWithIndex { case (i, iter) =>
      // Fail the first stage attempt. Here we use the task attempt ID to determine this.
      // This job runs 2 stages, and we're in the second stage. Therefore, any task attempt
      // ID that's < 2 * numPartitions belongs to the first attempt of this stage.
      val taskContext = TaskContext.get()
      val isFirstStageAttempt = taskContext.taskAttemptId() < numPartitions * 2L
      if (isFirstStageAttempt) {
        throw new FetchFailedException(
          SparkEnv.get.blockManager.blockManagerId,
          sid,
          taskContext.partitionId(),
          taskContext.partitionId(),
          "simulated fetch failure")
      } else {
        iter
      }
    }

    // Register asserts in job completion callback to avoid flakiness
    listener.registerJobCompletionCallback { () =>
      val stageInfos = listener.getCompletedStageInfos
      assert(stageInfos.size === 4) // 1 shuffle map stage + 1 result stage, both are retried
      val mapStageId = stageInfos.head.stageId
      val mapStageInfo1stAttempt = stageInfos.head
      val mapStageInfo2ndAttempt = {
        stageInfos.tail.find(_.stageId == mapStageId).getOrElse {
          fail("expected two attempts of the same shuffle map stage.")
        }
      }
      val stageAccum1stAttempt = findTestAccum(mapStageInfo1stAttempt.accumulables.values)
      val stageAccum2ndAttempt = findTestAccum(mapStageInfo2ndAttempt.accumulables.values)
      // Both map stages should have succeeded, since the fetch failure happened in the
      // result stage, not the map stage. This means we should get the accumulator updates
      // from all partitions.
      assert(stageAccum1stAttempt.value.get.asInstanceOf[Long] === numPartitions)
      assert(stageAccum2ndAttempt.value.get.asInstanceOf[Long] === numPartitions)
      // Because this test resubmitted the map stage with all missing partitions, we should have
      // created a fresh set of internal accumulators in the 2nd stage attempt. Assert this is
      // the case by comparing the accumulator IDs between the two attempts.
      // Note: it would be good to also test the case where the map stage is resubmitted where
      // only a subset of the original partitions are missing. However, this scenario is very
      // difficult to construct without potentially introducing flakiness.
      assert(stageAccum1stAttempt.id != stageAccum2ndAttempt.id)
    }
    rdd.count()
    listener.awaitNextJobCompletion()
  }

  test("internal accumulators are registered for cleanups") {
    sc = new SparkContext("local", "test") {
      private val myCleaner = new SaveAccumContextCleaner(this)
      override def cleaner: Option[ContextCleaner] = Some(myCleaner)
    }
    assert(AccumulatorContext.numAccums == 0)
    sc.parallelize(1 to 100).map { i => (i, i) }.reduceByKey { _ + _ }.count()
    val numInternalAccums = TaskMetrics.empty.internalAccums.length
    // We ran 2 stages, so we should have 2 sets of internal accumulators, 1 for each stage
    assert(AccumulatorContext.numAccums === numInternalAccums * 2)
    val accumsRegistered = sc.cleaner match {
      case Some(cleaner: SaveAccumContextCleaner) => cleaner.accumsRegisteredForCleanup
      case _ => Seq.empty[Long]
    }
    // Make sure the same set of accumulators is registered for cleanup
    assert(accumsRegistered.size === numInternalAccums * 2)
    assert(accumsRegistered.toSet.size === AccumulatorContext.numAccums)
    accumsRegistered.foreach(id => assert(AccumulatorContext.get(id) != None))
  }

  /**
   * Return the accumulable info that matches the specified name.
   */
  private def findTestAccum(accums: Iterable[AccumulableInfo]): AccumulableInfo = {
    accums.find { a => a.name == Some(TEST_ACCUM) }.getOrElse {
      fail(s"unable to find internal accumulator called $TEST_ACCUM")
    }
  }

  /**
   * A special [[ContextCleaner]] that saves the IDs of the accumulators registered for cleanup.
   */
  private class SaveAccumContextCleaner(sc: SparkContext) extends ContextCleaner(sc) {
    private val accumsRegistered = new ArrayBuffer[Long]

    override def registerAccumulatorForCleanup(a: AccumulatorV2[_, _]): Unit = {
      accumsRegistered += a.id
      super.registerAccumulatorForCleanup(a)
    }

    def accumsRegisteredForCleanup: Seq[Long] = accumsRegistered.toArray
  }

}
