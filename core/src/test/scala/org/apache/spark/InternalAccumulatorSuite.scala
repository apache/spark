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

import org.apache.spark.scheduler.AccumulableInfo
import org.apache.spark.shuffle.FetchFailedException
import org.apache.spark.storage.{BlockId, BlockStatus}


class InternalAccumulatorSuite extends SparkFunSuite with LocalSparkContext {
  import InternalAccumulator._
  import AccumulatorParam._

  override def afterEach(): Unit = {
    try {
      Accumulators.clear()
    } finally {
      super.afterEach()
    }
  }

  test("get param") {
    assert(getParam(EXECUTOR_DESERIALIZE_TIME) === LongAccumulatorParam)
    assert(getParam(EXECUTOR_RUN_TIME) === LongAccumulatorParam)
    assert(getParam(RESULT_SIZE) === LongAccumulatorParam)
    assert(getParam(JVM_GC_TIME) === LongAccumulatorParam)
    assert(getParam(RESULT_SERIALIZATION_TIME) === LongAccumulatorParam)
    assert(getParam(MEMORY_BYTES_SPILLED) === LongAccumulatorParam)
    assert(getParam(DISK_BYTES_SPILLED) === LongAccumulatorParam)
    assert(getParam(PEAK_EXECUTION_MEMORY) === LongAccumulatorParam)
    assert(getParam(UPDATED_BLOCK_STATUSES) === UpdatedBlockStatusesAccumulatorParam)
    assert(getParam(TEST_ACCUM) === LongAccumulatorParam)
    // shuffle read
    assert(getParam(shuffleRead.REMOTE_BLOCKS_FETCHED) === IntAccumulatorParam)
    assert(getParam(shuffleRead.LOCAL_BLOCKS_FETCHED) === IntAccumulatorParam)
    assert(getParam(shuffleRead.REMOTE_BYTES_READ) === LongAccumulatorParam)
    assert(getParam(shuffleRead.LOCAL_BYTES_READ) === LongAccumulatorParam)
    assert(getParam(shuffleRead.FETCH_WAIT_TIME) === LongAccumulatorParam)
    assert(getParam(shuffleRead.RECORDS_READ) === LongAccumulatorParam)
    // shuffle write
    assert(getParam(shuffleWrite.BYTES_WRITTEN) === LongAccumulatorParam)
    assert(getParam(shuffleWrite.RECORDS_WRITTEN) === LongAccumulatorParam)
    assert(getParam(shuffleWrite.WRITE_TIME) === LongAccumulatorParam)
    // input
    assert(getParam(input.READ_METHOD) === StringAccumulatorParam)
    assert(getParam(input.RECORDS_READ) === LongAccumulatorParam)
    assert(getParam(input.BYTES_READ) === LongAccumulatorParam)
    // output
    assert(getParam(output.WRITE_METHOD) === StringAccumulatorParam)
    assert(getParam(output.RECORDS_WRITTEN) === LongAccumulatorParam)
    assert(getParam(output.BYTES_WRITTEN) === LongAccumulatorParam)
    // default to Long
    assert(getParam(METRICS_PREFIX + "anything") === LongAccumulatorParam)
    intercept[IllegalArgumentException] {
      getParam("something that does not start with the right prefix")
    }
  }

  test("create by name") {
    val executorRunTime = create(EXECUTOR_RUN_TIME)
    val updatedBlockStatuses = create(UPDATED_BLOCK_STATUSES)
    val shuffleRemoteBlocksRead = create(shuffleRead.REMOTE_BLOCKS_FETCHED)
    val inputReadMethod = create(input.READ_METHOD)
    assert(executorRunTime.name === Some(EXECUTOR_RUN_TIME))
    assert(updatedBlockStatuses.name === Some(UPDATED_BLOCK_STATUSES))
    assert(shuffleRemoteBlocksRead.name === Some(shuffleRead.REMOTE_BLOCKS_FETCHED))
    assert(inputReadMethod.name === Some(input.READ_METHOD))
    assert(executorRunTime.value.isInstanceOf[Long])
    assert(updatedBlockStatuses.value.isInstanceOf[Seq[_]])
    // We cannot assert the type of the value directly since the type parameter is erased.
    // Instead, try casting a `Seq` of expected type and see if it fails in run time.
    updatedBlockStatuses.setValueAny(Seq.empty[(BlockId, BlockStatus)])
    assert(shuffleRemoteBlocksRead.value.isInstanceOf[Int])
    assert(inputReadMethod.value.isInstanceOf[String])
    // default to Long
    val anything = create(METRICS_PREFIX + "anything")
    assert(anything.value.isInstanceOf[Long])
  }

  test("create") {
    val accums = createAll()
    val shuffleReadAccums = createShuffleReadAccums()
    val shuffleWriteAccums = createShuffleWriteAccums()
    val inputAccums = createInputAccums()
    val outputAccums = createOutputAccums()
    // assert they're all internal
    assert(accums.forall(_.isInternal))
    assert(shuffleReadAccums.forall(_.isInternal))
    assert(shuffleWriteAccums.forall(_.isInternal))
    assert(inputAccums.forall(_.isInternal))
    assert(outputAccums.forall(_.isInternal))
    // assert they all count on failures
    assert(accums.forall(_.countFailedValues))
    assert(shuffleReadAccums.forall(_.countFailedValues))
    assert(shuffleWriteAccums.forall(_.countFailedValues))
    assert(inputAccums.forall(_.countFailedValues))
    assert(outputAccums.forall(_.countFailedValues))
    // assert they all have names
    assert(accums.forall(_.name.isDefined))
    assert(shuffleReadAccums.forall(_.name.isDefined))
    assert(shuffleWriteAccums.forall(_.name.isDefined))
    assert(inputAccums.forall(_.name.isDefined))
    assert(outputAccums.forall(_.name.isDefined))
    // assert `accums` is a strict superset of the others
    val accumNames = accums.map(_.name.get).toSet
    val shuffleReadAccumNames = shuffleReadAccums.map(_.name.get).toSet
    val shuffleWriteAccumNames = shuffleWriteAccums.map(_.name.get).toSet
    val inputAccumNames = inputAccums.map(_.name.get).toSet
    val outputAccumNames = outputAccums.map(_.name.get).toSet
    assert(shuffleReadAccumNames.subsetOf(accumNames))
    assert(shuffleWriteAccumNames.subsetOf(accumNames))
    assert(inputAccumNames.subsetOf(accumNames))
    assert(outputAccumNames.subsetOf(accumNames))
  }

  test("naming") {
    val accums = createAll()
    val shuffleReadAccums = createShuffleReadAccums()
    val shuffleWriteAccums = createShuffleWriteAccums()
    val inputAccums = createInputAccums()
    val outputAccums = createOutputAccums()
    // assert that prefixes are properly namespaced
    assert(SHUFFLE_READ_METRICS_PREFIX.startsWith(METRICS_PREFIX))
    assert(SHUFFLE_WRITE_METRICS_PREFIX.startsWith(METRICS_PREFIX))
    assert(INPUT_METRICS_PREFIX.startsWith(METRICS_PREFIX))
    assert(OUTPUT_METRICS_PREFIX.startsWith(METRICS_PREFIX))
    assert(accums.forall(_.name.get.startsWith(METRICS_PREFIX)))
    // assert they all start with the expected prefixes
    assert(shuffleReadAccums.forall(_.name.get.startsWith(SHUFFLE_READ_METRICS_PREFIX)))
    assert(shuffleWriteAccums.forall(_.name.get.startsWith(SHUFFLE_WRITE_METRICS_PREFIX)))
    assert(inputAccums.forall(_.name.get.startsWith(INPUT_METRICS_PREFIX)))
    assert(outputAccums.forall(_.name.get.startsWith(OUTPUT_METRICS_PREFIX)))
  }

  test("internal accumulators in TaskContext") {
    val taskContext = TaskContext.empty()
    val accumUpdates = taskContext.taskMetrics.accumulatorUpdates()
    assert(accumUpdates.size > 0)
    assert(accumUpdates.forall(_.internal))
    val testAccum = taskContext.taskMetrics.getAccum(TEST_ACCUM)
    assert(accumUpdates.exists(_.id == testAccum.id))
  }

  test("internal accumulators in a stage") {
    val listener = new SaveInfoListener
    val numPartitions = 10
    sc = new SparkContext("local", "test")
    sc.addSparkListener(listener)
    // Have each task add 1 to the internal accumulator
    val rdd = sc.parallelize(1 to 100, numPartitions).mapPartitions { iter =>
      TaskContext.get().taskMetrics().getAccum(TEST_ACCUM) += 1
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
        TaskContext.get().taskMetrics().getAccum(TEST_ACCUM) += 1
        iter
      }
      .reduceByKey { case (x, y) => x + y }
      .mapPartitions { iter =>
        TaskContext.get().taskMetrics().getAccum(TEST_ACCUM) += 10
        iter
      }
      .repartition(numPartitions * 2)
      .mapPartitions { iter =>
        TaskContext.get().taskMetrics().getAccum(TEST_ACCUM) += 100
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
      .mapPartitions { iter => TaskContext.get().taskMetrics().getAccum(TEST_ACCUM) += 1; iter }
      .groupBy(identity)
    val sid = x.dependencies.head.asInstanceOf[ShuffleDependency[_, _, _]].shuffleHandle.shuffleId
    val rdd = x.mapPartitionsWithIndex { case (i, iter) =>
      // Fail the first stage attempt. Here we use the task attempt ID to determine this.
      // This job runs 2 stages, and we're in the second stage. Therefore, any task attempt
      // ID that's < 2 * numPartitions belongs to the first attempt of this stage.
      val taskContext = TaskContext.get()
      val isFirstStageAttempt = taskContext.taskAttemptId() < numPartitions * 2
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
    assert(Accumulators.originals.isEmpty)
    sc.parallelize(1 to 100).map { i => (i, i) }.reduceByKey { _ + _ }.count()
    val internalAccums = InternalAccumulator.createAll()
    // We ran 2 stages, so we should have 2 sets of internal accumulators, 1 for each stage
    assert(Accumulators.originals.size === internalAccums.size * 2)
    val accumsRegistered = sc.cleaner match {
      case Some(cleaner: SaveAccumContextCleaner) => cleaner.accumsRegisteredForCleanup
      case _ => Seq.empty[Long]
    }
    // Make sure the same set of accumulators is registered for cleanup
    assert(accumsRegistered.size === internalAccums.size * 2)
    assert(accumsRegistered.toSet === Accumulators.originals.keys.toSet)
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

    override def registerAccumulatorForCleanup(a: Accumulable[_, _]): Unit = {
      accumsRegistered += a.id
      super.registerAccumulatorForCleanup(a)
    }

    def accumsRegisteredForCleanup: Seq[Long] = accumsRegistered.toArray
  }

}
