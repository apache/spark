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

import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer
import scala.ref.WeakReference

import org.scalatest.Matchers
import org.scalatest.exceptions.TestFailedException

import org.apache.spark.scheduler._


class AccumulatorSuite extends SparkFunSuite with Matchers with LocalSparkContext {
  import InternalAccumulator._

  implicit def setAccum[A]: AccumulableParam[mutable.Set[A], A] =
    new AccumulableParam[mutable.Set[A], A] {
      def addInPlace(t1: mutable.Set[A], t2: mutable.Set[A]) : mutable.Set[A] = {
        t1 ++= t2
        t1
      }
      def addAccumulator(t1: mutable.Set[A], t2: A) : mutable.Set[A] = {
        t1 += t2
        t1
      }
      def zero(t: mutable.Set[A]) : mutable.Set[A] = {
        new mutable.HashSet[A]()
      }
    }

  test ("basic accumulation"){
    sc = new SparkContext("local", "test")
    val acc : Accumulator[Int] = sc.accumulator(0)

    val d = sc.parallelize(1 to 20)
    d.foreach{x => acc += x}
    acc.value should be (210)

    val longAcc = sc.accumulator(0L)
    val maxInt = Integer.MAX_VALUE.toLong
    d.foreach{x => longAcc += maxInt + x}
    longAcc.value should be (210L + maxInt * 20)
  }

  test ("value not assignable from tasks") {
    sc = new SparkContext("local", "test")
    val acc : Accumulator[Int] = sc.accumulator(0)

    val d = sc.parallelize(1 to 20)
    an [Exception] should be thrownBy {d.foreach{x => acc.value = x}}
  }

  test ("add value to collection accumulators") {
    val maxI = 1000
    for (nThreads <- List(1, 10)) { // test single & multi-threaded
      sc = new SparkContext("local[" + nThreads + "]", "test")
      val acc: Accumulable[mutable.Set[Any], Any] = sc.accumulable(new mutable.HashSet[Any]())
      val d = sc.parallelize(1 to maxI)
      d.foreach {
        x => acc += x
      }
      val v = acc.value.asInstanceOf[mutable.Set[Int]]
      for (i <- 1 to maxI) {
        v should contain(i)
      }
      resetSparkContext()
    }
  }

  test ("value not readable in tasks") {
    val maxI = 1000
    for (nThreads <- List(1, 10)) { // test single & multi-threaded
      sc = new SparkContext("local[" + nThreads + "]", "test")
      val acc: Accumulable[mutable.Set[Any], Any] = sc.accumulable(new mutable.HashSet[Any]())
      val d = sc.parallelize(1 to maxI)
      an [SparkException] should be thrownBy {
        d.foreach {
          x => acc.value += x
        }
      }
      resetSparkContext()
    }
  }

  test ("collection accumulators") {
    val maxI = 1000
    for (nThreads <- List(1, 10)) {
      // test single & multi-threaded
      sc = new SparkContext("local[" + nThreads + "]", "test")
      val setAcc = sc.accumulableCollection(mutable.HashSet[Int]())
      val bufferAcc = sc.accumulableCollection(mutable.ArrayBuffer[Int]())
      val mapAcc = sc.accumulableCollection(mutable.HashMap[Int, String]())
      val d = sc.parallelize((1 to maxI) ++ (1 to maxI))
      d.foreach {
        x => {setAcc += x; bufferAcc += x; mapAcc += (x -> x.toString)}
      }

      // Note that this is typed correctly -- no casts necessary
      setAcc.value.size should be (maxI)
      bufferAcc.value.size should be (2 * maxI)
      mapAcc.value.size should be (maxI)
      for (i <- 1 to maxI) {
        setAcc.value should contain(i)
        bufferAcc.value should contain(i)
        mapAcc.value should contain (i -> i.toString)
      }
      resetSparkContext()
    }
  }

  test ("localValue readable in tasks") {
    val maxI = 1000
    for (nThreads <- List(1, 10)) { // test single & multi-threaded
      sc = new SparkContext("local[" + nThreads + "]", "test")
      val acc: Accumulable[mutable.Set[Any], Any] = sc.accumulable(new mutable.HashSet[Any]())
      val groupedInts = (1 to (maxI/20)).map {x => (20 * (x - 1) to 20 * x).toSet}
      val d = sc.parallelize(groupedInts)
      d.foreach {
        x => acc.localValue ++= x
      }
      acc.value should be ( (0 to maxI).toSet)
      resetSparkContext()
    }
  }

  test ("garbage collection") {
    // Create an accumulator and let it go out of scope to test that it's properly garbage collected
    sc = new SparkContext("local", "test")
    var acc: Accumulable[mutable.Set[Any], Any] = sc.accumulable(new mutable.HashSet[Any]())
    val accId = acc.id
    val ref = WeakReference(acc)

    // Ensure the accumulator is present
    assert(ref.get.isDefined)

    // Remove the explicit reference to it and allow weak reference to get garbage collected
    acc = null
    System.gc()
    assert(ref.get.isEmpty)

    Accumulators.remove(accId)
    assert(!Accumulators.originals.get(accId).isDefined)
  }

  test("internal accumulators in TaskContext") {
    sc = new SparkContext("local", "test")
    val accums = InternalAccumulator.create(sc)
    val taskContext = new TaskContextImpl(0, 0, 0, 0, null, null, accums)
    val internalMetricsToAccums = taskContext.internalMetricsToAccumulators
    val collectedInternalAccums = taskContext.collectInternalAccumulators()
    val collectedAccums = taskContext.collectAccumulators()
    assert(internalMetricsToAccums.size > 0)
    assert(internalMetricsToAccums.values.forall(_.isInternal))
    assert(internalMetricsToAccums.contains(TEST_ACCUMULATOR))
    val testAccum = internalMetricsToAccums(TEST_ACCUMULATOR)
    assert(collectedInternalAccums.size === internalMetricsToAccums.size)
    assert(collectedInternalAccums.size === collectedAccums.size)
    assert(collectedInternalAccums.contains(testAccum.id))
    assert(collectedAccums.contains(testAccum.id))
  }

  test("internal accumulators in a stage") {
    val listener = new SaveInfoListener
    val numPartitions = 10
    sc = new SparkContext("local", "test")
    sc.addSparkListener(listener)
    // Have each task add 1 to the internal accumulator
    val rdd = sc.parallelize(1 to 100, numPartitions).mapPartitions { iter =>
      TaskContext.get().internalMetricsToAccumulators(TEST_ACCUMULATOR) += 1
      iter
    }
    // Register asserts in job completion callback to avoid flakiness
    listener.registerJobCompletionCallback { _ =>
      val stageInfos = listener.getCompletedStageInfos
      val taskInfos = listener.getCompletedTaskInfos
      assert(stageInfos.size === 1)
      assert(taskInfos.size === numPartitions)
      // The accumulator values should be merged in the stage
      val stageAccum = findAccumulableInfo(stageInfos.head.accumulables.values, TEST_ACCUMULATOR)
      assert(stageAccum.value.toLong === numPartitions)
      // The accumulator should be updated locally on each task
      val taskAccumValues = taskInfos.map { taskInfo =>
        val taskAccum = findAccumulableInfo(taskInfo.accumulables, TEST_ACCUMULATOR)
        assert(taskAccum.update.isDefined)
        assert(taskAccum.update.get.toLong === 1)
        taskAccum.value.toLong
      }
      // Each task should keep track of the partial value on the way, i.e. 1, 2, ... numPartitions
      assert(taskAccumValues.sorted === (1L to numPartitions).toSeq)
    }
    rdd.count()
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
        TaskContext.get().internalMetricsToAccumulators(TEST_ACCUMULATOR) += 1
        iter
      }
      .reduceByKey { case (x, y) => x + y }
      .mapPartitions { iter =>
        TaskContext.get().internalMetricsToAccumulators(TEST_ACCUMULATOR) += 10
        iter
      }
      .repartition(numPartitions * 2)
      .mapPartitions { iter =>
        TaskContext.get().internalMetricsToAccumulators(TEST_ACCUMULATOR) += 100
        iter
      }
    // Register asserts in job completion callback to avoid flakiness
    listener.registerJobCompletionCallback { _ =>
      // We ran 3 stages, and the accumulator values should be distinct
      val stageInfos = listener.getCompletedStageInfos
      assert(stageInfos.size === 3)
      val (firstStageAccum, secondStageAccum, thirdStageAccum) =
        (findAccumulableInfo(stageInfos(0).accumulables.values, TEST_ACCUMULATOR),
        findAccumulableInfo(stageInfos(1).accumulables.values, TEST_ACCUMULATOR),
        findAccumulableInfo(stageInfos(2).accumulables.values, TEST_ACCUMULATOR))
      assert(firstStageAccum.value.toLong === numPartitions)
      assert(secondStageAccum.value.toLong === numPartitions * 10)
      assert(thirdStageAccum.value.toLong === numPartitions * 2 * 100)
    }
    rdd.count()
  }

  test("internal accumulators in fully resubmitted stages") {
    testInternalAccumulatorsWithFailedTasks((i: Int) => true) // fail all tasks
  }

  test("internal accumulators in partially resubmitted stages") {
    testInternalAccumulatorsWithFailedTasks((i: Int) => i % 2 == 0) // fail a subset
  }

  /**
   * Return the accumulable info that matches the specified name.
   */
  private def findAccumulableInfo(
      accums: Iterable[AccumulableInfo],
      name: String): AccumulableInfo = {
    accums.find { a => a.name == name }.getOrElse {
      throw new TestFailedException(s"internal accumulator '$name' not found", 0)
    }
  }

  /**
   * Test whether internal accumulators are merged properly if some tasks fail.
   */
  private def testInternalAccumulatorsWithFailedTasks(failCondition: (Int => Boolean)): Unit = {
    val listener = new SaveInfoListener
    val numPartitions = 10
    val numFailedPartitions = (0 until numPartitions).count(failCondition)
    // This says use 1 core and retry tasks up to 2 times
    sc = new SparkContext("local[1, 2]", "test")
    sc.addSparkListener(listener)
    val rdd = sc.parallelize(1 to 100, numPartitions).mapPartitionsWithIndex { case (i, iter) =>
      val taskContext = TaskContext.get()
      taskContext.internalMetricsToAccumulators(TEST_ACCUMULATOR) += 1
      // Fail the first attempts of a subset of the tasks
      if (failCondition(i) && taskContext.attemptNumber() == 0) {
        throw new Exception("Failing a task intentionally.")
      }
      iter
    }
    // Register asserts in job completion callback to avoid flakiness
    listener.registerJobCompletionCallback { _ =>
      val stageInfos = listener.getCompletedStageInfos
      val taskInfos = listener.getCompletedTaskInfos
      assert(stageInfos.size === 1)
      assert(taskInfos.size === numPartitions + numFailedPartitions)
      val stageAccum = findAccumulableInfo(stageInfos.head.accumulables.values, TEST_ACCUMULATOR)
      // We should not double count values in the merged accumulator
      assert(stageAccum.value.toLong === numPartitions)
      val taskAccumValues = taskInfos.flatMap { taskInfo =>
        if (!taskInfo.failed) {
          // If a task succeeded, its update value should always be 1
          val taskAccum = findAccumulableInfo(taskInfo.accumulables, TEST_ACCUMULATOR)
          assert(taskAccum.update.isDefined)
          assert(taskAccum.update.get.toLong === 1)
          Some(taskAccum.value.toLong)
        } else {
          // If a task failed, we should not get its accumulator values
          assert(taskInfo.accumulables.isEmpty)
          None
        }
      }
      assert(taskAccumValues.sorted === (1L to numPartitions).toSeq)
    }
    rdd.count()
  }

}

private[spark] object AccumulatorSuite {

  /**
   * Run one or more Spark jobs and verify that the peak execution memory accumulator
   * is updated afterwards.
   */
  def verifyPeakExecutionMemorySet(
      sc: SparkContext,
      testName: String)(testBody: => Unit): Unit = {
    val listener = new SaveInfoListener
    sc.addSparkListener(listener)
    // Register asserts in job completion callback to avoid flakiness
    listener.registerJobCompletionCallback { jobId =>
      if (jobId == 0) {
        // The first job is a dummy one to verify that the accumulator does not already exist
        val accums = listener.getCompletedStageInfos.flatMap(_.accumulables.values)
        assert(!accums.exists(_.name == InternalAccumulator.PEAK_EXECUTION_MEMORY))
      } else {
        // In the subsequent jobs, verify that peak execution memory is updated
        val accum = listener.getCompletedStageInfos
          .flatMap(_.accumulables.values)
          .find(_.name == InternalAccumulator.PEAK_EXECUTION_MEMORY)
          .getOrElse {
          throw new TestFailedException(
            s"peak execution memory accumulator not set in '$testName'", 0)
        }
        assert(accum.value.toLong > 0)
      }
    }
    // Run the jobs
    sc.parallelize(1 to 10).count()
    testBody
  }
}

/**
 * A simple listener that keeps track of the TaskInfos and StageInfos of all completed jobs.
 */
private class SaveInfoListener extends SparkListener {
  private val completedStageInfos: ArrayBuffer[StageInfo] = new ArrayBuffer[StageInfo]
  private val completedTaskInfos: ArrayBuffer[TaskInfo] = new ArrayBuffer[TaskInfo]
  private var jobCompletionCallback: (Int => Unit) = null // parameter is job ID

  def getCompletedStageInfos: Seq[StageInfo] = completedStageInfos.toArray.toSeq
  def getCompletedTaskInfos: Seq[TaskInfo] = completedTaskInfos.toArray.toSeq

  /** Register a callback to be called on job end. */
  def registerJobCompletionCallback(callback: (Int => Unit)): Unit = {
    jobCompletionCallback = callback
  }

  override def onJobEnd(jobEnd: SparkListenerJobEnd): Unit = {
    if (jobCompletionCallback != null) {
      jobCompletionCallback(jobEnd.jobId)
    }
  }

  override def onStageCompleted(stageCompleted: SparkListenerStageCompleted): Unit = {
    completedStageInfos += stageCompleted.stageInfo
  }

  override def onTaskEnd(taskEnd: SparkListenerTaskEnd): Unit = {
    completedTaskInfos += taskEnd.taskInfo
  }
}
