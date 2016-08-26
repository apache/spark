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

import java.util.concurrent.Semaphore
import javax.annotation.concurrent.GuardedBy

import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer
import scala.ref.WeakReference
import scala.util.control.NonFatal

import org.scalatest.Matchers
import org.scalatest.exceptions.TestFailedException

import org.apache.spark.AccumulatorParam.StringAccumulatorParam
import org.apache.spark.scheduler._
import org.apache.spark.serializer.JavaSerializer
import org.apache.spark.util.{AccumulatorContext, AccumulatorMetadata, AccumulatorV2, LongAccumulator}


class AccumulatorSuite extends SparkFunSuite with Matchers with LocalSparkContext {
  import AccumulatorSuite.createLongAccum

  override def afterEach(): Unit = {
    try {
      AccumulatorContext.clear()
    } finally {
      super.afterEach()
    }
  }

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

  test("accumulator serialization") {
    val ser = new JavaSerializer(new SparkConf).newInstance()
    val acc = createLongAccum("x")
    acc.add(5)
    assert(acc.value == 5)
    assert(acc.isAtDriverSide)

    // serialize and de-serialize it, to simulate sending accumulator to executor.
    val acc2 = ser.deserialize[LongAccumulator](ser.serialize(acc))
    // value is reset on the executors
    assert(acc2.value == 0)
    assert(!acc2.isAtDriverSide)

    acc2.add(10)
    // serialize and de-serialize it again, to simulate sending accumulator back to driver.
    val acc3 = ser.deserialize[LongAccumulator](ser.serialize(acc2))
    // value is not reset on the driver
    assert(acc3.value == 10)
    assert(acc3.isAtDriverSide)
  }

  test ("basic accumulation") {
    sc = new SparkContext("local", "test")
    val acc: Accumulator[Int] = sc.accumulator(0)

    val d = sc.parallelize(1 to 20)
    d.foreach{x => acc += x}
    acc.value should be (210)

    val longAcc = sc.accumulator(0L)
    val maxInt = Integer.MAX_VALUE.toLong
    d.foreach{x => longAcc += maxInt + x}
    longAcc.value should be (210L + maxInt * 20)
  }

  test("value not assignable from tasks") {
    sc = new SparkContext("local", "test")
    val acc: Accumulator[Int] = sc.accumulator(0)

    val d = sc.parallelize(1 to 20)
    an [SparkException] should be thrownBy {d.foreach{x => acc.value = x}}
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

  test("value not readable in tasks") {
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
      acc.value should be ((0 to maxI).toSet)
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

    AccumulatorContext.remove(accId)
    assert(!AccumulatorContext.get(accId).isDefined)
  }

  test("get accum") {
    // Don't register with SparkContext for cleanup
    var acc = createLongAccum("a")
    val accId = acc.id
    val ref = WeakReference(acc)
    assert(ref.get.isDefined)

    // Remove the explicit reference to it and allow weak reference to get garbage collected
    acc = null
    System.gc()
    assert(ref.get.isEmpty)

    // Getting a garbage collected accum should throw error
    intercept[IllegalAccessError] {
      AccumulatorContext.get(accId)
    }

    // Getting a normal accumulator. Note: this has to be separate because referencing an
    // accumulator above in an `assert` would keep it from being garbage collected.
    val acc2 = createLongAccum("b")
    assert(AccumulatorContext.get(acc2.id) === Some(acc2))

    // Getting an accumulator that does not exist should return None
    assert(AccumulatorContext.get(100000).isEmpty)
  }

  test("string accumulator param") {
    val acc = new Accumulator("", StringAccumulatorParam, Some("darkness"))
    assert(acc.value === "")
    acc.setValue("feeds")
    assert(acc.value === "feeds")
    acc.add("your")
    assert(acc.value === "your") // value is overwritten, not concatenated
    acc += "soul"
    assert(acc.value === "soul")
    acc ++= "with"
    assert(acc.value === "with")
    acc.merge("kindness")
    assert(acc.value === "kindness")
  }
}

private[spark] object AccumulatorSuite {
  import InternalAccumulator._

  /**
   * Create a long accumulator and register it to [[AccumulatorContext]].
   */
  def createLongAccum(
      name: String,
      countFailedValues: Boolean = false,
      initValue: Long = 0,
      id: Long = AccumulatorContext.newId()): LongAccumulator = {
    val acc = new LongAccumulator
    acc.setValue(initValue)
    acc.metadata = AccumulatorMetadata(id, Some(name), countFailedValues)
    AccumulatorContext.register(acc)
    acc
  }

  /**
   * Make an [[AccumulableInfo]] out of an [[Accumulable]] with the intent to use the
   * info as an accumulator update.
   */
  def makeInfo(a: AccumulatorV2[_, _]): AccumulableInfo = a.toInfo(Some(a.value), None)

  /**
   * Run one or more Spark jobs and verify that in at least one job the peak execution memory
   * accumulator is updated afterwards.
   */
  def verifyPeakExecutionMemorySet(
      sc: SparkContext,
      testName: String)(testBody: => Unit): Unit = {
    val listener = new SaveInfoListener
    sc.addSparkListener(listener)
    testBody
    // wait until all events have been processed before proceeding to assert things
    sc.listenerBus.waitUntilEmpty(10 * 1000)
    val accums = listener.getCompletedStageInfos.flatMap(_.accumulables.values)
    val isSet = accums.exists { a =>
      a.name == Some(PEAK_EXECUTION_MEMORY) && a.value.exists(_.asInstanceOf[Long] > 0L)
    }
    if (!isSet) {
      throw new TestFailedException(s"peak execution memory accumulator not set in '$testName'", 0)
    }
  }
}

/**
 * A simple listener that keeps track of the TaskInfos and StageInfos of all completed jobs.
 */
private class SaveInfoListener extends SparkListener {
  type StageId = Int
  type StageAttemptId = Int

  private val completedStageInfos = new ArrayBuffer[StageInfo]
  private val completedTaskInfos =
    new mutable.HashMap[(StageId, StageAttemptId), ArrayBuffer[TaskInfo]]

  // Callback to call when a job completes. Parameter is job ID.
  @GuardedBy("this")
  private var jobCompletionCallback: () => Unit = null
  private val jobCompletionSem = new Semaphore(0)
  private var exception: Throwable = null

  def getCompletedStageInfos: Seq[StageInfo] = completedStageInfos.toArray.toSeq
  def getCompletedTaskInfos: Seq[TaskInfo] = completedTaskInfos.values.flatten.toSeq
  def getCompletedTaskInfos(stageId: StageId, stageAttemptId: StageAttemptId): Seq[TaskInfo] =
    completedTaskInfos.getOrElse((stageId, stageAttemptId), Seq.empty[TaskInfo])

  /**
   * If `jobCompletionCallback` is set, block until the next call has finished.
   * If the callback failed with an exception, throw it.
   */
  def awaitNextJobCompletion(): Unit = {
    if (jobCompletionCallback != null) {
      jobCompletionSem.acquire()
      if (exception != null) {
        throw exception
      }
    }
  }

  /**
   * Register a callback to be called on job end.
   * A call to this should be followed by [[awaitNextJobCompletion]].
   */
  def registerJobCompletionCallback(callback: () => Unit): Unit = {
    jobCompletionCallback = callback
  }

  override def onJobEnd(jobEnd: SparkListenerJobEnd): Unit = {
    if (jobCompletionCallback != null) {
      try {
        jobCompletionCallback()
      } catch {
        // Store any exception thrown here so we can throw them later in the main thread.
        // Otherwise, if `jobCompletionCallback` threw something it wouldn't fail the test.
        case NonFatal(e) => exception = e
      } finally {
        jobCompletionSem.release()
      }
    }
  }

  override def onStageCompleted(stageCompleted: SparkListenerStageCompleted): Unit = {
    completedStageInfos += stageCompleted.stageInfo
  }

  override def onTaskEnd(taskEnd: SparkListenerTaskEnd): Unit = {
    completedTaskInfos.getOrElseUpdate(
      (taskEnd.stageId, taskEnd.stageAttemptId), new ArrayBuffer[TaskInfo]) += taskEnd.taskInfo
  }
}
