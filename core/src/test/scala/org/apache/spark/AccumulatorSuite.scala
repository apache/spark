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

import javax.annotation.concurrent.GuardedBy

import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer
import scala.ref.WeakReference
import scala.util.control.NonFatal

import org.scalatest.Matchers
import org.scalatest.exceptions.TestFailedException

import org.apache.spark.scheduler._
import org.apache.spark.serializer.JavaSerializer


class AccumulatorSuite extends SparkFunSuite with Matchers with LocalSparkContext {
  import AccumulatorParam._

  override def afterEach(): Unit = {
    try {
      Accumulators.clear()
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

  test("value not assignable from tasks") {
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

  test("get accum") {
    sc = new SparkContext("local", "test")
    // Don't register with SparkContext for cleanup
    var acc = new Accumulable[Int, Int](0, IntAccumulatorParam, None, true, true)
    val accId = acc.id
    val ref = WeakReference(acc)
    assert(ref.get.isDefined)
    Accumulators.register(ref.get.get)

    // Remove the explicit reference to it and allow weak reference to get garbage collected
    acc = null
    System.gc()
    assert(ref.get.isEmpty)

    // Getting a garbage collected accum should throw error
    intercept[IllegalAccessError] {
      Accumulators.get(accId)
    }

    // Getting a normal accumulator. Note: this has to be separate because referencing an
    // accumulator above in an `assert` would keep it from being garbage collected.
    val acc2 = new Accumulable[Long, Long](0L, LongAccumulatorParam, None, true, true)
    Accumulators.register(acc2)
    assert(Accumulators.get(acc2.id) === Some(acc2))

    // Getting an accumulator that does not exist should return None
    assert(Accumulators.get(100000).isEmpty)
  }

  test("only external accums are automatically registered") {
    val accEx = new Accumulator(0, IntAccumulatorParam, Some("external"), internal = false)
    val accIn = new Accumulator(0, IntAccumulatorParam, Some("internal"), internal = true)
    assert(!accEx.isInternal)
    assert(accIn.isInternal)
    assert(Accumulators.get(accEx.id).isDefined)
    assert(Accumulators.get(accIn.id).isEmpty)
  }

  test("copy") {
    val acc1 = new Accumulable[Long, Long](456L, LongAccumulatorParam, Some("x"), true, false)
    val acc2 = acc1.copy()
    assert(acc1.id === acc2.id)
    assert(acc1.value === acc2.value)
    assert(acc1.name === acc2.name)
    assert(acc1.isInternal === acc2.isInternal)
    assert(acc1.countFailedValues === acc2.countFailedValues)
    assert(acc1 !== acc2)
    // Modifying one does not affect the other
    acc1.add(44L)
    assert(acc1.value === 500L)
    assert(acc2.value === 456L)
    acc2.add(144L)
    assert(acc1.value === 500L)
    assert(acc2.value === 600L)
  }

  test("register multiple accums with same ID") {
    // Make sure these are internal accums so we don't automatically register them already
    val acc1 = new Accumulable[Int, Int](0, IntAccumulatorParam, None, true, true)
    val acc2 = acc1.copy()
    assert(acc1 !== acc2)
    assert(acc1.id === acc2.id)
    assert(Accumulators.originals.isEmpty)
    assert(Accumulators.get(acc1.id).isEmpty)
    Accumulators.register(acc1)
    Accumulators.register(acc2)
    // The second one does not override the first one
    assert(Accumulators.originals.size === 1)
    assert(Accumulators.get(acc1.id) === Some(acc1))
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

  test("list accumulator param") {
    val acc = new Accumulator(Seq.empty[Int], new ListAccumulatorParam[Int], Some("numbers"))
    assert(acc.value === Seq.empty[Int])
    acc.add(Seq(1, 2))
    assert(acc.value === Seq(1, 2))
    acc += Seq(3, 4)
    assert(acc.value === Seq(1, 2, 3, 4))
    acc ++= Seq(5, 6)
    assert(acc.value === Seq(1, 2, 3, 4, 5, 6))
    acc.merge(Seq(7, 8))
    assert(acc.value === Seq(1, 2, 3, 4, 5, 6, 7, 8))
    acc.setValue(Seq(9, 10))
    assert(acc.value === Seq(9, 10))
  }

  test("value is reset on the executors") {
    val acc1 = new Accumulator(0, IntAccumulatorParam, Some("thing"), internal = false)
    val acc2 = new Accumulator(0L, LongAccumulatorParam, Some("thing2"), internal = false)
    val externalAccums = Seq(acc1, acc2)
    val internalAccums = InternalAccumulator.createAll()
    // Set some values; these should not be observed later on the "executors"
    acc1.setValue(10)
    acc2.setValue(20L)
    internalAccums
      .find(_.name == Some(InternalAccumulator.TEST_ACCUM))
      .get.asInstanceOf[Accumulator[Long]]
      .setValue(30L)
    // Simulate the task being serialized and sent to the executors.
    val dummyTask = new DummyTask(internalAccums, externalAccums)
    val serInstance = new JavaSerializer(new SparkConf).newInstance()
    val taskSer = Task.serializeWithDependencies(
      dummyTask, mutable.HashMap(), mutable.HashMap(), serInstance)
    // Now we're on the executors.
    // Deserialize the task and assert that its accumulators are zero'ed out.
    val (_, _, taskBytes) = Task.deserializeWithDependencies(taskSer)
    val taskDeser = serInstance.deserialize[DummyTask](
      taskBytes, Thread.currentThread.getContextClassLoader)
    // Assert that executors see only zeros
    taskDeser.externalAccums.foreach { a => assert(a.localValue == a.zero) }
    taskDeser.internalAccums.foreach { a => assert(a.localValue == a.zero) }
  }

}

private[spark] object AccumulatorSuite {

  import InternalAccumulator._

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
  private var calledJobCompletionCallback: Boolean = false
  private var exception: Throwable = null

  def getCompletedStageInfos: Seq[StageInfo] = completedStageInfos.toArray.toSeq
  def getCompletedTaskInfos: Seq[TaskInfo] = completedTaskInfos.values.flatten.toSeq
  def getCompletedTaskInfos(stageId: StageId, stageAttemptId: StageAttemptId): Seq[TaskInfo] =
    completedTaskInfos.get((stageId, stageAttemptId)).getOrElse(Seq.empty[TaskInfo])

  /**
   * If `jobCompletionCallback` is set, block until the next call has finished.
   * If the callback failed with an exception, throw it.
   */
  def awaitNextJobCompletion(): Unit = synchronized {
    if (jobCompletionCallback != null) {
      while (!calledJobCompletionCallback) {
        wait()
      }
      calledJobCompletionCallback = false
      if (exception != null) {
        exception = null
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

  override def onJobEnd(jobEnd: SparkListenerJobEnd): Unit = synchronized {
    if (jobCompletionCallback != null) {
      try {
        jobCompletionCallback()
      } catch {
        // Store any exception thrown here so we can throw them later in the main thread.
        // Otherwise, if `jobCompletionCallback` threw something it wouldn't fail the test.
        case NonFatal(e) => exception = e
      } finally {
        calledJobCompletionCallback = true
        notify()
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


/**
 * A dummy [[Task]] that contains internal and external [[Accumulator]]s.
 */
private[spark] class DummyTask(
    val internalAccums: Seq[Accumulator[_]],
    val externalAccums: Seq[Accumulator[_]])
  extends Task[Int](0, 0, 0, internalAccums) {
  override def runTask(c: TaskContext): Int = 1
}
