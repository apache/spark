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

import java.util.concurrent.Semaphore

import scala.collection.mutable

import org.scalatest.{BeforeAndAfter, BeforeAndAfterAll, FunSuite}
import org.scalatest.Matchers

import org.apache.spark.{LocalSparkContext, SparkContext}
import org.apache.spark.executor.TaskMetrics

class SparkListenerSuite extends FunSuite with LocalSparkContext with Matchers
  with BeforeAndAfter with BeforeAndAfterAll {

  /** Length of time to wait while draining listener events. */
  val WAIT_TIMEOUT_MILLIS = 10000

  before {
    sc = new SparkContext("local", "SparkListenerSuite")
  }

  override def afterAll() {
    System.clearProperty("spark.akka.frameSize")
  }

  test("basic creation and shutdown of LiveListenerBus") {
    val counter = new BasicJobCounter
    val bus = new LiveListenerBus
    bus.addListener(counter)

    // Listener bus hasn't started yet, so posting events should not increment counter
    (1 to 5).foreach { _ => bus.post(SparkListenerJobEnd(0, JobSucceeded)) }
    assert(counter.count === 0)

    // Starting listener bus should flush all buffered events
    bus.start()
    assert(bus.waitUntilEmpty(WAIT_TIMEOUT_MILLIS))
    assert(counter.count === 5)

    // After listener bus has stopped, posting events should not increment counter
    bus.stop()
    (1 to 5).foreach { _ => bus.post(SparkListenerJobEnd(0, JobSucceeded)) }
    assert(counter.count === 5)

    // Listener bus must not be started twice
    intercept[IllegalStateException] {
      val bus = new LiveListenerBus
      bus.start()
      bus.start()
    }

    // ... or stopped before starting
    intercept[IllegalStateException] {
      val bus = new LiveListenerBus
      bus.stop()
    }
  }

  test("bus.stop() waits for the event queue to completely drain") {
    @volatile var drained = false

    // When Listener has started
    val listenerStarted = new Semaphore(0)

    // Tells the listener to stop blocking
    val listenerWait = new Semaphore(0)

    // When stopper has started
    val stopperStarted = new Semaphore(0)

    // When stopper has returned
    val stopperReturned = new Semaphore(0)

    class BlockingListener extends SparkListener {
      override def onJobEnd(jobEnd: SparkListenerJobEnd) = {
        listenerStarted.release()
        listenerWait.acquire()
        drained = true
      }
    }

    val bus = new LiveListenerBus
    val blockingListener = new BlockingListener

    bus.addListener(blockingListener)
    bus.start()
    bus.post(SparkListenerJobEnd(0, JobSucceeded))

    listenerStarted.acquire()
    // Listener should be blocked after start
    assert(!drained)

    new Thread("ListenerBusStopper") {
      override def run() {
        stopperStarted.release()
        // stop() will block until notify() is called below
        bus.stop()
        stopperReturned.release()
      }
    }.start()

    stopperStarted.acquire()
    // Listener should remain blocked after stopper started
    assert(!drained)

    // unblock Listener to let queue drain
    listenerWait.release()
    stopperReturned.acquire()
    assert(drained)
  }

  test("basic creation of StageInfo") {
    val listener = new SaveStageAndTaskInfo
    sc.addSparkListener(listener)
    val rdd1 = sc.parallelize(1 to 100, 4)
    val rdd2 = rdd1.map(_.toString)
    rdd2.setName("Target RDD")
    rdd2.count()

    assert(sc.listenerBus.waitUntilEmpty(WAIT_TIMEOUT_MILLIS))

    listener.stageInfos.size should be {1}
    val (stageInfo, taskInfoMetrics) = listener.stageInfos.head
    stageInfo.rddInfos.size should be {2}
    stageInfo.rddInfos.forall(_.numPartitions == 4) should be {true}
    stageInfo.rddInfos.exists(_.name == "Target RDD") should be {true}
    stageInfo.numTasks should be {4}
    stageInfo.submissionTime should be ('defined)
    stageInfo.completionTime should be ('defined)
    taskInfoMetrics.length should be {4}
  }

  test("basic creation of StageInfo with shuffle") {
    val listener = new SaveStageAndTaskInfo
    sc.addSparkListener(listener)
    val rdd1 = sc.parallelize(1 to 100, 4)
    val rdd2 = rdd1.filter(_ % 2 == 0).map(i => (i, i))
    val rdd3 = rdd2.reduceByKey(_ + _)
    rdd1.setName("Un")
    rdd2.setName("Deux")
    rdd3.setName("Trois")

    rdd1.count()
    assert(sc.listenerBus.waitUntilEmpty(WAIT_TIMEOUT_MILLIS))
    listener.stageInfos.size should be {1}
    val stageInfo1 = listener.stageInfos.keys.find(_.stageId == 0).get
    stageInfo1.rddInfos.size should be {1} // ParallelCollectionRDD
    stageInfo1.rddInfos.forall(_.numPartitions == 4) should be {true}
    stageInfo1.rddInfos.exists(_.name == "Un") should be {true}
    listener.stageInfos.clear()

    rdd2.count()
    assert(sc.listenerBus.waitUntilEmpty(WAIT_TIMEOUT_MILLIS))
    listener.stageInfos.size should be {1}
    val stageInfo2 = listener.stageInfos.keys.find(_.stageId == 1).get
    stageInfo2.rddInfos.size should be {3} // ParallelCollectionRDD, FilteredRDD, MappedRDD
    stageInfo2.rddInfos.forall(_.numPartitions == 4) should be {true}
    stageInfo2.rddInfos.exists(_.name == "Deux") should be {true}
    listener.stageInfos.clear()

    rdd3.count()
    assert(sc.listenerBus.waitUntilEmpty(WAIT_TIMEOUT_MILLIS))
    listener.stageInfos.size should be {2} // Shuffle map stage + result stage
    val stageInfo3 = listener.stageInfos.keys.find(_.stageId == 3).get
    stageInfo3.rddInfos.size should be {1} // ShuffledRDD
    stageInfo3.rddInfos.forall(_.numPartitions == 4) should be {true}
    stageInfo3.rddInfos.exists(_.name == "Trois") should be {true}
  }

  test("StageInfo with fewer tasks than partitions") {
    val listener = new SaveStageAndTaskInfo
    sc.addSparkListener(listener)
    val rdd1 = sc.parallelize(1 to 100, 4)
    val rdd2 = rdd1.map(_.toString)
    sc.runJob(rdd2, (items: Iterator[String]) => items.size, Seq(0, 1), true)

    assert(sc.listenerBus.waitUntilEmpty(WAIT_TIMEOUT_MILLIS))

    listener.stageInfos.size should be {1}
    val (stageInfo, _) = listener.stageInfos.head
    stageInfo.numTasks should be {2}
    stageInfo.rddInfos.size should be {2}
    stageInfo.rddInfos.forall(_.numPartitions == 4) should be {true}
  }

  test("local metrics") {
    val listener = new SaveStageAndTaskInfo
    sc.addSparkListener(listener)
    sc.addSparkListener(new StatsReportListener)
    // just to make sure some of the tasks take a noticeable amount of time
    val w = { i: Int =>
      if (i == 0)
        Thread.sleep(100)
      i
    }

    val d = sc.parallelize(0 to 1e4.toInt, 64).map(w)
    d.count()
    assert(sc.listenerBus.waitUntilEmpty(WAIT_TIMEOUT_MILLIS))
    listener.stageInfos.size should be (1)

    val d2 = d.map { i => w(i) -> i * 2 }.setName("shuffle input 1")
    val d3 = d.map { i => w(i) -> (0 to (i % 5)) }.setName("shuffle input 2")
    val d4 = d2.cogroup(d3, 64).map { case (k, (v1, v2)) =>
      w(k) -> (v1.size, v2.size)
    }
    d4.setName("A Cogroup")
    d4.collectAsMap()

    assert(sc.listenerBus.waitUntilEmpty(WAIT_TIMEOUT_MILLIS))
    listener.stageInfos.size should be (4)
    listener.stageInfos.foreach { case (stageInfo, taskInfoMetrics) =>
      /**
       * Small test, so some tasks might take less than 1 millisecond, but average should be greater
       * than 0 ms.
       */
      checkNonZeroAvg(
        taskInfoMetrics.map(_._2.executorRunTime),
        stageInfo + " executorRunTime")
      checkNonZeroAvg(
        taskInfoMetrics.map(_._2.executorDeserializeTime),
        stageInfo + " executorDeserializeTime")

      /* Test is disabled (SEE SPARK-2208)
      if (stageInfo.rddInfos.exists(_.name == d4.name)) {
        checkNonZeroAvg(
          taskInfoMetrics.map(_._2.shuffleReadMetrics.get.fetchWaitTime),
          stageInfo + " fetchWaitTime")
      }
      */

      taskInfoMetrics.foreach { case (taskInfo, taskMetrics) =>
        taskMetrics.resultSize should be > (0l)
        if (stageInfo.rddInfos.exists(info => info.name == d2.name || info.name == d3.name)) {
          taskMetrics.inputMetrics should not be ('defined)
          taskMetrics.outputMetrics should not be ('defined)
          taskMetrics.shuffleWriteMetrics should be ('defined)
          taskMetrics.shuffleWriteMetrics.get.shuffleBytesWritten should be > (0l)
        }
        if (stageInfo.rddInfos.exists(_.name == d4.name)) {
          taskMetrics.shuffleReadMetrics should be ('defined)
          val sm = taskMetrics.shuffleReadMetrics.get
          sm.totalBlocksFetched should be (128)
          sm.localBlocksFetched should be (128)
          sm.remoteBlocksFetched should be (0)
          sm.remoteBytesRead should be (0l)
        }
      }
    }
  }

  test("onTaskGettingResult() called when result fetched remotely") {
    val listener = new SaveTaskEvents
    sc.addSparkListener(listener)

    // Make a task whose result is larger than the akka frame size
    System.setProperty("spark.akka.frameSize", "1")
    val akkaFrameSize =
      sc.env.actorSystem.settings.config.getBytes("akka.remote.netty.tcp.maximum-frame-size").toInt
    val result = sc.parallelize(Seq(1), 1)
      .map { x => 1.to(akkaFrameSize).toArray }
      .reduce { case (x, y) => x }
    assert(result === 1.to(akkaFrameSize).toArray)

    assert(sc.listenerBus.waitUntilEmpty(WAIT_TIMEOUT_MILLIS))
    val TASK_INDEX = 0
    assert(listener.startedTasks.contains(TASK_INDEX))
    assert(listener.startedGettingResultTasks.contains(TASK_INDEX))
    assert(listener.endedTasks.contains(TASK_INDEX))
  }

  test("onTaskGettingResult() not called when result sent directly") {
    val listener = new SaveTaskEvents
    sc.addSparkListener(listener)

    // Make a task whose result is larger than the akka frame size
    val result = sc.parallelize(Seq(1), 1).map(2 * _).reduce { case (x, y) => x }
    assert(result === 2)

    assert(sc.listenerBus.waitUntilEmpty(WAIT_TIMEOUT_MILLIS))
    val TASK_INDEX = 0
    assert(listener.startedTasks.contains(TASK_INDEX))
    assert(listener.startedGettingResultTasks.isEmpty)
    assert(listener.endedTasks.contains(TASK_INDEX))
  }

  test("onTaskEnd() should be called for all started tasks, even after job has been killed") {
    val WAIT_TIMEOUT_MILLIS = 10000
    val listener = new SaveTaskEvents
    sc.addSparkListener(listener)

    val numTasks = 10
    val f = sc.parallelize(1 to 10000, numTasks).map { i => Thread.sleep(10); i }.countAsync()
    // Wait until one task has started (because we want to make sure that any tasks that are started
    // have corresponding end events sent to the listener).
    var finishTime = System.currentTimeMillis + WAIT_TIMEOUT_MILLIS
    listener.synchronized {
      var remainingWait = finishTime - System.currentTimeMillis
      while (listener.startedTasks.isEmpty && remainingWait > 0) {
        listener.wait(remainingWait)
        remainingWait = finishTime - System.currentTimeMillis
      }
      assert(!listener.startedTasks.isEmpty)
    }

    f.cancel()

    // Ensure that onTaskEnd is called for all started tasks.
    finishTime = System.currentTimeMillis + WAIT_TIMEOUT_MILLIS
    listener.synchronized {
      var remainingWait = finishTime - System.currentTimeMillis
      while (listener.endedTasks.size < listener.startedTasks.size && remainingWait > 0) {
        listener.wait(finishTime - System.currentTimeMillis)
        remainingWait = finishTime - System.currentTimeMillis
      }
      assert(listener.endedTasks.size === listener.startedTasks.size)
    }
  }

  test("SparkListener moves on if a listener throws an exception") {
    val badListener = new BadListener
    val jobCounter1 = new BasicJobCounter
    val jobCounter2 = new BasicJobCounter
    val bus = new LiveListenerBus

    // Propagate events to bad listener first
    bus.addListener(badListener)
    bus.addListener(jobCounter1)
    bus.addListener(jobCounter2)
    bus.start()

    // Post events to all listeners, and wait until the queue is drained
    (1 to 5).foreach { _ => bus.post(SparkListenerJobEnd(0, JobSucceeded)) }
    assert(bus.waitUntilEmpty(WAIT_TIMEOUT_MILLIS))

    // The exception should be caught, and the event should be propagated to other listeners
    assert(bus.listenerThreadIsAlive)
    assert(jobCounter1.count === 5)
    assert(jobCounter2.count === 5)
  }

  /**
   * Assert that the given list of numbers has an average that is greater than zero.
   */
  private def checkNonZeroAvg(m: Traversable[Long], msg: String) {
    assert(m.sum / m.size.toDouble > 0.0, msg)
  }

  /**
   * A simple listener that counts the number of jobs observed.
   */
  private class BasicJobCounter extends SparkListener {
    var count = 0
    override def onJobEnd(job: SparkListenerJobEnd) = count += 1
  }

  /**
   * A simple listener that saves all task infos and task metrics.
   */
  private class SaveStageAndTaskInfo extends SparkListener {
    val stageInfos = mutable.Map[StageInfo, Seq[(TaskInfo, TaskMetrics)]]()
    var taskInfoMetrics = mutable.Buffer[(TaskInfo, TaskMetrics)]()

    override def onTaskEnd(task: SparkListenerTaskEnd) {
      val info = task.taskInfo
      val metrics = task.taskMetrics
      if (info != null && metrics != null) {
        taskInfoMetrics += ((info, metrics))
      }
    }

    override def onStageCompleted(stage: SparkListenerStageCompleted) {
      stageInfos(stage.stageInfo) = taskInfoMetrics
      taskInfoMetrics = mutable.Buffer.empty
    }
  }

  /**
   * A simple listener that saves the task indices for all task events.
   */
  private class SaveTaskEvents extends SparkListener {
    val startedTasks = new mutable.HashSet[Int]()
    val startedGettingResultTasks = new mutable.HashSet[Int]()
    val endedTasks = new mutable.HashSet[Int]()

    override def onTaskStart(taskStart: SparkListenerTaskStart) = synchronized {
      startedTasks += taskStart.taskInfo.index
      notify()
    }

    override def onTaskEnd(taskEnd: SparkListenerTaskEnd) = synchronized {
      endedTasks += taskEnd.taskInfo.index
      notify()
    }

    override def onTaskGettingResult(taskGettingResult: SparkListenerTaskGettingResult) {
      startedGettingResultTasks += taskGettingResult.taskInfo.index
    }
  }

  /**
   * A simple listener that throws an exception on job end.
   */
  private class BadListener extends SparkListener {
    override def onJobEnd(jobEnd: SparkListenerJobEnd) = { throw new Exception }
  }

}
