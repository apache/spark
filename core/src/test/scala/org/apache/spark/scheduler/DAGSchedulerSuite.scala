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

import scala.collection.mutable.{ArrayBuffer, HashSet, HashMap, Map}
import scala.language.reflectiveCalls
import scala.util.control.NonFatal

import org.scalatest.BeforeAndAfter
import org.scalatest.concurrent.Timeouts
import org.scalatest.time.SpanSugar._

import org.apache.spark._
import org.apache.spark.rdd.RDD
import org.apache.spark.scheduler.SchedulingMode.SchedulingMode
import org.apache.spark.storage.{BlockId, BlockManagerId, BlockManagerMaster}
import org.apache.spark.util.CallSite
import org.apache.spark.executor.TaskMetrics

class DAGSchedulerEventProcessLoopTester(dagScheduler: DAGScheduler)
  extends DAGSchedulerEventProcessLoop(dagScheduler) {

  override def post(event: DAGSchedulerEvent): Unit = {
    try {
      // Forward event to `onReceive` directly to avoid processing event asynchronously.
      onReceive(event)
    } catch {
      case NonFatal(e) => onError(e)
    }
  }
}

/**
 * An RDD for passing to DAGScheduler. These RDDs will use the dependencies and
 * preferredLocations (if any) that are passed to them. They are deliberately not executable
 * so we can test that DAGScheduler does not try to execute RDDs locally.
 */
class MyRDD(
    sc: SparkContext,
    numPartitions: Int,
    dependencies: List[Dependency[_]],
    locations: Seq[Seq[String]] = Nil) extends RDD[(Int, Int)](sc, dependencies) with Serializable {
  override def compute(split: Partition, context: TaskContext): Iterator[(Int, Int)] =
    throw new RuntimeException("should not be reached")
  override def getPartitions: Array[Partition] = (0 until numPartitions).map(i => new Partition {
    override def index: Int = i
  }).toArray
  override def getPreferredLocations(split: Partition): Seq[String] =
    if (locations.isDefinedAt(split.index)) locations(split.index) else Nil
  override def toString: String = "DAGSchedulerSuiteRDD " + id
}

class DAGSchedulerSuiteDummyException extends Exception

class DAGSchedulerSuite
  extends SparkFunSuite with BeforeAndAfter with LocalSparkContext with Timeouts {

  val conf = new SparkConf
  /** Set of TaskSets the DAGScheduler has requested executed. */
  val taskSets = scala.collection.mutable.Buffer[TaskSet]()

  /** Stages for which the DAGScheduler has called TaskScheduler.cancelTasks(). */
  val cancelledStages = new HashSet[Int]()

  val taskScheduler = new TaskScheduler() {
    override def rootPool: Pool = null
    override def schedulingMode: SchedulingMode = SchedulingMode.NONE
    override def start() = {}
    override def stop() = {}
    override def executorHeartbeatReceived(execId: String, taskMetrics: Array[(Long, TaskMetrics)],
      blockManagerId: BlockManagerId): Boolean = true
    override def submitTasks(taskSet: TaskSet) = {
      // normally done by TaskSetManager
      taskSet.tasks.foreach(_.epoch = mapOutputTracker.getEpoch)
      taskSets += taskSet
    }
    override def cancelTasks(stageId: Int, interruptThread: Boolean) {
      cancelledStages += stageId
    }
    override def setDAGScheduler(dagScheduler: DAGScheduler) = {}
    override def defaultParallelism() = 2
    override def executorLost(executorId: String, reason: ExecutorLossReason): Unit = {}
    override def applicationAttemptId(): Option[String] = None
  }

  /** Length of time to wait while draining listener events. */
  val WAIT_TIMEOUT_MILLIS = 10000
  val sparkListener = new SparkListener() {
    val submittedStageInfos = new HashSet[StageInfo]
    val successfulStages = new HashSet[Int]
    val failedStages = new ArrayBuffer[Int]
    val stageByOrderOfExecution = new ArrayBuffer[Int]

    override def onStageSubmitted(stageSubmitted: SparkListenerStageSubmitted) {
      submittedStageInfos += stageSubmitted.stageInfo
    }

    override def onStageCompleted(stageCompleted: SparkListenerStageCompleted) {
      val stageInfo = stageCompleted.stageInfo
      stageByOrderOfExecution += stageInfo.stageId
      if (stageInfo.failureReason.isEmpty) {
        successfulStages += stageInfo.stageId
      } else {
        failedStages += stageInfo.stageId
      }
    }
  }

  var mapOutputTracker: MapOutputTrackerMaster = null
  var scheduler: DAGScheduler = null
  var dagEventProcessLoopTester: DAGSchedulerEventProcessLoop = null

  /**
   * Set of cache locations to return from our mock BlockManagerMaster.
   * Keys are (rdd ID, partition ID). Anything not present will return an empty
   * list of cache locations silently.
   */
  val cacheLocations = new HashMap[(Int, Int), Seq[BlockManagerId]]
  // stub out BlockManagerMaster.getLocations to use our cacheLocations
  val blockManagerMaster = new BlockManagerMaster(null, conf, true) {
      override def getLocations(blockIds: Array[BlockId]): IndexedSeq[Seq[BlockManagerId]] = {
        blockIds.map {
          _.asRDDId.map(id => (id.rddId -> id.splitIndex)).flatMap(key => cacheLocations.get(key)).
            getOrElse(Seq())
        }.toIndexedSeq
      }
      override def removeExecutor(execId: String) {
        // don't need to propagate to the driver, which we don't have
      }
    }

  /** The list of results that DAGScheduler has collected. */
  val results = new HashMap[Int, Any]()
  var failure: Exception = _
  val jobListener = new JobListener() {
    override def taskSucceeded(index: Int, result: Any) = results.put(index, result)
    override def jobFailed(exception: Exception) = { failure = exception }
  }

  before {
    sc = new SparkContext("local", "DAGSchedulerSuite")
    sparkListener.submittedStageInfos.clear()
    sparkListener.successfulStages.clear()
    sparkListener.failedStages.clear()
    failure = null
    sc.addSparkListener(sparkListener)
    taskSets.clear()
    cancelledStages.clear()
    cacheLocations.clear()
    results.clear()
    mapOutputTracker = new MapOutputTrackerMaster(conf)
    scheduler = new DAGScheduler(
        sc,
        taskScheduler,
        sc.listenerBus,
        mapOutputTracker,
        blockManagerMaster,
        sc.env)
    dagEventProcessLoopTester = new DAGSchedulerEventProcessLoopTester(scheduler)
  }

  after {
    scheduler.stop()
  }

  override def afterAll() {
    super.afterAll()
  }

  /**
   * Type of RDD we use for testing. Note that we should never call the real RDD compute methods.
   * This is a pair RDD type so it can always be used in ShuffleDependencies.
   */
  type PairOfIntsRDD = RDD[(Int, Int)]

  /**
   * Process the supplied event as if it were the top of the DAGScheduler event queue, expecting
   * the scheduler not to exit.
   *
   * After processing the event, submit waiting stages as is done on most iterations of the
   * DAGScheduler event loop.
   */
  private def runEvent(event: DAGSchedulerEvent) {
    dagEventProcessLoopTester.post(event)
  }

  /**
   * When we submit dummy Jobs, this is the compute function we supply. Except in a local test
   * below, we do not expect this function to ever be executed; instead, we will return results
   * directly through CompletionEvents.
   */
  private val jobComputeFunc = (context: TaskContext, it: Iterator[(_)]) =>
     it.next.asInstanceOf[Tuple2[_, _]]._1

  /** Send the given CompletionEvent messages for the tasks in the TaskSet. */
  private def complete(taskSet: TaskSet, results: Seq[(TaskEndReason, Any)]) {
    assert(taskSet.tasks.size >= results.size)
    for ((result, i) <- results.zipWithIndex) {
      if (i < taskSet.tasks.size) {
        runEvent(CompletionEvent(
          taskSet.tasks(i), result._1, result._2, null, createFakeTaskInfo(), null))
      }
    }
  }

  private def completeWithAccumulator(accumId: Long, taskSet: TaskSet,
                                      results: Seq[(TaskEndReason, Any)]) {
    assert(taskSet.tasks.size >= results.size)
    for ((result, i) <- results.zipWithIndex) {
      if (i < taskSet.tasks.size) {
        runEvent(CompletionEvent(taskSet.tasks(i), result._1, result._2,
          Map[Long, Any]((accumId, 1)), createFakeTaskInfo(), null))
      }
    }
  }

  /** Sends the rdd to the scheduler for scheduling and returns the job id. */
  private def submit(
      rdd: RDD[_],
      partitions: Array[Int],
      func: (TaskContext, Iterator[_]) => _ = jobComputeFunc,
      listener: JobListener = jobListener): Int = {
    val jobId = scheduler.nextJobId.getAndIncrement()
    runEvent(JobSubmitted(jobId, rdd, func, partitions, CallSite("", ""), listener))
    jobId
  }

  /** Sends TaskSetFailed to the scheduler. */
  private def failed(taskSet: TaskSet, message: String) {
    runEvent(TaskSetFailed(taskSet, message, None))
  }

  /** Sends JobCancelled to the DAG scheduler. */
  private def cancel(jobId: Int) {
    runEvent(JobCancelled(jobId))
  }

  test("[SPARK-3353] parent stage should have lower stage id") {
    sparkListener.stageByOrderOfExecution.clear()
    sc.parallelize(1 to 10).map(x => (x, x)).reduceByKey(_ + _, 4).count()
    sc.listenerBus.waitUntilEmpty(WAIT_TIMEOUT_MILLIS)
    assert(sparkListener.stageByOrderOfExecution.length === 2)
    assert(sparkListener.stageByOrderOfExecution(0) < sparkListener.stageByOrderOfExecution(1))
  }

  test("zero split job") {
    var numResults = 0
    val fakeListener = new JobListener() {
      override def taskSucceeded(partition: Int, value: Any) = numResults += 1
      override def jobFailed(exception: Exception) = throw exception
    }
    val jobId = submit(new MyRDD(sc, 0, Nil), Array(), listener = fakeListener)
    assert(numResults === 0)
    cancel(jobId)
  }

  test("run trivial job") {
    submit(new MyRDD(sc, 1, Nil), Array(0))
    complete(taskSets(0), List((Success, 42)))
    assert(results === Map(0 -> 42))
    assertDataStructuresEmpty()
  }

  test("run trivial job w/ dependency") {
    val baseRdd = new MyRDD(sc, 1, Nil)
    val finalRdd = new MyRDD(sc, 1, List(new OneToOneDependency(baseRdd)))
    submit(finalRdd, Array(0))
    complete(taskSets(0), Seq((Success, 42)))
    assert(results === Map(0 -> 42))
    assertDataStructuresEmpty()
  }

  test("cache location preferences w/ dependency") {
    val baseRdd = new MyRDD(sc, 1, Nil).cache()
    val finalRdd = new MyRDD(sc, 1, List(new OneToOneDependency(baseRdd)))
    cacheLocations(baseRdd.id -> 0) =
      Seq(makeBlockManagerId("hostA"), makeBlockManagerId("hostB"))
    submit(finalRdd, Array(0))
    val taskSet = taskSets(0)
    assertLocations(taskSet, Seq(Seq("hostA", "hostB")))
    complete(taskSet, Seq((Success, 42)))
    assert(results === Map(0 -> 42))
    assertDataStructuresEmpty()
  }

  test("regression test for getCacheLocs") {
    val rdd = new MyRDD(sc, 3, Nil).cache()
    cacheLocations(rdd.id -> 0) =
      Seq(makeBlockManagerId("hostA"), makeBlockManagerId("hostB"))
    cacheLocations(rdd.id -> 1) =
      Seq(makeBlockManagerId("hostB"), makeBlockManagerId("hostC"))
    cacheLocations(rdd.id -> 2) =
      Seq(makeBlockManagerId("hostC"), makeBlockManagerId("hostD"))
    val locs = scheduler.getCacheLocs(rdd).map(_.map(_.host))
    assert(locs === Seq(Seq("hostA", "hostB"), Seq("hostB", "hostC"), Seq("hostC", "hostD")))
  }

  /**
   * This test ensures that if a particular RDD is cached, RDDs earlier in the dependency chain
   * are not computed. It constructs the following chain of dependencies:
   * +---+ shuffle +---+    +---+    +---+
   * | A |<--------| B |<---| C |<---| D |
   * +---+         +---+    +---+    +---+
   * Here, B is derived from A by performing a shuffle, C has a one-to-one dependency on B,
   * and D similarly has a one-to-one dependency on C. If none of the RDDs were cached, this
   * set of RDDs would result in a two stage job: one ShuffleMapStage, and a ResultStage that
   * reads the shuffled data from RDD A. This test ensures that if C is cached, the scheduler
   * doesn't perform a shuffle, and instead computes the result using a single ResultStage
   * that reads C's cached data.
   */
  test("getMissingParentStages should consider all ancestor RDDs' cache statuses") {
    val rddA = new MyRDD(sc, 1, Nil)
    val rddB = new MyRDD(sc, 1, List(new ShuffleDependency(rddA, null)))
    val rddC = new MyRDD(sc, 1, List(new OneToOneDependency(rddB))).cache()
    val rddD = new MyRDD(sc, 1, List(new OneToOneDependency(rddC)))
    cacheLocations(rddC.id -> 0) =
      Seq(makeBlockManagerId("hostA"), makeBlockManagerId("hostB"))
    submit(rddD, Array(0))
    assert(scheduler.runningStages.size === 1)
    // Make sure that the scheduler is running the final result stage.
    // Because C is cached, the shuffle map stage to compute A does not need to be run.
    assert(scheduler.runningStages.head.isInstanceOf[ResultStage])
  }

  test("avoid exponential blowup when getting preferred locs list") {
    // Build up a complex dependency graph with repeated zip operations, without preferred locations
    var rdd: RDD[_] = new MyRDD(sc, 1, Nil)
    (1 to 30).foreach(_ => rdd = rdd.zip(rdd))
    // getPreferredLocs runs quickly, indicating that exponential graph traversal is avoided.
    failAfter(10 seconds) {
      val preferredLocs = scheduler.getPreferredLocs(rdd, 0)
      // No preferred locations are returned.
      assert(preferredLocs.length === 0)
    }
  }

  test("unserializable task") {
    val unserializableRdd = new MyRDD(sc, 1, Nil) {
      class UnserializableClass
      val unserializable = new UnserializableClass
    }
    submit(unserializableRdd, Array(0))
    assert(failure.getMessage.startsWith(
      "Job aborted due to stage failure: Task not serializable:"))
    sc.listenerBus.waitUntilEmpty(WAIT_TIMEOUT_MILLIS)
    assert(sparkListener.failedStages.contains(0))
    assert(sparkListener.failedStages.size === 1)
    assertDataStructuresEmpty()
  }

  test("trivial job failure") {
    submit(new MyRDD(sc, 1, Nil), Array(0))
    failed(taskSets(0), "some failure")
    assert(failure.getMessage === "Job aborted due to stage failure: some failure")
    sc.listenerBus.waitUntilEmpty(WAIT_TIMEOUT_MILLIS)
    assert(sparkListener.failedStages.contains(0))
    assert(sparkListener.failedStages.size === 1)
    assertDataStructuresEmpty()
  }

  test("trivial job cancellation") {
    val rdd = new MyRDD(sc, 1, Nil)
    val jobId = submit(rdd, Array(0))
    cancel(jobId)
    assert(failure.getMessage === s"Job $jobId cancelled ")
    sc.listenerBus.waitUntilEmpty(WAIT_TIMEOUT_MILLIS)
    assert(sparkListener.failedStages.contains(0))
    assert(sparkListener.failedStages.size === 1)
    assertDataStructuresEmpty()
  }

  test("job cancellation no-kill backend") {
    // make sure that the DAGScheduler doesn't crash when the TaskScheduler
    // doesn't implement killTask()
    val noKillTaskScheduler = new TaskScheduler() {
      override def rootPool: Pool = null
      override def schedulingMode: SchedulingMode = SchedulingMode.NONE
      override def start(): Unit = {}
      override def stop(): Unit = {}
      override def submitTasks(taskSet: TaskSet): Unit = {
        taskSets += taskSet
      }
      override def cancelTasks(stageId: Int, interruptThread: Boolean) {
        throw new UnsupportedOperationException
      }
      override def setDAGScheduler(dagScheduler: DAGScheduler): Unit = {}
      override def defaultParallelism(): Int = 2
      override def executorHeartbeatReceived(
          execId: String,
          taskMetrics: Array[(Long, TaskMetrics)],
          blockManagerId: BlockManagerId): Boolean = true
      override def executorLost(executorId: String, reason: ExecutorLossReason): Unit = {}
      override def applicationAttemptId(): Option[String] = None
    }
    val noKillScheduler = new DAGScheduler(
      sc,
      noKillTaskScheduler,
      sc.listenerBus,
      mapOutputTracker,
      blockManagerMaster,
      sc.env)
    dagEventProcessLoopTester = new DAGSchedulerEventProcessLoopTester(noKillScheduler)
    val jobId = submit(new MyRDD(sc, 1, Nil), Array(0))
    cancel(jobId)
    // Because the job wasn't actually cancelled, we shouldn't have received a failure message.
    assert(failure === null)

    // When the task set completes normally, state should be correctly updated.
    complete(taskSets(0), Seq((Success, 42)))
    assert(results === Map(0 -> 42))
    assertDataStructuresEmpty()

    sc.listenerBus.waitUntilEmpty(WAIT_TIMEOUT_MILLIS)
    assert(sparkListener.failedStages.isEmpty)
    assert(sparkListener.successfulStages.contains(0))
  }

  test("run trivial shuffle") {
    val shuffleMapRdd = new MyRDD(sc, 2, Nil)
    val shuffleDep = new ShuffleDependency(shuffleMapRdd, null)
    val shuffleId = shuffleDep.shuffleId
    val reduceRdd = new MyRDD(sc, 1, List(shuffleDep))
    submit(reduceRdd, Array(0))
    complete(taskSets(0), Seq(
        (Success, makeMapStatus("hostA", 1)),
        (Success, makeMapStatus("hostB", 1))))
    assert(mapOutputTracker.getMapSizesByExecutorId(shuffleId, 0).map(_._1).toSet ===
      HashSet(makeBlockManagerId("hostA"), makeBlockManagerId("hostB")))
    complete(taskSets(1), Seq((Success, 42)))
    assert(results === Map(0 -> 42))
    assertDataStructuresEmpty()
  }

  test("run trivial shuffle with fetch failure") {
    val shuffleMapRdd = new MyRDD(sc, 2, Nil)
    val shuffleDep = new ShuffleDependency(shuffleMapRdd, null)
    val shuffleId = shuffleDep.shuffleId
    val reduceRdd = new MyRDD(sc, 2, List(shuffleDep))
    submit(reduceRdd, Array(0, 1))
    complete(taskSets(0), Seq(
        (Success, makeMapStatus("hostA", reduceRdd.partitions.size)),
        (Success, makeMapStatus("hostB", reduceRdd.partitions.size))))
    // the 2nd ResultTask failed
    complete(taskSets(1), Seq(
        (Success, 42),
        (FetchFailed(makeBlockManagerId("hostA"), shuffleId, 0, 0, "ignored"), null)))
    // this will get called
    // blockManagerMaster.removeExecutor("exec-hostA")
    // ask the scheduler to try it again
    scheduler.resubmitFailedStages()
    // have the 2nd attempt pass
    complete(taskSets(2), Seq((Success, makeMapStatus("hostA", reduceRdd.partitions.size))))
    // we can see both result blocks now
    assert(mapOutputTracker.getMapSizesByExecutorId(shuffleId, 0).map(_._1.host).toSet ===
      HashSet("hostA", "hostB"))
    complete(taskSets(3), Seq((Success, 43)))
    assert(results === Map(0 -> 42, 1 -> 43))
    assertDataStructuresEmpty()
  }

  test("trivial shuffle with multiple fetch failures") {
    val shuffleMapRdd = new MyRDD(sc, 2, Nil)
    val shuffleDep = new ShuffleDependency(shuffleMapRdd, null)
    val shuffleId = shuffleDep.shuffleId
    val reduceRdd = new MyRDD(sc, 2, List(shuffleDep))
    submit(reduceRdd, Array(0, 1))
    complete(taskSets(0), Seq(
      (Success, makeMapStatus("hostA", reduceRdd.partitions.size)),
      (Success, makeMapStatus("hostB", reduceRdd.partitions.size))))
    // The MapOutputTracker should know about both map output locations.
    assert(mapOutputTracker.getMapSizesByExecutorId(shuffleId, 0).map(_._1.host).toSet ===
      HashSet("hostA", "hostB"))

    // The first result task fails, with a fetch failure for the output from the first mapper.
    runEvent(CompletionEvent(
      taskSets(1).tasks(0),
      FetchFailed(makeBlockManagerId("hostA"), shuffleId, 0, 0, "ignored"),
      null,
      Map[Long, Any](),
      createFakeTaskInfo(),
      null))
    sc.listenerBus.waitUntilEmpty(WAIT_TIMEOUT_MILLIS)
    assert(sparkListener.failedStages.contains(1))

    // The second ResultTask fails, with a fetch failure for the output from the second mapper.
    runEvent(CompletionEvent(
      taskSets(1).tasks(0),
      FetchFailed(makeBlockManagerId("hostA"), shuffleId, 1, 1, "ignored"),
      null,
      Map[Long, Any](),
      createFakeTaskInfo(),
      null))
    // The SparkListener should not receive redundant failure events.
    sc.listenerBus.waitUntilEmpty(WAIT_TIMEOUT_MILLIS)
    assert(sparkListener.failedStages.size == 1)
  }

  /**
   * This tests the case where another FetchFailed comes in while the map stage is getting
   * re-run.
   */
  test("late fetch failures don't cause multiple concurrent attempts for the same map stage") {
    val shuffleMapRdd = new MyRDD(sc, 2, Nil)
    val shuffleDep = new ShuffleDependency(shuffleMapRdd, null)
    val shuffleId = shuffleDep.shuffleId
    val reduceRdd = new MyRDD(sc, 2, List(shuffleDep))
    submit(reduceRdd, Array(0, 1))

    val mapStageId = 0
    def countSubmittedMapStageAttempts(): Int = {
      sparkListener.submittedStageInfos.count(_.stageId == mapStageId)
    }

    // The map stage should have been submitted.
    assert(countSubmittedMapStageAttempts() === 1)

    complete(taskSets(0), Seq(
      (Success, makeMapStatus("hostA", 2)),
      (Success, makeMapStatus("hostB", 2))))
    // The MapOutputTracker should know about both map output locations.
    assert(mapOutputTracker.getMapSizesByExecutorId(shuffleId, 0).map(_._1.host).toSet ===
      HashSet("hostA", "hostB"))
    assert(mapOutputTracker.getMapSizesByExecutorId(shuffleId, 1).map(_._1.host).toSet ===
      HashSet("hostA", "hostB"))

    // The first result task fails, with a fetch failure for the output from the first mapper.
    runEvent(CompletionEvent(
      taskSets(1).tasks(0),
      FetchFailed(makeBlockManagerId("hostA"), shuffleId, 0, 0, "ignored"),
      null,
      Map[Long, Any](),
      createFakeTaskInfo(),
      null))
    sc.listenerBus.waitUntilEmpty(WAIT_TIMEOUT_MILLIS)
    assert(sparkListener.failedStages.contains(1))

    // Trigger resubmission of the failed map stage.
    runEvent(ResubmitFailedStages)
    sc.listenerBus.waitUntilEmpty(WAIT_TIMEOUT_MILLIS)

    // Another attempt for the map stage should have been submitted, resulting in 2 total attempts.
    assert(countSubmittedMapStageAttempts() === 2)

    // The second ResultTask fails, with a fetch failure for the output from the second mapper.
    runEvent(CompletionEvent(
      taskSets(1).tasks(1),
      FetchFailed(makeBlockManagerId("hostB"), shuffleId, 1, 1, "ignored"),
      null,
      Map[Long, Any](),
      createFakeTaskInfo(),
      null))

    // Another ResubmitFailedStages event should not result in another attempt for the map
    // stage being run concurrently.
    // NOTE: the actual ResubmitFailedStages may get called at any time during this, but it
    // shouldn't effect anything -- our calling it just makes *SURE* it gets called between the
    // desired event and our check.
    runEvent(ResubmitFailedStages)
    sc.listenerBus.waitUntilEmpty(WAIT_TIMEOUT_MILLIS)
    assert(countSubmittedMapStageAttempts() === 2)

  }

  /**
    * This tests the case where a late FetchFailed comes in after the map stage has finished getting
    * retried and a new reduce stage starts running.
    */
  test("extremely late fetch failures don't cause multiple concurrent attempts for " +
      "the same stage") {
    val shuffleMapRdd = new MyRDD(sc, 2, Nil)
    val shuffleDep = new ShuffleDependency(shuffleMapRdd, null)
    val shuffleId = shuffleDep.shuffleId
    val reduceRdd = new MyRDD(sc, 2, List(shuffleDep))
    submit(reduceRdd, Array(0, 1))

    def countSubmittedReduceStageAttempts(): Int = {
      sparkListener.submittedStageInfos.count(_.stageId == 1)
    }
    def countSubmittedMapStageAttempts(): Int = {
      sparkListener.submittedStageInfos.count(_.stageId == 0)
    }

    // The map stage should have been submitted.
    sc.listenerBus.waitUntilEmpty(WAIT_TIMEOUT_MILLIS)
    assert(countSubmittedMapStageAttempts() === 1)

    // Complete the map stage.
    complete(taskSets(0), Seq(
      (Success, makeMapStatus("hostA", 2)),
      (Success, makeMapStatus("hostB", 2))))

    // The reduce stage should have been submitted.
    sc.listenerBus.waitUntilEmpty(WAIT_TIMEOUT_MILLIS)
    assert(countSubmittedReduceStageAttempts() === 1)

    // The first result task fails, with a fetch failure for the output from the first mapper.
    runEvent(CompletionEvent(
      taskSets(1).tasks(0),
      FetchFailed(makeBlockManagerId("hostA"), shuffleId, 0, 0, "ignored"),
      null,
      Map[Long, Any](),
      createFakeTaskInfo(),
      null))

    // Trigger resubmission of the failed map stage and finish the re-started map task.
    runEvent(ResubmitFailedStages)
    complete(taskSets(2), Seq((Success, makeMapStatus("hostA", 1))))

    // Because the map stage finished, another attempt for the reduce stage should have been
    // submitted, resulting in 2 total attempts for each the map and the reduce stage.
    sc.listenerBus.waitUntilEmpty(WAIT_TIMEOUT_MILLIS)
    assert(countSubmittedMapStageAttempts() === 2)
    assert(countSubmittedReduceStageAttempts() === 2)

    // A late FetchFailed arrives from the second task in the original reduce stage.
    runEvent(CompletionEvent(
      taskSets(1).tasks(1),
      FetchFailed(makeBlockManagerId("hostB"), shuffleId, 1, 1, "ignored"),
      null,
      Map[Long, Any](),
      createFakeTaskInfo(),
      null))

    // Running ResubmitFailedStages shouldn't result in any more attempts for the map stage, because
    // the FetchFailed should have been ignored
    runEvent(ResubmitFailedStages)

    // The FetchFailed from the original reduce stage should be ignored.
    assert(countSubmittedMapStageAttempts() === 2)
  }

  test("ignore late map task completions") {
    val shuffleMapRdd = new MyRDD(sc, 2, Nil)
    val shuffleDep = new ShuffleDependency(shuffleMapRdd, null)
    val shuffleId = shuffleDep.shuffleId
    val reduceRdd = new MyRDD(sc, 2, List(shuffleDep))
    submit(reduceRdd, Array(0, 1))
    // pretend we were told hostA went away
    val oldEpoch = mapOutputTracker.getEpoch
    runEvent(ExecutorLost("exec-hostA"))
    val newEpoch = mapOutputTracker.getEpoch
    assert(newEpoch > oldEpoch)
    val taskSet = taskSets(0)
    // should be ignored for being too old
    runEvent(CompletionEvent(taskSet.tasks(0), Success, makeMapStatus("hostA",
      reduceRdd.partitions.size), null, createFakeTaskInfo(), null))
    // should work because it's a non-failed host
    runEvent(CompletionEvent(taskSet.tasks(0), Success, makeMapStatus("hostB",
      reduceRdd.partitions.size), null, createFakeTaskInfo(), null))
    // should be ignored for being too old
    runEvent(CompletionEvent(taskSet.tasks(0), Success, makeMapStatus("hostA",
      reduceRdd.partitions.size), null, createFakeTaskInfo(), null))
    // should work because it's a new epoch
    taskSet.tasks(1).epoch = newEpoch
    runEvent(CompletionEvent(taskSet.tasks(1), Success, makeMapStatus("hostA",
      reduceRdd.partitions.size), null, createFakeTaskInfo(), null))
    assert(mapOutputTracker.getMapSizesByExecutorId(shuffleId, 0).map(_._1).toSet ===
           HashSet(makeBlockManagerId("hostB"), makeBlockManagerId("hostA")))
    complete(taskSets(1), Seq((Success, 42), (Success, 43)))
    assert(results === Map(0 -> 42, 1 -> 43))
    assertDataStructuresEmpty()
  }

  test("run shuffle with map stage failure") {
    val shuffleMapRdd = new MyRDD(sc, 2, Nil)
    val shuffleDep = new ShuffleDependency(shuffleMapRdd, null)
    val reduceRdd = new MyRDD(sc, 2, List(shuffleDep))
    submit(reduceRdd, Array(0, 1))

    // Fail the map stage.  This should cause the entire job to fail.
    val stageFailureMessage = "Exception failure in map stage"
    failed(taskSets(0), stageFailureMessage)
    assert(failure.getMessage === s"Job aborted due to stage failure: $stageFailureMessage")

    // Listener bus should get told about the map stage failing, but not the reduce stage
    // (since the reduce stage hasn't been started yet).
    sc.listenerBus.waitUntilEmpty(WAIT_TIMEOUT_MILLIS)
    assert(sparkListener.failedStages.toSet === Set(0))

    assertDataStructuresEmpty()
  }

  /**
   * Makes sure that failures of stage used by multiple jobs are correctly handled.
   *
   * This test creates the following dependency graph:
   *
   * shuffleMapRdd1     shuffleMapRDD2
   *        |     \        |
   *        |      \       |
   *        |       \      |
   *        |        \     |
   *   reduceRdd1    reduceRdd2
   *
   * We start both shuffleMapRdds and then fail shuffleMapRdd1.  As a result, the job listeners for
   * reduceRdd1 and reduceRdd2 should both be informed that the job failed.  shuffleMapRDD2 should
   * also be cancelled, because it is only used by reduceRdd2 and reduceRdd2 cannot complete
   * without shuffleMapRdd1.
   */
  test("failure of stage used by two jobs") {
    val shuffleMapRdd1 = new MyRDD(sc, 2, Nil)
    val shuffleDep1 = new ShuffleDependency(shuffleMapRdd1, null)
    val shuffleMapRdd2 = new MyRDD(sc, 2, Nil)
    val shuffleDep2 = new ShuffleDependency(shuffleMapRdd2, null)

    val reduceRdd1 = new MyRDD(sc, 2, List(shuffleDep1))
    val reduceRdd2 = new MyRDD(sc, 2, List(shuffleDep1, shuffleDep2))

    // We need to make our own listeners for this test, since by default submit uses the same
    // listener for all jobs, and here we want to capture the failure for each job separately.
    class FailureRecordingJobListener() extends JobListener {
      var failureMessage: String = _
      override def taskSucceeded(index: Int, result: Any) {}
      override def jobFailed(exception: Exception): Unit = { failureMessage = exception.getMessage }
    }
    val listener1 = new FailureRecordingJobListener()
    val listener2 = new FailureRecordingJobListener()

    submit(reduceRdd1, Array(0, 1), listener = listener1)
    submit(reduceRdd2, Array(0, 1), listener = listener2)

    val stageFailureMessage = "Exception failure in map stage"
    failed(taskSets(0), stageFailureMessage)

    assert(cancelledStages.toSet === Set(0, 2))

    // Make sure the listeners got told about both failed stages.
    sc.listenerBus.waitUntilEmpty(WAIT_TIMEOUT_MILLIS)
    assert(sparkListener.successfulStages.isEmpty)
    assert(sparkListener.failedStages.toSet === Set(0, 2))

    assert(listener1.failureMessage === s"Job aborted due to stage failure: $stageFailureMessage")
    assert(listener2.failureMessage === s"Job aborted due to stage failure: $stageFailureMessage")
    assertDataStructuresEmpty()
  }

  test("run trivial shuffle with out-of-band failure and retry") {
    val shuffleMapRdd = new MyRDD(sc, 2, Nil)
    val shuffleDep = new ShuffleDependency(shuffleMapRdd, null)
    val shuffleId = shuffleDep.shuffleId
    val reduceRdd = new MyRDD(sc, 1, List(shuffleDep))
    submit(reduceRdd, Array(0))
    // blockManagerMaster.removeExecutor("exec-hostA")
    // pretend we were told hostA went away
    runEvent(ExecutorLost("exec-hostA"))
    // DAGScheduler will immediately resubmit the stage after it appears to have no pending tasks
    // rather than marking it is as failed and waiting.
    complete(taskSets(0), Seq(
        (Success, makeMapStatus("hostA", 1)),
       (Success, makeMapStatus("hostB", 1))))
    // have hostC complete the resubmitted task
    complete(taskSets(1), Seq((Success, makeMapStatus("hostC", 1))))
    assert(mapOutputTracker.getMapSizesByExecutorId(shuffleId, 0).map(_._1).toSet ===
           HashSet(makeBlockManagerId("hostC"), makeBlockManagerId("hostB")))
    complete(taskSets(2), Seq((Success, 42)))
    assert(results === Map(0 -> 42))
    assertDataStructuresEmpty()
  }

  test("recursive shuffle failures") {
    val shuffleOneRdd = new MyRDD(sc, 2, Nil)
    val shuffleDepOne = new ShuffleDependency(shuffleOneRdd, null)
    val shuffleTwoRdd = new MyRDD(sc, 2, List(shuffleDepOne))
    val shuffleDepTwo = new ShuffleDependency(shuffleTwoRdd, null)
    val finalRdd = new MyRDD(sc, 1, List(shuffleDepTwo))
    submit(finalRdd, Array(0))
    // have the first stage complete normally
    complete(taskSets(0), Seq(
        (Success, makeMapStatus("hostA", 2)),
        (Success, makeMapStatus("hostB", 2))))
    // have the second stage complete normally
    complete(taskSets(1), Seq(
        (Success, makeMapStatus("hostA", 1)),
        (Success, makeMapStatus("hostC", 1))))
    // fail the third stage because hostA went down
    complete(taskSets(2), Seq(
        (FetchFailed(makeBlockManagerId("hostA"), shuffleDepTwo.shuffleId, 0, 0, "ignored"), null)))
    // TODO assert this:
    // blockManagerMaster.removeExecutor("exec-hostA")
    // have DAGScheduler try again
    scheduler.resubmitFailedStages()
    complete(taskSets(3), Seq((Success, makeMapStatus("hostA", 2))))
    complete(taskSets(4), Seq((Success, makeMapStatus("hostA", 1))))
    complete(taskSets(5), Seq((Success, 42)))
    assert(results === Map(0 -> 42))
    assertDataStructuresEmpty()
  }

  test("cached post-shuffle") {
    val shuffleOneRdd = new MyRDD(sc, 2, Nil).cache()
    val shuffleDepOne = new ShuffleDependency(shuffleOneRdd, null)
    val shuffleTwoRdd = new MyRDD(sc, 2, List(shuffleDepOne)).cache()
    val shuffleDepTwo = new ShuffleDependency(shuffleTwoRdd, null)
    val finalRdd = new MyRDD(sc, 1, List(shuffleDepTwo))
    submit(finalRdd, Array(0))
    cacheLocations(shuffleTwoRdd.id -> 0) = Seq(makeBlockManagerId("hostD"))
    cacheLocations(shuffleTwoRdd.id -> 1) = Seq(makeBlockManagerId("hostC"))
    // complete stage 2
    complete(taskSets(0), Seq(
        (Success, makeMapStatus("hostA", 2)),
        (Success, makeMapStatus("hostB", 2))))
    // complete stage 1
    complete(taskSets(1), Seq(
        (Success, makeMapStatus("hostA", 1)),
        (Success, makeMapStatus("hostB", 1))))
    // pretend stage 0 failed because hostA went down
    complete(taskSets(2), Seq(
        (FetchFailed(makeBlockManagerId("hostA"), shuffleDepTwo.shuffleId, 0, 0, "ignored"), null)))
    // TODO assert this:
    // blockManagerMaster.removeExecutor("exec-hostA")
    // DAGScheduler should notice the cached copy of the second shuffle and try to get it rerun.
    scheduler.resubmitFailedStages()
    assertLocations(taskSets(3), Seq(Seq("hostD")))
    // allow hostD to recover
    complete(taskSets(3), Seq((Success, makeMapStatus("hostD", 1))))
    complete(taskSets(4), Seq((Success, 42)))
    assert(results === Map(0 -> 42))
    assertDataStructuresEmpty()
  }

  test("misbehaved accumulator should not crash DAGScheduler and SparkContext") {
    val acc = new Accumulator[Int](0, new AccumulatorParam[Int] {
      override def addAccumulator(t1: Int, t2: Int): Int = t1 + t2
      override def zero(initialValue: Int): Int = 0
      override def addInPlace(r1: Int, r2: Int): Int = {
        throw new DAGSchedulerSuiteDummyException
      }
    })

    // Run this on executors
    sc.parallelize(1 to 10, 2).foreach { item => acc.add(1) }

    // Make sure we can still run commands
    assert(sc.parallelize(1 to 10, 2).count() === 10)
  }

  test("misbehaved resultHandler should not crash DAGScheduler and SparkContext") {
    val e = intercept[SparkDriverExecutionException] {
      val rdd = sc.parallelize(1 to 10, 2)
      sc.runJob[Int, Int](
        rdd,
        (context: TaskContext, iter: Iterator[Int]) => iter.size,
        Seq(0, 1),
        (part: Int, result: Int) => throw new DAGSchedulerSuiteDummyException)
    }
    assert(e.getCause.isInstanceOf[DAGSchedulerSuiteDummyException])

    // Make sure we can still run commands
    assert(sc.parallelize(1 to 10, 2).count() === 10)
  }

  test("getPartitions exceptions should not crash DAGScheduler and SparkContext (SPARK-8606)") {
    val e1 = intercept[DAGSchedulerSuiteDummyException] {
      val rdd = new MyRDD(sc, 2, Nil) {
        override def getPartitions: Array[Partition] = {
          throw new DAGSchedulerSuiteDummyException
        }
      }
      rdd.reduceByKey(_ + _, 1).count()
    }

    // Make sure we can still run commands
    assert(sc.parallelize(1 to 10, 2).count() === 10)
  }

  test("getPreferredLocations errors should not crash DAGScheduler and SparkContext (SPARK-8606)") {
    val e1 = intercept[SparkException] {
      val rdd = new MyRDD(sc, 2, Nil) {
        override def getPreferredLocations(split: Partition): Seq[String] = {
          throw new DAGSchedulerSuiteDummyException
        }
      }
      rdd.count()
    }
    assert(e1.getMessage.contains(classOf[DAGSchedulerSuiteDummyException].getName))

    // Make sure we can still run commands
    assert(sc.parallelize(1 to 10, 2).count() === 10)
  }

  test("accumulator not calculated for resubmitted result stage") {
    // just for register
    val accum = new Accumulator[Int](0, AccumulatorParam.IntAccumulatorParam)
    val finalRdd = new MyRDD(sc, 1, Nil)
    submit(finalRdd, Array(0))
    completeWithAccumulator(accum.id, taskSets(0), Seq((Success, 42)))
    completeWithAccumulator(accum.id, taskSets(0), Seq((Success, 42)))
    assert(results === Map(0 -> 42))

    val accVal = Accumulators.originals(accum.id).get.get.value

    assert(accVal === 1)

    assertDataStructuresEmpty()
  }

  ignore("reduce tasks should be placed locally with map output") {
    // Create an shuffleMapRdd with 1 partition
    val shuffleMapRdd = new MyRDD(sc, 1, Nil)
    val shuffleDep = new ShuffleDependency(shuffleMapRdd, null)
    val shuffleId = shuffleDep.shuffleId
    val reduceRdd = new MyRDD(sc, 1, List(shuffleDep))
    submit(reduceRdd, Array(0))
    complete(taskSets(0), Seq(
        (Success, makeMapStatus("hostA", 1))))
    assert(mapOutputTracker.getMapSizesByExecutorId(shuffleId, 0).map(_._1).toSet ===
           HashSet(makeBlockManagerId("hostA")))

    // Reducer should run on the same host that map task ran
    val reduceTaskSet = taskSets(1)
    assertLocations(reduceTaskSet, Seq(Seq("hostA")))
    complete(reduceTaskSet, Seq((Success, 42)))
    assert(results === Map(0 -> 42))
    assertDataStructuresEmpty()
  }

  ignore("reduce task locality preferences should only include machines with largest map outputs") {
    val numMapTasks = 4
    // Create an shuffleMapRdd with more partitions
    val shuffleMapRdd = new MyRDD(sc, numMapTasks, Nil)
    val shuffleDep = new ShuffleDependency(shuffleMapRdd, null)
    val shuffleId = shuffleDep.shuffleId
    val reduceRdd = new MyRDD(sc, 1, List(shuffleDep))
    submit(reduceRdd, Array(0))

    val statuses = (1 to numMapTasks).map { i =>
      (Success, makeMapStatus("host" + i, 1, (10*i).toByte))
    }
    complete(taskSets(0), statuses)

    // Reducer should prefer the last 3 hosts as they have 20%, 30% and 40% of data
    val hosts = (1 to numMapTasks).map(i => "host" + i).reverse.take(numMapTasks - 1)

    val reduceTaskSet = taskSets(1)
    assertLocations(reduceTaskSet, Seq(hosts))
    complete(reduceTaskSet, Seq((Success, 42)))
    assert(results === Map(0 -> 42))
    assertDataStructuresEmpty()
  }

  test("stages with both narrow and shuffle dependencies use narrow ones for locality") {
    // Create an RDD that has both a shuffle dependency and a narrow dependency (e.g. for a join)
    val rdd1 = new MyRDD(sc, 1, Nil)
    val rdd2 = new MyRDD(sc, 1, Nil, locations = Seq(Seq("hostB")))
    val shuffleDep = new ShuffleDependency(rdd1, null)
    val narrowDep = new OneToOneDependency(rdd2)
    val shuffleId = shuffleDep.shuffleId
    val reduceRdd = new MyRDD(sc, 1, List(shuffleDep, narrowDep))
    submit(reduceRdd, Array(0))
    complete(taskSets(0), Seq(
      (Success, makeMapStatus("hostA", 1))))
    assert(mapOutputTracker.getMapSizesByExecutorId(shuffleId, 0).map(_._1).toSet ===
      HashSet(makeBlockManagerId("hostA")))

    // Reducer should run where RDD 2 has preferences, even though though it also has a shuffle dep
    val reduceTaskSet = taskSets(1)
    assertLocations(reduceTaskSet, Seq(Seq("hostB")))
    complete(reduceTaskSet, Seq((Success, 42)))
    assert(results === Map(0 -> 42))
    assertDataStructuresEmpty()
  }

  test("Spark exceptions should include call site in stack trace") {
    val e = intercept[SparkException] {
      sc.parallelize(1 to 10, 2).map { _ => throw new RuntimeException("uh-oh!") }.count()
    }

    // Does not include message, ONLY stack trace.
    val stackTraceString = e.getStackTraceString

    // should actually include the RDD operation that invoked the method:
    assert(stackTraceString.contains("org.apache.spark.rdd.RDD.count"))

    // should include the FunSuite setup:
    assert(stackTraceString.contains("org.scalatest.FunSuite"))
  }

  /**
   * Assert that the supplied TaskSet has exactly the given hosts as its preferred locations.
   * Note that this checks only the host and not the executor ID.
   */
  private def assertLocations(taskSet: TaskSet, hosts: Seq[Seq[String]]) {
    assert(hosts.size === taskSet.tasks.size)
    for ((taskLocs, expectedLocs) <- taskSet.tasks.map(_.preferredLocations).zip(hosts)) {
      assert(taskLocs.map(_.host).toSet === expectedLocs.toSet)
    }
  }

  private def makeMapStatus(host: String, reduces: Int, sizes: Byte = 2): MapStatus =
    MapStatus(makeBlockManagerId(host), Array.fill[Long](reduces)(sizes))

  private def makeBlockManagerId(host: String): BlockManagerId =
    BlockManagerId("exec-" + host, host, 12345)

  private def assertDataStructuresEmpty(): Unit = {
    assert(scheduler.activeJobs.isEmpty)
    assert(scheduler.failedStages.isEmpty)
    assert(scheduler.jobIdToActiveJob.isEmpty)
    assert(scheduler.jobIdToStageIds.isEmpty)
    assert(scheduler.stageIdToStage.isEmpty)
    assert(scheduler.runningStages.isEmpty)
    assert(scheduler.shuffleToMapStage.isEmpty)
    assert(scheduler.waitingStages.isEmpty)
    assert(scheduler.outputCommitCoordinator.isEmpty)
  }

  // Nothing in this test should break if the task info's fields are null, but
  // OutputCommitCoordinator requires the task info itself to not be null.
  private def createFakeTaskInfo(): TaskInfo = {
    val info = new TaskInfo(0, 0, 0, 0L, "", "", TaskLocality.ANY, false)
    info.finishTime = 1  // to prevent spurious errors in JobProgressListener
    info
  }

}

