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

import java.util.{ArrayList => JArrayList, Collections => JCollections, Properties}
import java.util.concurrent.{CountDownLatch, Delayed, ScheduledFuture, TimeUnit}
import java.util.concurrent.atomic.{AtomicBoolean, AtomicLong, AtomicReference}

import scala.annotation.meta.param
import scala.collection.JavaConverters._
import scala.collection.mutable.{ArrayBuffer, HashMap, HashSet, Map}
import scala.language.reflectiveCalls
import scala.util.control.NonFatal

import org.mockito.ArgumentMatchers.any
import org.mockito.Mockito._
import org.mockito.invocation.InvocationOnMock
import org.roaringbitmap.RoaringBitmap
import org.scalatest.concurrent.{Signaler, ThreadSignaler, TimeLimits}
import org.scalatest.exceptions.TestFailedException
import org.scalatest.time.SpanSugar._

import org.apache.spark._
import org.apache.spark.broadcast.BroadcastManager
import org.apache.spark.executor.ExecutorMetrics
import org.apache.spark.internal.config
import org.apache.spark.internal.config.Tests
import org.apache.spark.network.shuffle.ExternalBlockStoreClient
import org.apache.spark.rdd.{DeterministicLevel, RDD}
import org.apache.spark.resource.{ExecutorResourceRequests, ResourceProfile, ResourceProfileBuilder, TaskResourceProfile, TaskResourceRequests}
import org.apache.spark.resource.ResourceUtils.{FPGA, GPU}
import org.apache.spark.scheduler.SchedulingMode.SchedulingMode
import org.apache.spark.scheduler.local.LocalSchedulerBackend
import org.apache.spark.shuffle.{FetchFailedException, MetadataFetchFailedException}
import org.apache.spark.storage.{BlockId, BlockManager, BlockManagerId, BlockManagerMaster}
import org.apache.spark.util.{AccumulatorContext, AccumulatorV2, CallSite, Clock, LongAccumulator, SystemClock, Utils}

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

  override def onError(e: Throwable): Unit = {
    logError("Error in DAGSchedulerEventLoop: ", e)
    dagScheduler.stop()
    throw e
  }

}

class MyCheckpointRDD(
    sc: SparkContext,
    numPartitions: Int,
    dependencies: List[Dependency[_]],
    locations: Seq[Seq[String]] = Nil,
    @(transient @param) tracker: MapOutputTrackerMaster = null,
    indeterminate: Boolean = false)
  extends MyRDD(sc, numPartitions, dependencies, locations, tracker, indeterminate) {

  // Allow doCheckpoint() on this RDD.
  override def compute(split: Partition, context: TaskContext): Iterator[(Int, Int)] =
    Iterator.empty
}

/**
 * An RDD for passing to DAGScheduler. These RDDs will use the dependencies and
 * preferredLocations (if any) that are passed to them. They are deliberately not executable
 * so we can test that DAGScheduler does not try to execute RDDs locally.
 *
 * Optionally, one can pass in a list of locations to use as preferred locations for each task,
 * and a MapOutputTrackerMaster to enable reduce task locality. We pass the tracker separately
 * because, in this test suite, it won't be the same as sc.env.mapOutputTracker.
 */
class MyRDD(
    sc: SparkContext,
    numPartitions: Int,
    dependencies: List[Dependency[_]],
    locations: Seq[Seq[String]] = Nil,
    @(transient @param) tracker: MapOutputTrackerMaster = null,
    indeterminate: Boolean = false)
  extends RDD[(Int, Int)](sc, dependencies) with Serializable {

  override def compute(split: Partition, context: TaskContext): Iterator[(Int, Int)] =
    throw new RuntimeException("should not be reached")

  override def getPartitions: Array[Partition] = (0 until numPartitions).map(i => new Partition {
    override def index: Int = i
  }).toArray

  override protected def getOutputDeterministicLevel = {
    if (indeterminate) DeterministicLevel.INDETERMINATE else super.getOutputDeterministicLevel
  }

  override def getPreferredLocations(partition: Partition): Seq[String] = {
    if (locations.isDefinedAt(partition.index)) {
      locations(partition.index)
    } else if (tracker != null && dependencies.size == 1 &&
        dependencies(0).isInstanceOf[ShuffleDependency[_, _, _]]) {
      // If we have only one shuffle dependency, use the same code path as ShuffledRDD for locality
      val dep = dependencies(0).asInstanceOf[ShuffleDependency[_, _, _]]
      tracker.getPreferredLocationsForShuffle(dep, partition.index)
    } else {
      Nil
    }
  }

  override def toString: String = "DAGSchedulerSuiteRDD " + id
}

class DummyScheduledFuture(
    val delay: Long,
    val registerMergeResults: Boolean)
  extends ScheduledFuture[Int] {

  override def get(timeout: Long, unit: TimeUnit): Int =
    throw new IllegalStateException("should not be reached")

  override def getDelay(unit: TimeUnit): Long = delay

  override def compareTo(o: Delayed): Int =
    throw new IllegalStateException("should not be reached")

  override def cancel(mayInterruptIfRunning: Boolean): Boolean = true

  override def isCancelled: Boolean =
    throw new IllegalStateException("should not be reached")

  override def isDone: Boolean =
    throw new IllegalStateException("should not be reached")

  override def get(): Int =
    throw new IllegalStateException("should not be reached")
}

class DAGSchedulerSuiteDummyException extends Exception

class DAGSchedulerSuite extends SparkFunSuite with TempLocalSparkContext with TimeLimits {

  import DAGSchedulerSuite._

  // Necessary to make ScalaTest 3.x interrupt a thread on the JVM like ScalaTest 2.2.x
  implicit val defaultSignaler: Signaler = ThreadSignaler

  private var firstInit: Boolean = _
  /** Set of TaskSets the DAGScheduler has requested executed. */
  val taskSets = scala.collection.mutable.Buffer[TaskSet]()

  /** Stages for which the DAGScheduler has called TaskScheduler.cancelTasks(). */
  val cancelledStages = new HashSet[Int]()

  val tasksMarkedAsCompleted = new ArrayBuffer[Task[_]]()

  val taskScheduler = new TaskScheduler() {
    val executorsPendingDecommission = new HashMap[String, ExecutorDecommissionState]
    override def schedulingMode: SchedulingMode = SchedulingMode.FIFO
    override def rootPool: Pool = new Pool("", schedulingMode, 0, 0)
    override def start() = {}
    override def stop(exitCode: Int) = {}
    override def executorHeartbeatReceived(
        execId: String,
        accumUpdates: Array[(Long, Seq[AccumulatorV2[_, _]])],
        blockManagerId: BlockManagerId,
        executorUpdates: Map[(Int, Int), ExecutorMetrics]): Boolean = true
    override def submitTasks(taskSet: TaskSet) = {
      // normally done by TaskSetManager
      taskSet.tasks.foreach(_.epoch = mapOutputTracker.getEpoch)
      taskSets += taskSet
    }
    override def cancelTasks(stageId: Int, interruptThread: Boolean): Unit = {
      cancelledStages += stageId
    }
    override def killTaskAttempt(
      taskId: Long, interruptThread: Boolean, reason: String): Boolean = false
    override def killAllTaskAttempts(
      stageId: Int, interruptThread: Boolean, reason: String): Unit = {}
    override def notifyPartitionCompletion(stageId: Int, partitionId: Int): Unit = {
      taskSets.filter(_.stageId == stageId).lastOption.foreach { ts =>
        val tasks = ts.tasks.filter(_.partitionId == partitionId)
        assert(tasks.length == 1)
        tasksMarkedAsCompleted += tasks.head
      }
    }
    override def setDAGScheduler(dagScheduler: DAGScheduler) = {}
    override def defaultParallelism() = 2
    override def executorLost(executorId: String, reason: ExecutorLossReason): Unit = {}
    override def workerRemoved(workerId: String, host: String, message: String): Unit = {}
    override def applicationAttemptId(): Option[String] = None
    override def executorDecommission(
      executorId: String,
      decommissionInfo: ExecutorDecommissionInfo): Unit = {
      executorsPendingDecommission(executorId) =
        ExecutorDecommissionState(0, decommissionInfo.workerHost)
    }
    override def getExecutorDecommissionState(
      executorId: String): Option[ExecutorDecommissionState] = {
      executorsPendingDecommission.get(executorId)
    }
  }

  /**
   * Listeners which records some information to verify in UTs. Getter-kind methods in this class
   * ensures the value is returned after ensuring there's no event to process, as well as the
   * value is immutable: prevent showing odd result by race condition.
   */
  class EventInfoRecordingListener extends SparkListener {
    private val _submittedStageInfos = new HashSet[StageInfo]
    private val _successfulStages = new HashSet[Int]
    private val _failedStages = new ArrayBuffer[Int]
    private val _stageByOrderOfExecution = new ArrayBuffer[Int]
    private val _endedTasks = new HashSet[Long]

    override def onStageSubmitted(stageSubmitted: SparkListenerStageSubmitted): Unit = {
      _submittedStageInfos += stageSubmitted.stageInfo
    }

    override def onStageCompleted(stageCompleted: SparkListenerStageCompleted): Unit = {
      val stageInfo = stageCompleted.stageInfo
      _stageByOrderOfExecution += stageInfo.stageId
      if (stageInfo.failureReason.isEmpty) {
        _successfulStages += stageInfo.stageId
      } else {
        _failedStages += stageInfo.stageId
      }
    }

    override def onTaskEnd(taskEnd: SparkListenerTaskEnd): Unit = {
      _endedTasks += taskEnd.taskInfo.taskId
    }

    def submittedStageInfos: Set[StageInfo] = {
      waitForListeners()
      _submittedStageInfos.toSet
    }

    def successfulStages: Set[Int] = {
      waitForListeners()
      _successfulStages.toSet
    }

    def failedStages: List[Int] = {
      waitForListeners()
      _failedStages.toList
    }

    def stageByOrderOfExecution: List[Int] = {
      waitForListeners()
      _stageByOrderOfExecution.toList
    }

    def endedTasks: Set[Long] = {
      waitForListeners()
      _endedTasks.toSet
    }

    private def waitForListeners(): Unit = sc.listenerBus.waitUntilEmpty()
  }

  var sparkListener: EventInfoRecordingListener = null

  var blockManagerMaster: BlockManagerMaster = null
  var mapOutputTracker: MapOutputTrackerMaster = null
  var broadcastManager: BroadcastManager = null
  var securityMgr: SecurityManager = null
  var scheduler: DAGScheduler = null
  var dagEventProcessLoopTester: DAGSchedulerEventProcessLoop = null

  /**
   * Set of cache locations to return from our mock BlockManagerMaster.
   * Keys are (rdd ID, partition ID). Anything not present will return an empty
   * list of cache locations silently.
   */
  val cacheLocations = new HashMap[(Int, Int), Seq[BlockManagerId]]
  // stub out BlockManagerMaster.getLocations to use our cacheLocations
  class MyBlockManagerMaster(conf: SparkConf) extends BlockManagerMaster(null, null, conf, true) {
    override def getLocations(blockIds: Array[BlockId]): IndexedSeq[Seq[BlockManagerId]] = {
      blockIds.map {
        _.asRDDId.map { id => (id.rddId -> id.splitIndex)
        }.flatMap { key => cacheLocations.get(key)
        }.getOrElse(Seq())
      }.toIndexedSeq
    }
    override def removeExecutor(execId: String): Unit = {
      // don't need to propagate to the driver, which we don't have
    }

    override def removeShufflePushMergerLocation(host: String): Unit = {
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

  /** A simple helper class for creating custom JobListeners */
  class SimpleListener extends JobListener {
    val results = new HashMap[Int, Any]
    var failure: Exception = null
    override def taskSucceeded(index: Int, result: Any): Unit = results.put(index, result)
    override def jobFailed(exception: Exception): Unit = { failure = exception }
  }

  class MyMapOutputTrackerMaster(
      conf: SparkConf,
      broadcastManager: BroadcastManager)
    extends MapOutputTrackerMaster(conf, broadcastManager, true) {

    override def sendTracker(message: Any): Unit = {
      // no-op, just so we can stop this to avoid leaking threads
    }
  }

  class MyDAGScheduler(
      sc: SparkContext,
      taskScheduler: TaskScheduler,
      listenerBus: LiveListenerBus,
      mapOutputTracker: MapOutputTrackerMaster,
      blockManagerMaster: BlockManagerMaster,
      env: SparkEnv,
      clock: Clock = new SystemClock(),
      shuffleMergeFinalize: Boolean = true,
      shuffleMergeRegister: Boolean = true
  ) extends DAGScheduler(
      sc, taskScheduler, listenerBus, mapOutputTracker, blockManagerMaster, env, clock) {
    /**
     * Schedules shuffle merge finalize.
     */
    override private[scheduler] def scheduleShuffleMergeFinalize(
        shuffleMapStage: ShuffleMapStage,
        delay: Long,
        registerMergeResults: Boolean = true): Unit = {
      if (shuffleMergeRegister && registerMergeResults) {
        for (part <- 0 until shuffleMapStage.shuffleDep.partitioner.numPartitions) {
          val mergeStatuses = Seq((part, makeMergeStatus("",
            shuffleMapStage.shuffleDep.shuffleMergeId)))
          handleRegisterMergeStatuses(shuffleMapStage, mergeStatuses)
        }
      }

      shuffleMapStage.shuffleDep.getFinalizeTask match {
        case Some(_) =>
          assert(delay == 0 && registerMergeResults)
        case None =>
      }

      shuffleMapStage.shuffleDep.setFinalizeTask(
          new DummyScheduledFuture(delay, registerMergeResults))
      if (shuffleMergeFinalize) {
        handleShuffleMergeFinalized(shuffleMapStage, shuffleMapStage.shuffleDep.shuffleMergeId)
      }
    }
  }

  override def beforeEach(): Unit = {
    super.beforeEach()
    firstInit = true
  }

  override def sc: SparkContext = {
    val sc = super.sc
    if (firstInit) {
      init(sc)
      firstInit = false
    }
    sc
  }

  private def init(sc: SparkContext): Unit = {
    sparkListener = new EventInfoRecordingListener
    failure = null
    sc.addSparkListener(sparkListener)
    taskSets.clear()
    tasksMarkedAsCompleted.clear()
    cancelledStages.clear()
    cacheLocations.clear()
    results.clear()
    securityMgr = new SecurityManager(sc.getConf)
    broadcastManager = new BroadcastManager(true, sc.getConf)
    mapOutputTracker = spy(new MyMapOutputTrackerMaster(sc.getConf, broadcastManager))
    blockManagerMaster = spy(new MyBlockManagerMaster(sc.getConf))
    scheduler = new MyDAGScheduler(
      sc,
      taskScheduler,
      sc.listenerBus,
      mapOutputTracker,
      blockManagerMaster,
      sc.env)

    dagEventProcessLoopTester = new DAGSchedulerEventProcessLoopTester(scheduler)
  }

  override def afterEach(): Unit = {
    try {
      scheduler.stop()
      dagEventProcessLoopTester.stop()
      mapOutputTracker.stop()
      broadcastManager.stop()
    } finally {
      super.afterEach()
    }
  }

  override def afterAll(): Unit = {
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
  private def runEvent(event: DAGSchedulerEvent): Unit = {
    // Ensure the initialization of various components
    sc
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
  private def complete(taskSet: TaskSet, taskEndInfos: Seq[(TaskEndReason, Any)]): Unit = {
    assert(taskSet.tasks.size >= taskEndInfos.size)
    for ((result, i) <- taskEndInfos.zipWithIndex) {
      if (i < taskSet.tasks.size) {
        runEvent(makeCompletionEvent(taskSet.tasks(i), result._1, result._2))
      }
    }
  }

  private def completeWithAccumulator(
      accumId: Long,
      taskSet: TaskSet,
      results: Seq[(TaskEndReason, Any)]): Unit = {
    assert(taskSet.tasks.size >= results.size)
    for ((result, i) <- results.zipWithIndex) {
      if (i < taskSet.tasks.size) {
        runEvent(makeCompletionEvent(
          taskSet.tasks(i),
          result._1,
          result._2,
          Seq(AccumulatorSuite.createLongAccum("", initValue = 1, id = accumId))))
      }
    }
  }

  /** Submits a job to the scheduler and returns the job id. */
  private def submit(
      rdd: RDD[_],
      partitions: Array[Int],
      func: (TaskContext, Iterator[_]) => _ = jobComputeFunc,
      listener: JobListener = jobListener,
      properties: Properties = null): Int = {
    val jobId = scheduler.nextJobId.getAndIncrement()
    runEvent(JobSubmitted(jobId, rdd, func, partitions, CallSite("", ""), listener, properties))
    jobId
  }

  /** Submits a map stage to the scheduler and returns the job id. */
  private def submitMapStage(
      shuffleDep: ShuffleDependency[_, _, _],
      listener: JobListener = jobListener): Int = {
    val jobId = scheduler.nextJobId.getAndIncrement()
    runEvent(MapStageSubmitted(jobId, shuffleDep, CallSite("", ""), listener))
    jobId
  }

  /** Sends TaskSetFailed to the scheduler. */
  private def failed(taskSet: TaskSet, message: String): Unit = {
    runEvent(TaskSetFailed(taskSet, message, None))
  }

  /** Sends JobCancelled to the DAG scheduler. */
  private def cancel(jobId: Int): Unit = {
    runEvent(JobCancelled(jobId, None))
  }

  /** Make some tasks in task set success and check results. */
  private def completeAndCheckAnswer(
      taskSet: TaskSet,
      taskEndInfos: Seq[(TaskEndReason, Any)],
      expected: Map[Int, Any]): Unit = {
    complete(taskSet, taskEndInfos)
    assert(this.results === expected)
  }

  /** Sends ShufflePushCompleted to the DAG scheduler. */
  private def pushComplete(
      shuffleId: Int, shuffleMergeId: Int, mapIndex: Int): Unit = {
    runEvent(ShufflePushCompleted(shuffleId, shuffleMergeId, mapIndex))
  }

  test("[SPARK-3353] parent stage should have lower stage id") {
    sc.parallelize(1 to 10).map(x => (x, x)).reduceByKey(_ + _, 4).count()
    val stageByOrderOfExecution = sparkListener.stageByOrderOfExecution
    assert(stageByOrderOfExecution.length === 2)
    assert(stageByOrderOfExecution(0) < stageByOrderOfExecution(1))
  }

  /**
   * This test ensures that DAGScheduler build stage graph correctly.
   *
   * Suppose you have the following DAG:
   *
   * [A] <--(s_A)-- [B] <--(s_B)-- [C] <--(s_C)-- [D]
   *             \                /
   *               <-------------
   *
   * Here, RDD B has a shuffle dependency on RDD A, and RDD C has shuffle dependency on both
   * B and A. The shuffle dependency IDs are numbers in the DAGScheduler, but to make the example
   * easier to understand, let's call the shuffled data from A shuffle dependency ID s_A and the
   * shuffled data from B shuffle dependency ID s_B.
   *
   * Note: [] means an RDD, () means a shuffle dependency.
   */
  test("[SPARK-13902] Ensure no duplicate stages are created") {
    val rddA = new MyRDD(sc, 1, Nil)
    val shuffleDepA = new ShuffleDependency(rddA, new HashPartitioner(1))
    val s_A = shuffleDepA.shuffleId

    val rddB = new MyRDD(sc, 1, List(shuffleDepA), tracker = mapOutputTracker)
    val shuffleDepB = new ShuffleDependency(rddB, new HashPartitioner(1))
    val s_B = shuffleDepB.shuffleId

    val rddC = new MyRDD(sc, 1, List(shuffleDepA, shuffleDepB), tracker = mapOutputTracker)
    val shuffleDepC = new ShuffleDependency(rddC, new HashPartitioner(1))
    val s_C = shuffleDepC.shuffleId

    val rddD = new MyRDD(sc, 1, List(shuffleDepC), tracker = mapOutputTracker)

    submit(rddD, Array(0))

    assert(scheduler.shuffleIdToMapStage.size === 3)
    assert(scheduler.activeJobs.size === 1)

    val mapStageA = scheduler.shuffleIdToMapStage(s_A)
    val mapStageB = scheduler.shuffleIdToMapStage(s_B)
    val mapStageC = scheduler.shuffleIdToMapStage(s_C)
    val finalStage = scheduler.activeJobs.head.finalStage

    assert(mapStageA.parents.isEmpty)
    assert(mapStageB.parents === List(mapStageA))
    assert(mapStageC.parents === List(mapStageA, mapStageB))
    assert(finalStage.parents === List(mapStageC))

    completeShuffleMapStageSuccessfully(0, 0, 1)
    completeShuffleMapStageSuccessfully(1, 0, 1)
    completeShuffleMapStageSuccessfully(2, 0, 1)
    completeAndCheckAnswer(taskSets(3), Seq((Success, 42)), Map(0 -> 42))
    assertDataStructuresEmpty()
  }

  test("All shuffle files on the storage endpoint should be cleaned up when it is lost") {
    conf.set(config.SHUFFLE_SERVICE_ENABLED.key, "true")
    conf.set("spark.files.fetchFailure.unRegisterOutputOnHost", "true")
    runEvent(ExecutorAdded("hostA-exec1", "hostA"))
    runEvent(ExecutorAdded("hostA-exec2", "hostA"))
    runEvent(ExecutorAdded("hostB-exec", "hostB"))
    val firstRDD = new MyRDD(sc, 3, Nil)
    val firstShuffleDep = new ShuffleDependency(firstRDD, new HashPartitioner(3))
    val firstShuffleId = firstShuffleDep.shuffleId
    val shuffleMapRdd = new MyRDD(sc, 3, List(firstShuffleDep))
    val shuffleDep = new ShuffleDependency(shuffleMapRdd, new HashPartitioner(3))
    val secondShuffleId = shuffleDep.shuffleId
    val reduceRdd = new MyRDD(sc, 1, List(shuffleDep))
    submit(reduceRdd, Array(0))
    // map stage1 completes successfully, with one task on each executor
    complete(taskSets(0), Seq(
      (Success,
        MapStatus(
          BlockManagerId("hostA-exec1", "hostA", 12345), Array.fill[Long](1)(2), mapTaskId = 5)),
      (Success,
        MapStatus(
          BlockManagerId("hostA-exec2", "hostA", 12345), Array.fill[Long](1)(2), mapTaskId = 6)),
      (Success, makeMapStatus("hostB", 1, mapTaskId = 7))
    ))
    // map stage2 completes successfully, with one task on each executor
    complete(taskSets(1), Seq(
      (Success,
        MapStatus(
          BlockManagerId("hostA-exec1", "hostA", 12345), Array.fill[Long](1)(2), mapTaskId = 8)),
      (Success,
        MapStatus(
          BlockManagerId("hostA-exec2", "hostA", 12345), Array.fill[Long](1)(2), mapTaskId = 9)),
      (Success, makeMapStatus("hostB", 1, mapTaskId = 10))
    ))
    // make sure our test setup is correct
    val initialMapStatus1 = mapOutputTracker.shuffleStatuses(firstShuffleId).mapStatuses
    //  val initialMapStatus1 = mapOutputTracker.mapStatuses.get(0).get
    assert(initialMapStatus1.count(_ != null) === 3)
    assert(initialMapStatus1.map{_.location.executorId}.toSet ===
      Set("hostA-exec1", "hostA-exec2", "hostB-exec"))
    assert(initialMapStatus1.map{_.mapId}.toSet === Set(5, 6, 7))

    val initialMapStatus2 = mapOutputTracker.shuffleStatuses(secondShuffleId).mapStatuses
    //  val initialMapStatus1 = mapOutputTracker.mapStatuses.get(0).get
    assert(initialMapStatus2.count(_ != null) === 3)
    assert(initialMapStatus2.map{_.location.executorId}.toSet ===
      Set("hostA-exec1", "hostA-exec2", "hostB-exec"))
    assert(initialMapStatus2.map{_.mapId}.toSet === Set(8, 9, 10))

    // reduce stage fails with a fetch failure from one host
    complete(taskSets(2), Seq(
      (FetchFailed(BlockManagerId("hostA-exec2", "hostA", 12345),
        firstShuffleId, 0L, 0, 0, "ignored"),
        null)
    ))

    // Here is the main assertion -- make sure that we de-register
    // the map outputs for both map stage from both executors on hostA

    val mapStatus1 = mapOutputTracker.shuffleStatuses(firstShuffleId).mapStatuses
    assert(mapStatus1.count(_ != null) === 1)
    assert(mapStatus1(2).location.executorId === "hostB-exec")
    assert(mapStatus1(2).location.host === "hostB")

    val mapStatus2 = mapOutputTracker.shuffleStatuses(secondShuffleId).mapStatuses
    assert(mapStatus2.count(_ != null) === 1)
    assert(mapStatus2(2).location.executorId === "hostB-exec")
    assert(mapStatus2(2).location.host === "hostB")
  }

  test("SPARK-32003: All shuffle files for executor should be cleaned up on fetch failure") {
    conf.set(config.SHUFFLE_SERVICE_ENABLED.key, "true")

    val shuffleMapRdd = new MyRDD(sc, 3, Nil)
    val shuffleDep = new ShuffleDependency(shuffleMapRdd, new HashPartitioner(3))
    val shuffleId = shuffleDep.shuffleId
    val reduceRdd = new MyRDD(sc, 3, List(shuffleDep), tracker = mapOutputTracker)

    submit(reduceRdd, Array(0, 1, 2))
    // Map stage completes successfully,
    // two tasks are run on an executor on hostA and one on an executor on hostB
    completeShuffleMapStageSuccessfully(0, 0, 3, Seq("hostA", "hostA", "hostB"))
    // Now the executor on hostA is lost
    runEvent(ExecutorLost("hostA-exec", ExecutorExited(-100, false, "Container marked as failed")))
    // Executor is removed but shuffle files are not unregistered
    verify(blockManagerMaster, times(1)).removeExecutor("hostA-exec")
    verify(mapOutputTracker, times(0)).removeOutputsOnExecutor("hostA-exec")

    // The MapOutputTracker has all the shuffle files
    val mapStatuses = mapOutputTracker.shuffleStatuses(shuffleId).mapStatuses
    assert(mapStatuses.count(_ != null) === 3)
    assert(mapStatuses.count(s => s != null && s.location.executorId == "hostA-exec") === 2)
    assert(mapStatuses.count(s => s != null && s.location.executorId == "hostB-exec") === 1)

    // Now a fetch failure from the lost executor occurs
    complete(taskSets(1), Seq(
      (FetchFailed(makeBlockManagerId("hostA"), shuffleId, 0L, 0, 0, "ignored"), null)
    ))
    // blockManagerMaster.removeExecutor is not called again
    // but shuffle files are unregistered
    verify(blockManagerMaster, times(1)).removeExecutor("hostA-exec")
    verify(mapOutputTracker, times(1)).removeOutputsOnExecutor("hostA-exec")

    // Shuffle files for hostA-exec should be lost
    assert(mapStatuses.count(_ != null) === 1)
    assert(mapStatuses.count(s => s != null && s.location.executorId == "hostA-exec") === 0)
    assert(mapStatuses.count(s => s != null && s.location.executorId == "hostB-exec") === 1)

    // Additional fetch failure from the executor does not result in further call to
    // mapOutputTracker.removeOutputsOnExecutor
    complete(taskSets(1), Seq(
      (FetchFailed(makeBlockManagerId("hostA"), shuffleId, 0L, 1, 0, "ignored"), null)
    ))
    verify(mapOutputTracker, times(1)).removeOutputsOnExecutor("hostA-exec")
  }

  test("zero split job") {
    var numResults = 0
    var failureReason: Option[Exception] = None
    val fakeListener = new JobListener() {
      override def taskSucceeded(partition: Int, value: Any): Unit = numResults += 1
      override def jobFailed(exception: Exception): Unit = {
        failureReason = Some(exception)
      }
    }
    val jobId = submit(new MyRDD(sc, 0, Nil), Array(), listener = fakeListener)
    assert(numResults === 0)
    cancel(jobId)
    assert(failureReason.isDefined)
    assert(failureReason.get.getMessage() === "Job 0 cancelled ")
  }

  test("run trivial job") {
    submit(new MyRDD(sc, 1, Nil), Array(0))
    completeAndCheckAnswer(taskSets(0), Seq((Success, 42)), Map(0 -> 42))
    assertDataStructuresEmpty()
  }

  test("run trivial job w/ dependency") {
    val baseRdd = new MyRDD(sc, 1, Nil)
    val finalRdd = new MyRDD(sc, 1, List(new OneToOneDependency(baseRdd)))
    submit(finalRdd, Array(0))
    completeAndCheckAnswer(taskSets(0), Seq((Success, 42)), Map(0 -> 42))
    assertDataStructuresEmpty()
  }

  test("equals and hashCode AccumulableInfo") {
    val accInfo1 = new AccumulableInfo(
      1, Some("a1"), Some("delta1"), Some("val1"), internal = true, countFailedValues = false)
    val accInfo2 = new AccumulableInfo(
      1, Some("a1"), Some("delta1"), Some("val1"), internal = false, countFailedValues = false)
    val accInfo3 = new AccumulableInfo(
      1, Some("a1"), Some("delta1"), Some("val1"), internal = false, countFailedValues = false)
    assert(accInfo1 !== accInfo2)
    assert(accInfo2 === accInfo3)
    assert(accInfo2.hashCode() === accInfo3.hashCode())
  }

  test("cache location preferences w/ dependency") {
    val baseRdd = new MyRDD(sc, 1, Nil).cache()
    val finalRdd = new MyRDD(sc, 1, List(new OneToOneDependency(baseRdd)))
    cacheLocations(baseRdd.id -> 0) =
      Seq(makeBlockManagerId("hostA"), makeBlockManagerId("hostB"))
    submit(finalRdd, Array(0))
    val taskSet = taskSets(0)
    assertLocations(taskSet, Seq(Seq("hostA", "hostB")))
    completeAndCheckAnswer(taskSet, Seq((Success, 42)), Map(0 -> 42))
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
    val rddB = new MyRDD(sc, 1, List(new ShuffleDependency(rddA, new HashPartitioner(1))),
      tracker = mapOutputTracker)
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
    failAfter(10.seconds) {
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
    assert(sparkListener.failedStages === Seq(0))
    assertDataStructuresEmpty()
  }

  test("trivial job failure") {
    submit(new MyRDD(sc, 1, Nil), Array(0))
    failed(taskSets(0), "some failure")
    assert(failure.getMessage === "Job aborted due to stage failure: some failure")
    assert(sparkListener.failedStages === Seq(0))
    assertDataStructuresEmpty()
  }

  test("trivial job cancellation") {
    val rdd = new MyRDD(sc, 1, Nil)
    val jobId = submit(rdd, Array(0))
    cancel(jobId)
    assert(failure.getMessage === s"Job $jobId cancelled ")
    assert(sparkListener.failedStages === Seq(0))
    assertDataStructuresEmpty()
  }

  test("job cancellation no-kill backend") {
    // make sure that the DAGScheduler doesn't crash when the TaskScheduler
    // doesn't implement killTask()
    val noKillTaskScheduler = new TaskScheduler() {
      override def schedulingMode: SchedulingMode = SchedulingMode.FIFO
      override def rootPool: Pool = new Pool("", schedulingMode, 0, 0)
      override def start(): Unit = {}
      override def stop(exitCode: Int): Unit = {}
      override def submitTasks(taskSet: TaskSet): Unit = {
        taskSets += taskSet
      }
      override def cancelTasks(stageId: Int, interruptThread: Boolean): Unit = {
        throw new UnsupportedOperationException
      }
      override def killTaskAttempt(
          taskId: Long, interruptThread: Boolean, reason: String): Boolean = {
        throw new UnsupportedOperationException
      }
      override def killAllTaskAttempts(
          stageId: Int, interruptThread: Boolean, reason: String): Unit = {
        throw new UnsupportedOperationException
      }
      override def notifyPartitionCompletion(stageId: Int, partitionId: Int): Unit = {
        throw new UnsupportedOperationException
      }
      override def setDAGScheduler(dagScheduler: DAGScheduler): Unit = {}
      override def defaultParallelism(): Int = 2
      override def executorHeartbeatReceived(
          execId: String,
          accumUpdates: Array[(Long, Seq[AccumulatorV2[_, _]])],
          blockManagerId: BlockManagerId,
          executorUpdates: Map[(Int, Int), ExecutorMetrics]): Boolean = true
      override def executorLost(executorId: String, reason: ExecutorLossReason): Unit = {}
      override def workerRemoved(workerId: String, host: String, message: String): Unit = {}
      override def applicationAttemptId(): Option[String] = None
      override def executorDecommission(
        executorId: String,
        decommissionInfo: ExecutorDecommissionInfo): Unit = {}
      override def getExecutorDecommissionState(
        executorId: String): Option[ExecutorDecommissionState] = None
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
    completeAndCheckAnswer(taskSets(0), Seq((Success, 42)), Map(0 -> 42))
    assertDataStructuresEmpty()

    assert(sparkListener.failedStages.isEmpty)
    assert(sparkListener.successfulStages.contains(0))
  }

  test("run trivial shuffle") {
    val shuffleMapRdd = new MyRDD(sc, 2, Nil)
    val shuffleDep = new ShuffleDependency(shuffleMapRdd, new HashPartitioner(1))
    val shuffleId = shuffleDep.shuffleId
    val reduceRdd = new MyRDD(sc, 1, List(shuffleDep), tracker = mapOutputTracker)
    submit(reduceRdd, Array(0))
    completeShuffleMapStageSuccessfully(0, 0, 1)
    assert(mapOutputTracker.getMapSizesByExecutorId(shuffleId, 0).map(_._1).toSet ===
      HashSet(makeBlockManagerId("hostA"), makeBlockManagerId("hostB")))
    completeAndCheckAnswer(taskSets(1), Seq((Success, 42)), Map(0 -> 42))
    assertDataStructuresEmpty()
  }

  test("run trivial shuffle with fetch failure") {
    val shuffleMapRdd = new MyRDD(sc, 2, Nil)
    val shuffleDep = new ShuffleDependency(shuffleMapRdd, new HashPartitioner(2))
    val shuffleId = shuffleDep.shuffleId
    val reduceRdd = new MyRDD(sc, 2, List(shuffleDep), tracker = mapOutputTracker)
    submit(reduceRdd, Array(0, 1))
    completeShuffleMapStageSuccessfully(0, 0, reduceRdd.partitions.length)
    // the 2nd ResultTask failed
    complete(taskSets(1), Seq(
      (Success, 42),
      (FetchFailed(makeBlockManagerId("hostA"), shuffleId, 0L, 0, 0, "ignored"), null)))
    verify(blockManagerMaster, times(1)).removeExecutor("hostA-exec")
    // ask the scheduler to try it again
    scheduler.resubmitFailedStages()
    // have the 2nd attempt pass
    complete(taskSets(2), Seq((Success, makeMapStatus("hostA", reduceRdd.partitions.length))))
    // we can see both result blocks now
    assert(mapOutputTracker.getMapSizesByExecutorId(shuffleId, 0).map(_._1.host).toSet ===
      HashSet("hostA", "hostB"))
    completeAndCheckAnswer(taskSets(3), Seq((Success, 43)), Map(0 -> 42, 1 -> 43))
    assertDataStructuresEmpty()
  }

  private val shuffleFileLossTests = Seq(
    ("executor process lost with shuffle service", ExecutorProcessLost("", None), true, false),
    ("worker lost with shuffle service", ExecutorProcessLost("", Some("hostA")), true, true),
    ("worker lost without shuffle service", ExecutorProcessLost("", Some("hostA")), false, true),
    ("executor failure with shuffle service", ExecutorKilled, true, false),
    ("executor failure without shuffle service", ExecutorKilled, false, true))

  for ((eventDescription, event, shuffleServiceOn, expectFileLoss) <- shuffleFileLossTests) {
    val maybeLost = if (expectFileLoss) {
      "lost"
    } else {
      "not lost"
    }
    test(s"shuffle files $maybeLost when $eventDescription") {
      conf.set(config.SHUFFLE_SERVICE_ENABLED.key, shuffleServiceOn.toString)
      assert(sc.env.blockManager.externalShuffleServiceEnabled == shuffleServiceOn)

      val shuffleMapRdd = new MyRDD(sc, 2, Nil)
      val shuffleDep = new ShuffleDependency(shuffleMapRdd, new HashPartitioner(1))
      val shuffleId = shuffleDep.shuffleId
      val reduceRdd = new MyRDD(sc, 1, List(shuffleDep), tracker = mapOutputTracker)
      submit(reduceRdd, Array(0))
      completeShuffleMapStageSuccessfully(0, 0, 1)
      val expectHostFileLoss = event match {
        case ExecutorProcessLost(_, workerHost, _) => workerHost.isDefined
        case _ => false
      }
      runEvent(ExecutorLost("hostA-exec", event))
      verify(blockManagerMaster, times(1)).removeExecutor("hostA-exec")
      if (expectFileLoss) {
        if (expectHostFileLoss) {
          verify(mapOutputTracker, times(1)).removeOutputsOnHost("hostA")
        } else {
          verify(mapOutputTracker, times(1)).removeOutputsOnExecutor("hostA-exec")
        }
        intercept[MetadataFetchFailedException] {
          mapOutputTracker.getMapSizesByExecutorId(shuffleId, 0)
        }
      } else {
        verify(mapOutputTracker, times(0)).removeOutputsOnExecutor("hostA-exec")
        assert(mapOutputTracker.getMapSizesByExecutorId(shuffleId, 0).map(_._1).toSet ===
          HashSet(makeBlockManagerId("hostA"), makeBlockManagerId("hostB")))
      }
    }
  }

  test("SPARK-28967 properties must be cloned before posting to listener bus for 0 partition") {
    val properties = new Properties()
    val func = (context: TaskContext, it: Iterator[(_)]) => 1
    val resultHandler = (taskIndex: Int, result: Int) => {}
    val assertionError = new AtomicReference[TestFailedException](
      new TestFailedException("Listener didn't receive expected JobStart event", 0))
    val listener = new SparkListener() {
      override def onJobStart(event: SparkListenerJobStart): Unit = {
        try {
          // spark.job.description can be implicitly set for 0 partition jobs.
          // So event.properties and properties can be different. See SPARK-29997.
          event.properties.remove(SparkContext.SPARK_JOB_DESCRIPTION)
          properties.remove(SparkContext.SPARK_JOB_DESCRIPTION)

          assert(event.properties.equals(properties), "Expected same content of properties, " +
            s"but got properties with different content. props in caller ${properties} /" +
            s" props in event ${event.properties}")
          assert(event.properties.ne(properties), "Expected instance with different identity, " +
            "but got same instance.")
          assertionError.set(null)
        } catch {
          case e: TestFailedException => assertionError.set(e)
        }
      }
    }
    sc.addSparkListener(listener)

    // 0 partition
    val testRdd = new MyRDD(sc, 0, Nil)
    val waiter = scheduler.submitJob(testRdd, func, Seq.empty, CallSite.empty,
      resultHandler, properties)
    sc.listenerBus.waitUntilEmpty()
    assert(assertionError.get() === null)
  }

  // Helper function to validate state when creating tests for task failures
  private def checkStageId(stageId: Int, attempt: Int, stageAttempt: TaskSet): Unit = {
    assert(stageAttempt.stageId === stageId)
    assert(stageAttempt.stageAttemptId == attempt)
  }

  // Helper functions to extract commonly used code in Fetch Failure test cases
  private def setupStageAbortTest(sc: SparkContext): Unit = {
    sc.listenerBus.addToSharedQueue(new EndListener())
    ended = false
    jobResult = null
  }

  // Create a new Listener to confirm that the listenerBus sees the JobEnd message
  // when we abort the stage. This message will also be consumed by the EventLoggingListener
  // so this will propagate up to the user.
  var ended = false
  var jobResult : JobResult = null

  class EndListener extends SparkListener {
    override def onJobEnd(jobEnd: SparkListenerJobEnd): Unit = {
      jobResult = jobEnd.jobResult
      ended = true
    }
  }

  /**
   * Common code to get the next stage attempt, confirm it's the one we expect, and complete it
   * successfully.
   *
   * @param stageId - The current stageId
   * @param attemptIdx - The current attempt count
   * @param numShufflePartitions - The number of partitions in the next stage
   * @param hostNames - Host on which each task in the task set is executed. In case no hostNames
   *                  are provided, the tasks will progressively complete on hostA, hostB, etc.
   */
  private def completeShuffleMapStageSuccessfully(
      stageId: Int,
      attemptIdx: Int,
      numShufflePartitions: Int,
      hostNames: Seq[String] = Seq.empty[String]): Unit = {
    def compareStageAttempt(taskSet: TaskSet): Boolean = {
      taskSet.stageId == stageId && taskSet.stageAttemptId == attemptIdx
    }

    val stageAttemptOpt = taskSets.find(compareStageAttempt(_))
    assert(stageAttemptOpt.isDefined)
    val stageAttempt = stageAttemptOpt.get
    complete(stageAttempt, stageAttempt.tasks.zipWithIndex.map {
      case (task, idx) =>
        val hostName = if (idx < hostNames.size) {
          hostNames(idx)
        } else {
          s"host${('A' + idx).toChar}"
        }
        (Success, makeMapStatus(hostName, numShufflePartitions))
    }.toSeq)
  }

  /**
   * Common code to get the next stage attempt, confirm it's the one we expect, and complete it
   * with all FetchFailure.
   *
   * @param stageId - The current stageId
   * @param attemptIdx - The current attempt count
   * @param shuffleDep - The shuffle dependency of the stage with a fetch failure
   */
  private def completeNextStageWithFetchFailure(
      stageId: Int,
      attemptIdx: Int,
      shuffleDep: ShuffleDependency[_, _, _]): Unit = {
    val stageAttempt = taskSets.last
    checkStageId(stageId, attemptIdx, stageAttempt)
    complete(stageAttempt, stageAttempt.tasks.zipWithIndex.map { case (task, idx) =>
      (FetchFailed(makeBlockManagerId("hostA"), shuffleDep.shuffleId, 0L, 0, idx, "ignored"), null)
    }.toSeq)
  }

  /**
   * Common code to get the next result stage attempt, confirm it's the one we expect, and
   * complete it with a success where we return 42.
   *
   * @param stageId - The current stageId
   * @param attemptIdx - The current attempt count
   */
  private def completeNextResultStageWithSuccess(
      stageId: Int,
      attemptIdx: Int,
      partitionToResult: Int => Int = _ => 42): Unit = {
    val stageAttempt = taskSets.last
    checkStageId(stageId, attemptIdx, stageAttempt)
    assert(scheduler.stageIdToStage(stageId).isInstanceOf[ResultStage])
    val taskResults = stageAttempt.tasks.zipWithIndex.map { case (task, idx) =>
      (Success, partitionToResult(idx))
    }
    complete(stageAttempt, taskResults.toSeq)
  }

  /**
   * In this test, we simulate a job where many tasks in the same stage fail. We want to show
   * that many fetch failures inside a single stage attempt do not trigger an abort
   * on their own, but only when there are enough failing stage attempts.
   */
  test("Single stage fetch failure should not abort the stage.") {
    setupStageAbortTest(sc)

    val parts = 8
    val shuffleMapRdd = new MyRDD(sc, parts, Nil)
    val shuffleDep = new ShuffleDependency(shuffleMapRdd, new HashPartitioner(parts))
    val reduceRdd = new MyRDD(sc, parts, List(shuffleDep), tracker = mapOutputTracker)
    submit(reduceRdd, (0 until parts).toArray)

    completeShuffleMapStageSuccessfully(0, 0, numShufflePartitions = parts)

    completeNextStageWithFetchFailure(1, 0, shuffleDep)

    // Resubmit and confirm that now all is well
    scheduler.resubmitFailedStages()

    assert(scheduler.runningStages.nonEmpty)
    assert(!ended)

    // Complete stage 0 and then stage 1 with a "42"
    completeShuffleMapStageSuccessfully(0, 1, numShufflePartitions = parts)
    completeNextResultStageWithSuccess(1, 1)

    // Confirm job finished successfully
    sc.listenerBus.waitUntilEmpty()
    assert(ended)
    assert(results === (0 until parts).map { idx => idx -> 42 }.toMap)
    assertDataStructuresEmpty()
  }

  test("SPARK-40481: Multiple consecutive stage fetch failures from decommissioned executor " +
    "should not fail job when ignoreDecommissionFetchFailure is enabled.") {
    conf.set(config.STAGE_IGNORE_DECOMMISSION_FETCH_FAILURE.key, "true")

    setupStageAbortTest(sc)
    val parts = 2
    val shuffleMapRdd = new MyRDD(sc, parts, Nil)
    val shuffleDep = new ShuffleDependency(shuffleMapRdd, new HashPartitioner(parts))
    val reduceRdd = new MyRDD(sc, parts, List(shuffleDep), tracker = mapOutputTracker)
    submit(reduceRdd, (0 until parts).toArray)

    for (attempt <- 0 until scheduler.maxConsecutiveStageAttempts) {
      // Complete all the tasks for the current attempt of stage 0 successfully
      completeShuffleMapStageSuccessfully(0, attempt, numShufflePartitions = parts,
        Seq("hostA", "hostB"))

      // Only make first attempt fail due to executor decommission
      if (attempt == 0) {
        taskScheduler.executorDecommission("hostA-exec", ExecutorDecommissionInfo(""))
      } else {
        taskScheduler.executorsPendingDecommission.clear()
      }
      // Now we should have a new taskSet, for a new attempt of stage 1.
      // Fail all these tasks with FetchFailure
      completeNextStageWithFetchFailure(1, attempt, shuffleDep)

      // this will trigger a resubmission of stage 0, since we've lost some of its
      // map output, for the next iteration through the loop
      scheduler.resubmitFailedStages()
   }

    // Confirm job finished successfully
    sc.listenerBus.waitUntilEmpty()
    assert(scheduler.runningStages.nonEmpty)
    assert(!ended)
  }

  /**
   * In this test we simulate a job failure where the first stage completes successfully and
   * the second stage fails due to a fetch failure. Multiple successive fetch failures of a stage
   * trigger an overall job abort to avoid endless retries.
   */
  test("Multiple consecutive stage fetch failures should lead to job being aborted.") {
    setupStageAbortTest(sc)

    val shuffleMapRdd = new MyRDD(sc, 2, Nil)
    val shuffleDep = new ShuffleDependency(shuffleMapRdd, new HashPartitioner(2))
    val reduceRdd = new MyRDD(sc, 2, List(shuffleDep), tracker = mapOutputTracker)
    submit(reduceRdd, Array(0, 1))

    for (attempt <- 0 until scheduler.maxConsecutiveStageAttempts) {
      // Complete all the tasks for the current attempt of stage 0 successfully
      completeShuffleMapStageSuccessfully(0, attempt, numShufflePartitions = 2)

      // Now we should have a new taskSet, for a new attempt of stage 1.
      // Fail all these tasks with FetchFailure
      completeNextStageWithFetchFailure(1, attempt, shuffleDep)

      // this will trigger a resubmission of stage 0, since we've lost some of its
      // map output, for the next iteration through the loop
      scheduler.resubmitFailedStages()

      if (attempt < scheduler.maxConsecutiveStageAttempts - 1) {
        assert(scheduler.runningStages.nonEmpty)
        assert(!ended)
      } else {
        // Stage should have been aborted and removed from running stages
        assertDataStructuresEmpty()
        sc.listenerBus.waitUntilEmpty()
        assert(ended)
        jobResult match {
          case JobFailed(reason) =>
            assert(reason.getMessage.contains("ResultStage 1 () has failed the maximum"))
          case other => fail(s"expected JobFailed, not $other")
        }
      }
    }
  }

  /**
   * In this test, we create a job with two consecutive shuffles, and simulate 2 failures for each
   * shuffle fetch. In total In total, the job has had four failures overall but not four failures
   * for a particular stage, and as such should not be aborted.
   */
  test("Failures in different stages should not trigger an overall abort") {
    setupStageAbortTest(sc)

    val shuffleOneRdd = new MyRDD(sc, 2, Nil).cache()
    val shuffleDepOne = new ShuffleDependency(shuffleOneRdd, new HashPartitioner(2))
    val shuffleTwoRdd = new MyRDD(sc, 2, List(shuffleDepOne), tracker = mapOutputTracker).cache()
    val shuffleDepTwo = new ShuffleDependency(shuffleTwoRdd, new HashPartitioner(1))
    val finalRdd = new MyRDD(sc, 1, List(shuffleDepTwo), tracker = mapOutputTracker)
    submit(finalRdd, Array(0))

    // In the first two iterations, Stage 0 succeeds and stage 1 fails. In the next two iterations,
    // stage 2 fails.
    for (attempt <- 0 until scheduler.maxConsecutiveStageAttempts) {
      // Complete all the tasks for the current attempt of stage 0 successfully
      completeShuffleMapStageSuccessfully(0, attempt, numShufflePartitions = 2)

      if (attempt < scheduler.maxConsecutiveStageAttempts / 2) {
        // Now we should have a new taskSet, for a new attempt of stage 1.
        // Fail all these tasks with FetchFailure
        completeNextStageWithFetchFailure(1, attempt, shuffleDepOne)
      } else {
        completeShuffleMapStageSuccessfully(1, attempt, numShufflePartitions = 1)

        // Fail stage 2
        completeNextStageWithFetchFailure(2,
          attempt - scheduler.maxConsecutiveStageAttempts / 2, shuffleDepTwo)
      }

      // this will trigger a resubmission of stage 0, since we've lost some of its
      // map output, for the next iteration through the loop
      scheduler.resubmitFailedStages()
    }

    completeShuffleMapStageSuccessfully(0, 4, numShufflePartitions = 2)
    completeShuffleMapStageSuccessfully(1, 4, numShufflePartitions = 1)

    // Succeed stage2 with a "42"
    completeNextResultStageWithSuccess(2, scheduler.maxConsecutiveStageAttempts / 2)

    assert(results === Map(0 -> 42))
    assertDataStructuresEmpty()
  }

  /**
   * In this test we demonstrate that only consecutive failures trigger a stage abort. A stage may
   * fail multiple times, succeed, then fail a few more times (because its run again by downstream
   * dependencies). The total number of failed attempts for one stage will go over the limit,
   * but that doesn't matter, since they have successes in the middle.
   */
  test("Non-consecutive stage failures don't trigger abort") {
    setupStageAbortTest(sc)

    val shuffleOneRdd = new MyRDD(sc, 2, Nil).cache()
    val shuffleDepOne = new ShuffleDependency(shuffleOneRdd, new HashPartitioner(2))
    val shuffleTwoRdd = new MyRDD(sc, 2, List(shuffleDepOne), tracker = mapOutputTracker).cache()
    val shuffleDepTwo = new ShuffleDependency(shuffleTwoRdd, new HashPartitioner(1))
    val finalRdd = new MyRDD(sc, 1, List(shuffleDepTwo), tracker = mapOutputTracker)
    submit(finalRdd, Array(0))

    // First, execute stages 0 and 1, failing stage 1 up to MAX-1 times.
    for (attempt <- 0 until scheduler.maxConsecutiveStageAttempts - 1) {
      // Make each task in stage 0 success
      completeShuffleMapStageSuccessfully(0, attempt, numShufflePartitions = 2)

      // Now we should have a new taskSet, for a new attempt of stage 1.
      // Fail these tasks with FetchFailure
      completeNextStageWithFetchFailure(1, attempt, shuffleDepOne)

      scheduler.resubmitFailedStages()

      // Confirm we have not yet aborted
      assert(scheduler.runningStages.nonEmpty)
      assert(!ended)
    }

    // Rerun stage 0 and 1 to step through the task set
    completeShuffleMapStageSuccessfully(0, 3, numShufflePartitions = 2)
    completeShuffleMapStageSuccessfully(1, 3, numShufflePartitions = 1)

    // Fail stage 2 so that stage 1 is resubmitted when we call scheduler.resubmitFailedStages()
    completeNextStageWithFetchFailure(2, 0, shuffleDepTwo)

    scheduler.resubmitFailedStages()

    // Rerun stage 0 to step through the task set
    completeShuffleMapStageSuccessfully(0, 4, numShufflePartitions = 2)

    // Now again, fail stage 1 (up to MAX_FAILURES) but confirm that this doesn't trigger an abort
    // since we succeeded in between.
    completeNextStageWithFetchFailure(1, 4, shuffleDepOne)

    scheduler.resubmitFailedStages()

    // Confirm we have not yet aborted
    assert(scheduler.runningStages.nonEmpty)
    assert(!ended)

    // Next, succeed all and confirm output
    // Rerun stage 0 + 1
    completeShuffleMapStageSuccessfully(0, 5, numShufflePartitions = 2)
    completeShuffleMapStageSuccessfully(1, 5, numShufflePartitions = 1)

    // Succeed stage 2 and verify results
    completeNextResultStageWithSuccess(2, 1)

    assertDataStructuresEmpty()
    sc.listenerBus.waitUntilEmpty()
    assert(ended)
    assert(results === Map(0 -> 42))
  }

  test("trivial shuffle with multiple fetch failures") {
    val shuffleMapRdd = new MyRDD(sc, 2, Nil)
    val shuffleDep = new ShuffleDependency(shuffleMapRdd, new HashPartitioner(2))
    val shuffleId = shuffleDep.shuffleId
    val reduceRdd = new MyRDD(sc, 2, List(shuffleDep), tracker = mapOutputTracker)
    submit(reduceRdd, Array(0, 1))
    completeShuffleMapStageSuccessfully(0, 0, reduceRdd.partitions.length)
    // The MapOutputTracker should know about both map output locations.
    assert(mapOutputTracker.getMapSizesByExecutorId(shuffleId, 0).map(_._1.host).toSet ===
      HashSet("hostA", "hostB"))

    // The first result task fails, with a fetch failure for the output from the first mapper.
    runEvent(makeCompletionEvent(
      taskSets(1).tasks(0),
      FetchFailed(makeBlockManagerId("hostA"), shuffleId, 0L, 0, 0, "ignored"),
      null))
    assert(sparkListener.failedStages.contains(1))

    // The second ResultTask fails, with a fetch failure for the output from the second mapper.
    runEvent(makeCompletionEvent(
      taskSets(1).tasks(0),
      FetchFailed(makeBlockManagerId("hostA"), shuffleId, 1L, 1, 1, "ignored"),
      null))
    // The SparkListener should not receive redundant failure events.
    assert(sparkListener.failedStages.size === 1)
  }

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
      FetchFailed(makeBlockManagerId("hostA"), shuffleId, 0L, 0, 0, "ignored"),
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
      (Success, makeMapStatus("hostA", reduceRdd.partitions.length))))
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

  /**
   * This tests the case where another FetchFailed comes in while the map stage is getting
   * re-run.
   */
  test("late fetch failures don't cause multiple concurrent attempts for the same map stage") {
    val shuffleMapRdd = new MyRDD(sc, 2, Nil)
    val shuffleDep = new ShuffleDependency(shuffleMapRdd, new HashPartitioner(2))
    val shuffleId = shuffleDep.shuffleId
    val reduceRdd = new MyRDD(sc, 2, List(shuffleDep), tracker = mapOutputTracker)
    submit(reduceRdd, Array(0, 1))

    val mapStageId = 0
    def countSubmittedMapStageAttempts(): Int = {
      sparkListener.submittedStageInfos.count(_.stageId == mapStageId)
    }

    // The map stage should have been submitted.
    assert(countSubmittedMapStageAttempts() === 1)

    completeShuffleMapStageSuccessfully(0, 0, 2)
    // The MapOutputTracker should know about both map output locations.
    assert(mapOutputTracker.getMapSizesByExecutorId(shuffleId, 0).map(_._1.host).toSet ===
      HashSet("hostA", "hostB"))
    assert(mapOutputTracker.getMapSizesByExecutorId(shuffleId, 1).map(_._1.host).toSet ===
      HashSet("hostA", "hostB"))

    // The first result task fails, with a fetch failure for the output from the first mapper.
    runEvent(makeCompletionEvent(
      taskSets(1).tasks(0),
      FetchFailed(makeBlockManagerId("hostA"), shuffleId, 0L, 0, 0, "ignored"),
      null))
    assert(sparkListener.failedStages.contains(1))

    // Trigger resubmission of the failed map stage.
    runEvent(ResubmitFailedStages)

    // Another attempt for the map stage should have been submitted, resulting in 2 total attempts.
    assert(countSubmittedMapStageAttempts() === 2)

    // The second ResultTask fails, with a fetch failure for the output from the second mapper.
    runEvent(makeCompletionEvent(
      taskSets(1).tasks(1),
      FetchFailed(makeBlockManagerId("hostB"), shuffleId, 1L, 1, 1, "ignored"),
      null))

    // Another ResubmitFailedStages event should not result in another attempt for the map
    // stage being run concurrently.
    // NOTE: the actual ResubmitFailedStages may get called at any time during this, but it
    // shouldn't effect anything -- our calling it just makes *SURE* it gets called between the
    // desired event and our check.
    runEvent(ResubmitFailedStages)
    assert(countSubmittedMapStageAttempts() === 2)

  }

  /**
   * This tests the case where a late FetchFailed comes in after the map stage has finished getting
   * retried and a new reduce stage starts running.
   */
  test("extremely late fetch failures don't cause multiple concurrent attempts for " +
    "the same stage") {
    val shuffleMapRdd = new MyRDD(sc, 2, Nil)
    val shuffleDep = new ShuffleDependency(shuffleMapRdd, new HashPartitioner(2))
    val shuffleId = shuffleDep.shuffleId
    val reduceRdd = new MyRDD(sc, 2, List(shuffleDep), tracker = mapOutputTracker)
    submit(reduceRdd, Array(0, 1))

    def countSubmittedReduceStageAttempts(): Int = {
      sparkListener.submittedStageInfos.count(_.stageId == 1)
    }
    def countSubmittedMapStageAttempts(): Int = {
      sparkListener.submittedStageInfos.count(_.stageId == 0)
    }

    // The map stage should have been submitted.
    assert(countSubmittedMapStageAttempts() === 1)

    // Complete the map stage.
    completeShuffleMapStageSuccessfully(0, 0, 2)

    // The reduce stage should have been submitted.
    assert(countSubmittedReduceStageAttempts() === 1)

    // The first result task fails, with a fetch failure for the output from the first mapper.
    runEvent(makeCompletionEvent(
      taskSets(1).tasks(0),
      FetchFailed(makeBlockManagerId("hostA"), shuffleId, 0L, 0, 0, "ignored"),
      null))

    // Trigger resubmission of the failed map stage and finish the re-started map task.
    runEvent(ResubmitFailedStages)
    complete(taskSets(2), Seq((Success, makeMapStatus("hostA", 1))))

    // Because the map stage finished, another attempt for the reduce stage should have been
    // submitted, resulting in 2 total attempts for each the map and the reduce stage.
    assert(countSubmittedMapStageAttempts() === 2)
    assert(countSubmittedReduceStageAttempts() === 2)

    // A late FetchFailed arrives from the second task in the original reduce stage.
    runEvent(makeCompletionEvent(
      taskSets(1).tasks(1),
      FetchFailed(makeBlockManagerId("hostB"), shuffleId, 1L, 1, 1, "ignored"),
      null))

    // Running ResubmitFailedStages shouldn't result in any more attempts for the map stage, because
    // the FetchFailed should have been ignored
    runEvent(ResubmitFailedStages)

    // The FetchFailed from the original reduce stage should be ignored.
    assert(countSubmittedMapStageAttempts() === 2)
  }

  test("task events always posted in speculation / when stage is killed") {
    val baseRdd = new MyRDD(sc, 4, Nil)
    val finalRdd = new MyRDD(sc, 4, List(new OneToOneDependency(baseRdd)))
    submit(finalRdd, Array(0, 1, 2, 3))

    // complete two tasks
    runEvent(makeCompletionEvent(
      taskSets(0).tasks(0), Success, 42,
      Seq.empty, Array.empty, createFakeTaskInfoWithId(0)))
    runEvent(makeCompletionEvent(
      taskSets(0).tasks(1), Success, 42,
      Seq.empty, Array.empty, createFakeTaskInfoWithId(1)))
    // verify stage exists
    assert(scheduler.stageIdToStage.contains(0))
    assert(sparkListener.endedTasks.size === 2)

    // finish other 2 tasks
    runEvent(makeCompletionEvent(
      taskSets(0).tasks(2), Success, 42,
      Seq.empty, Array.empty, createFakeTaskInfoWithId(2)))
    runEvent(makeCompletionEvent(
      taskSets(0).tasks(3), Success, 42,
      Seq.empty, Array.empty, createFakeTaskInfoWithId(3)))
    assert(sparkListener.endedTasks.size === 4)

    // verify the stage is done
    assert(!scheduler.stageIdToStage.contains(0))

    // Stage should be complete. Finish one other Successful task to simulate what can happen
    // with a speculative task and make sure the event is sent out
    runEvent(makeCompletionEvent(
      taskSets(0).tasks(3), Success, 42,
      Seq.empty, Array.empty, createFakeTaskInfoWithId(5)))
    assert(sparkListener.endedTasks.size === 5)

    // make sure non successful tasks also send out event
    runEvent(makeCompletionEvent(
      taskSets(0).tasks(3), UnknownReason, 42,
      Seq.empty, Array.empty, createFakeTaskInfoWithId(6)))
    assert(sparkListener.endedTasks.size === 6)
  }

  test("ignore late map task completions") {
    val shuffleMapRdd = new MyRDD(sc, 2, Nil)
    val shuffleDep = new ShuffleDependency(shuffleMapRdd, new HashPartitioner(2))
    val shuffleId = shuffleDep.shuffleId
    val reduceRdd = new MyRDD(sc, 2, List(shuffleDep), tracker = mapOutputTracker)
    submit(reduceRdd, Array(0, 1))

    // pretend we were told hostA went away
    val oldEpoch = mapOutputTracker.getEpoch
    runEvent(ExecutorLost("hostA-exec", ExecutorKilled))
    val newEpoch = mapOutputTracker.getEpoch
    assert(newEpoch > oldEpoch)

    // now start completing some tasks in the shuffle map stage, under different hosts
    // and epochs, and make sure scheduler updates its state correctly
    val taskSet = taskSets(0)
    val shuffleStage = scheduler.stageIdToStage(taskSet.stageId).asInstanceOf[ShuffleMapStage]
    assert(shuffleStage.numAvailableOutputs === 0)

    // should be ignored for being too old
    runEvent(makeCompletionEvent(
      taskSet.tasks(0),
      Success,
      makeMapStatus("hostA", reduceRdd.partitions.size)))
    assert(shuffleStage.numAvailableOutputs === 0)

    // should work because it's a non-failed host (so the available map outputs will increase)
    runEvent(makeCompletionEvent(
      taskSet.tasks(0),
      Success,
      makeMapStatus("hostB", reduceRdd.partitions.size)))
    assert(shuffleStage.numAvailableOutputs === 1)

    // should be ignored for being too old
    runEvent(makeCompletionEvent(
      taskSet.tasks(0),
      Success,
      makeMapStatus("hostA", reduceRdd.partitions.size)))
    assert(shuffleStage.numAvailableOutputs === 1)

    // should work because it's a new epoch, which will increase the number of available map
    // outputs, and also finish the stage
    taskSet.tasks(1).epoch = newEpoch
    runEvent(makeCompletionEvent(
      taskSet.tasks(1),
      Success,
      makeMapStatus("hostA", reduceRdd.partitions.size)))
    assert(shuffleStage.numAvailableOutputs === 2)
    assert(mapOutputTracker.getMapSizesByExecutorId(shuffleId, 0).map(_._1).toSet ===
      HashSet(makeBlockManagerId("hostB"), makeBlockManagerId("hostA")))

    // finish the next stage normally, which completes the job
    completeAndCheckAnswer(taskSets(1), Seq((Success, 42), (Success, 43)), Map(0 -> 42, 1 -> 43))
    assertDataStructuresEmpty()
  }

  test("run shuffle with map stage failure") {
    val shuffleMapRdd = new MyRDD(sc, 2, Nil)
    val shuffleDep = new ShuffleDependency(shuffleMapRdd, new HashPartitioner(2))
    val reduceRdd = new MyRDD(sc, 2, List(shuffleDep), tracker = mapOutputTracker)
    submit(reduceRdd, Array(0, 1))

    // Fail the map stage.  This should cause the entire job to fail.
    val stageFailureMessage = "Exception failure in map stage"
    failed(taskSets(0), stageFailureMessage)
    assert(failure.getMessage === s"Job aborted due to stage failure: $stageFailureMessage")

    // Listener bus should get told about the map stage failing, but not the reduce stage
    // (since the reduce stage hasn't been started yet).
    assert(sparkListener.failedStages.toSet === Set(0))

    assertDataStructuresEmpty()
  }

  /**
   * Run two jobs, with a shared dependency.  We simulate a fetch failure in the second job, which
   * requires regenerating some outputs of the shared dependency.  One key aspect of this test is
   * that the second job actually uses a different stage for the shared dependency (a "skipped"
   * stage).
   */
  test("shuffle fetch failure in a reused shuffle dependency") {
    // Run the first job successfully, which creates one shuffle dependency

    val shuffleMapRdd = new MyRDD(sc, 2, Nil)
    val shuffleDep = new ShuffleDependency(shuffleMapRdd, new HashPartitioner(2))
    val reduceRdd = new MyRDD(sc, 2, List(shuffleDep))
    submit(reduceRdd, Array(0, 1))

    completeShuffleMapStageSuccessfully(0, 0, 2)
    completeNextResultStageWithSuccess(1, 0)
    assert(results === Map(0 -> 42, 1 -> 42))
    assertDataStructuresEmpty()

    // submit another job w/ the shared dependency, and have a fetch failure
    val reduce2 = new MyRDD(sc, 2, List(shuffleDep))
    submit(reduce2, Array(0, 1))
    // Note that the stage numbering here is only b/c the shared dependency produces a new, skipped
    // stage.  If instead it reused the existing stage, then this would be stage 2
    completeNextStageWithFetchFailure(3, 0, shuffleDep)
    scheduler.resubmitFailedStages()

    // the scheduler now creates a new task set to regenerate the missing map output, but this time
    // using a different stage, the "skipped" one

    // SPARK-9809 -- this stage is submitted without a task for each partition (because some of
    // the shuffle map output is still available from stage 0); make sure we've still got internal
    // accumulators setup
    assert(scheduler.stageIdToStage(2).latestInfo.taskMetrics != null)
    completeShuffleMapStageSuccessfully(2, 1, 2)
    completeNextResultStageWithSuccess(3, 1, idx => idx + 1234)
    assert(results === Map(0 -> 1234, 1 -> 1235))

    assertDataStructuresEmpty()
  }

  /**
   * This test runs a three stage job, with a fetch failure in stage 1.  but during the retry, we
   * have completions from both the first & second attempt of stage 1.  So all the map output is
   * available before we finish any task set for stage 1.  We want to make sure that we don't
   * submit stage 2 until the map output for stage 1 is registered
   */
  test("don't submit stage until its dependencies map outputs are registered (SPARK-5259)") {
    val firstRDD = new MyRDD(sc, 3, Nil)
    val firstShuffleDep = new ShuffleDependency(firstRDD, new HashPartitioner(3))
    val firstShuffleId = firstShuffleDep.shuffleId
    val shuffleMapRdd = new MyRDD(sc, 3, List(firstShuffleDep))
    val shuffleDep = new ShuffleDependency(shuffleMapRdd, new HashPartitioner(1))
    val reduceRdd = new MyRDD(sc, 1, List(shuffleDep))
    submit(reduceRdd, Array(0))

    // things start out smoothly, stage 0 completes with no issues
    completeShuffleMapStageSuccessfully(
      0, 0, shuffleMapRdd.partitions.length, Seq("hostB", "hostB", "hostA"))

    // then one executor dies, and a task fails in stage 1
    runEvent(ExecutorLost("hostA-exec", ExecutorKilled))
    runEvent(makeCompletionEvent(
      taskSets(1).tasks(0),
      FetchFailed(null, firstShuffleId, 2L, 2, 0, "Fetch failed"),
      null))

    // so we resubmit stage 0, which completes happily
    scheduler.resubmitFailedStages()
    val stage0Resubmit = taskSets(2)
    assert(stage0Resubmit.stageId == 0)
    assert(stage0Resubmit.stageAttemptId === 1)
    val task = stage0Resubmit.tasks(0)
    assert(task.partitionId === 2)
    runEvent(makeCompletionEvent(
      task,
      Success,
      makeMapStatus("hostC", shuffleMapRdd.partitions.length)))

    // now here is where things get tricky : we will now have a task set representing
    // the second attempt for stage 1, but we *also* have some tasks for the first attempt for
    // stage 1 still going
    val stage1Resubmit = taskSets(3)
    assert(stage1Resubmit.stageId == 1)
    assert(stage1Resubmit.stageAttemptId === 1)
    assert(stage1Resubmit.tasks.length === 3)

    // we'll have some tasks finish from the first attempt, and some finish from the second attempt,
    // so that we actually have all stage outputs, though no attempt has completed all its
    // tasks
    runEvent(makeCompletionEvent(
      taskSets(3).tasks(0),
      Success,
      makeMapStatus("hostC", reduceRdd.partitions.length)))
    runEvent(makeCompletionEvent(
      taskSets(3).tasks(1),
      Success,
      makeMapStatus("hostC", reduceRdd.partitions.length)))
    // late task finish from the first attempt
    runEvent(makeCompletionEvent(
      taskSets(1).tasks(2),
      Success,
      makeMapStatus("hostB", reduceRdd.partitions.length)))

    // What should happen now is that we submit stage 2.  However, we might not see an error
    // b/c of DAGScheduler's error handling (it tends to swallow errors and just log them).  But
    // we can check some conditions.
    // Note that the really important thing here is not so much that we submit stage 2 *immediately*
    // but that we don't end up with some error from these interleaved completions.  It would also
    // be OK (though sub-optimal) if stage 2 simply waited until the resubmission of stage 1 had
    // all its tasks complete

    // check that we have all the map output for stage 0 (it should have been there even before
    // the last round of completions from stage 1, but just to double check it hasn't been messed
    // up) and also the newly available stage 1
    val stageToReduceIdxs = Seq(
      0 -> (0 until 3),
      1 -> (0 until 1)
    )
    for {
      (stage, reduceIdxs) <- stageToReduceIdxs
      reduceIdx <- reduceIdxs
    } {
      // this would throw an exception if the map status hadn't been registered
      val statuses = mapOutputTracker.getMapSizesByExecutorId(stage, reduceIdx)
      // really we should have already thrown an exception rather than fail either of these
      // asserts, but just to be extra defensive let's double check the statuses are OK
      assert(statuses != null)
      assert(statuses.nonEmpty)
    }

    // and check that stage 2 has been submitted
    assert(taskSets.size == 5)
    val stage2TaskSet = taskSets(4)
    assert(stage2TaskSet.stageId == 2)
    assert(stage2TaskSet.stageAttemptId == 0)
  }

  /**
   * We lose an executor after completing some shuffle map tasks on it.  Those tasks get
   * resubmitted, and when they finish the job completes normally
   */
  test("register map outputs correctly after ExecutorLost and task Resubmitted") {
    val firstRDD = new MyRDD(sc, 3, Nil)
    val firstShuffleDep = new ShuffleDependency(firstRDD, new HashPartitioner(2))
    val reduceRdd = new MyRDD(sc, 5, List(firstShuffleDep))
    submit(reduceRdd, Array(0))

    // complete some of the tasks from the first stage, on one host
    runEvent(makeCompletionEvent(
      taskSets(0).tasks(0),
      Success,
      makeMapStatus("hostA", reduceRdd.partitions.length)))
    runEvent(makeCompletionEvent(
      taskSets(0).tasks(1),
      Success,
      makeMapStatus("hostA", reduceRdd.partitions.length)))

    // now that host goes down
    runEvent(ExecutorLost("hostA-exec", ExecutorKilled))

    // so we resubmit those tasks
    runEvent(makeCompletionEvent(taskSets(0).tasks(0), Resubmitted, null))
    runEvent(makeCompletionEvent(taskSets(0).tasks(1), Resubmitted, null))

    // now complete everything on a different host
    completeShuffleMapStageSuccessfully(
      0, 0, reduceRdd.partitions.length, Seq("hostB", "hostB", "hostB"))

    // now we should submit stage 1, and the map output from stage 0 should be registered

    // check that we have all the map output for stage 0
    reduceRdd.partitions.indices.foreach { reduceIdx =>
      val statuses = mapOutputTracker.getMapSizesByExecutorId(0, reduceIdx)
      // really we should have already thrown an exception rather than fail either of these
      // asserts, but just to be extra defensive let's double check the statuses are OK
      assert(statuses != null)
      assert(statuses.nonEmpty)
    }

    // and check that stage 1 has been submitted
    assert(taskSets.size == 2)
    val stage1TaskSet = taskSets(1)
    assert(stage1TaskSet.stageId == 1)
    assert(stage1TaskSet.stageAttemptId == 0)
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
    val shuffleDep1 = new ShuffleDependency(shuffleMapRdd1, new HashPartitioner(2))
    val shuffleMapRdd2 = new MyRDD(sc, 2, Nil)
    val shuffleDep2 = new ShuffleDependency(shuffleMapRdd2, new HashPartitioner(2))

    val reduceRdd1 = new MyRDD(sc, 2, List(shuffleDep1), tracker = mapOutputTracker)
    val reduceRdd2 = new MyRDD(sc, 2, List(shuffleDep1, shuffleDep2), tracker = mapOutputTracker)

    // We need to make our own listeners for this test, since by default submit uses the same
    // listener for all jobs, and here we want to capture the failure for each job separately.
    class FailureRecordingJobListener() extends JobListener {
      var failureMessage: String = _
      override def taskSucceeded(index: Int, result: Any): Unit = {}
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
    assert(sparkListener.successfulStages.isEmpty)
    assert(sparkListener.failedStages.toSet === Set(0, 2))

    assert(listener1.failureMessage === s"Job aborted due to stage failure: $stageFailureMessage")
    assert(listener2.failureMessage === s"Job aborted due to stage failure: $stageFailureMessage")
    assertDataStructuresEmpty()
  }

  def checkJobPropertiesAndPriority(taskSet: TaskSet, expected: String, priority: Int): Unit = {
    assert(taskSet.properties != null)
    assert(taskSet.properties.getProperty("testProperty") === expected)
    assert(taskSet.priority === priority)
  }

  def launchJobsThatShareStageAndCancelFirst(): ShuffleDependency[Int, Int, Nothing] = {
    val baseRdd = new MyRDD(sc, 1, Nil)
    val shuffleDep1 = new ShuffleDependency(baseRdd, new HashPartitioner(1))
    val intermediateRdd = new MyRDD(sc, 1, List(shuffleDep1))
    val shuffleDep2 = new ShuffleDependency(intermediateRdd, new HashPartitioner(1))
    val finalRdd1 = new MyRDD(sc, 1, List(shuffleDep2))
    val finalRdd2 = new MyRDD(sc, 1, List(shuffleDep2))
    val job1Properties = new Properties()
    val job2Properties = new Properties()
    job1Properties.setProperty("testProperty", "job1")
    job2Properties.setProperty("testProperty", "job2")

    // Run jobs 1 & 2, both referencing the same stage, then cancel job1.
    // Note that we have to submit job2 before we cancel job1 to have them actually share
    // *Stages*, and not just shuffle dependencies, due to skipped stages (at least until
    // we address SPARK-10193.)
    val jobId1 = submit(finalRdd1, Array(0), properties = job1Properties)
    val jobId2 = submit(finalRdd2, Array(0), properties = job2Properties)
    assert(scheduler.activeJobs.nonEmpty)
    val testProperty1 = scheduler.jobIdToActiveJob(jobId1).properties.getProperty("testProperty")

    // remove job1 as an ActiveJob
    cancel(jobId1)

    // job2 should still be running
    assert(scheduler.activeJobs.nonEmpty)
    val testProperty2 = scheduler.jobIdToActiveJob(jobId2).properties.getProperty("testProperty")
    assert(testProperty1 != testProperty2)
    // NB: This next assert isn't necessarily the "desired" behavior; it's just to document
    // the current behavior.  We've already submitted the TaskSet for stage 0 based on job1, but
    // even though we have cancelled that job and are now running it because of job2, we haven't
    // updated the TaskSet's properties.  Changing the properties to "job2" is likely the more
    // correct behavior.
    val job1Id = 0  // TaskSet priority for Stages run with "job1" as the ActiveJob
    checkJobPropertiesAndPriority(taskSets(0), "job1", job1Id)
    completeShuffleMapStageSuccessfully(0, 0, 1)

    shuffleDep1
  }

  /**
   * Makes sure that tasks for a stage used by multiple jobs are submitted with the properties of a
   * later, active job if they were previously run under a job that is no longer active
   */
  test("stage used by two jobs, the first no longer active (SPARK-6880)") {
    launchJobsThatShareStageAndCancelFirst()

    // The next check is the key for SPARK-6880.  For the stage which was shared by both job1 and
    // job2 but never had any tasks submitted for job1, the properties of job2 are now used to run
    // the stage.
    checkJobPropertiesAndPriority(taskSets(1), "job2", 1)

    completeShuffleMapStageSuccessfully(1, 0, 1)
    assert(taskSets(2).properties != null)
    complete(taskSets(2), Seq((Success, 42)))
    assert(results === Map(0 -> 42))
    assert(scheduler.activeJobs.isEmpty)

    assertDataStructuresEmpty()
  }

  /**
   * Makes sure that tasks for a stage used by multiple jobs are submitted with the properties of a
   * later, active job if they were previously run under a job that is no longer active, even when
   * there are fetch failures
   */
  test("stage used by two jobs, some fetch failures, and the first job no longer active " +
    "(SPARK-6880)") {
    val shuffleDep1 = launchJobsThatShareStageAndCancelFirst()
    val job2Id = 1  // TaskSet priority for Stages run with "job2" as the ActiveJob

    // lets say there is a fetch failure in this task set, which makes us go back and
    // run stage 0, attempt 1
    completeNextStageWithFetchFailure(1, 0, shuffleDep1)
    scheduler.resubmitFailedStages()

    // stage 0, attempt 1 should have the properties of job2
    assert(taskSets(2).stageId === 0)
    assert(taskSets(2).stageAttemptId === 1)
    checkJobPropertiesAndPriority(taskSets(2), "job2", job2Id)

    // run the rest of the stages normally, checking that they have the correct properties
    completeShuffleMapStageSuccessfully(0, 1, 1)
    checkJobPropertiesAndPriority(taskSets(3), "job2", job2Id)
    completeShuffleMapStageSuccessfully(1, 1, 1)
    checkJobPropertiesAndPriority(taskSets(4), "job2", job2Id)
    complete(taskSets(4), Seq((Success, 42)))
    assert(results === Map(0 -> 42))
    assert(scheduler.activeJobs.isEmpty)

    assertDataStructuresEmpty()
  }

  /**
   * In this test, we run a map stage where one of the executors fails but we still receive a
   * "zombie" complete message from a task that ran on that executor. We want to make sure the
   * stage is resubmitted so that the task that ran on the failed executor is re-executed, and
   * that the stage is only marked as finished once that task completes.
   */
  test("run trivial shuffle with out-of-band executor failure and retry") {
    val shuffleMapRdd = new MyRDD(sc, 2, Nil)
    val shuffleDep = new ShuffleDependency(shuffleMapRdd, new HashPartitioner(1))
    val shuffleId = shuffleDep.shuffleId
    val reduceRdd = new MyRDD(sc, 1, List(shuffleDep), tracker = mapOutputTracker)
    submit(reduceRdd, Array(0))
    // Tell the DAGScheduler that hostA was lost.
    runEvent(ExecutorLost("hostA-exec", ExecutorKilled))
    completeShuffleMapStageSuccessfully(0, 0, 1)

    // At this point, no more tasks are running for the stage (and the TaskSetManager considers the
    // stage complete), but the tasks that ran on HostA need to be re-run, so the DAGScheduler
    // should re-submit the stage with one task (the task that originally ran on HostA).
    assert(taskSets.size === 2)
    assert(taskSets(1).tasks.size === 1)

    // Make sure that the stage that was re-submitted was the ShuffleMapStage (not the reduce
    // stage, which shouldn't be run until all of the tasks in the ShuffleMapStage complete on
    // alive executors).
    assert(taskSets(1).tasks(0).isInstanceOf[ShuffleMapTask])

    // have hostC complete the resubmitted task
    complete(taskSets(1), Seq((Success, makeMapStatus("hostC", 1))))
    assert(mapOutputTracker.getMapSizesByExecutorId(shuffleId, 0).map(_._1).toSet ===
      HashSet(makeBlockManagerId("hostC"), makeBlockManagerId("hostB")))

    // Make sure that the reduce stage was now submitted.
    assert(taskSets.size === 3)
    assert(taskSets(2).tasks(0).isInstanceOf[ResultTask[_, _]])

    // Complete the reduce stage.
    complete(taskSets(2), Seq((Success, 42)))
    assert(results === Map(0 -> 42))
    assertDataStructuresEmpty()
  }

  test("recursive shuffle failures") {
    val shuffleOneRdd = new MyRDD(sc, 2, Nil)
    val shuffleDepOne = new ShuffleDependency(shuffleOneRdd, new HashPartitioner(2))
    val shuffleTwoRdd = new MyRDD(sc, 2, List(shuffleDepOne), tracker = mapOutputTracker)
    val shuffleDepTwo = new ShuffleDependency(shuffleTwoRdd, new HashPartitioner(1))
    val finalRdd = new MyRDD(sc, 1, List(shuffleDepTwo), tracker = mapOutputTracker)
    submit(finalRdd, Array(0))
    // have the first stage complete normally
    completeShuffleMapStageSuccessfully(0, 0, 2)
    // have the second stage complete normally
    completeShuffleMapStageSuccessfully(1, 0, 1, Seq("hostA", "hostC"))
    // fail the third stage because hostA went down
    completeNextStageWithFetchFailure(2, 0, shuffleDepTwo)
    // TODO assert this:
    // blockManagerMaster.removeExecutor("hostA-exec")
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
    val shuffleDepOne = new ShuffleDependency(shuffleOneRdd, new HashPartitioner(2))
    val shuffleTwoRdd = new MyRDD(sc, 2, List(shuffleDepOne), tracker = mapOutputTracker).cache()
    val shuffleDepTwo = new ShuffleDependency(shuffleTwoRdd, new HashPartitioner(1))
    val finalRdd = new MyRDD(sc, 1, List(shuffleDepTwo), tracker = mapOutputTracker)
    submit(finalRdd, Array(0))
    cacheLocations(shuffleTwoRdd.id -> 0) = Seq(makeBlockManagerId("hostD"))
    cacheLocations(shuffleTwoRdd.id -> 1) = Seq(makeBlockManagerId("hostC"))
    // complete stage 0
    completeShuffleMapStageSuccessfully(0, 0, 2)
    // complete stage 1
    completeShuffleMapStageSuccessfully(1, 0, 1)
    // pretend stage 2 failed because hostA went down
    completeNextStageWithFetchFailure(2, 0, shuffleDepTwo)
    // TODO assert this:
    // blockManagerMaster.removeExecutor("hostA-exec")
    // DAGScheduler should notice the cached copy of the second shuffle and try to get it rerun.
    scheduler.resubmitFailedStages()
    assertLocations(taskSets(3), Seq(Seq("hostD")))
    // allow hostD to recover
    complete(taskSets(3), Seq((Success, makeMapStatus("hostD", 1))))
    complete(taskSets(4), Seq((Success, 42)))
    assert(results === Map(0 -> 42))
    assertDataStructuresEmpty()
  }

  test("SPARK-30388: shuffle fetch failed on speculative task, but original task succeed") {
    var completedStage: List[Int] = Nil
    val listener = new SparkListener() {
      override def onStageCompleted(event: SparkListenerStageCompleted): Unit = {
        completedStage = completedStage :+ event.stageInfo.stageId
      }
    }
    sc.addSparkListener(listener)

    val shuffleMapRdd = new MyRDD(sc, 2, Nil)
    val shuffleDep = new ShuffleDependency(shuffleMapRdd, new HashPartitioner(2))
    val reduceRdd = new MyRDD(sc, 2, List(shuffleDep))
    submit(reduceRdd, Array(0, 1))
    completeShuffleMapStageSuccessfully(0, 0, 2)
    sc.listenerBus.waitUntilEmpty()
    assert(completedStage === List(0))

    // result task 0.0 succeed
    runEvent(makeCompletionEvent(taskSets(1).tasks(0), Success, 42))
    // speculative result task 1.1 fetch failed
    val info = new TaskInfo(
      4, index = 1, attemptNumber = 1, partitionId = 1, 0L, "", "", TaskLocality.ANY, true)
    runEvent(makeCompletionEvent(
        taskSets(1).tasks(1),
        FetchFailed(makeBlockManagerId("hostA"), shuffleDep.shuffleId, 0L, 0, 1, "ignored"),
        null,
        Seq.empty,
        Array.empty,
        info
      )
    )
    sc.listenerBus.waitUntilEmpty()
    assert(completedStage === List(0, 1))

    Thread.sleep(DAGScheduler.RESUBMIT_TIMEOUT * 2)
    // map stage resubmitted
    assert(scheduler.runningStages.size === 1)
    val mapStage = scheduler.runningStages.head
    assert(mapStage.id === 0)
    assert(mapStage.latestInfo.failureReason.isEmpty)

    // original result task 1.0 succeed
    runEvent(makeCompletionEvent(taskSets(1).tasks(1), Success, 42))
    sc.listenerBus.waitUntilEmpty()
    assert(completedStage === List(0, 1, 1, 0))
    assert(scheduler.activeJobs.isEmpty)
  }

  test("misbehaved accumulator should not crash DAGScheduler and SparkContext") {
    val acc = new LongAccumulator {
      override def add(v: java.lang.Long): Unit = throw new DAGSchedulerSuiteDummyException
      override def add(v: Long): Unit = throw new DAGSchedulerSuiteDummyException
    }
    sc.register(acc)

    // Run this on executors
    sc.parallelize(1 to 10, 2).foreach { item => acc.add(1) }

    // Make sure we can still run commands
    assert(sc.parallelize(1 to 10, 2).count() === 10)
  }

  test("misbehaved accumulator should not impact other accumulators") {
    val bad = new LongAccumulator {
      override def merge(other: AccumulatorV2[java.lang.Long, java.lang.Long]): Unit = {
        throw new DAGSchedulerSuiteDummyException
      }
    }
    sc.register(bad, "bad")
    val good = sc.longAccumulator("good")

    sc.parallelize(1 to 10, 2).foreach { item =>
      bad.add(1)
      good.add(1)
    }

    // This is to ensure the `bad` accumulator did fail to update its value
    assert(bad.value == 0L)
    // Should be able to update the "good" accumulator
    assert(good.value == 10L)
  }

  /**
   * The job will be failed on first task throwing an error.
   *  Any subsequent task WILL throw a legitimate java.lang.UnsupportedOperationException.
   *  If multiple tasks, there exists a race condition between the SparkDriverExecutionExceptions
   *  and their differing causes as to which will represent result for job...
   */
  test("misbehaved resultHandler should not crash DAGScheduler and SparkContext") {
    failAfter(1.minute) { // If DAGScheduler crashes, the following test will hang forever
      for (error <- Seq(
        new DAGSchedulerSuiteDummyException,
        new AssertionError, // E.g., assert(foo == bar) fails
        new NotImplementedError // E.g., call a method with `???` implementation.
      )) {
        val e = intercept[SparkDriverExecutionException] {
          // Number of parallelized partitions implies number of tasks of job
          val rdd = sc.parallelize(1 to 10, 2)
          sc.runJob[Int, Int](
            rdd,
            (context: TaskContext, iter: Iterator[Int]) => iter.size,
            // For a robust test assertion, limit number of job tasks to 1; that is,
            // if multiple RDD partitions, use id of any one partition, say, first partition id=0
            Seq(0),
            (part: Int, result: Int) => throw error)
        }
        assert(e.getCause eq error)

        // Make sure we can still run commands on our SparkContext
        assert(sc.parallelize(1 to 10, 2).count() === 10)
      }
    }
  }

  test(s"invalid ${SparkContext.SPARK_JOB_INTERRUPT_ON_CANCEL} should not crash DAGScheduler") {
    sc.setLocalProperty(SparkContext.SPARK_JOB_INTERRUPT_ON_CANCEL, "invalid")
    try {
      intercept[SparkException] {
        sc.parallelize(1 to 1, 1).foreach { _ =>
          throw new DAGSchedulerSuiteDummyException
        }
      }
      // Verify the above job didn't crash DAGScheduler by running a simple job
      assert(sc.parallelize(1 to 10, 2).count() === 10)
    } finally {
      sc.setLocalProperty(SparkContext.SPARK_JOB_INTERRUPT_ON_CANCEL, null)
    }
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
    val accum = AccumulatorSuite.createLongAccum("a")
    val finalRdd = new MyRDD(sc, 1, Nil)
    submit(finalRdd, Array(0))
    completeWithAccumulator(accum.id, taskSets(0), Seq((Success, 42)))
    completeWithAccumulator(accum.id, taskSets(0), Seq((Success, 42)))
    assert(results === Map(0 -> 42))

    assert(accum.value === 1)
    assertDataStructuresEmpty()
  }

  test("accumulator not calculated for resubmitted task in result stage") {
    val accum = AccumulatorSuite.createLongAccum("a")
    val finalRdd = new MyRDD(sc, 2, Nil)
    submit(finalRdd, Array(0, 1))
    // finish the first task
    completeWithAccumulator(accum.id, taskSets(0), Seq((Success, 42)))
    // verify stage exists
    assert(scheduler.stageIdToStage.contains(0))

    // finish the first task again (simulate a speculative task or a resubmitted task)
    completeWithAccumulator(accum.id, taskSets(0), Seq((Success, 42)))
    assert(results === Map(0 -> 42))

    // The accumulator should only be updated once.
    assert(accum.value === 1)

    runEvent(makeCompletionEvent(taskSets(0).tasks(1), Success, 42))
    assertDataStructuresEmpty()
  }

  test("accumulators are updated on exception failures and task killed") {
    val acc1 = AccumulatorSuite.createLongAccum("ingenieur")
    val acc2 = AccumulatorSuite.createLongAccum("boulanger")
    val acc3 = AccumulatorSuite.createLongAccum("agriculteur")
    assert(AccumulatorContext.get(acc1.id).isDefined)
    assert(AccumulatorContext.get(acc2.id).isDefined)
    assert(AccumulatorContext.get(acc3.id).isDefined)
    val accUpdate1 = new LongAccumulator
    accUpdate1.metadata = acc1.metadata
    accUpdate1.setValue(15)
    val accUpdate2 = new LongAccumulator
    accUpdate2.metadata = acc2.metadata
    accUpdate2.setValue(13)
    val accUpdate3 = new LongAccumulator
    accUpdate3.metadata = acc3.metadata
    accUpdate3.setValue(18)

    val accumUpdates1 = Seq(accUpdate1, accUpdate2)
    val accumInfo1 = accumUpdates1.map(AccumulatorSuite.makeInfo)
    val exceptionFailure = new ExceptionFailure(
      new SparkException("fondue?"),
      accumInfo1).copy(accums = accumUpdates1)
    submit(new MyRDD(sc, 1, Nil), Array(0))
    runEvent(makeCompletionEvent(taskSets.head.tasks.head, exceptionFailure, "result"))

    assert(AccumulatorContext.get(acc1.id).get.value === 15L)
    assert(AccumulatorContext.get(acc2.id).get.value === 13L)

    val accumUpdates2 = Seq(accUpdate3)
    val accumInfo2 = accumUpdates2.map(AccumulatorSuite.makeInfo)

    val taskKilled = new TaskKilled( "test", accumInfo2, accums = accumUpdates2)
    runEvent(makeCompletionEvent(taskSets.head.tasks.head, taskKilled, "result"))

    assert(AccumulatorContext.get(acc3.id).get.value === 18L)
  }

  test("reduce tasks should be placed locally with map output") {
    // Create a shuffleMapRdd with 1 partition
    val shuffleMapRdd = new MyRDD(sc, 1, Nil)
    val shuffleDep = new ShuffleDependency(shuffleMapRdd, new HashPartitioner(1))
    val shuffleId = shuffleDep.shuffleId
    val reduceRdd = new MyRDD(sc, 1, List(shuffleDep), tracker = mapOutputTracker)
    submit(reduceRdd, Array(0))
    completeShuffleMapStageSuccessfully(0, 0, 1)
    assert(mapOutputTracker.getMapSizesByExecutorId(shuffleId, 0).map(_._1).toSet ===
      HashSet(makeBlockManagerId("hostA")))

    // Reducer should run on the same host that map task ran
    val reduceTaskSet = taskSets(1)
    assertLocations(reduceTaskSet, Seq(Seq("hostA")))
    complete(reduceTaskSet, Seq((Success, 42)))
    assert(results === Map(0 -> 42))
    assertDataStructuresEmpty()
  }

  test("reduce task locality preferences should only include machines with largest map outputs") {
    val numMapTasks = 4
    // Create a shuffleMapRdd with more partitions
    val shuffleMapRdd = new MyRDD(sc, numMapTasks, Nil)
    val shuffleDep = new ShuffleDependency(shuffleMapRdd, new HashPartitioner(1))
    val shuffleId = shuffleDep.shuffleId
    val reduceRdd = new MyRDD(sc, 1, List(shuffleDep), tracker = mapOutputTracker)
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
    val shuffleDep = new ShuffleDependency(rdd1, new HashPartitioner(1))
    val narrowDep = new OneToOneDependency(rdd2)
    val shuffleId = shuffleDep.shuffleId
    val reduceRdd = new MyRDD(sc, 1, List(shuffleDep, narrowDep), tracker = mapOutputTracker)
    submit(reduceRdd, Array(0))
    completeShuffleMapStageSuccessfully(0, 0, 1)
    assert(mapOutputTracker.getMapSizesByExecutorId(shuffleId, 0).map(_._1).toSet ===
      HashSet(makeBlockManagerId("hostA")))

    // Reducer should run where RDD 2 has preferences, even though it also has a shuffle dep
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
    val stackTraceString = Utils.exceptionString(e)

    // should actually include the RDD operation that invoked the method:
    assert(stackTraceString.contains("org.apache.spark.rdd.RDD.count"))

    // should include the FunSuite setup:
    assert(stackTraceString.contains("org.scalatest.funsuite.AnyFunSuite"))
  }

  test("catch errors in event loop") {
    // this is a test of our testing framework -- make sure errors in event loop don't get ignored

    // just run some bad event that will throw an exception -- we'll give a null TaskEndReason
    val rdd1 = new MyRDD(sc, 1, Nil)
    submit(rdd1, Array(0))
    intercept[Exception] {
      complete(taskSets(0), Seq(
        (null, makeMapStatus("hostA", 1))))
    }
  }

  test("simple map stage submission") {
    val shuffleMapRdd = new MyRDD(sc, 2, Nil)
    val shuffleDep = new ShuffleDependency(shuffleMapRdd, new HashPartitioner(1))
    val shuffleId = shuffleDep.shuffleId
    val reduceRdd = new MyRDD(sc, 1, List(shuffleDep), tracker = mapOutputTracker)

    // Submit a map stage by itself
    submitMapStage(shuffleDep)
    assert(results.size === 0)  // No results yet
    completeShuffleMapStageSuccessfully(0, 0, 1)
    assert(results.size === 1)
    results.clear()
    assertDataStructuresEmpty()

    // Submit a reduce job that depends on this map stage; it should directly do the reduce
    submit(reduceRdd, Array(0))
    completeNextResultStageWithSuccess(2, 0)
    assert(results === Map(0 -> 42))
    results.clear()
    assertDataStructuresEmpty()

    // Check that if we submit the map stage again, no tasks run
    submitMapStage(shuffleDep)
    assert(results.size === 1)
    assertDataStructuresEmpty()
  }

  test("map stage submission with reduce stage also depending on the data") {
    val shuffleMapRdd = new MyRDD(sc, 2, Nil)
    val shuffleDep = new ShuffleDependency(shuffleMapRdd, new HashPartitioner(1))
    val shuffleId = shuffleDep.shuffleId
    val reduceRdd = new MyRDD(sc, 1, List(shuffleDep), tracker = mapOutputTracker)

    // Submit the map stage by itself
    submitMapStage(shuffleDep)

    // Submit a reduce job that depends on this map stage
    submit(reduceRdd, Array(0))

    // Complete tasks for the map stage
    completeShuffleMapStageSuccessfully(0, 0, 1)
    assert(results.size === 1)
    results.clear()

    // Complete tasks for the reduce stage
    completeNextResultStageWithSuccess(1, 0)
    assert(results === Map(0 -> 42))
    results.clear()
    assertDataStructuresEmpty()

    // Check that if we submit the map stage again, no tasks run
    submitMapStage(shuffleDep)
    assert(results.size === 1)
    assertDataStructuresEmpty()
  }

  test("map stage submission with fetch failure") {
    val shuffleMapRdd = new MyRDD(sc, 2, Nil)
    val shuffleDep = new ShuffleDependency(shuffleMapRdd, new HashPartitioner(2))
    val shuffleId = shuffleDep.shuffleId
    val reduceRdd = new MyRDD(sc, 2, List(shuffleDep), tracker = mapOutputTracker)

    // Submit a map stage by itself
    submitMapStage(shuffleDep)
    completeShuffleMapStageSuccessfully(0, 0, reduceRdd.partitions.length)
    assert(results.size === 1)
    results.clear()
    assertDataStructuresEmpty()

    // Submit a reduce job that depends on this map stage, but where one reduce will fail a fetch
    submit(reduceRdd, Array(0, 1))
    complete(taskSets(1), Seq(
      (Success, 42),
      (FetchFailed(makeBlockManagerId("hostA"), shuffleId, 0L, 0, 0, "ignored"), null)))
    // Ask the scheduler to try it again; TaskSet 2 will rerun the map task that we couldn't fetch
    // from, then TaskSet 3 will run the reduce stage
    scheduler.resubmitFailedStages()
    complete(taskSets(2), Seq((Success, makeMapStatus("hostA", reduceRdd.partitions.length))))
    complete(taskSets(3), Seq((Success, 43)))
    assert(results === Map(0 -> 42, 1 -> 43))
    results.clear()
    assertDataStructuresEmpty()

    // Run another reduce job without a failure; this should just work
    submit(reduceRdd, Array(0, 1))
    complete(taskSets(4), Seq(
      (Success, 44),
      (Success, 45)))
    assert(results === Map(0 -> 44, 1 -> 45))
    results.clear()
    assertDataStructuresEmpty()

    // Resubmit the map stage; this should also just work
    submitMapStage(shuffleDep)
    assert(results.size === 1)
    results.clear()
    assertDataStructuresEmpty()
  }

  /**
   * In this test, we have three RDDs with shuffle dependencies, and we submit map stage jobs
   * that are waiting on each one, as well as a reduce job on the last one. We test that all of
   * these jobs complete even if there are some fetch failures in both shuffles.
   */
  test("map stage submission with multiple shared stages and failures") {
    val rdd1 = new MyRDD(sc, 2, Nil)
    val dep1 = new ShuffleDependency(rdd1, new HashPartitioner(2))
    val rdd2 = new MyRDD(sc, 2, List(dep1), tracker = mapOutputTracker)
    val dep2 = new ShuffleDependency(rdd2, new HashPartitioner(2))
    val rdd3 = new MyRDD(sc, 2, List(dep2), tracker = mapOutputTracker)

    val listener1 = new SimpleListener
    val listener2 = new SimpleListener
    val listener3 = new SimpleListener

    submitMapStage(dep1, listener1)
    submitMapStage(dep2, listener2)
    submit(rdd3, Array(0, 1), listener = listener3)

    // Complete the first stage
    assert(taskSets(0).stageId === 0)
    completeShuffleMapStageSuccessfully(0, 0, rdd1.partitions.length)
    assert(mapOutputTracker.getMapSizesByExecutorId(dep1.shuffleId, 0).map(_._1).toSet ===
      HashSet(makeBlockManagerId("hostA"), makeBlockManagerId("hostB")))
    assert(listener1.results.size === 1)

    // When attempting the second stage, show a fetch failure
    assert(taskSets(1).stageId === 1)
    complete(taskSets(1), Seq(
      (Success, makeMapStatus("hostA", rdd2.partitions.length)),
      (FetchFailed(makeBlockManagerId("hostA"), dep1.shuffleId, 0L, 0, 0, "ignored"), null)))
    scheduler.resubmitFailedStages()
    assert(listener2.results.size === 0)    // Second stage listener should not have a result yet

    // Stage 0 should now be running as task set 2; make its task succeed
    assert(taskSets(2).stageId === 0)
    complete(taskSets(2), Seq(
      (Success, makeMapStatus("hostC", rdd2.partitions.length))))
    assert(mapOutputTracker.getMapSizesByExecutorId(dep1.shuffleId, 0).map(_._1).toSet ===
      HashSet(makeBlockManagerId("hostC"), makeBlockManagerId("hostB")))
    assert(listener2.results.size === 0)    // Second stage listener should still not have a result

    // Stage 1 should now be running as task set 3; make its first task succeed
    assert(taskSets(3).stageId === 1)
    complete(taskSets(3), Seq(
      (Success, makeMapStatus("hostB", rdd2.partitions.length)),
      (Success, makeMapStatus("hostD", rdd2.partitions.length))))
    assert(mapOutputTracker.getMapSizesByExecutorId(dep2.shuffleId, 0).map(_._1).toSet ===
      HashSet(makeBlockManagerId("hostB"), makeBlockManagerId("hostD")))
    assert(listener2.results.size === 1)

    // Finally, the reduce job should be running as task set 4; make it see a fetch failure,
    // then make it run again and succeed
    assert(taskSets(4).stageId === 2)
    complete(taskSets(4), Seq(
      (Success, 52),
      (FetchFailed(makeBlockManagerId("hostD"), dep2.shuffleId, 0L, 0, 0, "ignored"), null)))
    scheduler.resubmitFailedStages()

    // TaskSet 5 will rerun stage 1's lost task, then TaskSet 6 will rerun stage 2
    assert(taskSets(5).stageId === 1)
    complete(taskSets(5), Seq(
      (Success, makeMapStatus("hostE", rdd2.partitions.length))))
    complete(taskSets(6), Seq(
      (Success, 53)))
    assert(listener3.results === Map(0 -> 52, 1 -> 53))
    assertDataStructuresEmpty()
  }

  test("Trigger mapstage's job listener in submitMissingTasks") {
    val rdd1 = new MyRDD(sc, 2, Nil)
    val dep1 = new ShuffleDependency(rdd1, new HashPartitioner(2))
    val rdd2 = new MyRDD(sc, 2, List(dep1), tracker = mapOutputTracker)
    val dep2 = new ShuffleDependency(rdd2, new HashPartitioner(2))

    val listener1 = new SimpleListener
    val listener2 = new SimpleListener

    submitMapStage(dep1, listener1)
    submitMapStage(dep2, listener2)

    // Complete the stage0.
    assert(taskSets(0).stageId === 0)
    completeShuffleMapStageSuccessfully(0, 0, rdd1.partitions.length)
    assert(mapOutputTracker.getMapSizesByExecutorId(dep1.shuffleId, 0).map(_._1).toSet ===
        HashSet(makeBlockManagerId("hostA"), makeBlockManagerId("hostB")))
    assert(listener1.results.size === 1)

    // When attempting stage1, trigger a fetch failure.
    assert(taskSets(1).stageId === 1)
    complete(taskSets(1), Seq(
      (Success, makeMapStatus("hostC", rdd2.partitions.length)),
      (FetchFailed(makeBlockManagerId("hostA"), dep1.shuffleId, 0L, 0, 0, "ignored"), null)))
    scheduler.resubmitFailedStages()
    // Stage1 listener should not have a result yet
    assert(listener2.results.size === 0)

    // Speculative task succeeded in stage1.
    runEvent(makeCompletionEvent(
      taskSets(1).tasks(1),
      Success,
      makeMapStatus("hostD", rdd2.partitions.length)))
    // stage1 listener still should not have a result, though there's no missing partitions
    // in it. Because stage1 has been failed and is not inside `runningStages` at this moment.
    assert(listener2.results.size === 0)

    // Stage0 should now be running as task set 2; make its task succeed
    assert(taskSets(2).stageId === 0)
    complete(taskSets(2), Seq(
      (Success, makeMapStatus("hostC", rdd2.partitions.length))))
    assert(mapOutputTracker.getMapSizesByExecutorId(dep1.shuffleId, 0).map(_._1).toSet ===
        Set(makeBlockManagerId("hostC"), makeBlockManagerId("hostB")))

    // After stage0 is finished, stage1 will be submitted and found there is no missing
    // partitions in it. Then listener got triggered.
    assert(listener2.results.size === 1)
    assertDataStructuresEmpty()
  }

  /**
   * In this test, we run a map stage where one of the executors fails but we still receive a
   * "zombie" complete message from that executor. We want to make sure the stage is not reported
   * as done until all tasks have completed.
   *
   * Most of the functionality in this test is tested in "run trivial shuffle with out-of-band
   * executor failure and retry".  However, that test uses ShuffleMapStages that are followed by
   * a ResultStage, whereas in this test, the ShuffleMapStage is tested in isolation, without a
   * ResultStage after it.
   */
  test("map stage submission with executor failure late map task completions") {
    val shuffleMapRdd = new MyRDD(sc, 3, Nil)
    val shuffleDep = new ShuffleDependency(shuffleMapRdd, new HashPartitioner(2))

    submitMapStage(shuffleDep)

    val oldTaskSet = taskSets(0)
    runEvent(makeCompletionEvent(oldTaskSet.tasks(0), Success, makeMapStatus("hostA", 2)))
    assert(results.size === 0)    // Map stage job should not be complete yet

    // Pretend host A was lost. This will cause the TaskSetManager to resubmit task 0, because it
    // completed on hostA.
    val oldEpoch = mapOutputTracker.getEpoch
    runEvent(ExecutorLost("hostA-exec", ExecutorKilled))
    val newEpoch = mapOutputTracker.getEpoch
    assert(newEpoch > oldEpoch)

    // Suppose we also get a completed event from task 1 on the same host; this should be ignored
    runEvent(makeCompletionEvent(oldTaskSet.tasks(1), Success, makeMapStatus("hostA", 2)))
    assert(results.size === 0)    // Map stage job should not be complete yet

    // A completion from another task should work because it's a non-failed host
    runEvent(makeCompletionEvent(oldTaskSet.tasks(2), Success, makeMapStatus("hostB", 2)))

    // At this point, no more tasks are running for the stage (and the TaskSetManager considers
    // the stage complete), but the task that ran on hostA needs to be re-run, so the map stage
    // shouldn't be marked as complete, and the DAGScheduler should re-submit the stage.
    assert(results.size === 0)
    assert(taskSets.size === 2)

    // Now complete tasks in the second task set
    val newTaskSet = taskSets(1)
    // 2 tasks should have been re-submitted, for tasks 0 and 1 (which ran on hostA).
    assert(newTaskSet.tasks.size === 2)
    // Complete task 0 from the original task set (i.e., not the one that's currently active).
    // This should still be counted towards the job being complete (but there's still one
    // outstanding task).
    runEvent(makeCompletionEvent(newTaskSet.tasks(0), Success, makeMapStatus("hostB", 2)))
    assert(results.size === 0)

    // Complete the final task, from the currently active task set.  There's still one
    // running task, task 0 in the currently active stage attempt, but the success of task 0 means
    // the DAGScheduler can mark the stage as finished.
    runEvent(makeCompletionEvent(newTaskSet.tasks(1), Success, makeMapStatus("hostB", 2)))
    assert(results.size === 1)    // Map stage job should now finally be complete
    assertDataStructuresEmpty()

    // Also test that a reduce stage using this shuffled data can immediately run
    val reduceRDD = new MyRDD(sc, 2, List(shuffleDep), tracker = mapOutputTracker)
    results.clear()
    submit(reduceRDD, Array(0, 1))
    complete(taskSets(2), Seq((Success, 42), (Success, 43)))
    assert(results === Map(0 -> 42, 1 -> 43))
    results.clear()
    assertDataStructuresEmpty()
  }

  /**
   * Checks the DAGScheduler's internal logic for traversing an RDD DAG by making sure that
   * getShuffleDependenciesAndResourceProfiles correctly returns the direct shuffle dependencies
   * of a particular RDD. The test creates the following RDD graph (where n denotes a narrow
   * dependency and s denotes a shuffle dependency):
   *
   * A <------------s---------,
   *                           \
   * B <--s-- C <--s-- D <--n------ E
   *
   * Here, the direct shuffle dependency of C is just the shuffle dependency on B. The direct
   * shuffle dependencies of E are the shuffle dependency on A and the shuffle dependency on C.
   */
  test("getShuffleDependenciesAndResourceProfiles correctly returns only direct shuffle parents") {
    val rddA = new MyRDD(sc, 2, Nil)
    val shuffleDepA = new ShuffleDependency(rddA, new HashPartitioner(1))
    val rddB = new MyRDD(sc, 2, Nil)
    val shuffleDepB = new ShuffleDependency(rddB, new HashPartitioner(1))
    val rddC = new MyRDD(sc, 1, List(shuffleDepB))
    val shuffleDepC = new ShuffleDependency(rddC, new HashPartitioner(1))
    val rddD = new MyRDD(sc, 1, List(shuffleDepC))
    val narrowDepD = new OneToOneDependency(rddD)
    val rddE = new MyRDD(sc, 1, List(shuffleDepA, narrowDepD), tracker = mapOutputTracker)

    val (shuffleDepsA, _) = scheduler.getShuffleDependenciesAndResourceProfiles(rddA)
    assert(shuffleDepsA === Set())
    val (shuffleDepsB, _) = scheduler.getShuffleDependenciesAndResourceProfiles(rddB)
    assert(shuffleDepsB === Set())
    val (shuffleDepsC, _) = scheduler.getShuffleDependenciesAndResourceProfiles(rddC)
    assert(shuffleDepsC === Set(shuffleDepB))
    val (shuffleDepsD, _) = scheduler.getShuffleDependenciesAndResourceProfiles(rddD)
    assert(shuffleDepsD === Set(shuffleDepC))
    val (shuffleDepsE, _) = scheduler.getShuffleDependenciesAndResourceProfiles(rddE)
    assert(shuffleDepsE === Set(shuffleDepA, shuffleDepC))
  }

  test("SPARK-17644: After one stage is aborted for too many failed attempts, subsequent stages" +
    "still behave correctly on fetch failures") {
    // Runs a job that always encounters a fetch failure, so should eventually be aborted
    def runJobWithPersistentFetchFailure: Unit = {
      val rdd1 = sc.makeRDD(Array(1, 2, 3, 4), 2).map(x => (x, 1)).groupByKey()
      val shuffleHandle =
        rdd1.dependencies.head.asInstanceOf[ShuffleDependency[_, _, _]].shuffleHandle
      rdd1.map {
        case (x, _) if (x == 1) =>
          throw new FetchFailedException(
            BlockManagerId("1", "1", 1), shuffleHandle.shuffleId, 0L, 0, 0, "test")
        case (x, _) => x
      }.count()
    }

    // Runs a job that encounters a single fetch failure but succeeds on the second attempt
    def runJobWithTemporaryFetchFailure: Unit = {
      val rdd1 = sc.makeRDD(Array(1, 2, 3, 4), 2).map(x => (x, 1)).groupByKey()
      val shuffleHandle =
        rdd1.dependencies.head.asInstanceOf[ShuffleDependency[_, _, _]].shuffleHandle
      rdd1.map {
        case (x, _) if (x == 1) && FailThisAttempt._fail.getAndSet(false) =>
          throw new FetchFailedException(
            BlockManagerId("1", "1", 1), shuffleHandle.shuffleId, 0L, 0, 0, "test")
      }
    }

    failAfter(10.seconds) {
      val e = intercept[SparkException] {
        runJobWithPersistentFetchFailure
      }
      assert(e.getMessage.contains("org.apache.spark.shuffle.FetchFailedException"))
    }

    // Run a second job that will fail due to a fetch failure.
    // This job will hang without the fix for SPARK-17644.
    failAfter(10.seconds) {
      val e = intercept[SparkException] {
        runJobWithPersistentFetchFailure
      }
      assert(e.getMessage.contains("org.apache.spark.shuffle.FetchFailedException"))
    }

    failAfter(10.seconds) {
      try {
        runJobWithTemporaryFetchFailure
      } catch {
        case e: Throwable => fail("A job with one fetch failure should eventually succeed")
      }
    }
  }

  test("[SPARK-19263] DAGScheduler should not submit multiple active tasksets," +
      " even with late completions from earlier stage attempts") {
    // Create 3 RDDs with shuffle dependencies on each other: rddA <--- rddB <--- rddC
    val rddA = new MyRDD(sc, 2, Nil)
    val shuffleDepA = new ShuffleDependency(rddA, new HashPartitioner(2))
    val shuffleIdA = shuffleDepA.shuffleId

    val rddB = new MyRDD(sc, 2, List(shuffleDepA), tracker = mapOutputTracker)
    val shuffleDepB = new ShuffleDependency(rddB, new HashPartitioner(2))

    val rddC = new MyRDD(sc, 2, List(shuffleDepB), tracker = mapOutputTracker)

    submit(rddC, Array(0, 1))

    // Complete both tasks in rddA.
    assert(taskSets(0).stageId === 0 && taskSets(0).stageAttemptId === 0)
    completeShuffleMapStageSuccessfully(0, 0, 2, Seq("hostA", "hostA"))

    // Fetch failed for task(stageId=1, stageAttemptId=0, partitionId=0) running on hostA
    // and task(stageId=1, stageAttemptId=0, partitionId=1) is still running.
    assert(taskSets(1).stageId === 1 && taskSets(1).stageAttemptId === 0)
    runEvent(makeCompletionEvent(
      taskSets(1).tasks(0),
      FetchFailed(makeBlockManagerId("hostA"), shuffleIdA, 0L, 0, 0,
        "Fetch failure of task: stageId=1, stageAttempt=0, partitionId=0"),
      result = null))

    // Both original tasks in rddA should be marked as failed, because they ran on the
    // failed hostA, so both should be resubmitted. Complete them on hostB successfully.
    scheduler.resubmitFailedStages()
    assert(taskSets(2).stageId === 0 && taskSets(2).stageAttemptId === 1
      && taskSets(2).tasks.size === 2)
    complete(taskSets(2), Seq(
      (Success, makeMapStatus("hostB", 2)),
      (Success, makeMapStatus("hostB", 2))))

    // Complete task(stageId=1, stageAttemptId=0, partitionId=1) running on failed hostA
    // successfully. The success should be ignored because the task started before the
    // executor failed, so the output may have been lost.
    runEvent(makeCompletionEvent(
      taskSets(1).tasks(1), Success, makeMapStatus("hostA", 2)))

    // task(stageId=1, stageAttemptId=1, partitionId=1) should be marked completed when
    // task(stageId=1, stageAttemptId=0, partitionId=1) finished
    // ideally we would verify that but no way to get into task scheduler to verify

    // Both tasks in rddB should be resubmitted, because none of them has succeeded truly.
    // Complete the task(stageId=1, stageAttemptId=1, partitionId=0) successfully.
    // Task(stageId=1, stageAttemptId=1, partitionId=1) of this new active stage attempt
    // is still running.
    assert(taskSets(3).stageId === 1 && taskSets(3).stageAttemptId === 1
      && taskSets(3).tasks.size === 2)
    runEvent(makeCompletionEvent(
      taskSets(3).tasks(0), Success, makeMapStatus("hostB", 2)))

    // At this point there should be no active task set for stageId=1 and we need
    // to resubmit because the output from (stageId=1, stageAttemptId=0, partitionId=1)
    // was ignored due to executor failure
    assert(taskSets.size === 5)
    assert(taskSets(4).stageId === 1 && taskSets(4).stageAttemptId === 2
      && taskSets(4).tasks.size === 1)

    // Complete task(stageId=1, stageAttempt=2, partitionId=1) successfully.
    runEvent(makeCompletionEvent(
      taskSets(4).tasks(0), Success, makeMapStatus("hostB", 2)))

    // Now the ResultStage should be submitted, because all of the tasks of rddB have
    // completed successfully on alive executors.
    assert(taskSets.size === 6 && taskSets(5).tasks(0).isInstanceOf[ResultTask[_, _]])
    complete(taskSets(5), Seq(
      (Success, 1),
      (Success, 1)))
  }

  test("task end event should have updated accumulators (SPARK-20342)") {
    val tasks = 10

    val accumId = new AtomicLong()
    val foundCount = new AtomicLong()
    val listener = new SparkListener() {
      override def onTaskEnd(event: SparkListenerTaskEnd): Unit = {
        event.taskInfo.accumulables.find(_.id == accumId.get).foreach { _ =>
          foundCount.incrementAndGet()
        }
      }
    }
    sc.addSparkListener(listener)

    // Try a few times in a loop to make sure. This is not guaranteed to fail when the bug exists,
    // but it should at least make the test flaky. If the bug is fixed, this should always pass.
    (1 to 10).foreach { i =>
      foundCount.set(0L)

      val accum = sc.longAccumulator(s"accum$i")
      accumId.set(accum.id)

      sc.parallelize(1 to tasks, tasks).foreach { _ =>
        accum.add(1L)
      }
      sc.listenerBus.waitUntilEmpty()
      assert(foundCount.get() === tasks)
    }
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

  private def constructIndeterminateStageFetchFailed(): (Int, Int) = {
    val shuffleMapRdd1 = new MyRDD(sc, 2, Nil, indeterminate = true)

    val shuffleDep1 = new ShuffleDependency(shuffleMapRdd1, new HashPartitioner(2))
    val shuffleId1 = shuffleDep1.shuffleId
    val shuffleMapRdd2 = new MyRDD(sc, 2, List(shuffleDep1), tracker = mapOutputTracker)

    val shuffleDep2 = new ShuffleDependency(shuffleMapRdd2, new HashPartitioner(2))
    val shuffleId2 = shuffleDep2.shuffleId
    val finalRdd = new MyRDD(sc, 2, List(shuffleDep2), tracker = mapOutputTracker)

    submit(finalRdd, Array(0, 1))

    // Finish the first shuffle map stage.
    completeShuffleMapStageSuccessfully(0, 0, 2)
    assert(mapOutputTracker.findMissingPartitions(shuffleId1) === Some(Seq.empty))

    // Finish the second shuffle map stage.
    completeShuffleMapStageSuccessfully(1, 0, 2, Seq("hostC", "hostD"))
    assert(mapOutputTracker.findMissingPartitions(shuffleId2) === Some(Seq.empty))

    // The first task of the final stage failed with fetch failure
    runEvent(makeCompletionEvent(
      taskSets(2).tasks(0),
      FetchFailed(makeBlockManagerId("hostC"), shuffleId2, 0L, 0, 0, "ignored"),
      null))
    (shuffleId1, shuffleId2)
  }

  test("SPARK-25341: abort stage while using old fetch protocol") {
    conf.set(config.SHUFFLE_USE_OLD_FETCH_PROTOCOL.key, "true")
    // Construct the scenario of indeterminate stage fetch failed.
    constructIndeterminateStageFetchFailed()
    // The job should fail because Spark can't rollback the shuffle map stage while
    // using old protocol.
    assert(failure != null && failure.getMessage.contains(
      "Spark can only do this while using the new shuffle block fetching protocol"))
  }

  test("SPARK-25341: retry all the succeeding stages when the map stage is indeterminate") {
    val (shuffleId1, shuffleId2) = constructIndeterminateStageFetchFailed()

    // Check status for all failedStages
    val failedStages = scheduler.failedStages.toSeq
    assert(failedStages.map(_.id) == Seq(1, 2))
    // Shuffle blocks of "hostC" is lost, so first task of the `shuffleMapRdd2` needs to retry.
    assert(failedStages.collect {
      case stage: ShuffleMapStage if stage.shuffleDep.shuffleId == shuffleId2 => stage
    }.head.findMissingPartitions() == Seq(0))
    // The result stage is still waiting for its 2 tasks to complete
    assert(failedStages.collect {
      case stage: ResultStage => stage
    }.head.findMissingPartitions() == Seq(0, 1))

    scheduler.resubmitFailedStages()

    // The first task of the `shuffleMapRdd2` failed with fetch failure
    runEvent(makeCompletionEvent(
      taskSets(3).tasks(0),
      FetchFailed(makeBlockManagerId("hostA"), shuffleId1, 0L, 0, 0, "ignored"),
      null))

    val newFailedStages = scheduler.failedStages.toSeq
    assert(newFailedStages.map(_.id) == Seq(0, 1))

    scheduler.resubmitFailedStages()

    // First shuffle map stage resubmitted and reran all tasks.
    assert(taskSets(4).stageId == 0)
    assert(taskSets(4).stageAttemptId == 1)
    assert(taskSets(4).tasks.length == 2)

    // Finish all stage.
    completeShuffleMapStageSuccessfully(0, 1, 2)
    assert(mapOutputTracker.findMissingPartitions(shuffleId1) === Some(Seq.empty))

    completeShuffleMapStageSuccessfully(1, 2, 2, Seq("hostC", "hostD"))
    assert(mapOutputTracker.findMissingPartitions(shuffleId2) === Some(Seq.empty))

    complete(taskSets(6), Seq((Success, 11), (Success, 12)))

    // Job successful ended.
    assert(results === Map(0 -> 11, 1 -> 12))
    results.clear()
    assertDataStructuresEmpty()
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

    // Finish the first shuffle map stages, with shuffle data on hostA and hostB.
    completeShuffleMapStageSuccessfully(0, 0, 2)
    assert(mapOutputTracker.findMissingPartitions(shuffleId1) === Some(Seq.empty))

    // Finish the second shuffle map stages, with shuffle data on hostB and hostD.
    completeShuffleMapStageSuccessfully(1, 0, 2, Seq("hostB", "hostD"))
    assert(mapOutputTracker.findMissingPartitions(shuffleId2) === Some(Seq.empty))

    // Executor lost on hostB, both of stage 0 and 1 should be rerun - as part of re-computation
    // of stage 2, as we have output on hostB for both stage 0 and stage 1 (see
    // completeShuffleMapStageSuccessfully).
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

  test("SPARK-29042: Sampled RDD with unordered input should be indeterminate") {
    val shuffleMapRdd1 = new MyRDD(sc, 2, Nil, indeterminate = false)

    val shuffleDep1 = new ShuffleDependency(shuffleMapRdd1, new HashPartitioner(2))
    val shuffleMapRdd2 = new MyRDD(sc, 2, List(shuffleDep1), tracker = mapOutputTracker)

    assert(shuffleMapRdd2.outputDeterministicLevel == DeterministicLevel.UNORDERED)

    val sampledRdd = shuffleMapRdd2.sample(true, 0.3, 1000L)
    assert(sampledRdd.outputDeterministicLevel == DeterministicLevel.INDETERMINATE)
  }

  private def assertResultStageFailToRollback(mapRdd: MyRDD): Unit = {
    val shuffleDep = new ShuffleDependency(mapRdd, new HashPartitioner(2))
    val shuffleId = shuffleDep.shuffleId
    val finalRdd = new MyRDD(sc, 2, List(shuffleDep), tracker = mapOutputTracker)

    submit(finalRdd, Array(0, 1))

    completeShuffleMapStageSuccessfully(taskSets.length - 1, 0, numShufflePartitions = 2)
    assert(mapOutputTracker.findMissingPartitions(shuffleId) === Some(Seq.empty))

    // Finish the first task of the result stage
    runEvent(makeCompletionEvent(
      taskSets.last.tasks(0), Success, 42,
      Seq.empty, Array.empty, createFakeTaskInfoWithId(0)))

    // Fail the second task with FetchFailed.
    runEvent(makeCompletionEvent(
      taskSets.last.tasks(1),
      FetchFailed(makeBlockManagerId("hostA"), shuffleId, 0L, 0, 0, "ignored"),
      null))

    // The job should fail because Spark can't rollback the result stage.
    assert(failure != null && failure.getMessage.contains("Spark cannot rollback"))
  }

  test("SPARK-23207: cannot rollback a result stage") {
    val shuffleMapRdd = new MyRDD(sc, 2, Nil, indeterminate = true)
    assertResultStageFailToRollback(shuffleMapRdd)
  }

  test("SPARK-23207: local checkpoint fail to rollback (checkpointed before)") {
    val shuffleMapRdd = new MyCheckpointRDD(sc, 2, Nil, indeterminate = true)
    shuffleMapRdd.localCheckpoint()
    shuffleMapRdd.doCheckpoint()
    assertResultStageFailToRollback(shuffleMapRdd)
  }

  test("SPARK-23207: local checkpoint fail to rollback (checkpointing now)") {
    val shuffleMapRdd = new MyCheckpointRDD(sc, 2, Nil, indeterminate = true)
    shuffleMapRdd.localCheckpoint()
    assertResultStageFailToRollback(shuffleMapRdd)
  }

  private def assertResultStageNotRolledBack(mapRdd: MyRDD): Unit = {
    val shuffleDep = new ShuffleDependency(mapRdd, new HashPartitioner(2))
    val shuffleId = shuffleDep.shuffleId
    val finalRdd = new MyRDD(sc, 2, List(shuffleDep), tracker = mapOutputTracker)

    submit(finalRdd, Array(0, 1))

    completeShuffleMapStageSuccessfully(taskSets.length - 1, 0, numShufflePartitions = 2)
    assert(mapOutputTracker.findMissingPartitions(shuffleId) === Some(Seq.empty))

    // Finish the first task of the result stage
    runEvent(makeCompletionEvent(
      taskSets.last.tasks(0), Success, 42,
      Seq.empty, Array.empty, createFakeTaskInfoWithId(0)))

    // Fail the second task with FetchFailed.
    runEvent(makeCompletionEvent(
      taskSets.last.tasks(1),
      FetchFailed(makeBlockManagerId("hostA"), shuffleId, 0L, 0, 0, "ignored"),
      null))

    assert(failure == null, "job should not fail")
    val failedStages = scheduler.failedStages.toSeq
    assert(failedStages.length == 2)
    // Shuffle blocks of "hostA" is lost, so first task of the `mapRdd` needs to retry.
    assert(failedStages.collect {
      case stage: ShuffleMapStage if stage.shuffleDep.shuffleId == shuffleId => stage
    }.head.findMissingPartitions() == Seq(0))
    // The first task of result stage remains completed.
    assert(failedStages.collect {
      case stage: ResultStage => stage
    }.head.findMissingPartitions() == Seq(1))
  }

  test("SPARK-23207: reliable checkpoint can avoid rollback (checkpointed before)") {
    withTempDir { dir =>
      sc.setCheckpointDir(dir.getCanonicalPath)
      val shuffleMapRdd = new MyCheckpointRDD(sc, 2, Nil, indeterminate = true)
      shuffleMapRdd.checkpoint()
      shuffleMapRdd.doCheckpoint()
      assertResultStageNotRolledBack(shuffleMapRdd)
    }
  }

  test("SPARK-23207: reliable checkpoint fail to rollback (checkpointing now)") {
    withTempDir { dir =>
      sc.setCheckpointDir(dir.getCanonicalPath)
      val shuffleMapRdd = new MyCheckpointRDD(sc, 2, Nil, indeterminate = true)
      shuffleMapRdd.checkpoint()
      assertResultStageFailToRollback(shuffleMapRdd)
    }
  }

  test("SPARK-27164: RDD.countApprox on empty RDDs schedules jobs which never complete") {
    val latch = new CountDownLatch(1)
    val jobListener = new SparkListener {
      override def onJobEnd(jobEnd: SparkListenerJobEnd): Unit = {
        latch.countDown()
      }
    }
    sc.addSparkListener(jobListener)
    sc.emptyRDD[Int].countApprox(10000).getFinalValue()
    assert(latch.await(10, TimeUnit.SECONDS))
  }

  test("Completions in zombie tasksets update status of non-zombie taskset") {
    val parts = 4
    val shuffleMapRdd = new MyRDD(sc, parts, Nil)
    val shuffleDep = new ShuffleDependency(shuffleMapRdd, new HashPartitioner(parts))
    val reduceRdd = new MyRDD(sc, parts, List(shuffleDep), tracker = mapOutputTracker)
    submit(reduceRdd, (0 until parts).toArray)
    assert(taskSets.length == 1)

    // Finish the first task of the shuffle map stage.
    runEvent(makeCompletionEvent(
      taskSets(0).tasks(0), Success, makeMapStatus("hostA", 4),
      Seq.empty, Array.empty, createFakeTaskInfoWithId(0)))

    // The second task of the shuffle map stage failed with FetchFailed.
    runEvent(makeCompletionEvent(
      taskSets(0).tasks(1),
      FetchFailed(makeBlockManagerId("hostB"), shuffleDep.shuffleId, 0L, 0, 0, "ignored"),
      null))

    scheduler.resubmitFailedStages()
    assert(taskSets.length == 2)
    // The first partition has completed already, so the new attempt only need to run 3 tasks.
    assert(taskSets(1).tasks.length == 3)

    // Finish the first task of the second attempt of the shuffle map stage.
    runEvent(makeCompletionEvent(
      taskSets(1).tasks(0), Success, makeMapStatus("hostA", 4),
      Seq.empty, Array.empty, createFakeTaskInfoWithId(0)))

    // Finish the third task of the first attempt of the shuffle map stage.
    runEvent(makeCompletionEvent(
      taskSets(0).tasks(2), Success, makeMapStatus("hostA", 4),
      Seq.empty, Array.empty, createFakeTaskInfoWithId(0)))
    assert(tasksMarkedAsCompleted.length == 1)
    assert(tasksMarkedAsCompleted.head.partitionId == 2)

    // Finish the forth task of the first attempt of the shuffle map stage.
    runEvent(makeCompletionEvent(
      taskSets(0).tasks(3), Success, makeMapStatus("hostA", 4),
      Seq.empty, Array.empty, createFakeTaskInfoWithId(0)))
    assert(tasksMarkedAsCompleted.length == 2)
    assert(tasksMarkedAsCompleted.last.partitionId == 3)

    // Now the shuffle map stage is completed, and the next stage is submitted.
    assert(taskSets.length == 3)

    // Finish
    complete(taskSets(2), Seq((Success, 42), (Success, 42), (Success, 42), (Success, 42)))
    assertDataStructuresEmpty()
  }

  test("test default resource profile") {
    val rdd = sc.parallelize(1 to 10).map(x => (x, x))
    val (shuffledeps, resourceprofiles) = scheduler.getShuffleDependenciesAndResourceProfiles(rdd)
    val rp = scheduler.mergeResourceProfilesForStage(resourceprofiles)
    assert(rp.id == scheduler.sc.resourceProfileManager.defaultResourceProfile.id)
  }

  test("test 1 resource profile") {
    val ereqs = new ExecutorResourceRequests().cores(4)
    val treqs = new TaskResourceRequests().cpus(1)
    val rp1 = new ResourceProfileBuilder().require(ereqs).require(treqs).build

    val rdd = sc.parallelize(1 to 10).map(x => (x, x)).withResources(rp1)
    val (shuffledeps, resourceprofiles) = scheduler.getShuffleDependenciesAndResourceProfiles(rdd)
    val rpMerged = scheduler.mergeResourceProfilesForStage(resourceprofiles)
    val expectedid = Option(rdd.getResourceProfile).map(_.id)
    assert(expectedid.isDefined)
    assert(expectedid.get != ResourceProfile.DEFAULT_RESOURCE_PROFILE_ID)
    assert(rpMerged.id == expectedid.get)
  }

  test("test 2 resource profiles errors by default") {
    import org.apache.spark.resource._
    val ereqs = new ExecutorResourceRequests().cores(4)
    val treqs = new TaskResourceRequests().cpus(1)
    val rp1 = new ResourceProfileBuilder().require(ereqs).require(treqs).build

    val ereqs2 = new ExecutorResourceRequests().cores(2)
    val treqs2 = new TaskResourceRequests().cpus(2)
    val rp2 = new ResourceProfileBuilder().require(ereqs2).require(treqs2).build

    val rdd = sc.parallelize(1 to 10).withResources(rp1).map(x => (x, x)).withResources(rp2)
    val error = intercept[IllegalArgumentException] {
      val (shuffledeps, resourceprofiles) = scheduler.getShuffleDependenciesAndResourceProfiles(rdd)
      scheduler.mergeResourceProfilesForStage(resourceprofiles)
    }.getMessage()

    assert(error.contains("Multiple ResourceProfiles specified in the RDDs"))
  }

  test("test 2 resource profile with merge conflict config true") {
    conf.set(config.RESOURCE_PROFILE_MERGE_CONFLICTS.key, "true")

    val ereqs = new ExecutorResourceRequests().cores(4)
    val treqs = new TaskResourceRequests().cpus(1)
    val rp1 = new ResourceProfileBuilder().require(ereqs).require(treqs).build

    val ereqs2 = new ExecutorResourceRequests().cores(2)
    val treqs2 = new TaskResourceRequests().cpus(2)
    val rp2 = new ResourceProfileBuilder().require(ereqs2).require(treqs2).build

    val rdd = sc.parallelize(1 to 10).withResources(rp1).map(x => (x, x)).withResources(rp2)
    val (shuffledeps, resourceprofiles) = scheduler.getShuffleDependenciesAndResourceProfiles(rdd)
    val mergedRp = scheduler.mergeResourceProfilesForStage(resourceprofiles)
    assert(mergedRp.getTaskCpus.get == 2)
    assert(mergedRp.getExecutorCores.get == 4)
  }

  test("test multiple resource profiles created from merging use same rp") {
    conf.set(config.RESOURCE_PROFILE_MERGE_CONFLICTS.key, "true")

    val ereqs = new ExecutorResourceRequests().cores(4)
    val treqs = new TaskResourceRequests().cpus(1)
    val rp1 = new ResourceProfileBuilder().require(ereqs).require(treqs).build

    val ereqs2 = new ExecutorResourceRequests().cores(2)
    val treqs2 = new TaskResourceRequests().cpus(2)
    val rp2 = new ResourceProfileBuilder().require(ereqs2).require(treqs2).build

    val rdd = sc.parallelize(1 to 10).withResources(rp1).map(x => (x, x)).withResources(rp2)
    val (_, resourceprofiles) = scheduler.getShuffleDependenciesAndResourceProfiles(rdd)
    val mergedRp = scheduler.mergeResourceProfilesForStage(resourceprofiles)
    assert(mergedRp.getTaskCpus.get == 2)
    assert(mergedRp.getExecutorCores.get == 4)

    // test that instead of creating a new merged profile, we use the already created one
    val rdd2 = sc.parallelize(1 to 10).withResources(rp1).map(x => (x, x)).withResources(rp2)
    val (_, resourceprofiles2) = scheduler.getShuffleDependenciesAndResourceProfiles(rdd2)
    val mergedRp2 = scheduler.mergeResourceProfilesForStage(resourceprofiles2)
    assert(mergedRp2.id === mergedRp.id)
    assert(mergedRp2.getTaskCpus.get == 2)
    assert(mergedRp2.getExecutorCores.get == 4)
  }

  test("test merge 2 resource profiles multiple configs") {
    val ereqs = new ExecutorResourceRequests().cores(4)
    val treqs = new TaskResourceRequests().cpus(2)
    val rp1 = new ResourceProfile(ereqs.requests, treqs.requests)
    val ereqs2 = new ExecutorResourceRequests().cores(2)
    val treqs2 = new TaskResourceRequests().cpus(1)
    val rp2 = new ResourceProfile(ereqs2.requests, treqs2.requests)
    var mergedRp = scheduler.mergeResourceProfiles(rp1, rp2)

    assert(mergedRp.getTaskCpus.get == 2)
    assert(mergedRp.getExecutorCores.get == 4)

    val ereqs3 = new ExecutorResourceRequests().cores(1).resource(GPU, 1, "disc")
    val treqs3 = new TaskResourceRequests().cpus(1).resource(GPU, 1)
    val rp3 = new ResourceProfile(ereqs3.requests, treqs3.requests)
    val ereqs4 = new ExecutorResourceRequests().cores(2)
    val treqs4 = new TaskResourceRequests().cpus(2)
    val rp4 = new ResourceProfile(ereqs4.requests, treqs4.requests)
    mergedRp = scheduler.mergeResourceProfiles(rp3, rp4)

    assert(mergedRp.getTaskCpus.get == 2)
    assert(mergedRp.getExecutorCores.get == 2)
    assert(mergedRp.executorResources.size == 2)
    assert(mergedRp.taskResources.size == 2)
    assert(mergedRp.executorResources.get(GPU).get.amount == 1)
    assert(mergedRp.executorResources.get(GPU).get.discoveryScript == "disc")
    assert(mergedRp.taskResources.get(GPU).get.amount == 1)

    val ereqs5 = new ExecutorResourceRequests().cores(1).memory("3g")
      .memoryOverhead("1g").pysparkMemory("2g").offHeapMemory("4g").resource(GPU, 1, "disc")
    val treqs5 = new TaskResourceRequests().cpus(1).resource(GPU, 1)
    val rp5 = new ResourceProfile(ereqs5.requests, treqs5.requests)
    val ereqs6 = new ExecutorResourceRequests().cores(8).resource(FPGA, 2, "fdisc")
    val treqs6 = new TaskResourceRequests().cpus(2).resource(FPGA, 1)
    val rp6 = new ResourceProfile(ereqs6.requests, treqs6.requests)
    mergedRp = scheduler.mergeResourceProfiles(rp5, rp6)

    assert(mergedRp.getTaskCpus.get == 2)
    assert(mergedRp.getExecutorCores.get == 8)
    assert(mergedRp.executorResources.size == 7)
    assert(mergedRp.taskResources.size == 3)
    assert(mergedRp.executorResources.get(GPU).get.amount == 1)
    assert(mergedRp.executorResources.get(GPU).get.discoveryScript == "disc")
    assert(mergedRp.taskResources.get(GPU).get.amount == 1)
    assert(mergedRp.executorResources.get(FPGA).get.amount == 2)
    assert(mergedRp.executorResources.get(FPGA).get.discoveryScript == "fdisc")
    assert(mergedRp.taskResources.get(FPGA).get.amount == 1)
    assert(mergedRp.executorResources.get(ResourceProfile.MEMORY).get.amount == 3072)
    assert(mergedRp.executorResources.get(ResourceProfile.PYSPARK_MEM).get.amount == 2048)
    assert(mergedRp.executorResources.get(ResourceProfile.OVERHEAD_MEM).get.amount == 1024)
    assert(mergedRp.executorResources.get(ResourceProfile.OFFHEAP_MEM).get.amount == 4096)

    val ereqs7 = new ExecutorResourceRequests().cores(1).memory("3g")
      .resource(GPU, 4, "disc")
    val treqs7 = new TaskResourceRequests().cpus(1).resource(GPU, 1)
    val rp7 = new ResourceProfile(ereqs7.requests, treqs7.requests)
    val ereqs8 = new ExecutorResourceRequests().cores(1).resource(GPU, 2, "fdisc")
    val treqs8 = new TaskResourceRequests().cpus(1).resource(GPU, 2)
    val rp8 = new ResourceProfile(ereqs8.requests, treqs8.requests)
    mergedRp = scheduler.mergeResourceProfiles(rp7, rp8)

    assert(mergedRp.getTaskCpus.get == 1)
    assert(mergedRp.getExecutorCores.get == 1)
    assert(mergedRp.executorResources.get(GPU).get.amount == 4)
    assert(mergedRp.executorResources.get(GPU).get.discoveryScript == "disc")
    assert(mergedRp.taskResources.get(GPU).get.amount == 2)
  }

  test("test merge 3 resource profiles") {
    conf.set(config.RESOURCE_PROFILE_MERGE_CONFLICTS.key, "true")
    val ereqs = new ExecutorResourceRequests().cores(4)
    val treqs = new TaskResourceRequests().cpus(1)
    val rp1 = new ResourceProfile(ereqs.requests, treqs.requests)
    val ereqs2 = new ExecutorResourceRequests().cores(2)
    val treqs2 = new TaskResourceRequests().cpus(1)
    val rp2 = new ResourceProfile(ereqs2.requests, treqs2.requests)
    val ereqs3 = new ExecutorResourceRequests().cores(3)
    val treqs3 = new TaskResourceRequests().cpus(2)
    val rp3 = new ResourceProfile(ereqs3.requests, treqs3.requests)
    val mergedRp = scheduler.mergeResourceProfilesForStage(HashSet(rp1, rp2, rp3))

    assert(mergedRp.getTaskCpus.get == 2)
    assert(mergedRp.getExecutorCores.get == 4)
  }

  test("test merge task resource profiles") {
    conf.set(config.RESOURCE_PROFILE_MERGE_CONFLICTS.key, "true")
    // Ensure the initialization of SparkEnv
    sc

    val treqs1 = new TaskResourceRequests().cpus(1)
    val rp1 = new TaskResourceProfile(treqs1.requests)
    val treqs2 = new TaskResourceRequests().cpus(1)
    val rp2 = new TaskResourceProfile(treqs2.requests)
    val treqs3 = new TaskResourceRequests().cpus(2)
    val rp3 = new TaskResourceProfile(treqs3.requests)
    val mergedRp = scheduler.mergeResourceProfilesForStage(HashSet(rp1, rp2, rp3))

    assert(mergedRp.isInstanceOf[TaskResourceProfile])
    assert(mergedRp.getTaskCpus.get == 2)
  }

  /**
   * Checks the DAGScheduler's internal logic for traversing an RDD DAG by making sure that
   * getShuffleDependenciesAndResourceProfiles correctly returns the direct shuffle dependencies
   * of a particular RDD. The test creates the following RDD graph (where n denotes a narrow
   * dependency and s denotes a shuffle dependency):
   *
   * A <------------s---------,
   *                           \
   * B <--s-- C <--s-- D <--n------ E
   *
   * Here, the direct shuffle dependency of C is just the shuffle dependency on B. The direct
   * shuffle dependencies of E are the shuffle dependency on A and the shuffle dependency on C.
   */
  test("getShuffleDependenciesAndResourceProfiles returns deps and profiles correctly") {
    import org.apache.spark.resource._
    val ereqs = new ExecutorResourceRequests().cores(4)
    val treqs = new TaskResourceRequests().cpus(1)
    val rp1 = new ResourceProfileBuilder().require(ereqs).require(treqs).build
    val ereqs2 = new ExecutorResourceRequests().cores(6)
    val treqs2 = new TaskResourceRequests().cpus(2)
    val rp2 = new ResourceProfileBuilder().require(ereqs2).require(treqs2).build

    val rddWithRp = new MyRDD(sc, 2, Nil).withResources(rp1)
    val rddA = new MyRDD(sc, 2, Nil).withResources(rp1)
    val shuffleDepA = new ShuffleDependency(rddA, new HashPartitioner(1))
    val rddB = new MyRDD(sc, 2, Nil)
    val shuffleDepB = new ShuffleDependency(rddB, new HashPartitioner(1))
    val rddWithRpDep = new OneToOneDependency(rddWithRp)
    val rddC = new MyRDD(sc, 1, List(rddWithRpDep, shuffleDepB)).withResources(rp2)
    val shuffleDepC = new ShuffleDependency(rddC, new HashPartitioner(1))
    val rddD = new MyRDD(sc, 1, List(shuffleDepC))
    val narrowDepD = new OneToOneDependency(rddD)
    val rddE = new MyRDD(sc, 1, List(shuffleDepA, narrowDepD), tracker = mapOutputTracker)

    val (shuffleDepsA, rprofsA) = scheduler.getShuffleDependenciesAndResourceProfiles(rddA)
    assert(shuffleDepsA === Set())
    assert(rprofsA === Set(rp1))
    val (shuffleDepsB, rprofsB) = scheduler.getShuffleDependenciesAndResourceProfiles(rddB)
    assert(shuffleDepsB === Set())
    assert(rprofsB === Set())
    val (shuffleDepsC, rprofsC) = scheduler.getShuffleDependenciesAndResourceProfiles(rddC)
    assert(shuffleDepsC === Set(shuffleDepB))
    assert(rprofsC === Set(rp1, rp2))
    val (shuffleDepsD, rprofsD) = scheduler.getShuffleDependenciesAndResourceProfiles(rddD)
    assert(shuffleDepsD === Set(shuffleDepC))
    assert(rprofsD === Set())
    val (shuffleDepsE, rprofsE) = scheduler.getShuffleDependenciesAndResourceProfiles(rddE)
    assert(shuffleDepsE === Set(shuffleDepA, shuffleDepC))
    assert(rprofsE === Set())
  }

  private def initPushBasedShuffleConfs(conf: SparkConf) = {
    conf.set(config.SHUFFLE_SERVICE_ENABLED, true)
    conf.set(config.PUSH_BASED_SHUFFLE_ENABLED, true)
    conf.set(config.PUSH_BASED_SHUFFLE_SIZE_MIN_SHUFFLE_SIZE_TO_WAIT, 1L)
    conf.set("spark.master", "pushbasedshuffleclustermanager")
    // Needed to run push-based shuffle tests in ad-hoc manner through IDE
    conf.set(Tests.IS_TESTING, true)
    // [SPARK-36705] Push-based shuffle does not work with Spark's default
    // JavaSerializer and will be disabled with it, as it does not support
    // object relocation
    conf.set(config.SERIALIZER, "org.apache.spark.serializer.KryoSerializer")
  }

  test("SPARK-32920: shuffle merge finalization") {
    initPushBasedShuffleConfs(conf)
    DAGSchedulerSuite.clearMergerLocs()
    DAGSchedulerSuite.addMergerLocs(Seq("host1", "host2", "host3", "host4", "host5"))
    val parts = 2
    val shuffleMapRdd = new MyRDD(sc, parts, Nil)
    val shuffleDep = new ShuffleDependency(shuffleMapRdd, new HashPartitioner(parts))
    val reduceRdd = new MyRDD(sc, parts, List(shuffleDep), tracker = mapOutputTracker)

    // Submit a reduce job that depends which will create a map stage
    submit(reduceRdd, (0 until parts).toArray)
    completeShuffleMapStageSuccessfully(0, 0, parts)
    assert(mapOutputTracker.getNumAvailableMergeResults(shuffleDep.shuffleId) == parts)
    completeNextResultStageWithSuccess(1, 0)
    assert(results === Map(0 -> 42, 1 -> 42))
    results.clear()
    assertDataStructuresEmpty()
  }

  test("SPARK-32920: merger locations not empty") {
    initPushBasedShuffleConfs(conf)
    conf.set(config.SHUFFLE_MERGER_LOCATIONS_MIN_STATIC_THRESHOLD, 3)
    DAGSchedulerSuite.clearMergerLocs()
    DAGSchedulerSuite.addMergerLocs(Seq("host1", "host2", "host3", "host4", "host5"))
    val parts = 2

    val shuffleMapRdd = new MyRDD(sc, parts, Nil)
    val shuffleDep = new ShuffleDependency(shuffleMapRdd, new HashPartitioner(parts))
    val reduceRdd = new MyRDD(sc, parts, List(shuffleDep), tracker = mapOutputTracker)

    // Submit a reduce job that depends which will create a map stage
    submit(reduceRdd, (0 until parts).toArray)
    completeShuffleMapStageSuccessfully(0, 0, parts)
    val shuffleStage = scheduler.stageIdToStage(0).asInstanceOf[ShuffleMapStage]
    assert(shuffleStage.shuffleDep.getMergerLocs.nonEmpty)

    assert(mapOutputTracker.getNumAvailableMergeResults(shuffleDep.shuffleId) == parts)
    completeNextResultStageWithSuccess(1, 0)
    assert(results === Map(0 -> 42, 1 -> 42))

    results.clear()
    assertDataStructuresEmpty()
  }

  test("SPARK-32920: merger locations reuse from shuffle dependency") {
    initPushBasedShuffleConfs(conf)
    conf.set(config.SHUFFLE_MERGER_MAX_RETAINED_LOCATIONS, 3)
    DAGSchedulerSuite.clearMergerLocs()
    DAGSchedulerSuite.addMergerLocs(Seq("host1", "host2", "host3", "host4", "host5"))
    val parts = 2

    val shuffleMapRdd = new MyRDD(sc, parts, Nil)
    val shuffleDep = new ShuffleDependency(shuffleMapRdd, new HashPartitioner(parts))
    val reduceRdd = new MyRDD(sc, parts, List(shuffleDep), tracker = mapOutputTracker)
    submit(reduceRdd, Array(0, 1))

    completeShuffleMapStageSuccessfully(0, 0, parts)
    assert(shuffleDep.getMergerLocs.nonEmpty)
    val mergerLocs = shuffleDep.getMergerLocs
    completeNextResultStageWithSuccess(1, 0 )

    // submit another job w/ the shared dependency, and have a fetch failure
    val reduce2 = new MyRDD(sc, 2, List(shuffleDep))
    submit(reduce2, Array(0, 1))
    // Note that the stage numbering here is only b/c the shared dependency produces a new, skipped
    // stage.  If instead it reused the existing stage, then this would be stage 2
    completeNextStageWithFetchFailure(3, 0, shuffleDep)
    scheduler.resubmitFailedStages()

    assert(scheduler.runningStages.nonEmpty)
    assert(scheduler.stageIdToStage(2)
      .asInstanceOf[ShuffleMapStage].shuffleDep.getMergerLocs.nonEmpty)
    val newMergerLocs = scheduler.stageIdToStage(2)
      .asInstanceOf[ShuffleMapStage].shuffleDep.getMergerLocs

    // Check if same merger locs is reused for the new stage with shared shuffle dependency
    assert(mergerLocs.zip(newMergerLocs).forall(x => x._1.host == x._2.host))
    completeShuffleMapStageSuccessfully(2, 1, 2)
    completeNextResultStageWithSuccess(3, 1, idx => idx + 1234)
    assert(results === Map(0 -> 1234, 1 -> 1235))

    assertDataStructuresEmpty()
  }

  test("SPARK-32920: Disable shuffle merge due to not enough mergers available") {
    initPushBasedShuffleConfs(conf)
    conf.set(config.SHUFFLE_MERGER_LOCATIONS_MIN_STATIC_THRESHOLD, 6)
    DAGSchedulerSuite.clearMergerLocs()
    DAGSchedulerSuite.addMergerLocs(Seq("host1", "host2", "host3", "host4", "host5"))
    val parts = 7

    val shuffleMapRdd = new MyRDD(sc, parts, Nil)
    val shuffleDep = new ShuffleDependency(shuffleMapRdd, new HashPartitioner(parts))
    val reduceRdd = new MyRDD(sc, parts, List(shuffleDep), tracker = mapOutputTracker)

    // Submit a reduce job that depends which will create a map stage
    submit(reduceRdd, (0 until parts).toArray)
    completeShuffleMapStageSuccessfully(0, 0, parts)
    val shuffleStage = scheduler.stageIdToStage(0).asInstanceOf[ShuffleMapStage]
    assert(!shuffleStage.shuffleDep.shuffleMergeEnabled)

    completeNextResultStageWithSuccess(1, 0)
    assert(results === Map(2 -> 42, 5 -> 42, 4 -> 42, 1 -> 42, 3 -> 42, 6 -> 42, 0 -> 42))

    results.clear()
    assertDataStructuresEmpty()
  }

  test("SPARK-32920: Ensure child stage should not start before all the" +
      " parent stages are completed with shuffle merge finalized for all the parent stages") {
    initPushBasedShuffleConfs(conf)
    DAGSchedulerSuite.clearMergerLocs()
    DAGSchedulerSuite.addMergerLocs(Seq("host1", "host2", "host3", "host4", "host5"))
    val parts = 1
    val shuffleMapRdd1 = new MyRDD(sc, parts, Nil)
    val shuffleDep1 = new ShuffleDependency(shuffleMapRdd1, new HashPartitioner(parts))

    val shuffleMapRdd2 = new MyRDD(sc, parts, Nil)
    val shuffleDep2 = new ShuffleDependency(shuffleMapRdd2, new HashPartitioner(parts))

    val reduceRdd = new MyRDD(sc, parts, List(shuffleDep1, shuffleDep2), tracker = mapOutputTracker)

    // Submit a reduce job
    submit(reduceRdd, (0 until parts).toArray)

    complete(taskSets(0), Seq((Success, makeMapStatus("hostA", 1))))
    val shuffleStage1 = scheduler.stageIdToStage(0).asInstanceOf[ShuffleMapStage]
    assert(shuffleStage1.shuffleDep.getMergerLocs.nonEmpty)

    complete(taskSets(1), Seq((Success, makeMapStatus("hostA", 1))))
    val shuffleStage2 = scheduler.stageIdToStage(1).asInstanceOf[ShuffleMapStage]
    assert(shuffleStage2.shuffleDep.getMergerLocs.nonEmpty)

    assert(shuffleStage2.shuffleDep.isShuffleMergeFinalizedMarked)
    assert(shuffleStage1.shuffleDep.isShuffleMergeFinalizedMarked)
    assert(mapOutputTracker.getNumAvailableMergeResults(shuffleDep1.shuffleId) == parts)
    assert(mapOutputTracker.getNumAvailableMergeResults(shuffleDep2.shuffleId) == parts)

    completeNextResultStageWithSuccess(2, 0)
    assert(results === Map(0 -> 42))
    results.clear()
    assertDataStructuresEmpty()
  }

  test("SPARK-32920: Reused ShuffleDependency with Shuffle Merge disabled for the corresponding" +
      " ShuffleDependency should not cause DAGScheduler to hang") {
    initPushBasedShuffleConfs(conf)
    conf.set(config.SHUFFLE_MERGER_LOCATIONS_MIN_STATIC_THRESHOLD, 10)
    DAGSchedulerSuite.clearMergerLocs()
    DAGSchedulerSuite.addMergerLocs(Seq("host1", "host2", "host3", "host4", "host5"))
    val parts = 20

    val shuffleMapRdd = new MyRDD(sc, parts, Nil)
    val shuffleDep = new ShuffleDependency(shuffleMapRdd, new HashPartitioner(parts))
    val reduceRdd = new MyRDD(sc, parts, List(shuffleDep), tracker = mapOutputTracker)
    val partitions = (0 until parts).toArray
    submit(reduceRdd, partitions)

    completeShuffleMapStageSuccessfully(0, 0, parts)
    val shuffleStage = scheduler.stageIdToStage(0).asInstanceOf[ShuffleMapStage]
    assert(!shuffleStage.shuffleDep.shuffleMergeEnabled)

    completeNextResultStageWithSuccess(1, 0)
    val reduce2 = new MyRDD(sc, parts, List(shuffleDep))
    submit(reduce2, partitions)
    // Stage 2 should not be executed as it should reuse the already computed shuffle output
    assert(scheduler.stageIdToStage(2).latestInfo.taskMetrics == null)
    completeNextResultStageWithSuccess(3, 0, idx => idx + 1234)

    val expected = (0 until parts).map(idx => (idx, idx + 1234))
    assert(results === expected.toMap)

    assertDataStructuresEmpty()
  }

  test("SPARK-32920: Reused ShuffleDependency with Shuffle Merge disabled for the corresponding" +
      " ShuffleDependency with shuffle data loss should recompute missing partitions") {
    initPushBasedShuffleConfs(conf)
    conf.set(config.SHUFFLE_MERGER_LOCATIONS_MIN_STATIC_THRESHOLD, 10)
    DAGSchedulerSuite.clearMergerLocs()
    DAGSchedulerSuite.addMergerLocs(Seq("host1", "host2", "host3", "host4", "host5"))
    val parts = 20

    val shuffleMapRdd = new MyRDD(sc, parts, Nil)
    val shuffleDep = new ShuffleDependency(shuffleMapRdd, new HashPartitioner(parts))
    val reduceRdd = new MyRDD(sc, parts, List(shuffleDep), tracker = mapOutputTracker)
    val partitions = (0 until parts).toArray
    submit(reduceRdd, partitions)

    completeShuffleMapStageSuccessfully(0, 0, parts)
    val shuffleStage = scheduler.stageIdToStage(0).asInstanceOf[ShuffleMapStage]
    assert(shuffleStage.shuffleDep.mergerLocs.isEmpty)

    completeNextResultStageWithSuccess(1, 0)

    DAGSchedulerSuite.clearMergerLocs()
    val hosts = (6 to parts).map {x => s"Host$x" }
    DAGSchedulerSuite.addMergerLocs(hosts)

    val reduce2 = new MyRDD(sc, parts, List(shuffleDep))
    submit(reduce2, partitions)
    // Note that the stage numbering here is only b/c the shared dependency produces a new, skipped
    // stage. If instead it reused the existing stage, then this would be stage 2
    completeNextStageWithFetchFailure(3, 0, shuffleDep)
    scheduler.resubmitFailedStages()

    val stage2 = scheduler.stageIdToStage(2).asInstanceOf[ShuffleMapStage]
    assert(stage2.shuffleDep.shuffleMergeEnabled)

    // the scheduler now creates a new task set to regenerate the missing map output, but this time
    // using a different stage, the "skipped" one
    assert(scheduler.stageIdToStage(2).latestInfo.taskMetrics != null)
    completeShuffleMapStageSuccessfully(2, 1, parts)
    completeNextResultStageWithSuccess(3, 1, idx => idx + 1234)

    val expected = (0 until parts).map(idx => (idx, idx + 1234))
    assert(results === expected.toMap)
    assertDataStructuresEmpty()
  }

  test("SPARK-32920: Empty RDD should not be computed") {
    initPushBasedShuffleConfs(conf)
    val data = sc.emptyRDD[Int]
    data.sortBy(x => x).collect()
    assert(taskSets.isEmpty)
    assertDataStructuresEmpty()
  }

  test("SPARK-32920: Cancelled stage should be marked finalized after the shuffle merge " +
    "is finalized") {
    initPushBasedShuffleConfs(conf)
    DAGSchedulerSuite.clearMergerLocs()
    DAGSchedulerSuite.addMergerLocs(Seq("host1", "host2", "host3", "host4", "host5"))
    scheduler = new MyDAGScheduler(
      sc,
      taskScheduler,
      sc.listenerBus,
      mapOutputTracker,
      blockManagerMaster,
      sc.env,
      shuffleMergeFinalize = false)
    dagEventProcessLoopTester = new DAGSchedulerEventProcessLoopTester(scheduler)

    val parts = 2
    val shuffleMapRdd = new MyRDD(sc, parts, Nil)
    val shuffleDep = new ShuffleDependency(shuffleMapRdd, new HashPartitioner(parts))
    val reduceRdd = new MyRDD(sc, parts, List(shuffleDep), tracker = mapOutputTracker)

    // Submit a reduce job that depends which will create a map stage
    submit(reduceRdd, (0 until parts).toArray)
    // Complete shuffle map stage successfully on hostA
    complete(taskSets(0), taskSets(0).tasks.zipWithIndex.map {
      case (task, _) =>
        (Success, makeMapStatus("hostA", parts))
    }.toSeq)

    assert(mapOutputTracker.getNumAvailableMergeResults(shuffleDep.shuffleId) == parts)
    val shuffleMapStageToCancel = scheduler.stageIdToStage(0).asInstanceOf[ShuffleMapStage]
    runEvent(StageCancelled(0, Option("Explicit cancel check")))
    scheduler.handleShuffleMergeFinalized(shuffleMapStageToCancel,
      shuffleMapStageToCancel.shuffleDep.shuffleMergeId)
    assert(mapOutputTracker.getNumAvailableMergeResults(shuffleDep.shuffleId) == 2)
    assert(shuffleMapStageToCancel.shuffleDep.isShuffleMergeFinalizedMarked)
  }

  test("SPARK-32920: SPARK-35549: Merge results should not get registered" +
    " after shuffle merge finalization") {
    initPushBasedShuffleConfs(conf)
    DAGSchedulerSuite.clearMergerLocs()
    DAGSchedulerSuite.addMergerLocs(Seq("host1", "host2", "host3", "host4", "host5"))

    scheduler = new MyDAGScheduler(
      sc,
      taskScheduler,
      sc.listenerBus,
      mapOutputTracker,
      blockManagerMaster,
      sc.env,
      shuffleMergeFinalize = false,
      shuffleMergeRegister = false)
    dagEventProcessLoopTester = new DAGSchedulerEventProcessLoopTester(scheduler)

    val parts = 2
    val shuffleMapRdd = new MyRDD(sc, parts, Nil)
    val shuffleDep = new ShuffleDependency(shuffleMapRdd, new HashPartitioner(parts))
    val reduceRdd = new MyRDD(sc, parts, List(shuffleDep), tracker = mapOutputTracker)

    // Submit a reduce job that depends which will create a map stage
    submit(reduceRdd, (0 until parts).toArray)
    // Complete shuffle map stage successfully on hostA
    complete(taskSets(0), taskSets(0).tasks.zipWithIndex.map {
      case (task, _) =>
        (Success, makeMapStatus("hostA", parts))
    }.toSeq)
    val shuffleMapStage = scheduler.stageIdToStage(0).asInstanceOf[ShuffleMapStage]
    scheduler.handleRegisterMergeStatuses(shuffleMapStage, Seq((0, makeMergeStatus("hostA",
      shuffleDep.shuffleMergeId))))
    scheduler.handleShuffleMergeFinalized(shuffleMapStage,
      shuffleMapStage.shuffleDep.shuffleMergeId)
    scheduler.handleRegisterMergeStatuses(shuffleMapStage, Seq((1, makeMergeStatus("hostA",
      shuffleDep.shuffleMergeId))))
    assert(mapOutputTracker.getNumAvailableMergeResults(shuffleDep.shuffleId) == 1)
  }

  test("SPARK-32920: Disable push based shuffle in the case of a barrier stage") {
    initPushBasedShuffleConfs(conf)
    DAGSchedulerSuite.clearMergerLocs()
    DAGSchedulerSuite.addMergerLocs(Seq("host1", "host2", "host3", "host4", "host5"))

    val parts = 2
    val shuffleMapRdd = new MyRDD(sc, parts, Nil).barrier().mapPartitions(iter => iter)
    val shuffleDep = new ShuffleDependency(shuffleMapRdd, new HashPartitioner(parts))
    val reduceRdd = new MyRDD(sc, parts, List(shuffleDep), tracker = mapOutputTracker)

    // Submit a reduce job that depends which will create a map stage
    submit(reduceRdd, (0 until parts).toArray)
    completeShuffleMapStageSuccessfully(0, 0, reduceRdd.partitions.length)
    val shuffleMapStage = scheduler.stageIdToStage(0).asInstanceOf[ShuffleMapStage]
    assert(!shuffleMapStage.shuffleDep.shuffleMergeAllowed)
  }

  test("SPARK-32920: metadata fetch failure should not unregister map status") {
    initPushBasedShuffleConfs(conf)
    val parts = 2
    val shuffleMapRdd = new MyRDD(sc, parts, Nil)
    val shuffleDep = new ShuffleDependency(shuffleMapRdd, new HashPartitioner(parts))

    val reduceRdd = new MyRDD(sc, parts, List(shuffleDep), tracker = mapOutputTracker)
    submit(reduceRdd, (0 until parts).toArray)
    assert(taskSets.length == 1)

    // Complete shuffle map stage successfully on hostA
    complete(taskSets(0), taskSets(0).tasks.zipWithIndex.map {
      case (task, _) =>
        (Success, makeMapStatus("hostA", parts))
    }.toSeq)

    assert(mapOutputTracker.getNumAvailableOutputs(shuffleDep.shuffleId) == parts)

    // Finish the first task
    runEvent(makeCompletionEvent(
      taskSets(1).tasks(0), Success, makeMapStatus("hostA", parts)))

    // The second task fails with Metadata Failed exception.
    val metadataFetchFailedEx = new MetadataFetchFailedException(
      shuffleDep.shuffleId, 1, "metadata failure");
    runEvent(makeCompletionEvent(
      taskSets(1).tasks(1), metadataFetchFailedEx.toTaskFailedReason, null))
    assert(mapOutputTracker.getNumAvailableOutputs(shuffleDep.shuffleId) == parts)
  }

  test("SPARK-32923: handle stage failure for indeterminate map stage with push-based shuffle") {
    initPushBasedShuffleConfs(conf)
    DAGSchedulerSuite.clearMergerLocs()
    DAGSchedulerSuite.addMergerLocs(Seq("host1", "host2", "host3", "host4", "host5"))
    val (shuffleId1, shuffleId2) = constructIndeterminateStageFetchFailed()

    // Check status for all failedStages
    val failedStages = scheduler.failedStages.toSeq
    assert(failedStages.map(_.id) == Seq(1, 2))
    // Shuffle blocks of "hostC" is lost, so first task of the `shuffleMapRdd2` needs to retry.
    assert(failedStages.collect {
      case stage: ShuffleMapStage if stage.shuffleDep.shuffleId == shuffleId2 => stage
    }.head.findMissingPartitions() == Seq(0))
    // The result stage is still waiting for its 2 tasks to complete
    assert(failedStages.collect {
      case stage: ResultStage => stage
    }.head.findMissingPartitions() == Seq(0, 1))
    // shuffleMergeId for indeterminate stages would start from 1
    assert(failedStages.collect {
      case stage: ShuffleMapStage => stage.shuffleDep.shuffleMergeId
    }.forall(x => x == 1))
    scheduler.resubmitFailedStages()

    // The first task of the `shuffleMapRdd2` failed with fetch failure
    runEvent(makeCompletionEvent(
      taskSets(3).tasks(0),
      FetchFailed(makeBlockManagerId("hostA"), shuffleId1, 0L, 0, 0, "ignored"),
      null))

    val newFailedStages = scheduler.failedStages.toSeq
    assert(newFailedStages.map(_.id) == Seq(0, 1))
    // shuffleMergeId for indeterminate failed stages should be 2
    assert(failedStages.collect {
      case stage: ShuffleMapStage => stage.shuffleDep.shuffleMergeId
    }.forall(x => x == 2))
    scheduler.resubmitFailedStages()

    // First shuffle map stage resubmitted and reran all tasks.
    assert(taskSets(4).stageId == 0)
    assert(taskSets(4).stageAttemptId == 1)
    assert(taskSets(4).tasks.length == 2)

    // Finish all stage.
    completeShuffleMapStageSuccessfully(0, 1, 2)
    assert(mapOutputTracker.findMissingPartitions(shuffleId1) === Some(Seq.empty))
    // shuffleMergeId should be 2 for the attempt number 1 for stage 0
    assert(mapOutputTracker.shuffleStatuses.get(shuffleId1).forall(
      _.mergeStatuses.forall(x => x.shuffleMergeId == 2)))
    assert(mapOutputTracker.getNumAvailableMergeResults(shuffleId1) == 2)

    completeShuffleMapStageSuccessfully(1, 2, 2, Seq("hostC", "hostD"))
    assert(mapOutputTracker.findMissingPartitions(shuffleId2) === Some(Seq.empty))
    // shuffleMergeId should be 2 for the attempt number 2 for stage 1
    assert(mapOutputTracker.shuffleStatuses.get(shuffleId2).forall(
      _.mergeStatuses.forall(x => x.shuffleMergeId == 3)))
    assert(mapOutputTracker.getNumAvailableMergeResults(shuffleId2) == 2)

    complete(taskSets(6), Seq((Success, 11), (Success, 12)))

    // Job successful ended.
    assert(results === Map(0 -> 11, 1 -> 12))
  }

  test("SPARK-33701: check adaptive shuffle merge finalization triggered after" +
    " stage completion") {
    initPushBasedShuffleConfs(conf)
    conf.set(config.SHUFFLE_MERGER_LOCATIONS_MIN_STATIC_THRESHOLD, 3)
    DAGSchedulerSuite.clearMergerLocs()
    DAGSchedulerSuite.addMergerLocs(Seq("host1", "host2", "host3", "host4", "host5"))
    val parts = 2

    val shuffleMapRdd1 = new MyRDD(sc, parts, Nil)
    val shuffleDep1 = new ShuffleDependency(shuffleMapRdd1, new HashPartitioner(parts))
    val shuffleMapRdd2 = new MyRDD(sc, parts, Nil)
    val shuffleDep2 = new ShuffleDependency(shuffleMapRdd2, new HashPartitioner(parts))
    val reduceRdd = new MyRDD(sc, parts, List(shuffleDep1, shuffleDep2),
      tracker = mapOutputTracker)

    // Submit a reduce job that depends which will create a map stage
    submit(reduceRdd, (0 until parts).toArray)

    val taskResults = taskSets(0).tasks.zipWithIndex.map {
      case (_, idx) =>
        (Success, makeMapStatus("host" + ('A' + idx).toChar, parts))
    }.toSeq
    for ((result, i) <- taskResults.zipWithIndex) {
      runEvent(makeCompletionEvent(taskSets(0).tasks(i), result._1, result._2))
    }
    // Verify finalize task is set with default delay of 10s and merge results are marked
    // for registration
    val shuffleStage1 = scheduler.stageIdToStage(0).asInstanceOf[ShuffleMapStage]
    assert(shuffleStage1.shuffleDep.shuffleMergeEnabled)
    val finalizeTask1 = shuffleStage1.shuffleDep.getFinalizeTask.get
      .asInstanceOf[DummyScheduledFuture]
    assert(finalizeTask1.delay == 10 && finalizeTask1.registerMergeResults)
    assert(shuffleStage1.shuffleDep.isShuffleMergeFinalizedMarked)

    complete(taskSets(1), taskSets(1).tasks.zipWithIndex.map {
      case (_, idx) =>
        (Success, makeMapStatus("host" + ('A' + idx).toChar, parts, 10))
    }.toSeq)
    val shuffleStage2 = scheduler.stageIdToStage(1).asInstanceOf[ShuffleMapStage]
    assert(shuffleStage2.shuffleDep.getFinalizeTask.nonEmpty)
    val finalizeTask2 = shuffleStage2.shuffleDep.getFinalizeTask.get
      .asInstanceOf[DummyScheduledFuture]
    assert(finalizeTask2.delay == 10 && finalizeTask2.registerMergeResults)

    assert(mapOutputTracker.
      getNumAvailableMergeResults(shuffleStage1.shuffleDep.shuffleId) == parts)
    assert(mapOutputTracker.
      getNumAvailableMergeResults(shuffleStage2.shuffleDep.shuffleId) == parts)
    completeNextResultStageWithSuccess(2, 0)
    assert(results === Map(0 -> 42, 1 -> 42))

    results.clear()
    assertDataStructuresEmpty()
  }

  test("SPARK-33701: check adaptive shuffle merge finalization triggered after minimum" +
    " threshold push complete") {
    initPushBasedShuffleConfs(conf)
    conf.set(config.PUSH_BASED_SHUFFLE_SIZE_MIN_SHUFFLE_SIZE_TO_WAIT, 10L)
    conf.set(config.SHUFFLE_MERGER_LOCATIONS_MIN_STATIC_THRESHOLD, 5)
    conf.set(config.PUSH_BASED_SHUFFLE_MIN_PUSH_RATIO, 0.5)
    DAGSchedulerSuite.clearMergerLocs()
    DAGSchedulerSuite.addMergerLocs(Seq("host1", "host2", "host3", "host4", "host5"))
    val parts = 4

    val shuffleMapRdd1 = new MyRDD(sc, parts, Nil)
    val shuffleDep1 = new ShuffleDependency(shuffleMapRdd1, new HashPartitioner(parts))
    val shuffleMapRdd2 = new MyRDD(sc, parts, Nil)
    val shuffleDep2 = new ShuffleDependency(shuffleMapRdd2, new HashPartitioner(parts))
    val reduceRdd = new MyRDD(sc, parts, List(shuffleDep1, shuffleDep2),
      tracker = mapOutputTracker)

    // Submit a reduce job that depends which will create a map stage
    submit(reduceRdd, (0 until parts).toArray)

    val taskResults = taskSets(0).tasks.zipWithIndex.map {
      case (_, idx) =>
        (Success, makeMapStatus("host" + ('A' + idx).toChar, parts))
    }.toSeq

    runEvent(makeCompletionEvent(taskSets(0).tasks(0), taskResults(0)._1, taskResults(0)._2))
    runEvent(makeCompletionEvent(taskSets(0).tasks(1), taskResults(0)._1, taskResults(0)._2))

    val shuffleStage1 = scheduler.stageIdToStage(0).asInstanceOf[ShuffleMapStage]
    assert(shuffleStage1.shuffleDep.shuffleMergeEnabled)

    pushComplete(shuffleStage1.shuffleDep.shuffleId, 0, 0)
    pushComplete(shuffleStage1.shuffleDep.shuffleId, 0, 1)

    // Minimum push complete for 2 tasks, should have scheduled merge finalization
    val finalizeTask = shuffleStage1.shuffleDep.getFinalizeTask.get
      .asInstanceOf[DummyScheduledFuture]
    assert(finalizeTask.registerMergeResults && finalizeTask.delay == 0)

    runEvent(makeCompletionEvent(taskSets(0).tasks(2), taskResults(0)._1, taskResults(0)._2))
    runEvent(makeCompletionEvent(taskSets(0).tasks(3), taskResults(0)._1, taskResults(0)._2))

    completeShuffleMapStageSuccessfully(1, 0, parts)

    completeNextResultStageWithSuccess(2, 0)
    assert(results === Map(0 -> 42, 1 -> 42, 2 -> 42, 3 -> 42))

    results.clear()
    assertDataStructuresEmpty()
  }

  // Test the behavior of stage cancellation during the spark.shuffle.push.finalize.timeout
  // wait for shuffle merge finalization
  test("SPARK-33701: check adaptive shuffle merge finalization behavior with stage " +
    "cancellation for determinate and indeterminate stages during " +
    "spark.shuffle.push.finalize.timeout wait") {
    initPushBasedShuffleConfs(conf)
    conf.set(config.PUSH_BASED_SHUFFLE_SIZE_MIN_SHUFFLE_SIZE_TO_WAIT, 10L)
    conf.set(config.SHUFFLE_MERGER_LOCATIONS_MIN_STATIC_THRESHOLD, 5)
    conf.set(config.PUSH_BASED_SHUFFLE_MIN_PUSH_RATIO, 0.5)
    DAGSchedulerSuite.clearMergerLocs()
    DAGSchedulerSuite.addMergerLocs(Seq("host1", "host2", "host3", "host4", "host5"))
    val parts = 4

    scheduler = new MyDAGScheduler(
      sc,
      taskScheduler,
      sc.listenerBus,
      mapOutputTracker,
      blockManagerMaster,
      sc.env,
      shuffleMergeFinalize = false)
    dagEventProcessLoopTester = new DAGSchedulerEventProcessLoopTester(scheduler)

    // Determinate stage
    val shuffleMapRdd1 = new MyRDD(sc, parts, Nil)
    val shuffleDep1 = new ShuffleDependency(shuffleMapRdd1, new HashPartitioner(parts))
    val shuffleMapRdd2 = new MyRDD(sc, parts, Nil)
    val shuffleDep2 = new ShuffleDependency(shuffleMapRdd2, new HashPartitioner(parts))
    val reduceRdd = new MyRDD(sc, parts, List(shuffleDep1, shuffleDep2),
      tracker = mapOutputTracker)

    // Submit a reduce job that depends which will create a map stage
    submit(reduceRdd, (0 until parts).toArray)

    val taskResults = taskSets(0).tasks.zipWithIndex.map {
      case (_, idx) =>
        (Success, makeMapStatus("host" + ('A' + idx).toChar, parts))
    }.toSeq

    for ((result, i) <- taskResults.zipWithIndex) {
      runEvent(makeCompletionEvent(taskSets(0).tasks(i), result._1, result._2))
    }
    val shuffleStage1 = scheduler.stageIdToStage(0).asInstanceOf[ShuffleMapStage]
    runEvent(StageCancelled(0, Option("Explicit cancel check")))
    scheduler.handleShuffleMergeFinalized(shuffleStage1, shuffleStage1.shuffleDep.shuffleMergeId)

    assert(shuffleStage1.shuffleDep.mergerLocs.nonEmpty)
    assert(shuffleStage1.shuffleDep.isShuffleMergeFinalizedMarked)
    assert(mapOutputTracker.
      getNumAvailableMergeResults(shuffleStage1.shuffleDep.shuffleId) == 4)

    // Indeterminate stage
    val shuffleMapIndeterminateRdd1 = new MyRDD(sc, parts, Nil, indeterminate = true)
    val shuffleIndeterminateDep1 = new ShuffleDependency(
      shuffleMapIndeterminateRdd1, new HashPartitioner(parts))
    val shuffleMapIndeterminateRdd2 = new MyRDD(sc, parts, Nil, indeterminate = true)
    val shuffleIndeterminateDep2 = new ShuffleDependency(
      shuffleMapIndeterminateRdd2, new HashPartitioner(parts))
    val reduceIndeterminateRdd = new MyRDD(sc, parts, List(
      shuffleIndeterminateDep1, shuffleIndeterminateDep2), tracker = mapOutputTracker)

    // Submit a reduce job that depends which will create a map stage
    submit(reduceIndeterminateRdd, (0 until parts).toArray)

    val indeterminateResults = taskSets(0).tasks.zipWithIndex.map {
      case (_, idx) =>
        (Success, makeMapStatus("host" + ('A' + idx).toChar, parts))
    }.toSeq

    for ((result, i) <- indeterminateResults.zipWithIndex) {
      runEvent(makeCompletionEvent(taskSets(0).tasks(i), result._1, result._2))
    }

    val shuffleIndeterminateStage = scheduler.stageIdToStage(3).asInstanceOf[ShuffleMapStage]
    assert(shuffleIndeterminateStage.isIndeterminate)
    scheduler.handleShuffleMergeFinalized(shuffleIndeterminateStage, 2)
    assert(shuffleIndeterminateStage.shuffleDep.shuffleMergeEnabled)
    assert(!shuffleIndeterminateStage.shuffleDep.isShuffleMergeFinalizedMarked)
  }

  // With Adaptive shuffle merge finalization, once minimum shuffle pushes complete after stage
  // completion, the existing shuffle merge finalization task with
  // delay = spark.shuffle.push.finalize.timeout should be replaced with a new shuffle merge
  // finalization task with delay = 0
  test("SPARK-33701: check adaptive shuffle merge finalization with minimum pushes complete" +
    " after the stage completion replacing the finalize task with delay = 0") {
    initPushBasedShuffleConfs(conf)
    conf.set(config.PUSH_BASED_SHUFFLE_SIZE_MIN_SHUFFLE_SIZE_TO_WAIT, 10L)
    conf.set(config.SHUFFLE_MERGER_LOCATIONS_MIN_STATIC_THRESHOLD, 5)
    conf.set(config.PUSH_BASED_SHUFFLE_MIN_PUSH_RATIO, 0.5)
    DAGSchedulerSuite.clearMergerLocs()
    DAGSchedulerSuite.addMergerLocs(Seq("host1", "host2", "host3", "host4", "host5"))
    val parts = 4

    scheduler = new MyDAGScheduler(
      sc,
      taskScheduler,
      sc.listenerBus,
      mapOutputTracker,
      blockManagerMaster,
      sc.env,
      shuffleMergeFinalize = false)
    dagEventProcessLoopTester = new DAGSchedulerEventProcessLoopTester(scheduler)

    // Determinate stage
    val shuffleMapRdd1 = new MyRDD(sc, parts, Nil)
    val shuffleDep1 = new ShuffleDependency(shuffleMapRdd1, new HashPartitioner(parts))
    val shuffleMapRdd2 = new MyRDD(sc, parts, Nil)
    val shuffleDep2 = new ShuffleDependency(shuffleMapRdd2, new HashPartitioner(parts))
    val reduceRdd = new MyRDD(sc, parts, List(shuffleDep1, shuffleDep2),
      tracker = mapOutputTracker)

    // Submit a reduce job that depends which will create a map stage
    submit(reduceRdd, (0 until parts).toArray)

    val taskResults = taskSets(0).tasks.zipWithIndex.map {
      case (_, idx) =>
        (Success, makeMapStatus("host" + ('A' + idx).toChar, parts))
    }.toSeq

    for ((result, i) <- taskResults.zipWithIndex) {
      runEvent(makeCompletionEvent(taskSets(0).tasks(i), result._1, result._2))
    }
    val shuffleStage1 = scheduler.stageIdToStage(0).asInstanceOf[ShuffleMapStage]
    assert(shuffleStage1.shuffleDep.shuffleMergeEnabled)
    assert(!shuffleStage1.shuffleDep.isShuffleMergeFinalizedMarked)
    val finalizeTask1 = shuffleStage1.shuffleDep.getFinalizeTask.get.
      asInstanceOf[DummyScheduledFuture]
    assert(finalizeTask1.delay == 10 && finalizeTask1.registerMergeResults)

    // Minimum shuffle pushes complete, replace the finalizeTask with delay = 10
    // with a finalizeTask with delay = 0
    pushComplete(shuffleStage1.shuffleDep.shuffleId, 0, 0)
    pushComplete(shuffleStage1.shuffleDep.shuffleId, 0, 1)

    // Existing finalizeTask with delay = 10 should be replaced with finalizeTask
    // with delay = 0
    val finalizeTask2 = shuffleStage1.shuffleDep.getFinalizeTask.get.
      asInstanceOf[DummyScheduledFuture]
    assert(finalizeTask2.delay == 0 && finalizeTask2.registerMergeResults)
  }

  test("SPARK-34826: Adaptively fetch shuffle mergers") {
    initPushBasedShuffleConfs(conf)
    conf.set(config.SHUFFLE_MERGER_LOCATIONS_MIN_STATIC_THRESHOLD, 2)
    DAGSchedulerSuite.clearMergerLocs()
    DAGSchedulerSuite.addMergerLocs(Seq("host1"))
    val parts = 2

    val shuffleMapRdd = new MyRDD(sc, parts, Nil)
    val shuffleDep = new ShuffleDependency(shuffleMapRdd, new HashPartitioner(parts))
    val reduceRdd = new MyRDD(sc, parts, List(shuffleDep), tracker = mapOutputTracker)

    // Submit a reduce job that depends which will create a map stage
    submit(reduceRdd, (0 until parts).toArray)

    runEvent(makeCompletionEvent(
      taskSets(0).tasks(0), Success, makeMapStatus("hostA", parts),
      Seq.empty, Array.empty, createFakeTaskInfoWithId(0)))

    val shuffleStage1 = scheduler.stageIdToStage(0).asInstanceOf[ShuffleMapStage]
    assert(!shuffleStage1.shuffleDep.shuffleMergeEnabled)
    assert(mapOutputTracker.getShufflePushMergerLocations(0).isEmpty)

    DAGSchedulerSuite.addMergerLocs(Seq("host2", "host3"))

    // host2 executor added event to trigger registering of shuffle merger locations
    // as shuffle mergers are tracked separately for test
    runEvent(ExecutorAdded("exec2", "host2"))

    // Check if new shuffle merger locations are available for push or not
    assert(mapOutputTracker.getShufflePushMergerLocations(0).size == 2)
    assert(shuffleStage1.shuffleDep.getMergerLocs.size == 2)

    // Complete remaining tasks in ShuffleMapStage 0
    runEvent(makeCompletionEvent(taskSets(0).tasks(1), Success,
      makeMapStatus("host1", parts), Seq.empty, Array.empty, createFakeTaskInfoWithId(1)))

    completeNextResultStageWithSuccess(1, 0)
    assert(results === Map(0 -> 42, 1 -> 42))

    results.clear()
    assertDataStructuresEmpty()
  }

  test("SPARK-34826: Adaptively fetch shuffle mergers with stage retry") {
    initPushBasedShuffleConfs(conf)
    conf.set(config.SHUFFLE_MERGER_LOCATIONS_MIN_STATIC_THRESHOLD, 2)
    DAGSchedulerSuite.clearMergerLocs()
    DAGSchedulerSuite.addMergerLocs(Seq("host1"))
    val parts = 2

    val shuffleMapRdd1 = new MyRDD(sc, parts, Nil)
    val shuffleDep1 = new ShuffleDependency(shuffleMapRdd1, new HashPartitioner(parts))
    val shuffleMapRdd2 = new MyRDD(sc, parts, Nil)
    val shuffleDep2 = new ShuffleDependency(shuffleMapRdd2, new HashPartitioner(parts))
    val reduceRdd = new MyRDD(sc, parts, List(shuffleDep1, shuffleDep2),
      tracker = mapOutputTracker)

    // Submit a reduce job that depends which will create a map stage
    submit(reduceRdd, (0 until parts).toArray)

    val taskResults = taskSets(0).tasks.zipWithIndex.map {
      case (_, idx) =>
        (Success, makeMapStatus("host" + idx, parts))
    }.toSeq

    val shuffleStage1 = scheduler.stageIdToStage(0).asInstanceOf[ShuffleMapStage]
    DAGSchedulerSuite.addMergerLocs(Seq("host2", "host3"))
    // host2 executor added event to trigger registering of shuffle merger locations
    // as shuffle mergers are tracked separately for test
    runEvent(ExecutorAdded("exec2", "host2"))
    // Check if new shuffle merger locations are available for push or not
    assert(mapOutputTracker.getShufflePushMergerLocations(0).size == 2)
    assert(shuffleStage1.shuffleDep.getMergerLocs.size == 2)
    val mergerLocsBeforeRetry = shuffleStage1.shuffleDep.getMergerLocs

    // Clear merger locations to check if new mergers are not getting set for the
    // retry of determinate stage
    DAGSchedulerSuite.clearMergerLocs()

    // Remove MapStatus on one of the host before the stage ends to trigger
    // a scenario where stage 0 needs to be resubmitted upon finishing all tasks.
    // Merge finalization should be scheduled in this case.
    for ((result, i) <- taskResults.zipWithIndex) {
      if (i == taskSets(0).tasks.size - 1) {
        mapOutputTracker.removeOutputsOnHost("host0")
      }
      runEvent(makeCompletionEvent(taskSets(0).tasks(i), result._1, result._2))
    }
    assert(shuffleStage1.shuffleDep.isShuffleMergeFinalizedMarked)

    DAGSchedulerSuite.addMergerLocs(Seq("host4", "host5"))
    // host4 executor added event shouldn't reset merger locations given merger locations
    // are already set
    runEvent(ExecutorAdded("exec4", "host4"))

    // Successfully completing the retry of stage 0.
    complete(taskSets(2), taskSets(2).tasks.zipWithIndex.map {
      case (_, idx) =>
        (Success, makeMapStatus("host" + idx, parts))
    }.toSeq)

    assert(shuffleStage1.shuffleDep.shuffleMergeId == 0)
    assert(shuffleStage1.shuffleDep.getMergerLocs.size == 2)
    assert(shuffleStage1.shuffleDep.isShuffleMergeFinalizedMarked)
    val newMergerLocs =
      scheduler.stageIdToStage(0).asInstanceOf[ShuffleMapStage].shuffleDep.getMergerLocs
    assert(mergerLocsBeforeRetry.sortBy(_.host) === newMergerLocs.sortBy(_.host))
    val shuffleStage2 = scheduler.stageIdToStage(1).asInstanceOf[ShuffleMapStage]
    complete(taskSets(1), taskSets(1).tasks.zipWithIndex.map {
      case (_, idx) =>
        (Success, makeMapStatus("host" + idx, parts, 10))
    }.toSeq)
    assert(shuffleStage2.shuffleDep.getMergerLocs.size == 2)
    completeNextResultStageWithSuccess(2, 0)
    assert(results === Map(0 -> 42, 1 -> 42))

    results.clear()
    assertDataStructuresEmpty()
  }

  test("SPARK-34826: Adaptively fetch shuffle mergers with stage retry for indeterminate stage") {
    initPushBasedShuffleConfs(conf)
    conf.set(config.SHUFFLE_MERGER_LOCATIONS_MIN_STATIC_THRESHOLD, 2)
    DAGSchedulerSuite.clearMergerLocs()
    DAGSchedulerSuite.addMergerLocs(Seq("host1"))
    val parts = 2

    val shuffleMapRdd1 = new MyRDD(sc, parts, Nil, indeterminate = true)
    val shuffleDep1 = new ShuffleDependency(shuffleMapRdd1, new HashPartitioner(parts))
    val shuffleMapRdd2 = new MyRDD(sc, parts, Nil, indeterminate = true)
    val shuffleDep2 = new ShuffleDependency(shuffleMapRdd2, new HashPartitioner(parts))
    val reduceRdd = new MyRDD(sc, parts, List(shuffleDep1, shuffleDep2),
      tracker = mapOutputTracker)

    // Submit a reduce job that depends which will create a map stage
    submit(reduceRdd, (0 until parts).toArray)

    val taskResults = taskSets(0).tasks.zipWithIndex.map {
      case (_, idx) =>
        (Success, makeMapStatus("host" + idx, parts))
    }.toSeq

    val shuffleStage1 = scheduler.stageIdToStage(0).asInstanceOf[ShuffleMapStage]
    DAGSchedulerSuite.addMergerLocs(Seq("host2", "host3"))
    // host2 executor added event to trigger registering of shuffle merger locations
    // as shuffle mergers are tracked separately for test
    runEvent(ExecutorAdded("exec2", "host2"))
    // Check if new shuffle merger locations are available for push or not
    assert(mapOutputTracker.getShufflePushMergerLocations(0).size == 2)
    assert(shuffleStage1.shuffleDep.getMergerLocs.size == 2)
    val mergerLocsBeforeRetry = shuffleStage1.shuffleDep.getMergerLocs

    // Clear merger locations to check if new mergers are getting set for the
    // retry of indeterminate stage
    DAGSchedulerSuite.clearMergerLocs()

    // Remove MapStatus on one of the host before the stage ends to trigger
    // a scenario where stage 0 needs to be resubmitted upon finishing all tasks.
    // Merge finalization should be scheduled in this case.
    for ((result, i) <- taskResults.zipWithIndex) {
      if (i == taskSets(0).tasks.size - 1) {
        mapOutputTracker.removeOutputsOnHost("host0")
      }
      runEvent(makeCompletionEvent(taskSets(0).tasks(i), result._1, result._2))
    }

    // Indeterminate stage should recompute all partitions, hence
    // isShuffleMergeFinalizedMarked should be false here
    assert(!shuffleStage1.shuffleDep.isShuffleMergeFinalizedMarked)

    DAGSchedulerSuite.addMergerLocs(Seq("host4", "host5"))
    // host4 executor added event should reset merger locations given merger locations
    // are already reset
    runEvent(ExecutorAdded("exec4", "host4"))
    assert(shuffleStage1.shuffleDep.getMergerLocs.size == 2)
    // Successfully completing the retry of stage 0.
    complete(taskSets(2), taskSets(2).tasks.zipWithIndex.map {
      case (_, idx) =>
        (Success, makeMapStatus("host" + idx, parts))
    }.toSeq)

    assert(shuffleStage1.shuffleDep.shuffleMergeId == 2)
    assert(shuffleStage1.shuffleDep.isShuffleMergeFinalizedMarked)
    val newMergerLocs =
      scheduler.stageIdToStage(0).asInstanceOf[ShuffleMapStage].shuffleDep.getMergerLocs
    assert(mergerLocsBeforeRetry.sortBy(_.host) !== newMergerLocs.sortBy(_.host))
    val shuffleStage2 = scheduler.stageIdToStage(1).asInstanceOf[ShuffleMapStage]
    complete(taskSets(1), taskSets(1).tasks.zipWithIndex.map {
      case (_, idx) =>
        (Success, makeMapStatus("host" + idx, parts, 10))
    }.toSeq)
    assert(shuffleStage2.shuffleDep.getMergerLocs.size == 2)
    completeNextResultStageWithSuccess(2, 0)
    assert(results === Map(0 -> 42, 1 -> 42))

    results.clear()
    assertDataStructuresEmpty()
  }

  test("SPARK-38987: corrupted merged shuffle block FetchFailure should unregister merge results") {
    initPushBasedShuffleConfs(conf)
    DAGSchedulerSuite.clearMergerLocs()
    DAGSchedulerSuite.addMergerLocs(Seq("host1", "host2", "host3", "host4", "host5"))

    scheduler = new MyDAGScheduler(
      sc,
      taskScheduler,
      sc.listenerBus,
      mapOutputTracker,
      blockManagerMaster,
      sc.env,
      shuffleMergeFinalize = false,
      shuffleMergeRegister = false)
    dagEventProcessLoopTester = new DAGSchedulerEventProcessLoopTester(scheduler)

    val parts = 2
    val shuffleMapRdd = new MyRDD(sc, parts, Nil)
    val shuffleDep = new ShuffleDependency(shuffleMapRdd, new HashPartitioner(parts))
    val reduceRdd = new MyRDD(sc, parts, List(shuffleDep), tracker = mapOutputTracker)

    // Submit a reduce job that depends which will create a map stage
    submit(reduceRdd, (0 until parts).toArray)

    val shuffleMapStage = scheduler.stageIdToStage(0).asInstanceOf[ShuffleMapStage]
    scheduler.handleRegisterMergeStatuses(shuffleMapStage,
      Seq((0, makeMergeStatus("hostA", shuffleDep.shuffleMergeId, isShufflePushMerger = true))))
    scheduler.handleShuffleMergeFinalized(shuffleMapStage,
      shuffleMapStage.shuffleDep.shuffleMergeId)
    scheduler.handleRegisterMergeStatuses(shuffleMapStage,
      Seq((1, makeMergeStatus("hostA", shuffleDep.shuffleMergeId, isShufflePushMerger = true))))

    assert(mapOutputTracker.getNumAvailableMergeResults(shuffleDep.shuffleId) == 1)

    // Complete shuffle map stage with FetchFailed on hostA
    complete(taskSets(0), taskSets(0).tasks.zipWithIndex.map {
      case (task, _) =>
        (FetchFailed(
          makeBlockManagerId("hostA", execId = Some(BlockManagerId.SHUFFLE_MERGER_IDENTIFIER)),
          shuffleDep.shuffleId, -1L, -1, 0, "corruption fetch failure"), null)
    }.toSeq)
    assert(mapOutputTracker.getNumAvailableMergeResults(shuffleDep.shuffleId) == 0)
  }

  test("SPARK-38987: All shuffle outputs for a shuffle push" +
    " merger executor should be cleaned up on a fetch failure when" +
    "spark.files.fetchFailure.unRegisterOutputOnHost is true") {
    initPushBasedShuffleConfs(conf)
    conf.set("spark.files.fetchFailure.unRegisterOutputOnHost", "true")

    val shuffleMapRdd = new MyRDD(sc, 3, Nil)
    val shuffleDep = new ShuffleDependency(shuffleMapRdd, new HashPartitioner(3))
    val shuffleId = shuffleDep.shuffleId
    val reduceRdd = new MyRDD(sc, 3, List(shuffleDep), tracker = mapOutputTracker)

    submit(reduceRdd, Array(0, 1, 2))
    // Map stage completes successfully,
    // two tasks are run on an executor on hostA and one on an executor on hostB
    completeShuffleMapStageSuccessfully(0, 0, 3, Seq("hostA", "hostA", "hostB"))
    // Now the executor on hostA is lost
    runEvent(ExecutorLost(BlockManagerId.SHUFFLE_MERGER_IDENTIFIER,
      ExecutorExited(-100, false, "Container marked as failed")))

    // Shuffle push merger executor should not be removed and the shuffle files are not unregistered
    verify(blockManagerMaster, times(0)).removeExecutor(BlockManagerId.SHUFFLE_MERGER_IDENTIFIER)
    verify(mapOutputTracker,
      times(0)).removeOutputsOnExecutor(BlockManagerId.SHUFFLE_MERGER_IDENTIFIER)

    // Now a fetch failure from the lost executor occurs
    complete(taskSets(1), Seq(
      (FetchFailed(BlockManagerId(BlockManagerId.SHUFFLE_MERGER_IDENTIFIER, "hostA", 12345),
        shuffleId, 0L, 0, 0, "ignored"), null)
    ))

    // Verify that we are not removing the executor,
    // and that we are only removing the outputs on the host
    verify(blockManagerMaster, times(0)).removeExecutor(BlockManagerId.SHUFFLE_MERGER_IDENTIFIER)
    verify(blockManagerMaster, times(1)).removeShufflePushMergerLocation("hostA")
    verify(mapOutputTracker,
      times(1)).removeOutputsOnHost("hostA")

    // There should be no map statuses or merge statuses on the host
    val shuffleStatuses = mapOutputTracker.shuffleStatuses(shuffleId)
    val mapStatuses = shuffleStatuses.mapStatuses
    val mergeStatuses = shuffleStatuses.mergeStatuses
    assert(mapStatuses.count(_ != null) === 1)
    assert(mapStatuses.count(s => s != null
      && s.location.executorId == BlockManagerId.SHUFFLE_MERGER_IDENTIFIER) === 0)
    assert(mergeStatuses.count(s => s != null
      && s.location.executorId == BlockManagerId.SHUFFLE_MERGER_IDENTIFIER) === 0)
    // hostB-exec should still have its shuffle files
    assert(mapStatuses.count(s => s != null && s.location.executorId == "hostB-exec") === 1)
  }

  Seq(true, false).foreach { registerMergeResults =>
    test("SPARK-40096: Send finalize events even if shuffle merger blocks indefinitely " +
      s"with registerMergeResults is ${registerMergeResults}") {
      initPushBasedShuffleConfs(conf)

      sc.conf.set("spark.shuffle.push.results.timeout", "1s")
      val scheduler = new DAGScheduler(
        sc,
        taskScheduler,
        sc.listenerBus,
        mapOutputTracker,
        blockManagerMaster,
        sc.env)

      val mergerLocs = Seq(makeBlockManagerId("hostA"), makeBlockManagerId("hostB"))
      val timeoutSecs = 1
      val sendRequestsLatch = new CountDownLatch(mergerLocs.size)
      val completeLatch = new CountDownLatch(mergerLocs.size)
      val canSendRequestLatch = new CountDownLatch(1)

      val blockStoreClient = mock(classOf[ExternalBlockStoreClient])
      val blockStoreClientField = classOf[BlockManager].getDeclaredField("blockStoreClient")
      blockStoreClientField.setAccessible(true)
      blockStoreClientField.set(sc.env.blockManager, blockStoreClient)

      val sentHosts = JCollections.synchronizedList(new JArrayList[String]())
      var hostAInterrupted = false
      doAnswer { (invoke: InvocationOnMock) =>
        val host = invoke.getArgument[String](0)
        try {
          if (host == "hostA") {
            sendRequestsLatch.countDown()
            canSendRequestLatch.await(timeoutSecs * 5, TimeUnit.SECONDS)
            // should not reach here, will get interrupted by DAGScheduler
            sentHosts.add(host)
          } else {
            sentHosts.add(host)
            sendRequestsLatch.countDown()
          }
        } catch {
          case _: InterruptedException => hostAInterrupted = true
        } finally {
          completeLatch.countDown()
        }
      }.when(blockStoreClient).finalizeShuffleMerge(any(), any(), any(), any(), any())

      val shuffleMapRdd = new MyRDD(sc, 1, Nil)
      val shuffleDep = new ShuffleDependency(shuffleMapRdd, new HashPartitioner(2))
      shuffleDep.setMergerLocs(mergerLocs)
      val shuffleStage = scheduler.createShuffleMapStage(shuffleDep, 0)

      scheduler.finalizeShuffleMerge(shuffleStage, registerMergeResults)
      sendRequestsLatch.await()
      verify(blockStoreClient, times(2))
        .finalizeShuffleMerge(any(), any(), any(), any(), any())
      assert(1 == sentHosts.size())
      assert(sentHosts.asScala.toSeq === Seq("hostB"))
      completeLatch.await()
      assert(hostAInterrupted)
    }
  }

  /**
   * Assert that the supplied TaskSet has exactly the given hosts as its preferred locations.
   * Note that this checks only the host and not the executor ID.
   */
  private def assertLocations(taskSet: TaskSet, hosts: Seq[Seq[String]]): Unit = {
    assert(hosts.size === taskSet.tasks.size)
    for ((taskLocs, expectedLocs) <- taskSet.tasks.map(_.preferredLocations).zip(hosts)) {
      assert(taskLocs.map(_.host).toSet === expectedLocs.toSet)
    }
  }

  private def assertDataStructuresEmpty(): Unit = {
    assert(scheduler.activeJobs.isEmpty)
    assert(scheduler.failedStages.isEmpty)
    assert(scheduler.jobIdToActiveJob.isEmpty)
    assert(scheduler.jobIdToStageIds.isEmpty)
    assert(scheduler.stageIdToStage.isEmpty)
    assert(scheduler.runningStages.isEmpty)
    assert(scheduler.shuffleIdToMapStage.isEmpty)
    assert(scheduler.waitingStages.isEmpty)
    assert(scheduler.outputCommitCoordinator.isEmpty)
  }

  // Nothing in this test should break if the task info's fields are null, but
  // OutputCommitCoordinator requires the task info itself to not be null.
  private def createFakeTaskInfo(): TaskInfo = {
    val info = new TaskInfo(0, 0, 0, 0, 0L, "", "", TaskLocality.ANY, false)
    info.finishTime = 1
    info
  }

  private def createFakeTaskInfoWithId(taskId: Long): TaskInfo = {
    val info = new TaskInfo(taskId, 0, 0, 0, 0L, "", "", TaskLocality.ANY, false)
    info.finishTime = 1
    info
  }

  private def makeCompletionEvent(
      task: Task[_],
      reason: TaskEndReason,
      result: Any,
      extraAccumUpdates: Seq[AccumulatorV2[_, _]] = Seq.empty,
      metricPeaks: Array[Long] = Array.empty,
      taskInfo: TaskInfo = createFakeTaskInfo()): CompletionEvent = {
    val accumUpdates = reason match {
      case Success => task.metrics.accumulators()
      case ef: ExceptionFailure => ef.accums
      case tk: TaskKilled => tk.accums
      case _ => Seq.empty
    }
    CompletionEvent(task, reason, result, accumUpdates ++ extraAccumUpdates, metricPeaks, taskInfo)
  }
}

object DAGSchedulerSuite {
  val mergerLocs = ArrayBuffer[BlockManagerId]()

  def makeMapStatus(host: String, reduces: Int, sizes: Byte = 2, mapTaskId: Long = -1): MapStatus =
    MapStatus(makeBlockManagerId(host), Array.fill[Long](reduces)(sizes), mapTaskId)

  def makeBlockManagerId(host: String, execId: Option[String] = None): BlockManagerId = {
    BlockManagerId(execId.getOrElse(host + "-exec"), host, 12345)
  }

  def makeMergeStatus(host: String, shuffleMergeId: Int, size: Long = 1000,
    isShufflePushMerger: Boolean = false): MergeStatus = {
    val execId = if (isShufflePushMerger) {
      Some(BlockManagerId.SHUFFLE_MERGER_IDENTIFIER)
    } else {
      None
    }
    MergeStatus(makeBlockManagerId(host, execId),
      shuffleMergeId, mock(classOf[RoaringBitmap]), size)
  }

  def addMergerLocs(locs: Seq[String]): Unit = {
    locs.foreach { loc => mergerLocs.append(makeBlockManagerId(loc)) }
  }

  def clearMergerLocs(): Unit = mergerLocs.clear()

}

object FailThisAttempt {
  val _fail = new AtomicBoolean(true)
}

private class PushBasedSchedulerBackend(
    conf: SparkConf,
    scheduler: TaskSchedulerImpl,
    cores: Int) extends LocalSchedulerBackend(conf, scheduler, cores) {

  override def getShufflePushMergerLocations(
      numPartitions: Int,
      resourceProfileId: Int): Seq[BlockManagerId] = {
    val mergerLocations = Utils.randomize(DAGSchedulerSuite.mergerLocs).take(numPartitions)
    if (mergerLocations.size < numPartitions && mergerLocations.size <
      conf.getInt(config.SHUFFLE_MERGER_LOCATIONS_MIN_STATIC_THRESHOLD.key, 5)) {
      Seq.empty[BlockManagerId]
    } else {
      mergerLocations
    }
  }

  // Currently this is only used in tests specifically for Push based shuffle
  override def maxNumConcurrentTasks(rp: ResourceProfile): Int = {
    2
  }
}

private class PushBasedClusterManager extends ExternalClusterManager {
  def canCreate(masterURL: String): Boolean = masterURL == "pushbasedshuffleclustermanager"

  override def createSchedulerBackend(
      sc: SparkContext,
      masterURL: String,
      scheduler: TaskScheduler): SchedulerBackend = {
    new PushBasedSchedulerBackend(sc.conf, scheduler.asInstanceOf[TaskSchedulerImpl], 1)
  }

  override def createTaskScheduler(
      sc: SparkContext,
      masterURL: String): TaskScheduler = new TaskSchedulerImpl(sc, 1, isLocal = true) {
    override def applicationAttemptId(): Option[String] = Some("1")
  }

  override def initialize(scheduler: TaskScheduler, backend: SchedulerBackend): Unit = {
    val sc = scheduler.asInstanceOf[TaskSchedulerImpl]
    sc.initialize(backend)
  }
}
