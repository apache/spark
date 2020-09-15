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

import scala.annotation.meta.param
import scala.collection.mutable.{ArrayBuffer, HashMap, HashSet, Map}

import org.mockito.Mockito.spy
import org.scalatest.concurrent.{Signaler, ThreadSignaler, TimeLimits}

import org.apache.spark._
import org.apache.spark.broadcast.BroadcastManager
import org.apache.spark.executor.ExecutorMetrics
import org.apache.spark.rdd.{DeterministicLevel, RDD}
import org.apache.spark.scheduler.SchedulingMode.SchedulingMode
import org.apache.spark.storage.{BlockId, BlockManagerId, BlockManagerMaster}
import org.apache.spark.util.{AccumulatorV2, CallSite}

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

class DAGSchedulerTestBase extends SparkFunSuite with TempLocalSparkContext with TimeLimits {

  import DAGSchedulerTestBase._

  // Necessary to make ScalaTest 3.x interrupt a thread on the JVM like ScalaTest 2.2.x
  implicit val defaultSignaler: Signaler = ThreadSignaler

  private var firstInit: Boolean = _
  /** Set of TaskSets the DAGScheduler has requested executed. */
  val taskSets = scala.collection.mutable.Buffer[TaskSet]()

  /** Stages for which the DAGScheduler has called TaskScheduler.cancelTasks(). */
  val cancelledStages = new HashSet[Int]()

  val tasksMarkedAsCompleted = new ArrayBuffer[Task[_]]()

  val taskScheduler = new TaskScheduler() {
    override def schedulingMode: SchedulingMode = SchedulingMode.FIFO
    override def rootPool: Pool = new Pool("", schedulingMode, 0, 0)
    override def start() = {}
    override def stop() = {}
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
      decommissionInfo: ExecutorDecommissionInfo): Unit = {}
    override def getExecutorDecommissionState(
      executorId: String): Option[ExecutorDecommissionState] = None
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
  }

  /** The list of results that DAGScheduler has collected. */
  val results = new HashMap[Int, Any]()
  var failure: Exception = _
  val jobListener = new JobListener() {
    override def taskSucceeded(index: Int, result: Any) = results.put(index, result)
    override def jobFailed(exception: Exception) = { failure = exception }
  }

  class MyMapOutputTrackerMaster(
      conf: SparkConf,
      broadcastManager: BroadcastManager)
    extends MapOutputTrackerMaster(conf, broadcastManager, true) {

    override def sendTracker(message: Any): Unit = {
      // no-op, just so we can stop this to avoid leaking threads
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
    broadcastManager = new BroadcastManager(true, sc.getConf, securityMgr)
    mapOutputTracker = spy(new MyMapOutputTrackerMaster(sc.getConf, broadcastManager))
    blockManagerMaster = spy(new MyBlockManagerMaster(sc.getConf))
    scheduler = new DAGScheduler(
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
  protected def runEvent(event: DAGSchedulerEvent): Unit = {
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
  protected def complete(taskSet: TaskSet, taskEndInfos: Seq[(TaskEndReason, Any)]): Unit = {
    assert(taskSet.tasks.size >= taskEndInfos.size)
    for ((result, i) <- taskEndInfos.zipWithIndex) {
      if (i < taskSet.tasks.size) {
        runEvent(makeCompletionEvent(taskSet.tasks(i), result._1, result._2))
      }
    }
  }

  /** Submits a job to the scheduler and returns the job id. */
  protected def submit(
      rdd: RDD[_],
      partitions: Array[Int],
      func: (TaskContext, Iterator[_]) => _ = jobComputeFunc,
      listener: JobListener = jobListener,
      properties: Properties = null): Int = {
    val jobId = scheduler.nextJobId.getAndIncrement()
    runEvent(JobSubmitted(jobId, rdd, func, partitions, CallSite("", ""), listener, properties))
    jobId
  }

  /** Sends TaskSetFailed to the scheduler. */
  protected def failed(taskSet: TaskSet, message: String): Unit = {
    runEvent(TaskSetFailed(taskSet, message, None))
  }

  /** Sends JobCancelled to the DAG scheduler. */
  protected def cancel(jobId: Int): Unit = {
    runEvent(JobCancelled(jobId, None))
  }

  // Helper function to validate state when creating tests for task failures
  private def checkStageId(stageId: Int, attempt: Int, stageAttempt: TaskSet): Unit = {
    assert(stageAttempt.stageId === stageId)
    assert(stageAttempt.stageAttemptId == attempt)
  }

  // Create a new Listener to confirm that the listenerBus sees the JobEnd message
  // when we abort the stage. This message will also be consumed by the EventLoggingListener
  // so this will propagate up to the user.
  var ended = false
  var jobResult : JobResult = null

  /**
   * Common code to get the next stage attempt, confirm it's the one we expect, and complete it
   * successfully.
   *
   * @param stageId - The current stageId
   * @param attemptIdx - The current attempt count
   * @param numShufflePartitions - The number of partitions in the next stage
   * @param hostNames - Host on which each task in the task set is executed
   */
  protected def completeShuffleMapStageSuccessfully(
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
  protected def completeNextStageWithFetchFailure(
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
  protected def completeNextResultStageWithSuccess(
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

  // Nothing in this test should break if the task info's fields are null, but
  // OutputCommitCoordinator requires the task info itself to not be null.
  private def createFakeTaskInfo(): TaskInfo = {
    val info = new TaskInfo(0, 0, 0, 0L, "", "", TaskLocality.ANY, false)
    info.finishTime = 1
    info
  }

  protected def makeCompletionEvent(
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

  protected def constructIndeterminateStageFetchFailed(): (Int, Int) = {
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

  protected def assertDataStructuresEmpty(): Unit = {
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
}

object DAGSchedulerTestBase {
  def makeMapStatus(host: String, reduces: Int, sizes: Byte = 2, mapTaskId: Long = -1): MapStatus =
    MapStatus(makeBlockManagerId(host), Array.fill[Long](reduces)(sizes), mapTaskId)

  def makeBlockManagerId(host: String): BlockManagerId = {
    BlockManagerId(host + "-exec", host, 12345)
  }
}
