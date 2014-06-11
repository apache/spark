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

import scala.Tuple2
import scala.collection.mutable.{HashSet, HashMap, Map}
import scala.language.reflectiveCalls

import akka.actor._
import akka.testkit.{ImplicitSender, TestKit, TestActorRef}
import org.scalatest.{BeforeAndAfter, FunSuiteLike}

import org.apache.spark._
import org.apache.spark.rdd.RDD
import org.apache.spark.scheduler.SchedulingMode.SchedulingMode
import org.apache.spark.storage.{BlockId, BlockManagerId, BlockManagerMaster}
import org.apache.spark.util.CallSite

class BuggyDAGEventProcessActor extends Actor {
  val state = 0
  def receive = {
    case _ => throw new SparkException("error")
  }
}

class DAGSchedulerSuite extends TestKit(ActorSystem("DAGSchedulerSuite")) with FunSuiteLike
  with ImplicitSender with BeforeAndAfter with LocalSparkContext {

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
  }

  /** Length of time to wait while draining listener events. */
  val WAIT_TIMEOUT_MILLIS = 10000
  val sparkListener = new SparkListener() {
    val successfulStages = new HashSet[Int]()
    val failedStages = new HashSet[Int]()
    override def onStageCompleted(stageCompleted: SparkListenerStageCompleted) {
      val stageInfo = stageCompleted.stageInfo
      if (stageInfo.failureReason.isEmpty) {
        successfulStages += stageInfo.stageId
      } else {
        failedStages += stageInfo.stageId
      }
    }
  }

  var mapOutputTracker: MapOutputTrackerMaster = null
  var scheduler: DAGScheduler = null
  var dagEventProcessTestActor: TestActorRef[DAGSchedulerEventProcessActor] = null

  /**
   * Set of cache locations to return from our mock BlockManagerMaster.
   * Keys are (rdd ID, partition ID). Anything not present will return an empty
   * list of cache locations silently.
   */
  val cacheLocations = new HashMap[(Int, Int), Seq[BlockManagerId]]
  // stub out BlockManagerMaster.getLocations to use our cacheLocations
  val blockManagerMaster = new BlockManagerMaster(null, conf) {
      override def getLocations(blockIds: Array[BlockId]): Seq[Seq[BlockManagerId]] = {
        blockIds.map {
          _.asRDDId.map(id => (id.rddId -> id.splitIndex)).flatMap(key => cacheLocations.get(key)).
            getOrElse(Seq())
        }.toSeq
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
    sparkListener.successfulStages.clear()
    sparkListener.failedStages.clear()
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
        sc.env) {
      override def runLocally(job: ActiveJob) {
        // don't bother with the thread while unit testing
        runLocallyWithinThread(job)
      }
    }
    dagEventProcessTestActor = TestActorRef[DAGSchedulerEventProcessActor](
      Props(classOf[DAGSchedulerEventProcessActor], scheduler))(system)
  }

  override def afterAll() {
    super.afterAll()
    TestKit.shutdownActorSystem(system)
  }

  /**
   * Type of RDD we use for testing. Note that we should never call the real RDD compute methods.
   * This is a pair RDD type so it can always be used in ShuffleDependencies.
   */
  type MyRDD = RDD[(Int, Int)]

  /**
   * Create an RDD for passing to DAGScheduler. These RDDs will use the dependencies and
   * preferredLocations (if any) that are passed to them. They are deliberately not executable
   * so we can test that DAGScheduler does not try to execute RDDs locally.
   */
  private def makeRdd(
        numPartitions: Int,
        dependencies: List[Dependency[_]],
        locations: Seq[Seq[String]] = Nil
      ): MyRDD = {
    val maxPartition = numPartitions - 1
    val newRDD = new MyRDD(sc, dependencies) {
      override def compute(split: Partition, context: TaskContext): Iterator[(Int, Int)] =
        throw new RuntimeException("should not be reached")
      override def getPartitions = (0 to maxPartition).map(i => new Partition {
        override def index = i
      }).toArray
      override def getPreferredLocations(split: Partition): Seq[String] =
        if (locations.isDefinedAt(split.index))
          locations(split.index)
        else
          Nil
      override def toString: String = "DAGSchedulerSuiteRDD " + id
    }
    newRDD
  }

  /**
   * Process the supplied event as if it were the top of the DAGScheduler event queue, expecting
   * the scheduler not to exit.
   *
   * After processing the event, submit waiting stages as is done on most iterations of the
   * DAGScheduler event loop.
   */
  private def runEvent(event: DAGSchedulerEvent) {
    dagEventProcessTestActor.receive(event)
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
        runEvent(CompletionEvent(taskSet.tasks(i), result._1, result._2, Map[Long, Any](), null, null))
      }
    }
  }

  /** Sends the rdd to the scheduler for scheduling and returns the job id. */
  private def submit(
      rdd: RDD[_],
      partitions: Array[Int],
      func: (TaskContext, Iterator[_]) => _ = jobComputeFunc,
      allowLocal: Boolean = false,
      listener: JobListener = jobListener): Int = {
    val jobId = scheduler.nextJobId.getAndIncrement()
    runEvent(JobSubmitted(jobId, rdd, func, partitions, allowLocal, CallSite("", ""), listener))
    jobId
  }

  /** Sends TaskSetFailed to the scheduler. */
  private def failed(taskSet: TaskSet, message: String) {
    runEvent(TaskSetFailed(taskSet, message))
  }

  /** Sends JobCancelled to the DAG scheduler. */
  private def cancel(jobId: Int) {
    runEvent(JobCancelled(jobId))
  }

  test("zero split job") {
    var numResults = 0
    val fakeListener = new JobListener() {
      override def taskSucceeded(partition: Int, value: Any) = numResults += 1
      override def jobFailed(exception: Exception) = throw exception
    }
    submit(makeRdd(0, Nil), Array(), listener = fakeListener)
    assert(numResults === 0)
  }

  test("run trivial job") {
    submit(makeRdd(1, Nil), Array(0))
    complete(taskSets(0), List((Success, 42)))
    assert(results === Map(0 -> 42))
    assertDataStructuresEmpty
  }

  test("local job") {
    val rdd = new MyRDD(sc, Nil) {
      override def compute(split: Partition, context: TaskContext): Iterator[(Int, Int)] =
        Array(42 -> 0).iterator
      override def getPartitions = Array( new Partition { override def index = 0 } )
      override def getPreferredLocations(split: Partition) = Nil
      override def toString = "DAGSchedulerSuite Local RDD"
    }
    val jobId = scheduler.nextJobId.getAndIncrement()
    runEvent(JobSubmitted(jobId, rdd, jobComputeFunc, Array(0), true, CallSite("", ""), jobListener))
    assert(results === Map(0 -> 42))
    assertDataStructuresEmpty
  }

  test("local job oom") {
    val rdd = new MyRDD(sc, Nil) {
      override def compute(split: Partition, context: TaskContext): Iterator[(Int, Int)] =
        throw new java.lang.OutOfMemoryError("test local job oom")
      override def getPartitions = Array( new Partition { override def index = 0 } )
      override def getPreferredLocations(split: Partition) = Nil
      override def toString = "DAGSchedulerSuite Local RDD"
    }
    val jobId = scheduler.nextJobId.getAndIncrement()
    runEvent(JobSubmitted(jobId, rdd, jobComputeFunc, Array(0), true, CallSite("", ""), jobListener))
    assert(results.size == 0)
    assertDataStructuresEmpty
  }

  test("run trivial job w/ dependency") {
    val baseRdd = makeRdd(1, Nil)
    val finalRdd = makeRdd(1, List(new OneToOneDependency(baseRdd)))
    submit(finalRdd, Array(0))
    complete(taskSets(0), Seq((Success, 42)))
    assert(results === Map(0 -> 42))
    assertDataStructuresEmpty
  }

  test("cache location preferences w/ dependency") {
    val baseRdd = makeRdd(1, Nil)
    val finalRdd = makeRdd(1, List(new OneToOneDependency(baseRdd)))
    cacheLocations(baseRdd.id -> 0) =
      Seq(makeBlockManagerId("hostA"), makeBlockManagerId("hostB"))
    submit(finalRdd, Array(0))
    val taskSet = taskSets(0)
    assertLocations(taskSet, Seq(Seq("hostA", "hostB")))
    complete(taskSet, Seq((Success, 42)))
    assert(results === Map(0 -> 42))
    assertDataStructuresEmpty
  }

  test("trivial job failure") {
    submit(makeRdd(1, Nil), Array(0))
    failed(taskSets(0), "some failure")
    assert(failure.getMessage === "Job aborted due to stage failure: some failure")
    assert(sc.listenerBus.waitUntilEmpty(WAIT_TIMEOUT_MILLIS))
    assert(sparkListener.failedStages.contains(0))
    assert(sparkListener.failedStages.size === 1)
    assertDataStructuresEmpty
  }

  test("trivial job cancellation") {
    val rdd = makeRdd(1, Nil)
    val jobId = submit(rdd, Array(0))
    cancel(jobId)
    assert(failure.getMessage === s"Job $jobId cancelled ")
    assert(sc.listenerBus.waitUntilEmpty(WAIT_TIMEOUT_MILLIS))
    assert(sparkListener.failedStages.contains(0))
    assert(sparkListener.failedStages.size === 1)
    assertDataStructuresEmpty
  }

  test("run trivial shuffle") {
    val shuffleMapRdd = makeRdd(2, Nil)
    val shuffleDep = new ShuffleDependency(shuffleMapRdd, null)
    val shuffleId = shuffleDep.shuffleId
    val reduceRdd = makeRdd(1, List(shuffleDep))
    submit(reduceRdd, Array(0))
    complete(taskSets(0), Seq(
        (Success, makeMapStatus("hostA", 1)),
        (Success, makeMapStatus("hostB", 1))))
    assert(mapOutputTracker.getServerStatuses(shuffleId, 0).map(_._1) ===
           Array(makeBlockManagerId("hostA"), makeBlockManagerId("hostB")))
    complete(taskSets(1), Seq((Success, 42)))
    assert(results === Map(0 -> 42))
    assertDataStructuresEmpty
  }

  test("run trivial shuffle with fetch failure") {
    val shuffleMapRdd = makeRdd(2, Nil)
    val shuffleDep = new ShuffleDependency(shuffleMapRdd, null)
    val shuffleId = shuffleDep.shuffleId
    val reduceRdd = makeRdd(2, List(shuffleDep))
    submit(reduceRdd, Array(0, 1))
    complete(taskSets(0), Seq(
        (Success, makeMapStatus("hostA", 1)),
        (Success, makeMapStatus("hostB", 1))))
    // the 2nd ResultTask failed
    complete(taskSets(1), Seq(
        (Success, 42),
        (FetchFailed(makeBlockManagerId("hostA"), shuffleId, 0, 0), null)))
    // this will get called
    // blockManagerMaster.removeExecutor("exec-hostA")
    // ask the scheduler to try it again
    scheduler.resubmitFailedStages()
    // have the 2nd attempt pass
    complete(taskSets(2), Seq((Success, makeMapStatus("hostA", 1))))
    // we can see both result blocks now
    assert(mapOutputTracker.getServerStatuses(shuffleId, 0).map(_._1.host) === Array("hostA", "hostB"))
    complete(taskSets(3), Seq((Success, 43)))
    assert(results === Map(0 -> 42, 1 -> 43))
    assertDataStructuresEmpty
  }

  test("ignore late map task completions") {
    val shuffleMapRdd = makeRdd(2, Nil)
    val shuffleDep = new ShuffleDependency(shuffleMapRdd, null)
    val shuffleId = shuffleDep.shuffleId
    val reduceRdd = makeRdd(2, List(shuffleDep))
    submit(reduceRdd, Array(0, 1))
    // pretend we were told hostA went away
    val oldEpoch = mapOutputTracker.getEpoch
    runEvent(ExecutorLost("exec-hostA"))
    val newEpoch = mapOutputTracker.getEpoch
    assert(newEpoch > oldEpoch)
    val noAccum = Map[Long, Any]()
    val taskSet = taskSets(0)
    // should be ignored for being too old
    runEvent(CompletionEvent(taskSet.tasks(0), Success, makeMapStatus("hostA", 1), noAccum, null, null))
    // should work because it's a non-failed host
    runEvent(CompletionEvent(taskSet.tasks(0), Success, makeMapStatus("hostB", 1), noAccum, null, null))
    // should be ignored for being too old
    runEvent(CompletionEvent(taskSet.tasks(0), Success, makeMapStatus("hostA", 1), noAccum, null, null))
    // should work because it's a new epoch
    taskSet.tasks(1).epoch = newEpoch
    runEvent(CompletionEvent(taskSet.tasks(1), Success, makeMapStatus("hostA", 1), noAccum, null, null))
    assert(mapOutputTracker.getServerStatuses(shuffleId, 0).map(_._1) ===
           Array(makeBlockManagerId("hostB"), makeBlockManagerId("hostA")))
    complete(taskSets(1), Seq((Success, 42), (Success, 43)))
    assert(results === Map(0 -> 42, 1 -> 43))
    assertDataStructuresEmpty
  }

  test("run shuffle with map stage failure") {
    val shuffleMapRdd = makeRdd(2, Nil)
    val shuffleDep = new ShuffleDependency(shuffleMapRdd, null)
    val reduceRdd = makeRdd(2, List(shuffleDep))
    submit(reduceRdd, Array(0, 1))

    // Fail the map stage.  This should cause the entire job to fail.
    val stageFailureMessage = "Exception failure in map stage"
    failed(taskSets(0), stageFailureMessage)
    assert(failure.getMessage === s"Job aborted due to stage failure: $stageFailureMessage")

    // Listener bus should get told about the map stage failing, but not the reduce stage
    // (since the reduce stage hasn't been started yet).
    assert(sc.listenerBus.waitUntilEmpty(WAIT_TIMEOUT_MILLIS))
    assert(sparkListener.failedStages.contains(1))
    assert(sparkListener.failedStages.size === 1)

    assertDataStructuresEmpty
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
    val shuffleMapRdd1 = makeRdd(2, Nil)
    val shuffleDep1 = new ShuffleDependency(shuffleMapRdd1, null)
    val shuffleMapRdd2 = makeRdd(2, Nil)
    val shuffleDep2 = new ShuffleDependency(shuffleMapRdd2, null)

    val reduceRdd1 = makeRdd(2, List(shuffleDep1))
    val reduceRdd2 = makeRdd(2, List(shuffleDep1, shuffleDep2))

    // We need to make our own listeners for this test, since by default submit uses the same
    // listener for all jobs, and here we want to capture the failure for each job separately.
    class FailureRecordingJobListener() extends JobListener {
      var failureMessage: String = _
      override def taskSucceeded(index: Int, result: Any) {}
      override def jobFailed(exception: Exception) = { failureMessage = exception.getMessage }
    }
    val listener1 = new FailureRecordingJobListener()
    val listener2 = new FailureRecordingJobListener()

    submit(reduceRdd1, Array(0, 1), listener=listener1)
    submit(reduceRdd2, Array(0, 1), listener=listener2)

    val stageFailureMessage = "Exception failure in map stage"
    failed(taskSets(0), stageFailureMessage)

    assert(cancelledStages.contains(1))

    // Make sure the listeners got told about both failed stages.
    assert(sc.listenerBus.waitUntilEmpty(WAIT_TIMEOUT_MILLIS))
    assert(sparkListener.successfulStages.isEmpty)
    assert(sparkListener.failedStages.contains(1))
    assert(sparkListener.failedStages.contains(3))
    assert(sparkListener.failedStages.size === 2)

    assert(listener1.failureMessage === s"Job aborted due to stage failure: $stageFailureMessage")
    assert(listener2.failureMessage === s"Job aborted due to stage failure: $stageFailureMessage")
    assertDataStructuresEmpty
  }

  test("run trivial shuffle with out-of-band failure and retry") {
    val shuffleMapRdd = makeRdd(2, Nil)
    val shuffleDep = new ShuffleDependency(shuffleMapRdd, null)
    val shuffleId = shuffleDep.shuffleId
    val reduceRdd = makeRdd(1, List(shuffleDep))
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
    assert(mapOutputTracker.getServerStatuses(shuffleId, 0).map(_._1) ===
           Array(makeBlockManagerId("hostC"), makeBlockManagerId("hostB")))
    complete(taskSets(2), Seq((Success, 42)))
    assert(results === Map(0 -> 42))
    assertDataStructuresEmpty
  }

  test("recursive shuffle failures") {
    val shuffleOneRdd = makeRdd(2, Nil)
    val shuffleDepOne = new ShuffleDependency(shuffleOneRdd, null)
    val shuffleTwoRdd = makeRdd(2, List(shuffleDepOne))
    val shuffleDepTwo = new ShuffleDependency(shuffleTwoRdd, null)
    val finalRdd = makeRdd(1, List(shuffleDepTwo))
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
        (FetchFailed(makeBlockManagerId("hostA"), shuffleDepTwo.shuffleId, 0, 0), null)))
    // TODO assert this:
    // blockManagerMaster.removeExecutor("exec-hostA")
    // have DAGScheduler try again
    scheduler.resubmitFailedStages()
    complete(taskSets(3), Seq((Success, makeMapStatus("hostA", 2))))
    complete(taskSets(4), Seq((Success, makeMapStatus("hostA", 1))))
    complete(taskSets(5), Seq((Success, 42)))
    assert(results === Map(0 -> 42))
    assertDataStructuresEmpty
  }

  test("cached post-shuffle") {
    val shuffleOneRdd = makeRdd(2, Nil)
    val shuffleDepOne = new ShuffleDependency(shuffleOneRdd, null)
    val shuffleTwoRdd = makeRdd(2, List(shuffleDepOne))
    val shuffleDepTwo = new ShuffleDependency(shuffleTwoRdd, null)
    val finalRdd = makeRdd(1, List(shuffleDepTwo))
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
        (FetchFailed(makeBlockManagerId("hostA"), shuffleDepTwo.shuffleId, 0, 0), null)))
    // TODO assert this:
    // blockManagerMaster.removeExecutor("exec-hostA")
    // DAGScheduler should notice the cached copy of the second shuffle and try to get it rerun.
    scheduler.resubmitFailedStages()
    assertLocations(taskSets(3), Seq(Seq("hostD")))
    // allow hostD to recover
    complete(taskSets(3), Seq((Success, makeMapStatus("hostD", 1))))
    complete(taskSets(4), Seq((Success, 42)))
    assert(results === Map(0 -> 42))
    assertDataStructuresEmpty
  }

  test("DAGSchedulerActorSupervisor closes the SparkContext when EventProcessActor crashes") {
    val actorSystem = ActorSystem("test")
    val supervisor = actorSystem.actorOf(
      Props(classOf[DAGSchedulerActorSupervisor], scheduler), "dagSupervisor")
    supervisor ! Props[BuggyDAGEventProcessActor]
    val child = expectMsgType[ActorRef]
    watch(child)
    child ! "hi"
    expectMsgPF(){ case Terminated(child) => () }
    assert(scheduler.sc.dagScheduler === null)
  }

  /**
   * Assert that the supplied TaskSet has exactly the given hosts as its preferred locations.
   * Note that this checks only the host and not the executor ID.
   */
  private def assertLocations(taskSet: TaskSet, hosts: Seq[Seq[String]]) {
    assert(hosts.size === taskSet.tasks.size)
    for ((taskLocs, expectedLocs) <- taskSet.tasks.map(_.preferredLocations).zip(hosts)) {
      assert(taskLocs.map(_.host) === expectedLocs)
    }
  }

  private def makeMapStatus(host: String, reduces: Int): MapStatus =
   new MapStatus(makeBlockManagerId(host), Array.fill[Byte](reduces)(2))

  private def makeBlockManagerId(host: String): BlockManagerId =
    BlockManagerId("exec-" + host, host, 12345, 0)

  private def assertDataStructuresEmpty = {
    assert(scheduler.pendingTasks.isEmpty)
    assert(scheduler.activeJobs.isEmpty)
    assert(scheduler.failedStages.isEmpty)
    assert(scheduler.jobIdToActiveJob.isEmpty)
    assert(scheduler.jobIdToStageIds.isEmpty)
    assert(scheduler.stageIdToJobIds.isEmpty)
    assert(scheduler.stageIdToStage.isEmpty)
    assert(scheduler.stageToInfos.isEmpty)
    assert(scheduler.resultStageToJob.isEmpty)
    assert(scheduler.runningStages.isEmpty)
    assert(scheduler.shuffleToMapStage.isEmpty)
    assert(scheduler.waitingStages.isEmpty)
  }
}

