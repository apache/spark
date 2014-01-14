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
import scala.collection.mutable.{HashMap, Map}

import org.apache.spark._
import org.apache.spark.rdd.RDD
import org.apache.spark.scheduler.SchedulingMode.SchedulingMode
import org.apache.spark.storage.{BlockId, BlockManagerId, BlockManagerMaster}
import org.scalatest.{BeforeAndAfter, FunSuite}

/**
 * Tests for DAGScheduler. These tests directly call the event processing functions in DAGScheduler
 * rather than spawning an event loop thread as happens in the real code. They use EasyMock
 * to mock out two classes that DAGScheduler interacts with: TaskScheduler (to which TaskSets are
 * submitted) and BlockManagerMaster (from which cache locations are retrieved and to which dead
 * host notifications are sent). In addition, tests may check for side effects on a non-mocked
 * MapOutputTracker instance.
 *
 * Tests primarily consist of running DAGScheduler#processEvent and
 * DAGScheduler#submitWaitingStages (via test utility functions like runEvent or respondToTaskSet)
 * and capturing the resulting TaskSets from the mock TaskScheduler.
 */
class DAGSchedulerSuite extends FunSuite with BeforeAndAfter with LocalSparkContext {
  val conf = new SparkConf
  /** Set of TaskSets the DAGScheduler has requested executed. */
  val taskSets = scala.collection.mutable.Buffer[TaskSet]()
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
    override def cancelTasks(stageId: Int) {}
    override def setDAGScheduler(dagScheduler: DAGScheduler) = {}
    override def defaultParallelism() = 2
  }

  var mapOutputTracker: MapOutputTrackerMaster = null
  var scheduler: DAGScheduler = null

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
  val listener = new JobListener() {
    override def taskSucceeded(index: Int, result: Any) = results.put(index, result)
    override def jobFailed(exception: Exception) = { failure = exception }
  }

  before {
    sc = new SparkContext("local", "DAGSchedulerSuite")
    taskSets.clear()
    cacheLocations.clear()
    results.clear()
    mapOutputTracker = new MapOutputTrackerMaster(conf)
    scheduler = new DAGScheduler(taskScheduler, mapOutputTracker, blockManagerMaster, sc.env) {
      override def runLocally(job: ActiveJob) {
        // don't bother with the thread while unit testing
        runLocallyWithinThread(job)
      }
    }
  }

  after {
    scheduler.stop()
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
    assert(!scheduler.processEvent(event))
    scheduler.submitWaitingStages()
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

  /** Sends the rdd to the scheduler for scheduling. */
  private def submit(
      rdd: RDD[_],
      partitions: Array[Int],
      func: (TaskContext, Iterator[_]) => _ = jobComputeFunc,
      allowLocal: Boolean = false,
      listener: JobListener = listener) {
    val jobId = scheduler.nextJobId.getAndIncrement()
    runEvent(JobSubmitted(jobId, rdd, func, partitions, allowLocal, null, listener))
  }

  /** Sends TaskSetFailed to the scheduler. */
  private def failed(taskSet: TaskSet, message: String) {
    runEvent(TaskSetFailed(taskSet, message))
  }

  test("zero split job") {
    val rdd = makeRdd(0, Nil)
    var numResults = 0
    val fakeListener = new JobListener() {
      override def taskSucceeded(partition: Int, value: Any) = numResults += 1
      override def jobFailed(exception: Exception) = throw exception
    }
    submit(rdd, Array(), listener = fakeListener)
    assert(numResults === 0)
  }

  test("run trivial job") {
    val rdd = makeRdd(1, Nil)
    submit(rdd, Array(0))
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
    runEvent(JobSubmitted(jobId, rdd, jobComputeFunc, Array(0), true, null, listener))
    assert(results === Map(0 -> 42))
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
    assert(failure.getMessage === "Job aborted: some failure")
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
    assert(scheduler.failed.isEmpty)
    assert(scheduler.idToActiveJob.isEmpty)
    assert(scheduler.jobIdToStageIds.isEmpty)
    assert(scheduler.stageIdToJobIds.isEmpty)
    assert(scheduler.stageIdToStage.isEmpty)
    assert(scheduler.stageToInfos.isEmpty)
    assert(scheduler.resultStageToJob.isEmpty)
    assert(scheduler.running.isEmpty)
    assert(scheduler.shuffleToMapStage.isEmpty)
    assert(scheduler.waiting.isEmpty)
  }
}
