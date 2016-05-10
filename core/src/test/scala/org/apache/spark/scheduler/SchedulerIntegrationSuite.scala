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

import java.util.concurrent.TimeoutException

import scala.collection.mutable.{ArrayBuffer, HashMap, HashSet}

import org.scalactic.TripleEquals
import org.scalatest.Assertions.AssertionsHelper
import org.scalatest.BeforeAndAfter

import org.apache.spark._
import org.apache.spark.TaskState._
import org.apache.spark.executor.TaskMetrics
import org.apache.spark.internal.Logging
import org.apache.spark.rdd.RDD
import org.apache.spark.scheduler.DAGSchedulerSuite._
import org.apache.spark.util.CallSite

/**
 * Tests for the  entire scheduler code -- DAGScheduler, TaskSchedulerImpl, TaskSets,
 * TaskSetManagers.
 *
 * Test cases are configured by providing a set of jobs to submit, and then simulating interaction
 * with spark's executors via a mocked backend (eg., task completion, task failure, executors
 * disconnecting, etc.).
 */
class SchedulerIntegrationSuite extends SparkFunSuite with BeforeAndAfter with LocalSparkContext {
  val conf = new SparkConf

  /** Set of TaskSets the DAGScheduler has requested executed. */
  val runningTaskSets = HashSet[TaskSet]()

  var taskScheduler: TaskSchedulerImpl = null
  var scheduler: DAGScheduler = null
  var backend: SingleCoreMockBackend = null

  before {
    runningTaskSets.clear()
    results.clear()
    sc = new SparkContext("mock[org.apache.spark.scheduler.SingleCoreMockBackend]",
      "SchedulerIntegrationSuite")
    backend = sc.schedulerBackend.asInstanceOf[SingleCoreMockBackend]
    taskScheduler = new TaskSchedulerImpl(sc) {
      override def submitTasks(taskSet: TaskSet): Unit = {
        runningTaskSets += taskSet
        super.submitTasks(taskSet)
      }
      override def taskSetFinished(manager: TaskSetManager): Unit = {
        runningTaskSets -= manager.taskSet
        super.taskSetFinished(manager)
      }
    }
    taskScheduler.initialize(sc.schedulerBackend)
    backend.taskScheduler = taskScheduler
    scheduler = new DAGScheduler(sc, taskScheduler)
    taskScheduler.setDAGScheduler(scheduler)
  }

  after {
    taskScheduler.stop()
    backend.stop()
    scheduler.stop()
  }

  /**
   * Process the supplied event as if it were the top of the DAGScheduler event queue, expecting
   * the scheduler not to exit.
   */
  private def runEvent(event: DAGSchedulerEvent) {
    scheduler.eventProcessLoop.post(event)
  }

  val results = new HashMap[Int, Any]()
  var failure: Exception = _
  val jobListener = new JobListener() {
    override def taskSucceeded(index: Int, result: Any) = results.put(index, result)
    override def jobFailed(exception: Exception) = { failure = exception }
  }

  /**
   * When we submit dummy Jobs, this is the compute function we supply.
   */
  private val jobComputeFunc: (TaskContext, scala.Iterator[_]) => Any = {
    (context: TaskContext, it: Iterator[(_)]) =>
      throw new RuntimeException("jobComputeFunc shouldn't get called in this mock")
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

  /**
   * Return true if the backend has more work to do, false otherwise.  It will block until it has
   * a definitive answer either way -- eg., if the backend does not appear to have any work, but
   * the dag scheduler has some events left to process, this will wait until the dag scheduler is
   * done processing enough events to say for sure.
   */
  private def backendHasWorkToDo: Boolean = {
    // the ordering is somewhat important here -- avoid waiting if we can (both to speed up test,
    // and also to test with more concurrency inside scheduler)
    if (backend.runningTasks.nonEmpty) {
      true
    } else if (runningTaskSets.isEmpty && scheduler.msgSchedulerEmpty &&
      scheduler.eventProcessLoop.isEmpty && taskScheduler.taskResultGetter.isEmpty ) {
      false
    } else if (runningTaskSets.nonEmpty) {
      // need to get all task results, as they might lead to finishing a taskSet
      waitUntil(() => taskScheduler.taskResultGetter.isEmpty)
      backendHasWorkToDo
    } else {
      waitUntil(() => taskScheduler.taskResultGetter.isEmpty)
      waitUntil(() => scheduler.eventProcessLoop.isEmpty)
      backendHasWorkToDo
    }
  }

  private def waitUntil(condition: () => Boolean): Unit = {
    val timeoutMillis = 1000L
    val finishTime = System.currentTimeMillis + timeoutMillis
    while (!condition()) {
      if (System.currentTimeMillis > finishTime) {
        throw new TimeoutException(
          s"Not ready after $timeoutMillis milliseconds")
      }
      /* Sleep rather than using wait/notify, because this is used only for testing and
       * wait/notify add overhead in the general case. */
      Thread.sleep(10)
    }
  }

  private def assertDataStructuresEmpty(): Unit = {
    assert(!backendHasWorkToDo)
    assert(runningTaskSets.isEmpty)
    assert(backend.runningTasks.isEmpty)
  }

  /**
   * Looks at all shuffleMapOutputs that are dependencies of the given RDD, and makes sure
   * they are all registered
   */
  private def assertMapOutputAvailable(targetRdd: MockRDD): Unit = {
    val shuffleIds = targetRdd.shuffleDeps.map{_.shuffleId}
    val nParts = targetRdd.numPartitions
    for {
      shuffleId <- shuffleIds
      reduceIdx <- (0 until nParts)
    } {
      val statuses = taskScheduler.mapOutputTracker.getMapSizesByExecutorId(shuffleId, reduceIdx)
      // really we should have already thrown an exception rather than fail either of these
      // asserts, but just to be extra defensive let's double check the statuses are OK
      assert(statuses != null)
      assert(statuses.nonEmpty)
    }
  }


  /** models a stage boundary with a single dependency, like a shuffle */
  def shuffle(nParts: Int, input: MockRDD): MockRDD = {
    val partitioner = new HashPartitioner(nParts)
    val shuffleDep = new ShuffleDependency(input, partitioner)
    new MockRDD(sc, nParts, List(shuffleDep))
  }

  /** models a stage boundary with multiple dependencies, like a join */
  def join(nParts: Int, inputs: MockRDD*): MockRDD = {
    val partitioner = new HashPartitioner(nParts)
    val shuffleDeps = inputs.map { inputRDD =>
      new ShuffleDependency(inputRDD, partitioner)
    }
    new MockRDD(sc, nParts, shuffleDeps)
  }

  /**
   * Very simple one stage job.  Backend successfully completes each task, one by one
   */
  test("super simple job") {
    submit(new MockRDD(sc, 10, Nil), (0 until 10).toArray)
    while (backendHasWorkToDo) {
      val task = backend.runningTasks.last
      backend.taskSuccess(task, 42)
    }
    assert(results === (0 until 10).map { _ -> 42 }.toMap)
    assertDataStructuresEmpty()
  }

  /**
   * 5 stage job, diamond dependencies.
   *
   * a ----> b ----> d --> result
   *    \--> c --/
   *
   * Backend successfully completes each task
   */
  test("multi-stage job") {
    val a = new MockRDD(sc, 2, Nil)
    val b = shuffle(10, a)
    val c = shuffle(20, a)
    val d = join(30, b, c)
    submit(d, (0 until 30).toArray)

    def stageToOutputParts(stageId: Int): Int = {
      stageId match {
        case 0 => 10
        case 2 => 20
        case _ => 30
      }
    }

    while (backendHasWorkToDo) {
      assert(backend.runningTasks.nonEmpty)
      val taskDescription = backend.runningTasks.last
      val taskSet = taskScheduler.taskIdToTaskSetManager(taskDescription.taskId).taskSet
      val task = taskSet.tasks(taskDescription.index)

      // make sure the required map output is available
      task.stageId match {
        case 1 => assertMapOutputAvailable(b)
        case 3 => assertMapOutputAvailable(c)
        case 4 => assertMapOutputAvailable(d)
        case _ => // no shuffle map input, nothing to check
      }

      (task.stageId, task.stageAttemptId, task.partitionId) match {
        case (stage, 0, _) if stage < 4 =>
          backend.taskSuccess(taskDescription, makeMapStatus("hostA", stageToOutputParts(stage)))
        case (4, 0, partition) =>
          backend.taskSuccess(taskDescription, 4321 + partition)
      }
    }
    assert(results === (0 until 30).map { idx => idx -> (4321 + idx) }.toMap)
    assertDataStructuresEmpty()
  }

  /**
   * 2 stage job, with a fetch failure.  Make sure that:
   * (a) map output is available whenever we run stage 1
   * (b) we get a second attempt for stage 0 & stage 1
   */
  test("job with fetch failure") {
    val input = new MockRDD(sc, 2, Nil)
    val shuffledRdd = shuffle(10, input)
    val shuffleId = shuffledRdd.shuffleDeps.head.shuffleId
    submit(shuffledRdd, (0 until 10).toArray)

    val stageToAttempts = new HashMap[Int, HashSet[Int]]()

    while (backendHasWorkToDo) {
      val taskDescription = backend.runningTasks.last
      val taskSet = taskScheduler.taskIdToTaskSetManager(taskDescription.taskId).taskSet
      val task = taskSet.tasks(taskDescription.index)
      stageToAttempts.getOrElseUpdate(task.stageId, new HashSet()) += task.stageAttemptId

      // make sure the required map output is available
      task.stageId match {
        case 1 => assertMapOutputAvailable(shuffledRdd)
        case _ => // no shuffle map input, nothing to check
      }

      (task.stageId, task.stageAttemptId, task.partitionId) match {
        case (0, _, _) =>
          backend.taskSuccess(taskDescription, makeMapStatus("hostA", 10))
        case (1, 0, 0) =>
          val fetchFailed = FetchFailed(makeBlockManagerId("hostA"), shuffleId, 0, 0, "ignored")
          backend.failTask(taskDescription, TaskState.FAILED, fetchFailed)
        case (1, _, partition) =>
          backend.taskSuccess(taskDescription, 42 + partition)
      }
    }
    assert(results === (0 until 10).map { idx => idx -> (42 + idx) }.toMap)
    assert(stageToAttempts === Map(0 -> Set(0, 1), 1 -> Set(0, 1)))
    assertDataStructuresEmpty()
  }
}

/**
 * A very simple mock backend that can just run one task at a time.
 */
private[spark] class SingleCoreMockBackend(
  conf: SparkConf,
  var taskScheduler: TaskSchedulerImpl) extends SchedulerBackend with Logging {

  val cores = 1

  override def start(): Unit = {}

  override def stop(): Unit = {}

  override def defaultParallelism(): Int = conf.getInt("spark.default.parallelism", cores)

  var freeCores = cores
  val localExecutorId = SparkContext.DRIVER_IDENTIFIER
  val localExecutorHostname = "localhost"
  val env = SparkEnv.get

  val runningTasks = ArrayBuffer[TaskDescription]()

  /**
   * This is called by the scheduler whenever it has tasks it would like to schedule
   */
  override def reviveOffers(): Unit = {
    val offers = Seq(new WorkerOffer(localExecutorId, localExecutorHostname, freeCores))
    val newTasks = taskScheduler.resourceOffers(offers).flatten
    synchronized {
      freeCores -= newTasks.size * taskScheduler.CPUS_PER_TASK
      runningTasks ++= newTasks
    }
  }

  def taskSuccess(task: TaskDescription, result: Any): Unit = {
    val ser = env.serializer.newInstance()
    val resultBytes = ser.serialize(result)
    val metrics = new TaskMetrics
    val directResult = new DirectTaskResult(resultBytes, Seq()) // no accumulator updates
    val serializedDirectResult = ser.serialize(directResult)
    taskScheduler.statusUpdate(task.taskId, TaskState.FINISHED, serializedDirectResult)
    synchronized {
      freeCores += taskScheduler.CPUS_PER_TASK
      runningTasks -= task
    }
    reviveOffers()
  }

  def failTask(task: TaskDescription, state: TaskState, result: Any): Unit = {
    val ser = env.serializer.newInstance()
    val resultBytes = ser.serialize(result)
    taskScheduler.statusUpdate(task.taskId, state, resultBytes)
    if (TaskState.isFinished(state)) {
      synchronized {
        freeCores += taskScheduler.CPUS_PER_TASK
        runningTasks -= task
      }
      reviveOffers()
    }
  }

}

class MockRDD(
  sc: SparkContext,
  val numPartitions: Int,
  val shuffleDeps: Seq[ShuffleDependency[Int, Int, Nothing]]
) extends RDD[(Int, Int)](sc, shuffleDeps) with Serializable {

  MockRDD.validate(numPartitions, shuffleDeps)

  override def compute(split: Partition, context: TaskContext): Iterator[(Int, Int)] =
    throw new RuntimeException("should not be reached")
  override def getPartitions: Array[Partition] = (0 until numPartitions).map(i => new Partition {
    override def index: Int = i
  }).toArray
  override def getPreferredLocations(split: Partition): Seq[String] = Nil
  override def toString: String = "MockRDD " + id
}

object MockRDD extends AssertionsHelper with TripleEquals {
  /**
   * make sure all the shuffle dependencies have a consistent number of output partitions
   * (mostly to make sure the test setup makes sense, not that Spark itself would get this wrong)
   */
  def validate(numPartitions: Int, dependencies: Seq[ShuffleDependency[_, _, _]]): Unit = {
    dependencies.foreach { dependency =>
      val partitioner = dependency.partitioner
      assert(partitioner != null)
      assert(partitioner.numPartitions === numPartitions)
    }
  }
}
