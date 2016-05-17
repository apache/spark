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
import java.util.concurrent.atomic.AtomicBoolean

import scala.collection.mutable.{ArrayBuffer, HashMap, HashSet}
import scala.concurrent.{Await, Future}
import scala.concurrent.duration.{Duration, SECONDS}
import scala.reflect.ClassTag

import org.scalactic.TripleEquals
import org.scalatest.Assertions.AssertionsHelper
import org.scalatest.BeforeAndAfter

import org.apache.spark._
import org.apache.spark.TaskState._
import org.apache.spark.executor.TaskMetrics
import org.apache.spark.internal.Logging
import org.apache.spark.rdd.RDD
import org.apache.spark.util.CallSite

/**
 * Tests for the  entire scheduler code -- DAGScheduler, TaskSchedulerImpl, TaskSets,
 * TaskSetManagers.
 *
 * Test cases are configured by providing a set of jobs to submit, and then simulating interaction
 * with spark's executors via a mocked backend (eg., task completion, task failure, executors
 * disconnecting, etc.).
 */
abstract class SchedulerIntegrationSuite[T <: MockBackend: ClassTag] extends SparkFunSuite
    with BeforeAndAfter with LocalSparkContext {
  val conf = new SparkConf

  /** Set of TaskSets the DAGScheduler has requested executed. */
  val runningTaskSets = HashSet[TaskSet]()

  var taskScheduler: TaskSchedulerImpl = null
  var scheduler: DAGScheduler = null
  var backend: T = _

  before {
    runningTaskSets.clear()
    results.clear()
    failure = null
    val backendClassName = implicitly[ClassTag[T]].runtimeClass.getName()
    sc = new SparkContext(s"mock[${backendClassName}]", this.getClass().getSimpleName())
    backend = sc.schedulerBackend.asInstanceOf[T]
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

  val results = new HashMap[Int, Any]()
  var failure: Throwable = _

  /**
   * When we submit dummy Jobs, this is the compute function we supply.
   */
  private val jobComputeFunc: (TaskContext, scala.Iterator[_]) => Any = {
    (context: TaskContext, it: Iterator[(_)]) =>
      throw new RuntimeException("jobComputeFunc shouldn't get called in this mock")
  }

  /** Submits a job to the scheduler, and returns a future which does a bit of error handling. */
  protected def submit(
      rdd: RDD[_],
      partitions: Array[Int],
      func: (TaskContext, Iterator[_]) => _ = jobComputeFunc): Future[Any] = {
    val waiter: JobWaiter[Any] = scheduler.submitJob(rdd, func, partitions.toSeq, CallSite("", ""),
      (index, res) => results(index) = res, new Properties())
    import scala.concurrent.ExecutionContext.Implicits.global
    waiter.completionFuture.recover { case ex =>
      failure = ex
    }
  }

  protected def assertDataStructuresEmpty(noFailure: Boolean = true): Unit = {
    if (noFailure) {
      assert(failure === null)
    }
    assert(scheduler.activeJobs.isEmpty)
    assert(runningTaskSets.isEmpty)
    assert(!backend.hasTasks)
  }

  /**
   * Looks at all shuffleMapOutputs that are dependencies of the given RDD, and makes sure
   * they are all registered
   */
  def assertMapOutputAvailable(targetRdd: MockRDD): Unit = {
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
    val shuffleDep = new ShuffleDependency[Int, Int, Nothing](input, partitioner)
    new MockRDD(sc, nParts, List(shuffleDep))
  }

  /** models a stage boundary with multiple dependencies, like a join */
  def join(nParts: Int, inputs: MockRDD*): MockRDD = {
    val partitioner = new HashPartitioner(nParts)
    val shuffleDeps = inputs.map { inputRDD =>
      new ShuffleDependency[Int, Int, Nothing](inputRDD, partitioner)
    }
    new MockRDD(sc, nParts, shuffleDeps)
  }

  /**
   * Helper which makes it a little easier to setup a test, which starts a mock backend in another
   * thread, responding to tasks with your custom function.  You also supply the "body" of your
   * test, where you submit jobs to your backend, wait for them to complete, then check
   * whatever conditions you want.  Note that this is *not* safe to all bad backends --
   * in particular, your `backendFunc` has to return quickly, it can't throw errors, (instead
   * it should send back the right TaskEndReason
   */
  def withBackend(backendFunc: () => Unit)(testBody: => Unit): Unit = {
    val backendContinue = new AtomicBoolean(true)
    val backendThread = new Thread("mock backend thread") {
      override def run(): Unit = {
        while (backendContinue.get()) {
          if (backend.hasTasksWaitingToRun) {
            backendFunc()
          } else {
            Thread.sleep(10)
          }
        }
      }
    }
    try {
      backendThread.start()
      testBody
    } finally {
      backendContinue.set(false)
      backendThread.join()
    }
  }

}

/**
 * Helper for running a backend in integration tests, does a bunch of the book-keeping
 * so individual tests can focus on just responding to tasks.  Individual tests will use
 * [[beginTask]], [[taskSuccess]], and [[taskFailed]].
 */
private[spark] abstract class MockBackend(
    conf: SparkConf,
    var taskScheduler: TaskSchedulerImpl) extends SchedulerBackend with Logging {

  /**
   * Test backends should call this to get a task that has been assigned to them by the scheduler.
   * Each task should be responded to with either [[taskSuccess]] or [[taskFailed]].
   */
  def beginTask(): TaskDescription = synchronized {
    val toRun = assignedTasksWaitingToRun.remove(assignedTasksWaitingToRun.size - 1)
    runningTasks += toRun
    toRun
  }

  /**
   * Tell the scheduler the task completed successfully, with the given result.  Also
   * updates some internal state for this mock.
   */
  def taskSuccess(task: TaskDescription, result: Any): Unit = {
    endTask(task)
    val ser = env.serializer.newInstance()
    val resultBytes = ser.serialize(result)
    val metrics = new TaskMetrics
    val directResult = new DirectTaskResult(resultBytes, Seq()) // no accumulator updates
    val serializedDirectResult = ser.serialize(directResult)
    taskScheduler.statusUpdate(task.taskId, TaskState.FINISHED, serializedDirectResult)
    synchronized {
      executorIdToExecutor(task.executorId).freeCores += taskScheduler.CPUS_PER_TASK
      freeCores += taskScheduler.CPUS_PER_TASK
      assignedTasksWaitingToRun -= task
    }
    reviveOffers()
  }

  /**
   * Tell the scheduler the task failed, with the given state and result (probably ExceptionFailure
   * or FetchFailed).  Also updates some internal state for this mock.
   */
  def taskFailed(task: TaskDescription, state: TaskState, result: Any): Unit = {
    endTask(task)
    val ser = env.serializer.newInstance()
    val resultBytes = ser.serialize(result)
    taskScheduler.statusUpdate(task.taskId, state, resultBytes)
    if (TaskState.isFinished(state)) {
      synchronized {
        executorIdToExecutor(task.executorId).freeCores += taskScheduler.CPUS_PER_TASK
        freeCores += taskScheduler.CPUS_PER_TASK
        assignedTasksWaitingToRun -= task
      }
      reviveOffers()
    }
  }

  private val assignedTasksWaitingToRun = ArrayBuffer[TaskDescription]()
  private val runningTasks = ArrayBuffer[TaskDescription]()

  def endTask(task: TaskDescription): Unit = synchronized {
    runningTasks -= task
  }

  def hasTasks: Boolean = synchronized {
    assignedTasksWaitingToRun.nonEmpty || runningTasks.nonEmpty
  }

  def hasTasksWaitingToRun: Boolean = synchronized {
    assignedTasksWaitingToRun.nonEmpty
  }

  override def start(): Unit = {}

  override def stop(): Unit = {}

  var freeCores: Int = _
  val env = SparkEnv.get

  def executorIdToExecutor: Map[String, ExecutorTaskStatus]

  def generateOffers(): Seq[WorkerOffer]

  /**
   * This is called by the scheduler whenever it has tasks it would like to schedule
   */
  override def reviveOffers(): Unit = {
    val offers: Seq[WorkerOffer] = generateOffers()
    val newTasks = taskScheduler.resourceOffers(offers).flatten
    synchronized {
      newTasks.foreach { task =>
        executorIdToExecutor(task.executorId).freeCores -= taskScheduler.CPUS_PER_TASK
      }
      freeCores -= newTasks.size * taskScheduler.CPUS_PER_TASK
      assignedTasksWaitingToRun ++= newTasks
    }
  }
}

/**
 * A very simple mock backend that can just run one task at a time.
 */
private[spark] class SingleCoreMockBackend(
  conf: SparkConf,
  taskScheduler: TaskSchedulerImpl) extends MockBackend(conf, taskScheduler) {

  val cores = 1

  override def defaultParallelism(): Int = conf.getInt("spark.default.parallelism", cores)

  freeCores = cores
  val localExecutorId = SparkContext.DRIVER_IDENTIFIER
  val localExecutorHostname = "localhost"

  val executorIdToExecutor: Map[String, ExecutorTaskStatus] = Map(
    localExecutorId -> new ExecutorTaskStatus(localExecutorHostname, localExecutorId, freeCores)
  )

  override def generateOffers(): Seq[WorkerOffer] = {
    Seq(new WorkerOffer(localExecutorId, localExecutorHostname, freeCores))
  }
}

class ExecutorTaskStatus(val host: String, val executorId: String, var freeCores: Int)

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

/**
 * Some very basic tests just to demonstrate the use of the test framework (and verify that it
 * works).
 */
class BasicSchedulerIntegrationSuite extends SchedulerIntegrationSuite[SingleCoreMockBackend] {

  /**
   * Very simple one stage job.  Backend successfully completes each task, one by one
   */
  test("super simple job") {
    def runBackend(): Unit = {
      val task = backend.beginTask()
      backend.taskSuccess(task, 42)
    }
    withBackend(runBackend _) {
      val jobFuture = submit(new MockRDD(sc, 10, Nil), (0 until 10).toArray)
      val duration = Duration(1, SECONDS)
      Await.ready(jobFuture, duration)
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

    def stageToOutputParts(stageId: Int): Int = {
      stageId match {
        case 0 => 10
        case 2 => 20
        case _ => 30
      }
    }

    val a = new MockRDD(sc, 2, Nil)
    val b = shuffle(10, a)
    val c = shuffle(20, a)
    val d = join(30, b, c)

    def runBackend(): Unit = {
      val taskDescription = backend.beginTask()
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
          backend.taskSuccess(taskDescription,
            DAGSchedulerSuite.makeMapStatus("hostA", stageToOutputParts(stage)))
        case (4, 0, partition) =>
          backend.taskSuccess(taskDescription, 4321 + partition)
      }
    }
    withBackend(runBackend _) {
      val jobFuture = submit(d, (0 until 30).toArray)
      val duration = Duration(1, SECONDS)
      Await.ready(jobFuture, duration)
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

    val stageToAttempts = new HashMap[Int, HashSet[Int]]()

    def runBackend(): Unit = {
      val taskDescription = backend.beginTask()
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
          backend.taskSuccess(taskDescription, DAGSchedulerSuite.makeMapStatus("hostA", 10))
        case (1, 0, 0) =>
          val fetchFailed = FetchFailed(
            DAGSchedulerSuite.makeBlockManagerId("hostA"), shuffleId, 0, 0, "ignored")
          backend.taskFailed(taskDescription, TaskState.FAILED, fetchFailed)
        case (1, _, partition) =>
          backend.taskSuccess(taskDescription, 42 + partition)
      }
    }
    withBackend(runBackend _) {
      val jobFuture = submit(shuffledRdd, (0 until 10).toArray)
      val duration = Duration(1, SECONDS)
      Await.ready(jobFuture, duration)
    }
    assert(results === (0 until 10).map { idx => idx -> (42 + idx) }.toMap)
    assert(stageToAttempts === Map(0 -> Set(0, 1), 1 -> Set(0, 1)))
    assertDataStructuresEmpty()
  }

  test("job failure after 4 attempts") {
    def runBackend(): Unit = {
      val task = backend.beginTask()
      val failure = new ExceptionFailure(new RuntimeException("test task failure"), Seq())
      backend.taskFailed(task, TaskState.FAILED, failure)
    }
    withBackend(runBackend _) {
      val jobFuture = submit(new MockRDD(sc, 10, Nil), (0 until 10).toArray)
      val duration = Duration(1, SECONDS)
      Await.ready(jobFuture, duration)
      failure.getMessage.contains("test task failure")
    }
    assert(results.isEmpty)
    assertDataStructuresEmpty(noFailure = false)
  }
}
