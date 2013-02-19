package spark.scheduler

import scala.collection.mutable.{Map, HashMap}

import org.scalatest.FunSuite
import org.scalatest.BeforeAndAfter
import org.scalatest.concurrent.TimeLimitedTests
import org.scalatest.mock.EasyMockSugar
import org.scalatest.time.{Span, Seconds}

import org.easymock.EasyMock._
import org.easymock.Capture
import org.easymock.EasyMock
import org.easymock.{IAnswer, IArgumentMatcher}

import akka.actor.ActorSystem

import spark.storage.BlockManager
import spark.storage.BlockManagerId
import spark.storage.BlockManagerMaster
import spark.{Dependency, ShuffleDependency, OneToOneDependency}
import spark.FetchFailedException
import spark.MapOutputTracker
import spark.RDD
import spark.SparkContext
import spark.SparkException
import spark.Partition
import spark.TaskContext
import spark.TaskEndReason

import spark.{FetchFailed, Success}

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
class DAGSchedulerSuite extends FunSuite with BeforeAndAfter with EasyMockSugar with TimeLimitedTests {

  // impose a time limit on this test in case we don't let the job finish, in which case
  // JobWaiter#getResult will hang.
  override val timeLimit = Span(5, Seconds)

  val sc: SparkContext = new SparkContext("local", "DAGSchedulerSuite")
  var scheduler: DAGScheduler = null
  val taskScheduler = mock[TaskScheduler]
  val blockManagerMaster = mock[BlockManagerMaster]
  var mapOutputTracker: MapOutputTracker = null
  var schedulerThread: Thread = null
  var schedulerException: Throwable = null

  /**
   * Set of EasyMock argument matchers that match a TaskSet for a given RDD.
   * We cache these so we do not create duplicate matchers for the same RDD.
   * This allows us to easily setup a sequence of expectations for task sets for
   * that RDD.
   */
  val taskSetMatchers = new HashMap[MyRDD, IArgumentMatcher]

  /**
   * Set of cache locations to return from our mock BlockManagerMaster.
   * Keys are (rdd ID, partition ID). Anything not present will return an empty
   * list of cache locations silently.
   */
  val cacheLocations = new HashMap[(Int, Int), Seq[BlockManagerId]]

  /**
   * JobWaiter for the last JobSubmitted event we pushed. To keep tests (most of which
   * will only submit one job) from needing to explicitly track it.
   */
  var lastJobWaiter: JobWaiter[Int] = null

  /**
   * Array into which we are accumulating the results from the last job asynchronously.
   */
  var lastJobResult: Array[Int] = null

  /**
   * Tell EasyMockSugar what mock objects we want to be configured by expecting {...}
   * and whenExecuting {...} */
  implicit val mocks = MockObjects(taskScheduler, blockManagerMaster)

  /**
   * Utility function to reset mocks and set expectations on them. EasyMock wants mock objects
   * to be reset after each time their expectations are set, and we tend to check mock object
   * calls over a single call to DAGScheduler.
   *
   * We also set a default expectation here that blockManagerMaster.getLocations can be called
   * and will return values from cacheLocations.
   */
  def resetExpecting(f: => Unit) {
    reset(taskScheduler)
    reset(blockManagerMaster)
    expecting {
      expectGetLocations()
      f
    }
  }

  before {
    taskSetMatchers.clear()
    cacheLocations.clear()
    val actorSystem = ActorSystem("test")
    mapOutputTracker = new MapOutputTracker(actorSystem, true)
    resetExpecting {
      taskScheduler.setListener(anyObject())
    }
    whenExecuting {
      scheduler = new DAGScheduler(taskScheduler, mapOutputTracker, blockManagerMaster, null)
    }
  }

  after {
    assert(scheduler.processEvent(StopDAGScheduler))
    resetExpecting {
      taskScheduler.stop()
    }
    whenExecuting {
      scheduler.stop()
    }
    sc.stop()
    System.clearProperty("spark.master.port")
  }

  def makeBlockManagerId(host: String): BlockManagerId =
    BlockManagerId("exec-" + host, host, 12345)

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
  def makeRdd(
        numPartitions: Int,
        dependencies: List[Dependency[_]],
        locations: Seq[Seq[String]] = Nil
      ): MyRDD = {
    val maxPartition = numPartitions - 1
    return new MyRDD(sc, dependencies) {
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
  }

  /**
   * EasyMock matcher method. For use as an argument matcher for a TaskSet whose first task
   * is from a particular RDD.
   */
  def taskSetForRdd(rdd: MyRDD): TaskSet = {
    val matcher = taskSetMatchers.getOrElseUpdate(rdd,
      new IArgumentMatcher {
        override def matches(actual: Any): Boolean = {
          val taskSet = actual.asInstanceOf[TaskSet]
          taskSet.tasks(0) match {
            case rt: ResultTask[_, _] => rt.rdd.id == rdd.id
            case smt: ShuffleMapTask => smt.rdd.id == rdd.id
            case _ => false
          }
        }
        override def appendTo(buf: StringBuffer) {
          buf.append("taskSetForRdd(" + rdd + ")")
        }
      })
    EasyMock.reportMatcher(matcher)
    return null
  }

  /**
   * Setup an EasyMock expectation to repsond to blockManagerMaster.getLocations() called from
   * cacheLocations.
   */
  def expectGetLocations(): Unit = {
    EasyMock.expect(blockManagerMaster.getLocations(anyObject().asInstanceOf[Array[String]])).
        andAnswer(new IAnswer[Seq[Seq[BlockManagerId]]] {
      override def answer(): Seq[Seq[BlockManagerId]] = {
        val blocks = getCurrentArguments()(0).asInstanceOf[Array[String]]
        return blocks.map { name =>
          val pieces = name.split("_")
          if (pieces(0) == "rdd") {
            val key = pieces(1).toInt -> pieces(2).toInt
            if (cacheLocations.contains(key)) {
              cacheLocations(key)
            } else {
              Seq[BlockManagerId]()
            }
          } else {
            Seq[BlockManagerId]()
          }
        }.toSeq
      }
    }).anyTimes()
  }

  /**
   * Process the supplied event as if it were the top of the DAGScheduler event queue, expecting
   * the scheduler not to exit.
   *
   * After processing the event, submit waiting stages as is done on most iterations of the
   * DAGScheduler event loop.
   */
  def runEvent(event: DAGSchedulerEvent) {
    assert(!scheduler.processEvent(event))
    scheduler.submitWaitingStages()
  }

  /**
   * Expect a TaskSet for the specified RDD to be submitted to the TaskScheduler. Should be
   * called from a resetExpecting { ... } block.
   *
   * Returns a easymock Capture that will contain the task set after the stage is submitted.
   * Most tests should use interceptStage() instead of this directly.
   */
  def expectStage(rdd: MyRDD): Capture[TaskSet] = {
    val taskSetCapture = new Capture[TaskSet]
    taskScheduler.submitTasks(and(capture(taskSetCapture), taskSetForRdd(rdd)))
    return taskSetCapture
  }

  /**
   * Expect the supplied code snippet to submit a stage for the specified RDD.
   * Return the resulting TaskSet. First marks all the tasks are belonging to the
   * current MapOutputTracker generation.
   */
  def interceptStage(rdd: MyRDD)(f: => Unit): TaskSet = {
    var capture: Capture[TaskSet] = null
    resetExpecting {
      capture = expectStage(rdd)
    }
    whenExecuting {
      f
    }
    val taskSet = capture.getValue
    for (task <- taskSet.tasks) {
      task.generation = mapOutputTracker.getGeneration
    }
    return taskSet
  }

  /**
   * Send the given CompletionEvent messages for the tasks in the TaskSet.
   */
  def respondToTaskSet(taskSet: TaskSet, results: Seq[(TaskEndReason, Any)]) {
    assert(taskSet.tasks.size >= results.size)
    for ((result, i) <- results.zipWithIndex) {
      if (i < taskSet.tasks.size) {
        runEvent(CompletionEvent(taskSet.tasks(i), result._1, result._2, Map[Long, Any]()))
      }
    }
  }

  /**
   * Assert that the supplied TaskSet has exactly the given preferredLocations.
   */
  def expectTaskSetLocations(taskSet: TaskSet, locations: Seq[Seq[String]]) {
    assert(locations.size === taskSet.tasks.size)
    for ((expectLocs, taskLocs) <-
            taskSet.tasks.map(_.preferredLocations).zip(locations)) {
      assert(expectLocs === taskLocs)
    }
  }

  /**
   * When we submit dummy Jobs, this is the compute function we supply. Except in a local test
   * below, we do not expect this function to ever be executed; instead, we will return results
   * directly through CompletionEvents.
   */
  def jobComputeFunc(context: TaskContext, it: Iterator[(Int, Int)]): Int =
     it.next._1.asInstanceOf[Int]


  /**
   * Start a job to compute the given RDD. Returns the JobWaiter that will
   * collect the result of the job via callbacks from DAGScheduler.
   */
  def submitRdd(rdd: MyRDD, allowLocal: Boolean = false): (JobWaiter[Int], Array[Int]) = {
    val resultArray = new Array[Int](rdd.partitions.size)
    val (toSubmit, waiter) = scheduler.prepareJob[(Int, Int), Int](
        rdd,
        jobComputeFunc,
        (0 to (rdd.partitions.size - 1)),
        "test-site",
        allowLocal,
        (i: Int, value: Int) => resultArray(i) = value
    )
    lastJobWaiter = waiter
    lastJobResult = resultArray
    runEvent(toSubmit)
    return (waiter, resultArray)
  }

  /**
   * Assert that a job we started has failed.
   */
  def expectJobException(waiter: JobWaiter[Int] = lastJobWaiter) {
    waiter.awaitResult() match {
      case JobSucceeded => fail()
      case JobFailed(_) => return
    }
  }

  /**
   * Assert that a job we started has succeeded and has the given result.
   */
  def expectJobResult(expected: Array[Int], waiter: JobWaiter[Int] = lastJobWaiter,
                      result: Array[Int] = lastJobResult) {
    waiter.awaitResult match {
      case JobSucceeded =>
        assert(expected === result)
      case JobFailed(_) =>
        fail()
    }
  }

  def makeMapStatus(host: String, reduces: Int): MapStatus =
    new MapStatus(makeBlockManagerId(host), Array.fill[Byte](reduces)(2))

  test("zero split job") {
    val rdd = makeRdd(0, Nil)
    var numResults = 0
    def accumulateResult(partition: Int, value: Int) {
      numResults += 1
    }
    scheduler.runJob(rdd, jobComputeFunc, Seq(), "test-site", false, accumulateResult)
    assert(numResults === 0)
  }

  test("run trivial job") {
    val rdd = makeRdd(1, Nil)
    val taskSet = interceptStage(rdd) { submitRdd(rdd) }
    respondToTaskSet(taskSet, List( (Success, 42) ))
    expectJobResult(Array(42))
  }

  test("local job") {
    val rdd = new MyRDD(sc, Nil) {
      override def compute(split: Partition, context: TaskContext): Iterator[(Int, Int)] =
        Array(42 -> 0).iterator
      override def getPartitions = Array( new Partition { override def index = 0 } )
      override def getPreferredLocations(split: Partition) = Nil
      override def toString = "DAGSchedulerSuite Local RDD"
    }
    submitRdd(rdd, true)
    expectJobResult(Array(42))
  }

  test("run trivial job w/ dependency") {
    val baseRdd = makeRdd(1, Nil)
    val finalRdd = makeRdd(1, List(new OneToOneDependency(baseRdd)))
    val taskSet = interceptStage(finalRdd) { submitRdd(finalRdd) }
    respondToTaskSet(taskSet, List( (Success, 42) ))
    expectJobResult(Array(42))
  }

  test("cache location preferences w/ dependency") {
    val baseRdd = makeRdd(1, Nil)
    val finalRdd = makeRdd(1, List(new OneToOneDependency(baseRdd)))
    cacheLocations(baseRdd.id -> 0) =
      Seq(makeBlockManagerId("hostA"), makeBlockManagerId("hostB"))
    val taskSet = interceptStage(finalRdd) { submitRdd(finalRdd) }
    expectTaskSetLocations(taskSet, List(Seq("hostA", "hostB")))
    respondToTaskSet(taskSet, List( (Success, 42) ))
    expectJobResult(Array(42))
  }

  test("trivial job failure") {
    val rdd = makeRdd(1, Nil)
    val taskSet = interceptStage(rdd) { submitRdd(rdd) }
    runEvent(TaskSetFailed(taskSet, "test failure"))
    expectJobException()
  }

  test("run trivial shuffle") {
    val shuffleMapRdd = makeRdd(2, Nil)
    val shuffleDep = new ShuffleDependency(shuffleMapRdd, null)
    val shuffleId = shuffleDep.shuffleId
    val reduceRdd = makeRdd(1, List(shuffleDep))

    val firstStage = interceptStage(shuffleMapRdd) { submitRdd(reduceRdd) }
    val secondStage = interceptStage(reduceRdd) {
      respondToTaskSet(firstStage, List(
        (Success, makeMapStatus("hostA", 1)),
        (Success, makeMapStatus("hostB", 1))
      ))
    }
    assert(mapOutputTracker.getServerStatuses(shuffleId, 0).map(_._1) ===
           Array(makeBlockManagerId("hostA"), makeBlockManagerId("hostB")))
    respondToTaskSet(secondStage, List( (Success, 42) ))
    expectJobResult(Array(42))
  }

  test("run trivial shuffle with fetch failure") {
    val shuffleMapRdd = makeRdd(2, Nil)
    val shuffleDep = new ShuffleDependency(shuffleMapRdd, null)
    val shuffleId = shuffleDep.shuffleId
    val reduceRdd = makeRdd(2, List(shuffleDep))

    val firstStage = interceptStage(shuffleMapRdd) { submitRdd(reduceRdd) }
    val secondStage = interceptStage(reduceRdd) {
      respondToTaskSet(firstStage, List(
        (Success, makeMapStatus("hostA", 1)),
        (Success, makeMapStatus("hostB", 1))
      ))
    }
    resetExpecting {
      blockManagerMaster.removeExecutor("exec-hostA")
    }
    whenExecuting {
      respondToTaskSet(secondStage, List(
        (Success, 42),
        (FetchFailed(makeBlockManagerId("hostA"), shuffleId, 0, 0), null)
      ))
    }
    val thirdStage = interceptStage(shuffleMapRdd) {
      scheduler.resubmitFailedStages()
    }
    val fourthStage = interceptStage(reduceRdd) {
      respondToTaskSet(thirdStage, List( (Success, makeMapStatus("hostA", 1)) ))
    }
    assert(mapOutputTracker.getServerStatuses(shuffleId, 0).map(_._1) ===
                   Array(makeBlockManagerId("hostA"), makeBlockManagerId("hostB")))
    respondToTaskSet(fourthStage, List( (Success, 43) ))
    expectJobResult(Array(42, 43))
  }

  test("ignore late map task completions") {
    val shuffleMapRdd = makeRdd(2, Nil)
    val shuffleDep = new ShuffleDependency(shuffleMapRdd, null)
    val shuffleId = shuffleDep.shuffleId
    val reduceRdd = makeRdd(2, List(shuffleDep))

    val taskSet = interceptStage(shuffleMapRdd) { submitRdd(reduceRdd) }
    val oldGeneration = mapOutputTracker.getGeneration
    resetExpecting {
      blockManagerMaster.removeExecutor("exec-hostA")
    }
    whenExecuting {
      runEvent(ExecutorLost("exec-hostA"))
    }
    val newGeneration = mapOutputTracker.getGeneration
    assert(newGeneration > oldGeneration)
    val noAccum = Map[Long, Any]()
    // We rely on the event queue being ordered and increasing the generation number by 1
    // should be ignored for being too old
    runEvent(CompletionEvent(taskSet.tasks(0), Success, makeMapStatus("hostA", 1), noAccum))
    // should work because it's a non-failed host
    runEvent(CompletionEvent(taskSet.tasks(0), Success, makeMapStatus("hostB", 1), noAccum))
    // should be ignored for being too old
    runEvent(CompletionEvent(taskSet.tasks(0), Success, makeMapStatus("hostA", 1), noAccum))
    taskSet.tasks(1).generation = newGeneration
    val secondStage = interceptStage(reduceRdd) {
      runEvent(CompletionEvent(taskSet.tasks(1), Success, makeMapStatus("hostA", 1), noAccum))
    }
    assert(mapOutputTracker.getServerStatuses(shuffleId, 0).map(_._1) ===
           Array(makeBlockManagerId("hostB"), makeBlockManagerId("hostA")))
    respondToTaskSet(secondStage, List( (Success, 42), (Success, 43) ))
    expectJobResult(Array(42, 43))
  }

  test("run trivial shuffle with out-of-band failure and retry") {
    val shuffleMapRdd = makeRdd(2, Nil)
    val shuffleDep = new ShuffleDependency(shuffleMapRdd, null)
    val shuffleId = shuffleDep.shuffleId
    val reduceRdd = makeRdd(1, List(shuffleDep))

    val firstStage = interceptStage(shuffleMapRdd) { submitRdd(reduceRdd) }
    resetExpecting {
      blockManagerMaster.removeExecutor("exec-hostA")
    }
    whenExecuting {
      runEvent(ExecutorLost("exec-hostA"))
    }
    // DAGScheduler will immediately resubmit the stage after it appears to have no pending tasks
    // rather than marking it is as failed and waiting.
    val secondStage = interceptStage(shuffleMapRdd) {
      respondToTaskSet(firstStage, List(
        (Success, makeMapStatus("hostA", 1)),
        (Success, makeMapStatus("hostB", 1))
      ))
    }
    val thirdStage = interceptStage(reduceRdd) {
      respondToTaskSet(secondStage, List(
        (Success, makeMapStatus("hostC", 1))
      ))
    }
    assert(mapOutputTracker.getServerStatuses(shuffleId, 0).map(_._1) ===
           Array(makeBlockManagerId("hostC"), makeBlockManagerId("hostB")))
    respondToTaskSet(thirdStage, List( (Success, 42) ))
    expectJobResult(Array(42))
  }

  test("recursive shuffle failures") {
    val shuffleOneRdd = makeRdd(2, Nil)
    val shuffleDepOne = new ShuffleDependency(shuffleOneRdd, null)
    val shuffleTwoRdd = makeRdd(2, List(shuffleDepOne))
    val shuffleDepTwo = new ShuffleDependency(shuffleTwoRdd, null)
    val finalRdd = makeRdd(1, List(shuffleDepTwo))

    val firstStage = interceptStage(shuffleOneRdd) { submitRdd(finalRdd) }
    val secondStage = interceptStage(shuffleTwoRdd) {
      respondToTaskSet(firstStage, List(
        (Success, makeMapStatus("hostA", 2)),
        (Success, makeMapStatus("hostB", 2))
      ))
    }
    val thirdStage = interceptStage(finalRdd) {
      respondToTaskSet(secondStage, List(
        (Success, makeMapStatus("hostA", 1)),
        (Success, makeMapStatus("hostC", 1))
      ))
    }
    resetExpecting {
      blockManagerMaster.removeExecutor("exec-hostA")
    }
    whenExecuting {
      respondToTaskSet(thirdStage, List(
        (FetchFailed(makeBlockManagerId("hostA"), shuffleDepTwo.shuffleId, 0, 0), null)
      ))
    }
    val recomputeOne = interceptStage(shuffleOneRdd) {
      scheduler.resubmitFailedStages()
    }
    val recomputeTwo = interceptStage(shuffleTwoRdd) {
      respondToTaskSet(recomputeOne, List(
        (Success, makeMapStatus("hostA", 2))
      ))
    }
    val finalStage = interceptStage(finalRdd) {
      respondToTaskSet(recomputeTwo, List(
        (Success, makeMapStatus("hostA", 1))
      ))
    }
    respondToTaskSet(finalStage, List( (Success, 42) ))
    expectJobResult(Array(42))
  }

  test("cached post-shuffle") {
    val shuffleOneRdd = makeRdd(2, Nil)
    val shuffleDepOne = new ShuffleDependency(shuffleOneRdd, null)
    val shuffleTwoRdd = makeRdd(2, List(shuffleDepOne))
    val shuffleDepTwo = new ShuffleDependency(shuffleTwoRdd, null)
    val finalRdd = makeRdd(1, List(shuffleDepTwo))

    val firstShuffleStage = interceptStage(shuffleOneRdd) { submitRdd(finalRdd) }
    cacheLocations(shuffleTwoRdd.id -> 0) = Seq(makeBlockManagerId("hostD"))
    cacheLocations(shuffleTwoRdd.id -> 1) = Seq(makeBlockManagerId("hostC"))
    val secondShuffleStage = interceptStage(shuffleTwoRdd) {
      respondToTaskSet(firstShuffleStage, List(
        (Success, makeMapStatus("hostA", 2)),
        (Success, makeMapStatus("hostB", 2))
      ))
    }
    val reduceStage = interceptStage(finalRdd) {
      respondToTaskSet(secondShuffleStage, List(
        (Success, makeMapStatus("hostA", 1)),
        (Success, makeMapStatus("hostB", 1))
      ))
    }
    resetExpecting {
      blockManagerMaster.removeExecutor("exec-hostA")
    }
    whenExecuting {
      respondToTaskSet(reduceStage, List(
        (FetchFailed(makeBlockManagerId("hostA"), shuffleDepTwo.shuffleId, 0, 0), null)
      ))
    }
    // DAGScheduler should notice the cached copy of the second shuffle and try to get it rerun.
    val recomputeTwo = interceptStage(shuffleTwoRdd) {
      scheduler.resubmitFailedStages()
    }
    expectTaskSetLocations(recomputeTwo, Seq(Seq("hostD")))
    val finalRetry = interceptStage(finalRdd) {
      respondToTaskSet(recomputeTwo, List(
        (Success, makeMapStatus("hostD", 1))
      ))
    }
    respondToTaskSet(finalRetry, List( (Success, 42) ))
    expectJobResult(Array(42))
  }

  test("cached post-shuffle but fails") {
    val shuffleOneRdd = makeRdd(2, Nil)
    val shuffleDepOne = new ShuffleDependency(shuffleOneRdd, null)
    val shuffleTwoRdd = makeRdd(2, List(shuffleDepOne))
    val shuffleDepTwo = new ShuffleDependency(shuffleTwoRdd, null)
    val finalRdd = makeRdd(1, List(shuffleDepTwo))

    val firstShuffleStage = interceptStage(shuffleOneRdd) { submitRdd(finalRdd) }
    cacheLocations(shuffleTwoRdd.id -> 0) = Seq(makeBlockManagerId("hostD"))
    cacheLocations(shuffleTwoRdd.id -> 1) = Seq(makeBlockManagerId("hostC"))
    val secondShuffleStage = interceptStage(shuffleTwoRdd) {
      respondToTaskSet(firstShuffleStage, List(
        (Success, makeMapStatus("hostA", 2)),
        (Success, makeMapStatus("hostB", 2))
      ))
    }
    val reduceStage = interceptStage(finalRdd) {
      respondToTaskSet(secondShuffleStage, List(
        (Success, makeMapStatus("hostA", 1)),
        (Success, makeMapStatus("hostB", 1))
      ))
    }
    resetExpecting {
      blockManagerMaster.removeExecutor("exec-hostA")
    }
    whenExecuting {
      respondToTaskSet(reduceStage, List(
        (FetchFailed(makeBlockManagerId("hostA"), shuffleDepTwo.shuffleId, 0, 0), null)
      ))
    }
    val recomputeTwoCached = interceptStage(shuffleTwoRdd) {
      scheduler.resubmitFailedStages()
    }
    expectTaskSetLocations(recomputeTwoCached, Seq(Seq("hostD")))
    intercept[FetchFailedException]{
      mapOutputTracker.getServerStatuses(shuffleDepOne.shuffleId, 0)
    }

    // Simulate the shuffle input data failing to be cached.
    cacheLocations.remove(shuffleTwoRdd.id -> 0)
    respondToTaskSet(recomputeTwoCached, List(
      (FetchFailed(null, shuffleDepOne.shuffleId, 0, 0), null)
    ))

    // After the fetch failure, DAGScheduler should recheck the cache and decide to resubmit
    // everything.
    val recomputeOne = interceptStage(shuffleOneRdd) {
      scheduler.resubmitFailedStages()
    }
    // We use hostA here to make sure DAGScheduler doesn't think it's still dead.
    val recomputeTwoUncached = interceptStage(shuffleTwoRdd) {
      respondToTaskSet(recomputeOne, List( (Success, makeMapStatus("hostA", 1)) ))
    }
    expectTaskSetLocations(recomputeTwoUncached, Seq(Seq[String]()))
    val finalRetry = interceptStage(finalRdd) {
      respondToTaskSet(recomputeTwoUncached, List( (Success, makeMapStatus("hostA", 1)) ))

    }
    respondToTaskSet(finalRetry, List( (Success, 42) ))
    expectJobResult(Array(42))
  }
}
