package spark.scheduler

import scala.collection.mutable.{Map, HashMap}

import org.scalatest.FunSuite
import org.scalatest.BeforeAndAfter
import org.scalatest.concurrent.AsyncAssertions
import org.scalatest.concurrent.TimeLimitedTests
import org.scalatest.mock.EasyMockSugar
import org.scalatest.time.{Span, Seconds}

import org.easymock.EasyMock._
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
import spark.Split
import spark.TaskContext
import spark.TaskEndReason

import spark.{FetchFailed, Success}

class DAGSchedulerSuite extends FunSuite
    with BeforeAndAfter with EasyMockSugar with TimeLimitedTests
    with AsyncAssertions with spark.Logging {

  // If we crash the DAGScheduler thread, our test will probably hang.
  override val timeLimit = Span(5, Seconds)

  val sc: SparkContext = new SparkContext("local", "DAGSchedulerSuite")
  var scheduler: DAGScheduler = null
  var w: Waiter = null
  val taskScheduler = mock[TaskScheduler]
  val blockManagerMaster = mock[BlockManagerMaster]
  var mapOutputTracker: MapOutputTracker = null
  var schedulerThread: Thread = null
  var schedulerException: Throwable = null
  val taskSetMatchers = new HashMap[MyRDD, IArgumentMatcher]
  val cacheLocations = new HashMap[(Int, Int), Seq[BlockManagerId]]

  implicit val mocks = MockObjects(taskScheduler, blockManagerMaster)

  def makeBlockManagerId(host: String): BlockManagerId =
    BlockManagerId("exec-" + host, host, 12345)

  def resetExpecting(f: => Unit) {
    reset(taskScheduler)
    reset(blockManagerMaster)
    expecting(f)
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
    w = new Waiter
    schedulerException = null
    schedulerThread = new Thread("DAGScheduler under test") {
      override def run() {
        try {
          scheduler.run()
        } catch {
          case t: Throwable =>
            logError("Got exception in DAGScheduler: ", t)
            schedulerException = t
        } finally {
          w.dismiss()
        }
      }
    }
    schedulerThread.start
    logInfo("finished before")
  }

  after {
    logInfo("started after")
    resetExpecting {
      taskScheduler.stop()
    }
    whenExecuting {
      scheduler.stop
      schedulerThread.join
    }
    w.await()
    if (schedulerException != null) {
      throw new Exception("Exception caught from scheduler thread", schedulerException)
    }
    System.clearProperty("spark.master.port")
  }

  // Type of RDD we use for testing. Note that we should never call the real RDD compute methods.
  // This is a pair RDD type so it can always be used in ShuffleDependencies.
  type MyRDD = RDD[(Int, Int)]

  def makeRdd(
        numSplits: Int,
        dependencies: List[Dependency[_]],
        locations: Seq[Seq[String]] = Nil
      ): MyRDD = {
    val maxSplit = numSplits - 1
    return new MyRDD(sc, dependencies) {
      override def compute(split: Split, context: TaskContext): Iterator[(Int, Int)] =
        throw new RuntimeException("should not be reached")
      override def getSplits() = (0 to maxSplit).map(i => new Split {
        override def index = i
      }).toArray
      override def getPreferredLocations(split: Split): Seq[String] =
        if (locations.isDefinedAt(split.index))
          locations(split.index)
        else
          Nil
      override def toString: String = "DAGSchedulerSuiteRDD " + id
    }
  }

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

  def expectStageAnd(rdd: MyRDD, results: Seq[(TaskEndReason, Any)],
      preferredLocations: Option[Seq[Seq[String]]] = None)(afterSubmit: TaskSet => Unit) {
    // TODO: Remember which submission
    EasyMock.expect(taskScheduler.submitTasks(taskSetForRdd(rdd))).andAnswer(new IAnswer[Unit] {
      override def answer(): Unit = {
        val taskSet = getCurrentArguments()(0).asInstanceOf[TaskSet]
        for (task <- taskSet.tasks) {
          task.generation = mapOutputTracker.getGeneration
        }
        afterSubmit(taskSet)
        preferredLocations match {
          case None =>
            for (taskLocs <- taskSet.tasks.map(_.preferredLocations)) {
              w { assert(taskLocs.size === 0) }
            }
          case Some(locations) =>
            w { assert(locations.size === taskSet.tasks.size) }
            for ((expectLocs, taskLocs) <-
                    taskSet.tasks.map(_.preferredLocations).zip(locations)) {
              w { assert(expectLocs === taskLocs) }
            }
        }
        w { assert(taskSet.tasks.size >= results.size)}
        for ((result, i) <- results.zipWithIndex) {
          if (i < taskSet.tasks.size) {
            scheduler.taskEnded(taskSet.tasks(i), result._1, result._2, Map[Long, Any]())
          }
        }
      }
    })
  }

  def expectStage(rdd: MyRDD, results: Seq[(TaskEndReason, Any)],
                  preferredLocations: Option[Seq[Seq[String]]] = None) {
    expectStageAnd(rdd, results, preferredLocations) { _ => }
  }

  def submitRdd(rdd: MyRDD, allowLocal: Boolean = false): Array[Int] = {
    return scheduler.runJob[(Int, Int), Int](
        rdd,
        (context: TaskContext, it: Iterator[(Int, Int)]) => it.next._1.asInstanceOf[Int],
        (0 to (rdd.splits.size - 1)),
        "test-site",
        allowLocal
    )
  }

  def makeMapStatus(host: String, reduces: Int): MapStatus =
    new MapStatus(makeBlockManagerId(host), Array.fill[Byte](reduces)(2))

  test("zero split job") {
    val rdd = makeRdd(0, Nil)
    resetExpecting {
      expectGetLocations()
      // deliberately expect no stages to be submitted
    }
    whenExecuting {
      assert(submitRdd(rdd) === Array[Int]())
    }
  }

  test("run trivial job") {
    val rdd = makeRdd(1, Nil)
    resetExpecting {
      expectGetLocations()
      expectStage(rdd, List( (Success, 42) ))
    }
    whenExecuting {
      assert(submitRdd(rdd) === Array(42))
    }
  }

  test("local job") {
    val rdd = new MyRDD(sc, Nil) {
      override def compute(split: Split, context: TaskContext): Iterator[(Int, Int)] =
        Array(42 -> 0).iterator
      override def getSplits() = Array( new Split { override def index = 0 } )
      override def getPreferredLocations(split: Split) = Nil
      override def toString = "DAGSchedulerSuite Local RDD"
    }
    resetExpecting {
      expectGetLocations()
      // deliberately expect no stages to be submitted
    }
    whenExecuting {
      assert(submitRdd(rdd, true) === Array(42))
    }
  }

  test("run trivial job w/ dependency") {
    val baseRdd = makeRdd(1, Nil)
    val finalRdd = makeRdd(1, List(new OneToOneDependency(baseRdd)))
    resetExpecting {
      expectGetLocations()
      expectStage(finalRdd, List( (Success, 42) ))
    }
    whenExecuting {
      assert(submitRdd(finalRdd) === Array(42))
    }
  }

  test("location preferences w/ dependency") {
    val baseRdd = makeRdd(1, Nil)
    val finalRdd = makeRdd(1, List(new OneToOneDependency(baseRdd)))
    resetExpecting {
      expectGetLocations()
      cacheLocations(baseRdd.id -> 0) =
        Seq(makeBlockManagerId("hostA"), makeBlockManagerId("hostB"))
      expectStage(finalRdd, List( (Success, 42) ),
                  Some(List(Seq("hostA", "hostB"))))
    }
    whenExecuting {
      assert(submitRdd(finalRdd) === Array(42))
    }
  }

  test("trivial job failure") {
    val rdd = makeRdd(1, Nil)
    resetExpecting {
      expectGetLocations()
      expectStageAnd(rdd, List()) { taskSet => scheduler.taskSetFailed(taskSet, "test failure") }
    }
    whenExecuting(taskScheduler, blockManagerMaster) {
      intercept[SparkException] { submitRdd(rdd) }
    }
  }

  test("run trivial shuffle") {
    val shuffleMapRdd = makeRdd(2, Nil)
    val shuffleDep = new ShuffleDependency(shuffleMapRdd, null)
    val shuffleId = shuffleDep.shuffleId
    val reduceRdd = makeRdd(1, List(shuffleDep))

    resetExpecting {
      expectGetLocations()
      expectStage(shuffleMapRdd, List(
        (Success, makeMapStatus("hostA", 1)),
        (Success, makeMapStatus("hostB", 1))
      ))
      expectStageAnd(reduceRdd, List( (Success, 42) )) { _ =>
        w { assert(mapOutputTracker.getServerStatuses(shuffleId, 0).map(_._1) ===
                   Array(makeBlockManagerId("hostA"), makeBlockManagerId("hostB"))) }
      }
    }
    whenExecuting {
      assert(submitRdd(reduceRdd) === Array(42))
    }
  }

  test("run trivial shuffle with fetch failure") {
    val shuffleMapRdd = makeRdd(2, Nil)
    val shuffleDep = new ShuffleDependency(shuffleMapRdd, null)
    val shuffleId = shuffleDep.shuffleId
    val reduceRdd = makeRdd(2, List(shuffleDep))

    resetExpecting {
      expectGetLocations()
      expectStage(shuffleMapRdd, List(
        (Success, makeMapStatus("hostA", 1)),
        (Success, makeMapStatus("hostB", 1))
      ))
      blockManagerMaster.removeExecutor("exec-hostA")
      expectStage(reduceRdd, List(
        (Success, 42),
        (FetchFailed(makeBlockManagerId("hostA"), shuffleId, 0, 0), null)
      ))
      // partial recompute
      expectStage(shuffleMapRdd, List( (Success, makeMapStatus("hostA", 1)) ))
      expectStageAnd(reduceRdd, List( (Success, 43) )) { _ =>
        w { assert(mapOutputTracker.getServerStatuses(shuffleId, 0).map(_._1) ===
                   Array(makeBlockManagerId("hostA"),
                         makeBlockManagerId("hostB"))) }
      }
    }
    whenExecuting {
      assert(submitRdd(reduceRdd) === Array(42, 43))
    }
  }

  test("ignore late map task completions") {
    val shuffleMapRdd = makeRdd(2, Nil)
    val shuffleDep = new ShuffleDependency(shuffleMapRdd, null)
    val shuffleId = shuffleDep.shuffleId
    val reduceRdd = makeRdd(2, List(shuffleDep))

    resetExpecting {
      expectGetLocations()
      expectStageAnd(shuffleMapRdd, List(
        (Success, makeMapStatus("hostA", 1))
      )) { taskSet =>
        val newGeneration = mapOutputTracker.getGeneration + 1
        scheduler.executorLost("exec-hostA")
        val noAccum = Map[Long, Any]()
        // We rely on the event queue being ordered and increasing the generation number by 1
        // should be ignored for being too old
        scheduler.taskEnded(taskSet.tasks(0), Success, makeMapStatus("hostA", 1), noAccum)
        // should work because it's a non-failed host
        scheduler.taskEnded(taskSet.tasks(0), Success, makeMapStatus("hostB", 1), noAccum)
        // should be ignored for being too old
        scheduler.taskEnded(taskSet.tasks(0), Success, makeMapStatus("hostA", 1), noAccum)
        // should be ignored (not end the stage) because it's too old
        scheduler.taskEnded(taskSet.tasks(1), Success, makeMapStatus("hostA", 1), noAccum)
        taskSet.tasks(1).generation = newGeneration
        scheduler.taskEnded(taskSet.tasks(1), Success, makeMapStatus("hostA", 1), noAccum)
      }
      blockManagerMaster.removeExecutor("exec-hostA")
      expectStageAnd(reduceRdd, List(
        (Success, 42), (Success, 43)
      )) { _ =>
        w { assert(mapOutputTracker.getServerStatuses(shuffleId, 0).map(_._1) ===
                   Array(makeBlockManagerId("hostB"), makeBlockManagerId("hostA"))) }
      }
    }
    whenExecuting {
      assert(submitRdd(reduceRdd) === Array(42, 43))
    }
  }

  test("run trivial shuffle with out-of-band failure") {
    val shuffleMapRdd = makeRdd(2, Nil)
    val shuffleDep = new ShuffleDependency(shuffleMapRdd, null)
    val shuffleId = shuffleDep.shuffleId
    val reduceRdd = makeRdd(1, List(shuffleDep))
    resetExpecting {
      expectGetLocations()
      blockManagerMaster.removeExecutor("exec-hostA")
      expectStageAnd(shuffleMapRdd, List(
        (Success, makeMapStatus("hostA", 1)),
        (Success, makeMapStatus("hostB", 1))
      )) { _ => scheduler.executorLost("exec-hostA") }
      expectStage(shuffleMapRdd, List(
        (Success, makeMapStatus("hostC", 1))
      ))
      expectStageAnd(reduceRdd, List( (Success, 42) )) { _ =>
        w { assert(mapOutputTracker.getServerStatuses(shuffleId, 0).map(_._1) ===
                   Array(makeBlockManagerId("hostC"),
                         makeBlockManagerId("hostB"))) }
      }
    }
    whenExecuting {
      assert(submitRdd(reduceRdd) === Array(42))
    }
  }

  test("recursive shuffle failures") {
    val shuffleOneRdd = makeRdd(2, Nil)
    val shuffleDepOne = new ShuffleDependency(shuffleOneRdd, null)
    val shuffleTwoRdd = makeRdd(2, List(shuffleDepOne))
    val shuffleDepTwo = new ShuffleDependency(shuffleTwoRdd, null)
    val finalRdd = makeRdd(1, List(shuffleDepTwo))

    resetExpecting {
      expectGetLocations()
      expectStage(shuffleOneRdd, List(
        (Success, makeMapStatus("hostA", 1)),
        (Success, makeMapStatus("hostB", 1))
      ))
      expectStage(shuffleTwoRdd, List(
        (Success, makeMapStatus("hostA", 1)),
        (Success, makeMapStatus("hostC", 1))
      ))
      blockManagerMaster.removeExecutor("exec-hostA")
      expectStage(finalRdd, List(
        (FetchFailed(makeBlockManagerId("hostA"), shuffleDepTwo.shuffleId, 0, 0), null)
      ))
      // triggers a partial recompute of the first stage, then the second
      expectStage(shuffleOneRdd, List(
        (Success, makeMapStatus("hostA", 1))
      ))
      expectStage(shuffleTwoRdd, List(
        (Success, makeMapStatus("hostA", 1))
      ))
      expectStage(finalRdd, List(
        (Success, 42)
      ))
    }
    whenExecuting {
      assert(submitRdd(finalRdd) === Array(42))
    }
  }

  test("cached post-shuffle") {
    val shuffleOneRdd = makeRdd(2, Nil)
    val shuffleDepOne = new ShuffleDependency(shuffleOneRdd, null)
    val shuffleTwoRdd = makeRdd(2, List(shuffleDepOne))
    val shuffleDepTwo = new ShuffleDependency(shuffleTwoRdd, null)
    val finalRdd = makeRdd(1, List(shuffleDepTwo))

    resetExpecting {
      expectGetLocations()
      expectStage(shuffleOneRdd, List(
        (Success, makeMapStatus("hostA", 1)),
        (Success, makeMapStatus("hostB", 1))
      ))
      expectStageAnd(shuffleTwoRdd, List(
        (Success, makeMapStatus("hostA", 1)),
        (Success, makeMapStatus("hostC", 1))
      )){ _ =>
        cacheLocations(shuffleTwoRdd.id -> 0) = Seq(makeBlockManagerId("hostD"))
        cacheLocations(shuffleTwoRdd.id -> 1) = Seq(makeBlockManagerId("hostC"))
      }
      blockManagerMaster.removeExecutor("exec-hostA")
      expectStage(finalRdd, List(
        (FetchFailed(makeBlockManagerId("hostA"), shuffleDepTwo.shuffleId, 0, 0), null)
      ))
      // since we have a cached copy of the missing split of shuffleTwoRdd, we shouldn't
      // immediately try to rerun shuffleOneRdd:
      expectStage(shuffleTwoRdd, List(
        (Success, makeMapStatus("hostD", 1))
      ), Some(Seq(List("hostD"))))
      expectStage(finalRdd, List(
        (Success, 42)
      ))
    }
    whenExecuting {
      assert(submitRdd(finalRdd) === Array(42))
    }
  }

  test("cached post-shuffle but fails") {
    val shuffleOneRdd = makeRdd(2, Nil)
    val shuffleDepOne = new ShuffleDependency(shuffleOneRdd, null)
    val shuffleTwoRdd = makeRdd(2, List(shuffleDepOne))
    val shuffleDepTwo = new ShuffleDependency(shuffleTwoRdd, null)
    val finalRdd = makeRdd(1, List(shuffleDepTwo))

    resetExpecting {
      expectGetLocations()
      expectStage(shuffleOneRdd, List(
        (Success, makeMapStatus("hostA", 1)),
        (Success, makeMapStatus("hostB", 1))
      ))
      expectStageAnd(shuffleTwoRdd, List(
        (Success, makeMapStatus("hostA", 1)),
        (Success, makeMapStatus("hostC", 1))
      )){ _ =>
        cacheLocations(shuffleTwoRdd.id -> 0) = Seq(makeBlockManagerId("hostD"))
        cacheLocations(shuffleTwoRdd.id -> 1) = Seq(makeBlockManagerId("hostC"))
      }
      blockManagerMaster.removeExecutor("exec-hostA")
      expectStage(finalRdd, List(
        (FetchFailed(makeBlockManagerId("hostA"), shuffleDepTwo.shuffleId, 0, 0), null)
      ))
      // since we have a cached copy of the missing split of shuffleTwoRdd, we shouldn't
      // immediately try to rerun shuffleOneRdd:
      expectStageAnd(shuffleTwoRdd, List(
        (FetchFailed(null, shuffleDepOne.shuffleId, 0, 0), null)
      ), Some(Seq(List("hostD")))) { _ =>
        w {
          intercept[FetchFailedException]{
            mapOutputTracker.getServerStatuses(shuffleDepOne.shuffleId, 0)
          }
        }
        cacheLocations.remove(shuffleTwoRdd.id -> 0)
      }
      // after that fetch failure, we should refetch the cache locations and try to recompute
      // the whole chain. Note that we will ignore that a fetch failure previously occured on
      // this host.
      expectStage(shuffleOneRdd, List( (Success, makeMapStatus("hostA", 1)) ))
      expectStage(shuffleTwoRdd, List( (Success, makeMapStatus("hostA", 1)) ))
      expectStage(finalRdd, List( (Success, 42) ))
    }
    whenExecuting {
      assert(submitRdd(finalRdd) === Array(42))
    }
  }
}

