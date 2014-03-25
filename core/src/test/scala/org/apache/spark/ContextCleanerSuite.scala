package org.apache.spark

import scala.collection.mutable.{ArrayBuffer, HashSet, SynchronizedSet}

import org.scalatest.{BeforeAndAfter, FunSuite}
import org.scalatest.concurrent.Eventually
import org.scalatest.concurrent.Eventually._
import org.scalatest.time.SpanSugar._

import org.apache.spark.SparkContext._
import org.apache.spark.storage.{RDDBlockId, ShuffleBlockId}
import org.apache.spark.rdd.{ShuffleCoGroupSplitDep, RDD}
import scala.util.Random
import java.lang.ref.WeakReference

class ContextCleanerSuite extends FunSuite with BeforeAndAfter with LocalSparkContext {

  implicit val defaultTimeout = timeout(10000 millis)

  before {
    sc = new SparkContext("local[2]", "CleanerSuite")
  }

  test("cleanup RDD") {
    val rdd = newRDD.persist()
    val collected = rdd.collect().toList
    val tester = new CleanerTester(sc, rddIds = Seq(rdd.id))

    // Explicit cleanup
    cleaner.cleanupRDD(rdd)
    tester.assertCleanup

    // verify that RDDs can be re-executed after cleaning up
    assert(rdd.collect().toList === collected)
  }

  test("cleanup shuffle") {
    val (rdd, shuffleDeps) = newRDDWithShuffleDependencies
    val collected = rdd.collect().toList
    val tester = new CleanerTester(sc, shuffleIds = shuffleDeps.map(_.shuffleId))

    // Explicit cleanup
    shuffleDeps.foreach(s => cleaner.cleanupShuffle(s))
    tester.assertCleanup

    // Verify that shuffles can be re-executed after cleaning up
    assert(rdd.collect().toList === collected)
  }

  test("automatically cleanup RDD") {
    var rdd = newRDD.persist()
    rdd.count()
    
    // test that GC does not cause RDD cleanup due to a strong reference
    val preGCTester =  new CleanerTester(sc, rddIds = Seq(rdd.id))
    runGC()
    intercept[Exception] {
      preGCTester.assertCleanup(timeout(1000 millis))
    }

    // test that GC causes RDD cleanup after dereferencing the RDD
    val postGCTester = new CleanerTester(sc, rddIds = Seq(rdd.id))
    rdd = null  // make RDD out of scope
    runGC()
    postGCTester.assertCleanup
  }

  test("automatically cleanup shuffle") {
    var rdd = newShuffleRDD
    rdd.count()

    // test that GC does not cause shuffle cleanup due to a strong reference
    val preGCTester =  new CleanerTester(sc, shuffleIds = Seq(0))
    runGC()
    intercept[Exception] {
      preGCTester.assertCleanup(timeout(1000 millis))
    }

    // test that GC causes shuffle cleanup after dereferencing the RDD
    val postGCTester = new CleanerTester(sc, shuffleIds = Seq(0))
    rdd = null  // make RDD out of scope, so that corresponding shuffle goes out of scope
    runGC()
    postGCTester.assertCleanup
  }

  test("automatically cleanup RDD + shuffle") {

    def randomRDD: RDD[_] = {
      val rdd: RDD[_] = Random.nextInt(3) match {
        case 0 => newRDD
        case 1 => newShuffleRDD
        case 2 => newPairRDD.join(newPairRDD)
      }
      if (Random.nextBoolean()) rdd.persist()
      rdd.count()
      rdd
    }

    val buffer = new ArrayBuffer[RDD[_]]
    for (i <- 1 to 500) {
      buffer += randomRDD
    }

    val rddIds = sc.persistentRdds.keys.toSeq
    val shuffleIds = 0 until sc.newShuffleId

    val preGCTester =  new CleanerTester(sc, rddIds, shuffleIds)
    runGC()
    intercept[Exception] {
      preGCTester.assertCleanup(timeout(1000 millis))
    }
    // test that GC causes shuffle cleanup after dereferencing the RDD
    val postGCTester = new CleanerTester(sc, rddIds, shuffleIds)
    buffer.clear()
    runGC()
    postGCTester.assertCleanup
  }

  def newRDD = sc.makeRDD(1 to 10)

  def newPairRDD = newRDD.map(_ -> 1)

  def newShuffleRDD = newPairRDD.reduceByKey(_ + _)

  def newRDDWithShuffleDependencies: (RDD[_], Seq[ShuffleDependency[_, _]]) = {
    def getAllDependencies(rdd: RDD[_]): Seq[Dependency[_]] = {
      rdd.dependencies ++ rdd.dependencies.flatMap { dep =>
        getAllDependencies(dep.rdd)
      }
    }
    val rdd = newShuffleRDD

    // Get all the shuffle dependencies
    val shuffleDeps = getAllDependencies(rdd).filter(_.isInstanceOf[ShuffleDependency[_, _]])
      .map(_.asInstanceOf[ShuffleDependency[_, _]])
    (rdd, shuffleDeps)
  }

  /** Run GC and make sure it actually has run */
  def runGC() {
    val weakRef = new WeakReference(new Object())
    val startTime = System.currentTimeMillis
    System.gc() // Make a best effort to run the garbage collection. It *usually* runs GC.
    // Wait until a weak reference object has been GCed
    while(System.currentTimeMillis - startTime < 10000 && weakRef.get != null) {
      System.gc()
      System.runFinalization()
      Thread.sleep(200)
    }
  }

  def cleaner = sc.cleaner
}


/** Class to test whether RDDs, shuffles, etc. have been successfully cleaned. */
class CleanerTester(sc: SparkContext, rddIds: Seq[Int] = Nil, shuffleIds: Seq[Int] = Nil)
  extends Logging {

  val toBeCleanedRDDIds = new HashSet[Int] with SynchronizedSet[Int] ++= rddIds
  val toBeCleanedShuffleIds = new HashSet[Int] with SynchronizedSet[Int] ++= shuffleIds

  val cleanerListener = new CleanerListener {
    def rddCleaned(rddId: Int): Unit = {
      toBeCleanedRDDIds -= rddId
      logInfo("RDD "+ rddId + " cleaned")
    }

    def shuffleCleaned(shuffleId: Int): Unit = {
      toBeCleanedShuffleIds -= shuffleId
      logInfo("Shuffle " + shuffleId + " cleaned")
    }
  }

  val MAX_VALIDATION_ATTEMPTS = 10
  val VALIDATION_ATTEMPT_INTERVAL = 100

  logInfo("Attempting to validate before cleanup:\n" + uncleanedResourcesToString)
  preCleanupValidate()
  sc.cleaner.attachListener(cleanerListener)

  /** Assert that all the stuff has been cleaned up */
  def assertCleanup(implicit waitTimeout: Eventually.Timeout) {
    try {
      eventually(waitTimeout, interval(10 millis)) {
        assert(isAllCleanedUp)
      }
      Thread.sleep(100) // to allow async cleanup actions to be completed
      postCleanupValidate()
    } finally {
      logInfo("Resources left from cleaning up:\n" + uncleanedResourcesToString)
    }
  }

  /** Verify that RDDs, shuffles, etc. occupy resources */
  private def preCleanupValidate() {
    assert(rddIds.nonEmpty || shuffleIds.nonEmpty, "Nothing to cleanup")

    // Verify the RDDs have been persisted and blocks are present
    assert(rddIds.forall(sc.persistentRdds.contains),
      "One or more RDDs have not been persisted, cannot start cleaner test")
    assert(rddIds.forall(rddId => blockManager.master.contains(rddBlockId(rddId))),
      "One or more RDDs' blocks cannot be found in block manager, cannot start cleaner test")

    // Verify the shuffle ids are registered and blocks are present
    assert(shuffleIds.forall(mapOutputTrackerMaster.containsShuffle),
      "One or more shuffles have not been registered cannot start cleaner test")
    assert(shuffleIds.forall(shuffleId => diskBlockManager.containsBlock(shuffleBlockId(shuffleId))),
      "One or more shuffles' blocks cannot be found in disk manager, cannot start cleaner test")
  }

  /**
   * Verify that RDDs, shuffles, etc. do not occupy resources. Tests multiple times as there is
   * as there is not guarantee on how long it will take clean up the resources.
   */
  private def postCleanupValidate() {
    var attempts = 0
    while (attempts < MAX_VALIDATION_ATTEMPTS) {
      attempts += 1
      logInfo("Attempt: " + attempts)
      try {
        // Verify all the RDDs have been unpersisted
        assert(rddIds.forall(!sc.persistentRdds.contains(_)))
        assert(rddIds.forall(rddId => !blockManager.master.contains(rddBlockId(rddId))))

        // Verify all the shuffle have been deregistered and cleaned up
        assert(shuffleIds.forall(!mapOutputTrackerMaster.containsShuffle(_)))
        assert(shuffleIds.forall(shuffleId =>
          !diskBlockManager.containsBlock(shuffleBlockId(shuffleId))))
        return
      } catch {
        case t: Throwable =>
          if (attempts >= MAX_VALIDATION_ATTEMPTS) {
            throw t
          } else {
            Thread.sleep(VALIDATION_ATTEMPT_INTERVAL)
          }
      }
    }
  }

  private def uncleanedResourcesToString = {
    s"""
      |\tRDDs = ${toBeCleanedRDDIds.mkString("[", ", ", "]")}
      |\tShuffles = ${toBeCleanedShuffleIds.mkString("[", ", ", "]")}
    """.stripMargin
  }

  private def isAllCleanedUp = toBeCleanedRDDIds.isEmpty && toBeCleanedShuffleIds.isEmpty

  private def shuffleBlockId(shuffleId: Int) = ShuffleBlockId(shuffleId, 0, 0)

  private def rddBlockId(rddId: Int) = RDDBlockId(rddId, 0)

  private def blockManager = sc.env.blockManager

  private def diskBlockManager = blockManager.diskBlockManager

  private def mapOutputTrackerMaster = sc.env.mapOutputTracker.asInstanceOf[MapOutputTrackerMaster]
}