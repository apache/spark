package org.apache.spark

import scala.collection.mutable.{ArrayBuffer, HashSet, SynchronizedSet}

import org.scalatest.{BeforeAndAfter, FunSuite}
import org.scalatest.concurrent.Eventually
import org.scalatest.concurrent.Eventually._
import org.scalatest.time.SpanSugar._

import org.apache.spark.SparkContext._
import org.apache.spark.storage.{RDDBlockId, ShuffleBlockId}
import org.apache.spark.rdd.RDD
import scala.util.Random
import java.lang.ref.WeakReference

class ContextCleanerSuite extends FunSuite with BeforeAndAfter with LocalSparkContext {

  implicit val defaultTimeout = timeout(10000 millis)

  before {
    sc = new SparkContext("local[2]", "CleanerSuite")
  }

  test("cleanup RDD") {
    val rdd = newRDD.persist()
    rdd.count()
    val tester = new CleanerTester(sc, rddIds = Seq(rdd.id))
    cleaner.cleanRDD(rdd.id)
    tester.assertCleanup
  }

  test("cleanup shuffle") {
    val rdd = newShuffleRDD
    rdd.count()
    val tester = new CleanerTester(sc, shuffleIds = Seq(0))
    cleaner.cleanShuffle(0)
    tester.assertCleanup
  }

  test("automatically cleanup RDD") {
    var rdd = newRDD.persist()
    rdd.count()
    
    // test that GC does not cause RDD cleanup due to a strong reference
    val preGCTester =  new CleanerTester(sc, rddIds = Seq(rdd.id))
    doGC()
    intercept[Exception] {
      preGCTester.assertCleanup(timeout(1000 millis))
    }

    // test that GC causes RDD cleanup after dereferencing the RDD
    val postGCTester = new CleanerTester(sc, rddIds = Seq(rdd.id))
    rdd = null  // make RDD out of scope
    doGC()
    postGCTester.assertCleanup
  }

  test("automatically cleanup shuffle") {
    var rdd = newShuffleRDD
    rdd.count()

    // test that GC does not cause shuffle cleanup due to a strong reference
    val preGCTester =  new CleanerTester(sc, shuffleIds = Seq(0))
    doGC()
    intercept[Exception] {
      preGCTester.assertCleanup(timeout(1000 millis))
    }

    // test that GC causes shuffle cleanup after dereferencing the RDD
    val postGCTester = new CleanerTester(sc, shuffleIds = Seq(0))
    rdd = null  // make RDD out of scope, so that corresponding shuffle goes out of scope
    doGC()
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
    for (i <- 1 to 1000) {
      buffer += randomRDD
    }

    val rddIds = sc.persistentRdds.keys.toSeq
    val shuffleIds = 0 until sc.newShuffleId

    val preGCTester =  new CleanerTester(sc, rddIds, shuffleIds)
    intercept[Exception] {
      preGCTester.assertCleanup(timeout(1000 millis))
    }

    // test that GC causes shuffle cleanup after dereferencing the RDD
    val postGCTester = new CleanerTester(sc, rddIds, shuffleIds)
    buffer.clear()
    doGC()
    postGCTester.assertCleanup
  }

  def newRDD = sc.makeRDD(1 to 10)

  def newPairRDD = newRDD.map(_ -> 1)

  def newShuffleRDD = newPairRDD.reduceByKey(_ + _)

  def doGC() {
    val weakRef = new WeakReference(new Object())
    val startTime = System.currentTimeMillis
    System.gc() // Make a best effort to run the garbage collection. It *usually* runs GC.
    System.runFinalization()  // Make a best effort to call finalizer on all cleaned objects.
    while(System.currentTimeMillis - startTime < 10000 && weakRef.get != null) {
      System.gc()
      System.runFinalization()
      Thread.sleep(100)
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

  logInfo("Attempting to validate before cleanup:\n" + uncleanedResourcesToString)
  preCleanupValidate()
  sc.cleaner.attachListener(cleanerListener)

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

  private def preCleanupValidate() {
    assert(rddIds.nonEmpty || shuffleIds.nonEmpty, "Nothing to cleanup")

    // Verify the RDDs have been persisted and blocks are present
    assert(rddIds.forall(sc.persistentRdds.contains),
      "One or more RDDs have not been persisted, cannot start cleaner test")
    assert(rddIds.forall(rddId => blockManager.master.contains(rddBlockId(rddId))),
      "One or more RDDs' blocks cannot be found in block manager, cannot start cleaner test")

    // Verify the shuffle ids are registered and blocks are present
    assert(shuffleIds.forall(mapOutputTrackerMaster.contains),
      "One or more shuffles have not been registered cannot start cleaner test")
    assert(shuffleIds.forall(shuffleId => diskBlockManager.contains(shuffleBlockId(shuffleId))),
      "One or more shuffles' blocks cannot be found in disk manager, cannot start cleaner test")
  }

  private def postCleanupValidate() {
    // Verify all the RDDs have been persisted
    assert(rddIds.forall(!sc.persistentRdds.contains(_)))
    assert(rddIds.forall(rddId => !blockManager.master.contains(rddBlockId(rddId))))

    // Verify all the shuffle have been deregistered and cleaned up
    assert(shuffleIds.forall(!mapOutputTrackerMaster.contains(_)))
    assert(shuffleIds.forall(shuffleId => !diskBlockManager.contains(shuffleBlockId(shuffleId))))
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