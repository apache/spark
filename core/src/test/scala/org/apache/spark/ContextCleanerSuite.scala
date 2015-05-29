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

package org.apache.spark

import java.lang.ref.WeakReference

import scala.collection.mutable.{HashSet, SynchronizedSet}
import scala.language.existentials
import scala.util.Random

import org.scalatest.BeforeAndAfter
import org.scalatest.concurrent.{PatienceConfiguration, Eventually}
import org.scalatest.concurrent.Eventually._
import org.scalatest.time.SpanSugar._

import org.apache.spark.SparkContext._
import org.apache.spark.rdd.{RDDCheckpointData, RDD}
import org.apache.spark.storage._
import org.apache.spark.shuffle.hash.HashShuffleManager
import org.apache.spark.shuffle.sort.SortShuffleManager
import org.apache.spark.storage.BroadcastBlockId
import org.apache.spark.storage.RDDBlockId
import org.apache.spark.storage.ShuffleBlockId
import org.apache.spark.storage.ShuffleIndexBlockId

/**
 * An abstract base class for context cleaner tests, which sets up a context with a config
 * suitable for cleaner tests and provides some utility functions. Subclasses can use different
 * config options, in particular, a different shuffle manager class
 */
abstract class ContextCleanerSuiteBase(val shuffleManager: Class[_] = classOf[HashShuffleManager])
  extends SparkFunSuite with BeforeAndAfter with LocalSparkContext
{
  implicit val defaultTimeout = timeout(10000 millis)
  val conf = new SparkConf()
    .setMaster("local[2]")
    .setAppName("ContextCleanerSuite")
    .set("spark.cleaner.referenceTracking.blocking", "true")
    .set("spark.cleaner.referenceTracking.blocking.shuffle", "true")
    .set("spark.shuffle.manager", shuffleManager.getName)

  before {
    sc = new SparkContext(conf)
  }

  after {
    if (sc != null) {
      sc.stop()
      sc = null
    }
  }

  // ------ Helper functions ------

  protected def newRDD() = sc.makeRDD(1 to 10)
  protected def newPairRDD() = newRDD().map(_ -> 1)
  protected def newShuffleRDD() = newPairRDD().reduceByKey(_ + _)
  protected def newBroadcast() = sc.broadcast(1 to 100)

  protected def newRDDWithShuffleDependencies(): (RDD[_], Seq[ShuffleDependency[_, _, _]]) = {
    def getAllDependencies(rdd: RDD[_]): Seq[Dependency[_]] = {
      rdd.dependencies ++ rdd.dependencies.flatMap { dep =>
        getAllDependencies(dep.rdd)
      }
    }
    val rdd = newShuffleRDD()

    // Get all the shuffle dependencies
    val shuffleDeps = getAllDependencies(rdd)
      .filter(_.isInstanceOf[ShuffleDependency[_, _, _]])
      .map(_.asInstanceOf[ShuffleDependency[_, _, _]])
    (rdd, shuffleDeps)
  }

  protected def randomRdd() = {
    val rdd: RDD[_] = Random.nextInt(3) match {
      case 0 => newRDD()
      case 1 => newShuffleRDD()
      case 2 => newPairRDD.join(newPairRDD())
    }
    if (Random.nextBoolean()) rdd.persist()
    rdd.count()
    rdd
  }

  /** Run GC and make sure it actually has run */
  protected def runGC() {
    val weakRef = new WeakReference(new Object())
    val startTime = System.currentTimeMillis
    System.gc() // Make a best effort to run the garbage collection. It *usually* runs GC.
    // Wait until a weak reference object has been GCed
    while (System.currentTimeMillis - startTime < 10000 && weakRef.get != null) {
      System.gc()
      Thread.sleep(200)
    }
  }

  protected def cleaner = sc.cleaner.get
}


/**
 * Basic ContextCleanerSuite, which uses sort-based shuffle
 */
class ContextCleanerSuite extends ContextCleanerSuiteBase {
  test("cleanup RDD") {
    val rdd = newRDD().persist()
    val collected = rdd.collect().toList
    val tester = new CleanerTester(sc, rddIds = Seq(rdd.id))

    // Explicit cleanup
    cleaner.doCleanupRDD(rdd.id, blocking = true)
    tester.assertCleanup()

    // Verify that RDDs can be re-executed after cleaning up
    assert(rdd.collect().toList === collected)
  }

  test("cleanup shuffle") {
    val (rdd, shuffleDeps) = newRDDWithShuffleDependencies()
    val collected = rdd.collect().toList
    val tester = new CleanerTester(sc, shuffleIds = shuffleDeps.map(_.shuffleId))

    // Explicit cleanup
    shuffleDeps.foreach(s => cleaner.doCleanupShuffle(s.shuffleId, blocking = true))
    tester.assertCleanup()

    // Verify that shuffles can be re-executed after cleaning up
    assert(rdd.collect().toList.equals(collected))
  }

  test("cleanup broadcast") {
    val broadcast = newBroadcast()
    val tester = new CleanerTester(sc, broadcastIds = Seq(broadcast.id))

    // Explicit cleanup
    cleaner.doCleanupBroadcast(broadcast.id, blocking = true)
    tester.assertCleanup()
  }

  test("automatically cleanup RDD") {
    var rdd = newRDD().persist()
    rdd.count()

    // Test that GC does not cause RDD cleanup due to a strong reference
    val preGCTester = new CleanerTester(sc, rddIds = Seq(rdd.id))
    runGC()
    intercept[Exception] {
      preGCTester.assertCleanup()(timeout(1000 millis))
    }

    // Test that GC causes RDD cleanup after dereferencing the RDD
    // Note rdd is used after previous GC to avoid early collection by the JVM
    val postGCTester = new CleanerTester(sc, rddIds = Seq(rdd.id))
    rdd = null // Make RDD out of scope
    runGC()
    postGCTester.assertCleanup()
  }

  test("automatically cleanup shuffle") {
    var rdd = newShuffleRDD()
    rdd.count()

    // Test that GC does not cause shuffle cleanup due to a strong reference
    val preGCTester = new CleanerTester(sc, shuffleIds = Seq(0))
    runGC()
    intercept[Exception] {
      preGCTester.assertCleanup()(timeout(1000 millis))
    }
    rdd.count()  // Defeat early collection by the JVM

    // Test that GC causes shuffle cleanup after dereferencing the RDD
    val postGCTester = new CleanerTester(sc, shuffleIds = Seq(0))
    rdd = null  // Make RDD out of scope, so that corresponding shuffle goes out of scope
    runGC()
    postGCTester.assertCleanup()
  }

  test("automatically cleanup broadcast") {
    var broadcast = newBroadcast()

    // Test that GC does not cause broadcast cleanup due to a strong reference
    val preGCTester = new CleanerTester(sc, broadcastIds = Seq(broadcast.id))
    runGC()
    intercept[Exception] {
      preGCTester.assertCleanup()(timeout(1000 millis))
    }

    // Test that GC causes broadcast cleanup after dereferencing the broadcast variable
    // Note broadcast is used after previous GC to avoid early collection by the JVM
    val postGCTester = new CleanerTester(sc, broadcastIds = Seq(broadcast.id))
    broadcast = null  // Make broadcast variable out of scope
    runGC()
    postGCTester.assertCleanup()
  }

  test("automatically cleanup checkpoint") {
    val checkpointDir = java.io.File.createTempFile("temp", "")
    checkpointDir.deleteOnExit()
    checkpointDir.delete()
    var rdd = newPairRDD
    sc.setCheckpointDir(checkpointDir.toString)
    rdd.checkpoint()
    rdd.cache()
    rdd.collect()
    var rddId = rdd.id

    // Confirm the checkpoint directory exists
    assert(RDDCheckpointData.rddCheckpointDataPath(sc, rddId).isDefined)
    val path = RDDCheckpointData.rddCheckpointDataPath(sc, rddId).get
    val fs = path.getFileSystem(sc.hadoopConfiguration)
    assert(fs.exists(path))

    // the checkpoint is not cleaned by default (without the configuration set)
    var postGCTester = new CleanerTester(sc, Seq(rddId), Nil, Nil, Nil)
    rdd = null // Make RDD out of scope, ok if collected earlier
    runGC()
    postGCTester.assertCleanup()
    assert(fs.exists(RDDCheckpointData.rddCheckpointDataPath(sc, rddId).get))

    sc.stop()
    val conf = new SparkConf().setMaster("local[2]").setAppName("cleanupCheckpoint").
      set("spark.cleaner.referenceTracking.cleanCheckpoints", "true")
    sc = new SparkContext(conf)
    rdd = newPairRDD
    sc.setCheckpointDir(checkpointDir.toString)
    rdd.checkpoint()
    rdd.cache()
    rdd.collect()
    rddId = rdd.id

    // Confirm the checkpoint directory exists
    assert(fs.exists(RDDCheckpointData.rddCheckpointDataPath(sc, rddId).get))

    // Reference rdd to defeat any early collection by the JVM
    rdd.count()

    // Test that GC causes checkpoint data cleanup after dereferencing the RDD
    postGCTester = new CleanerTester(sc, Seq(rddId), Nil, Nil, Seq(rddId))
    rdd = null // Make RDD out of scope
    runGC()
    postGCTester.assertCleanup()
    assert(!fs.exists(RDDCheckpointData.rddCheckpointDataPath(sc, rddId).get))
  }

  test("automatically cleanup RDD + shuffle + broadcast") {
    val numRdds = 100
    val numBroadcasts = 4 // Broadcasts are more costly
    val rddBuffer = (1 to numRdds).map(i => randomRdd()).toBuffer
    val broadcastBuffer = (1 to numBroadcasts).map(i => newBroadcast()).toBuffer
    val rddIds = sc.persistentRdds.keys.toSeq
    val shuffleIds = 0 until sc.newShuffleId
    val broadcastIds = broadcastBuffer.map(_.id)

    val preGCTester = new CleanerTester(sc, rddIds, shuffleIds, broadcastIds)
    runGC()
    intercept[Exception] {
      preGCTester.assertCleanup()(timeout(1000 millis))
    }

    // Test that GC triggers the cleanup of all variables after the dereferencing them
    val postGCTester = new CleanerTester(sc, rddIds, shuffleIds, broadcastIds)
    broadcastBuffer.clear()
    rddBuffer.clear()
    runGC()
    postGCTester.assertCleanup()

    // Make sure the broadcasted task closure no longer exists after GC.
    val taskClosureBroadcastId = broadcastIds.max + 1
    assert(sc.env.blockManager.master.getMatchingBlockIds({
      case BroadcastBlockId(`taskClosureBroadcastId`, _) => true
      case _ => false
    }, askSlaves = true).isEmpty)
  }

  test("automatically cleanup RDD + shuffle + broadcast in distributed mode") {
    sc.stop()

    val conf2 = new SparkConf()
      .setMaster("local-cluster[2, 1, 512]")
      .setAppName("ContextCleanerSuite")
      .set("spark.cleaner.referenceTracking.blocking", "true")
      .set("spark.cleaner.referenceTracking.blocking.shuffle", "true")
      .set("spark.shuffle.manager", shuffleManager.getName)
    sc = new SparkContext(conf2)

    val numRdds = 10
    val numBroadcasts = 4 // Broadcasts are more costly
    val rddBuffer = (1 to numRdds).map(i => randomRdd()).toBuffer
    val broadcastBuffer = (1 to numBroadcasts).map(i => newBroadcast()).toBuffer
    val rddIds = sc.persistentRdds.keys.toSeq
    val shuffleIds = 0 until sc.newShuffleId
    val broadcastIds = broadcastBuffer.map(_.id)

    val preGCTester = new CleanerTester(sc, rddIds, shuffleIds, broadcastIds)
    runGC()
    intercept[Exception] {
      preGCTester.assertCleanup()(timeout(1000 millis))
    }

    // Test that GC triggers the cleanup of all variables after the dereferencing them
    val postGCTester = new CleanerTester(sc, rddIds, shuffleIds, broadcastIds)
    broadcastBuffer.clear()
    rddBuffer.clear()
    runGC()
    postGCTester.assertCleanup()

    // Make sure the broadcasted task closure no longer exists after GC.
    val taskClosureBroadcastId = broadcastIds.max + 1
    assert(sc.env.blockManager.master.getMatchingBlockIds({
      case BroadcastBlockId(`taskClosureBroadcastId`, _) => true
      case _ => false
    }, askSlaves = true).isEmpty)
  }
}


/**
 * A copy of the shuffle tests for sort-based shuffle
 */
class SortShuffleContextCleanerSuite extends ContextCleanerSuiteBase(classOf[SortShuffleManager]) {
  test("cleanup shuffle") {
    val (rdd, shuffleDeps) = newRDDWithShuffleDependencies()
    val collected = rdd.collect().toList
    val tester = new CleanerTester(sc, shuffleIds = shuffleDeps.map(_.shuffleId))

    // Explicit cleanup
    shuffleDeps.foreach(s => cleaner.doCleanupShuffle(s.shuffleId, blocking = true))
    tester.assertCleanup()

    // Verify that shuffles can be re-executed after cleaning up
    assert(rdd.collect().toList.equals(collected))
  }

  test("automatically cleanup shuffle") {
    var rdd = newShuffleRDD()
    rdd.count()

    // Test that GC does not cause shuffle cleanup due to a strong reference
    val preGCTester = new CleanerTester(sc, shuffleIds = Seq(0))
    runGC()
    intercept[Exception] {
      preGCTester.assertCleanup()(timeout(1000 millis))
    }
    rdd.count()  // Defeat early collection by the JVM

    // Test that GC causes shuffle cleanup after dereferencing the RDD
    val postGCTester = new CleanerTester(sc, shuffleIds = Seq(0))
    rdd = null  // Make RDD out of scope, so that corresponding shuffle goes out of scope
    runGC()
    postGCTester.assertCleanup()
  }

  test("automatically cleanup RDD + shuffle + broadcast in distributed mode") {
    sc.stop()

    val conf2 = new SparkConf()
      .setMaster("local-cluster[2, 1, 512]")
      .setAppName("ContextCleanerSuite")
      .set("spark.cleaner.referenceTracking.blocking", "true")
      .set("spark.cleaner.referenceTracking.blocking.shuffle", "true")
      .set("spark.shuffle.manager", shuffleManager.getName)
    sc = new SparkContext(conf2)

    val numRdds = 10
    val numBroadcasts = 4 // Broadcasts are more costly
    val rddBuffer = (1 to numRdds).map(i => randomRdd).toBuffer
    val broadcastBuffer = (1 to numBroadcasts).map(i => newBroadcast).toBuffer
    val rddIds = sc.persistentRdds.keys.toSeq
    val shuffleIds = 0 until sc.newShuffleId()
    val broadcastIds = broadcastBuffer.map(_.id)

    val preGCTester = new CleanerTester(sc, rddIds, shuffleIds, broadcastIds)
    runGC()
    intercept[Exception] {
      preGCTester.assertCleanup()(timeout(1000 millis))
    }

    // Test that GC triggers the cleanup of all variables after the dereferencing them
    val postGCTester = new CleanerTester(sc, rddIds, shuffleIds, broadcastIds)
    broadcastBuffer.clear()
    rddBuffer.clear()
    runGC()
    postGCTester.assertCleanup()

    // Make sure the broadcasted task closure no longer exists after GC.
    val taskClosureBroadcastId = broadcastIds.max + 1
    assert(sc.env.blockManager.master.getMatchingBlockIds({
      case BroadcastBlockId(`taskClosureBroadcastId`, _) => true
      case _ => false
    }, askSlaves = true).isEmpty)
  }
}


/** Class to test whether RDDs, shuffles, etc. have been successfully cleaned. */
class CleanerTester(
    sc: SparkContext,
    rddIds: Seq[Int] = Seq.empty,
    shuffleIds: Seq[Int] = Seq.empty,
    broadcastIds: Seq[Long] = Seq.empty,
    checkpointIds: Seq[Long] = Seq.empty)
  extends Logging {

  val toBeCleanedRDDIds = new HashSet[Int] with SynchronizedSet[Int] ++= rddIds
  val toBeCleanedShuffleIds = new HashSet[Int] with SynchronizedSet[Int] ++= shuffleIds
  val toBeCleanedBroadcstIds = new HashSet[Long] with SynchronizedSet[Long] ++= broadcastIds
  val toBeCheckpointIds = new HashSet[Long] with SynchronizedSet[Long] ++= checkpointIds
  val isDistributed = !sc.isLocal

  val cleanerListener = new CleanerListener {
    def rddCleaned(rddId: Int): Unit = {
      toBeCleanedRDDIds -= rddId
      logInfo("RDD " + rddId + " cleaned")
    }

    def shuffleCleaned(shuffleId: Int): Unit = {
      toBeCleanedShuffleIds -= shuffleId
      logInfo("Shuffle " + shuffleId + " cleaned")
    }

    def broadcastCleaned(broadcastId: Long): Unit = {
      toBeCleanedBroadcstIds -= broadcastId
      logInfo("Broadcast " + broadcastId + " cleaned")
    }

    def accumCleaned(accId: Long): Unit = {
      logInfo("Cleaned accId " + accId + " cleaned")
    }

    def checkpointCleaned(rddId: Long): Unit = {
      toBeCheckpointIds -= rddId
      logInfo("checkpoint  " + rddId + " cleaned")
    }
  }

  val MAX_VALIDATION_ATTEMPTS = 10
  val VALIDATION_ATTEMPT_INTERVAL = 100

  logInfo("Attempting to validate before cleanup:\n" + uncleanedResourcesToString)
  preCleanupValidate()
  sc.cleaner.get.attachListener(cleanerListener)

  /** Assert that all the stuff has been cleaned up */
  def assertCleanup()(implicit waitTimeout: PatienceConfiguration.Timeout) {
    try {
      eventually(waitTimeout, interval(100 millis)) {
        assert(isAllCleanedUp)
      }
      postCleanupValidate()
    } finally {
      logInfo("Resources left from cleaning up:\n" + uncleanedResourcesToString)
    }
  }

  /** Verify that RDDs, shuffles, etc. occupy resources */
  private def preCleanupValidate() {
    assert(rddIds.nonEmpty || shuffleIds.nonEmpty || broadcastIds.nonEmpty ||
      checkpointIds.nonEmpty, "Nothing to cleanup")

    // Verify the RDDs have been persisted and blocks are present
    rddIds.foreach { rddId =>
      assert(
        sc.persistentRdds.contains(rddId),
        "RDD " + rddId + " have not been persisted, cannot start cleaner test"
      )

      assert(
        !getRDDBlocks(rddId).isEmpty,
        "Blocks of RDD " + rddId + " cannot be found in block manager, " +
          "cannot start cleaner test"
      )
    }

    // Verify the shuffle ids are registered and blocks are present
    shuffleIds.foreach { shuffleId =>
      assert(
        mapOutputTrackerMaster.containsShuffle(shuffleId),
        "Shuffle " + shuffleId + " have not been registered, cannot start cleaner test"
      )

      assert(
        !getShuffleBlocks(shuffleId).isEmpty,
        "Blocks of shuffle " + shuffleId + " cannot be found in block manager, " +
          "cannot start cleaner test"
      )
    }

    // Verify that the broadcast blocks are present
    broadcastIds.foreach { broadcastId =>
      assert(
        !getBroadcastBlocks(broadcastId).isEmpty,
        "Blocks of broadcast " + broadcastId + "cannot be found in block manager, " +
          "cannot start cleaner test"
      )
    }
  }

  /**
   * Verify that RDDs, shuffles, etc. do not occupy resources. Tests multiple times as there is
   * as there is not guarantee on how long it will take clean up the resources.
   */
  private def postCleanupValidate() {
    // Verify the RDDs have been persisted and blocks are present
    rddIds.foreach { rddId =>
      assert(
        !sc.persistentRdds.contains(rddId),
        "RDD " + rddId + " was not cleared from sc.persistentRdds"
      )

      assert(
        getRDDBlocks(rddId).isEmpty,
        "Blocks of RDD " + rddId + " were not cleared from block manager"
      )
    }

    // Verify the shuffle ids are registered and blocks are present
    shuffleIds.foreach { shuffleId =>
      assert(
        !mapOutputTrackerMaster.containsShuffle(shuffleId),
        "Shuffle " + shuffleId + " was not deregistered from map output tracker"
      )

      assert(
        getShuffleBlocks(shuffleId).isEmpty,
        "Blocks of shuffle " + shuffleId + " were not cleared from block manager"
      )
    }

    // Verify that the broadcast blocks are present
    broadcastIds.foreach { broadcastId =>
      assert(
        getBroadcastBlocks(broadcastId).isEmpty,
        "Blocks of broadcast " + broadcastId + " were not cleared from block manager"
      )
    }
  }

  private def uncleanedResourcesToString = {
    s"""
      |\tRDDs = ${toBeCleanedRDDIds.toSeq.sorted.mkString("[", ", ", "]")}
      |\tShuffles = ${toBeCleanedShuffleIds.toSeq.sorted.mkString("[", ", ", "]")}
      |\tBroadcasts = ${toBeCleanedBroadcstIds.toSeq.sorted.mkString("[", ", ", "]")}
    """.stripMargin
  }

  private def isAllCleanedUp =
    toBeCleanedRDDIds.isEmpty &&
    toBeCleanedShuffleIds.isEmpty &&
    toBeCleanedBroadcstIds.isEmpty &&
    toBeCheckpointIds.isEmpty

  private def getRDDBlocks(rddId: Int): Seq[BlockId] = {
    blockManager.master.getMatchingBlockIds( _ match {
      case RDDBlockId(`rddId`, _) => true
      case _ => false
    }, askSlaves = true)
  }

  private def getShuffleBlocks(shuffleId: Int): Seq[BlockId] = {
    blockManager.master.getMatchingBlockIds( _ match {
      case ShuffleBlockId(`shuffleId`, _, _) => true
      case ShuffleIndexBlockId(`shuffleId`, _, _) => true
      case _ => false
    }, askSlaves = true)
  }

  private def getBroadcastBlocks(broadcastId: Long): Seq[BlockId] = {
    blockManager.master.getMatchingBlockIds( _ match {
      case BroadcastBlockId(`broadcastId`, _) => true
      case _ => false
    }, askSlaves = true)
  }

  private def blockManager = sc.env.blockManager
  private def mapOutputTrackerMaster = sc.env.mapOutputTracker.asInstanceOf[MapOutputTrackerMaster]
}
