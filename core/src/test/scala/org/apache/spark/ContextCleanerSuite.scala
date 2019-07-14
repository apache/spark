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
import java.util.concurrent.TimeUnit

import scala.collection.mutable.HashSet
import scala.util.Random

import org.scalatest.BeforeAndAfter
import org.scalatest.concurrent.Eventually._
import org.scalatest.concurrent.PatienceConfiguration
import org.scalatest.time.SpanSugar._

import org.apache.spark.internal.{config, Logging}
import org.apache.spark.internal.config._
import org.apache.spark.rdd.{RDD, ReliableRDDCheckpointData}
import org.apache.spark.shuffle.sort.SortShuffleManager
import org.apache.spark.storage._

/**
 * An abstract base class for context cleaner tests, which sets up a context with a config
 * suitable for cleaner tests and provides some utility functions. Subclasses can use different
 * config options, in particular, a different shuffle manager class
 */
abstract class ContextCleanerSuiteBase(val shuffleManager: Class[_] = classOf[SortShuffleManager])
  extends SparkFunSuite with BeforeAndAfter with LocalSparkContext
{
  implicit val defaultTimeout = timeout(10.seconds)
  val conf = new SparkConf()
    .setMaster("local[2]")
    .setAppName("ContextCleanerSuite")
    .set(CLEANER_REFERENCE_TRACKING_BLOCKING, true)
    .set(CLEANER_REFERENCE_TRACKING_BLOCKING_SHUFFLE, true)
    .set(CLEANER_REFERENCE_TRACKING_CLEAN_CHECKPOINTS, true)
    .set(config.SHUFFLE_MANAGER, shuffleManager.getName)

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

  protected def newRDDWithShuffleDependencies():
      (RDD[(Int, Int)], Seq[ShuffleDependency[Int, Int, Int]]) = {
    def getAllDependencies(rdd: RDD[(Int, Int)]): Seq[Dependency[_]] = {
      rdd.dependencies ++ rdd.dependencies.flatMap { dep =>
        getAllDependencies(dep.rdd.asInstanceOf[RDD[(Int, Int)]])
      }
    }
    val rdd = newShuffleRDD()

    // Get all the shuffle dependencies
    val shuffleDeps = getAllDependencies(rdd)
      .filter(_.isInstanceOf[ShuffleDependency[_, _, _]])
      .map(_.asInstanceOf[ShuffleDependency[Int, Int, Int]])
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
    val startTimeNs = System.nanoTime()
    System.gc() // Make a best effort to run the garbage collection. It *usually* runs GC.
    // Wait until a weak reference object has been GCed
    while (System.nanoTime() - startTimeNs < TimeUnit.SECONDS.toNanos(10) && weakRef.get != null) {
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
      preGCTester.assertCleanup()(timeout(1.second))
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
      preGCTester.assertCleanup()(timeout(1.second))
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
      preGCTester.assertCleanup()(timeout(1.second))
    }

    // Test that GC causes broadcast cleanup after dereferencing the broadcast variable
    // Note broadcast is used after previous GC to avoid early collection by the JVM
    val postGCTester = new CleanerTester(sc, broadcastIds = Seq(broadcast.id))
    broadcast = null  // Make broadcast variable out of scope
    runGC()
    postGCTester.assertCleanup()
  }

  test("automatically cleanup normal checkpoint") {
    withTempDir { checkpointDir =>
      checkpointDir.delete()
      var rdd = newPairRDD()
      sc.setCheckpointDir(checkpointDir.toString)
      rdd.checkpoint()
      rdd.cache()
      rdd.collect()
      var rddId = rdd.id

      // Confirm the checkpoint directory exists
      assert(ReliableRDDCheckpointData.checkpointPath(sc, rddId).isDefined)
      val path = ReliableRDDCheckpointData.checkpointPath(sc, rddId).get
      val fs = path.getFileSystem(sc.hadoopConfiguration)
      assert(fs.exists(path))

      // the checkpoint is not cleaned by default (without the configuration set)
      var postGCTester = new CleanerTester(sc, Seq(rddId), Nil, Nil, Seq(rddId))
      rdd = null // Make RDD out of scope, ok if collected earlier
      runGC()
      postGCTester.assertCleanup()
      assert(!fs.exists(ReliableRDDCheckpointData.checkpointPath(sc, rddId).get))

      // Verify that checkpoints are NOT cleaned up if the config is not enabled
      sc.stop()
      val conf = new SparkConf()
        .setMaster("local[2]")
        .setAppName("cleanupCheckpoint")
        .set(CLEANER_REFERENCE_TRACKING_CLEAN_CHECKPOINTS, false)
      sc = new SparkContext(conf)
      rdd = newPairRDD()
      sc.setCheckpointDir(checkpointDir.toString)
      rdd.checkpoint()
      rdd.cache()
      rdd.collect()
      rddId = rdd.id

      // Confirm the checkpoint directory exists
      assert(fs.exists(ReliableRDDCheckpointData.checkpointPath(sc, rddId).get))

      // Reference rdd to defeat any early collection by the JVM
      rdd.count()

      // Test that GC causes checkpoint data cleanup after dereferencing the RDD
      postGCTester = new CleanerTester(sc, Seq(rddId))
      rdd = null // Make RDD out of scope
      runGC()
      postGCTester.assertCleanup()
      assert(fs.exists(ReliableRDDCheckpointData.checkpointPath(sc, rddId).get))
    }
  }

  test("automatically clean up local checkpoint") {
    // Note that this test is similar to the RDD cleanup
    // test because the same underlying mechanism is used!
    var rdd = newPairRDD().localCheckpoint()
    assert(rdd.checkpointData.isDefined)
    assert(rdd.checkpointData.get.checkpointRDD.isEmpty)
    rdd.count()
    assert(rdd.checkpointData.get.checkpointRDD.isDefined)

    // Test that GC does not cause checkpoint cleanup due to a strong reference
    val preGCTester = new CleanerTester(sc, rddIds = Seq(rdd.id))
    runGC()
    intercept[Exception] {
      preGCTester.assertCleanup()(timeout(1.second))
    }

    // Test that RDD going out of scope does cause the checkpoint blocks to be cleaned up
    val postGCTester = new CleanerTester(sc, rddIds = Seq(rdd.id))
    rdd = null
    runGC()
    postGCTester.assertCleanup()
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
      preGCTester.assertCleanup()(timeout(1.second))
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
      .setMaster("local-cluster[2, 1, 1024]")
      .setAppName("ContextCleanerSuite")
      .set(CLEANER_REFERENCE_TRACKING_BLOCKING, true)
      .set(CLEANER_REFERENCE_TRACKING_BLOCKING_SHUFFLE, true)
      .set(config.SHUFFLE_MANAGER, shuffleManager.getName)
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
      preGCTester.assertCleanup()(timeout(1.second))
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
 * Class to test whether RDDs, shuffles, etc. have been successfully cleaned.
 * The checkpoint here refers only to normal (reliable) checkpoints, not local checkpoints.
 */
class CleanerTester(
    sc: SparkContext,
    rddIds: Seq[Int] = Seq.empty,
    shuffleIds: Seq[Int] = Seq.empty,
    broadcastIds: Seq[Long] = Seq.empty,
    checkpointIds: Seq[Long] = Seq.empty)
  extends Logging {

  val toBeCleanedRDDIds = new HashSet[Int] ++= rddIds
  val toBeCleanedShuffleIds = new HashSet[Int] ++= shuffleIds
  val toBeCleanedBroadcstIds = new HashSet[Long] ++= broadcastIds
  val toBeCheckpointIds = new HashSet[Long] ++= checkpointIds
  val isDistributed = !sc.isLocal

  val cleanerListener = new CleanerListener {
    def rddCleaned(rddId: Int): Unit = {
      toBeCleanedRDDIds.synchronized { toBeCleanedRDDIds -= rddId }
      logInfo("RDD " + rddId + " cleaned")
    }

    def shuffleCleaned(shuffleId: Int): Unit = {
      toBeCleanedShuffleIds.synchronized { toBeCleanedShuffleIds -= shuffleId }
      logInfo("Shuffle " + shuffleId + " cleaned")
    }

    def broadcastCleaned(broadcastId: Long): Unit = {
      toBeCleanedBroadcstIds.synchronized { toBeCleanedBroadcstIds -= broadcastId }
      logInfo("Broadcast " + broadcastId + " cleaned")
    }

    def accumCleaned(accId: Long): Unit = {
      logInfo("Cleaned accId " + accId + " cleaned")
    }

    def checkpointCleaned(rddId: Long): Unit = {
      toBeCheckpointIds.synchronized { toBeCheckpointIds -= rddId }
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
      eventually(waitTimeout, interval(100.milliseconds)) {
        assert(isAllCleanedUp,
          "The following resources were not cleaned up:\n" + uncleanedResourcesToString)
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
    val s1 = toBeCleanedRDDIds.synchronized {
      toBeCleanedRDDIds.toSeq.sorted.mkString("[", ", ", "]")
    }
    val s2 = toBeCleanedShuffleIds.synchronized {
      toBeCleanedShuffleIds.toSeq.sorted.mkString("[", ", ", "]")
    }
    val s3 = toBeCleanedBroadcstIds.synchronized {
      toBeCleanedBroadcstIds.toSeq.sorted.mkString("[", ", ", "]")
    }
    s"""
       |\tRDDs = $s1
       |\tShuffles = $s2
       |\tBroadcasts = $s3
    """.stripMargin
  }

  private def isAllCleanedUp =
    toBeCleanedRDDIds.synchronized { toBeCleanedRDDIds.isEmpty } &&
    toBeCleanedShuffleIds.synchronized { toBeCleanedShuffleIds.isEmpty } &&
    toBeCleanedBroadcstIds.synchronized { toBeCleanedBroadcstIds.isEmpty } &&
    toBeCheckpointIds.synchronized { toBeCheckpointIds.isEmpty }

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
