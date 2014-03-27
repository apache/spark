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
import scala.util.Random

import org.scalatest.{BeforeAndAfter, FunSuite}
import org.scalatest.concurrent.Eventually
import org.scalatest.concurrent.Eventually._
import org.scalatest.time.SpanSugar._

import org.apache.spark.SparkContext._
import org.apache.spark.rdd.RDD
import org.apache.spark.storage.{BroadcastBlockId, RDDBlockId, ShuffleBlockId}

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
    tester.assertCleanup()

    // Verify that RDDs can be re-executed after cleaning up
    assert(rdd.collect().toList === collected)
  }

  test("cleanup shuffle") {
    val (rdd, shuffleDeps) = newRDDWithShuffleDependencies
    val collected = rdd.collect().toList
    val tester = new CleanerTester(sc, shuffleIds = shuffleDeps.map(_.shuffleId))

    // Explicit cleanup
    shuffleDeps.foreach(s => cleaner.cleanupShuffle(s))
    tester.assertCleanup()

    // Verify that shuffles can be re-executed after cleaning up
    assert(rdd.collect().toList === collected)
  }

  test("cleanup broadcast") {
    val broadcast = newBroadcast
    val tester = new CleanerTester(sc, broadcastIds = Seq(broadcast.id))

    // Explicit cleanup
    cleaner.cleanupBroadcast(broadcast)
    tester.assertCleanup()
  }

  test("automatically cleanup RDD") {
    var rdd = newRDD.persist()
    rdd.count()

    // Test that GC does not cause RDD cleanup due to a strong reference
    val preGCTester =  new CleanerTester(sc, rddIds = Seq(rdd.id))
    runGC()
    intercept[Exception] {
      preGCTester.assertCleanup()(timeout(1000 millis))
    }

    // Test that GC causes RDD cleanup after dereferencing the RDD
    val postGCTester = new CleanerTester(sc, rddIds = Seq(rdd.id))
    rdd = null // Make RDD out of scope
    runGC()
    postGCTester.assertCleanup()
  }

  test("automatically cleanup shuffle") {
    var rdd = newShuffleRDD
    rdd.count()

    // Test that GC does not cause shuffle cleanup due to a strong reference
    val preGCTester = new CleanerTester(sc, shuffleIds = Seq(0))
    runGC()
    intercept[Exception] {
      preGCTester.assertCleanup()(timeout(1000 millis))
    }

    // Test that GC causes shuffle cleanup after dereferencing the RDD
    val postGCTester = new CleanerTester(sc, shuffleIds = Seq(0))
    rdd = null  // Make RDD out of scope, so that corresponding shuffle goes out of scope
    runGC()
    postGCTester.assertCleanup()
  }

  test("automatically cleanup broadcast") {
    var broadcast = newBroadcast

    // Test that GC does not cause broadcast cleanup due to a strong reference
    val preGCTester =  new CleanerTester(sc, broadcastIds = Seq(broadcast.id))
    runGC()
    intercept[Exception] {
      preGCTester.assertCleanup()(timeout(1000 millis))
    }

    // Test that GC causes broadcast cleanup after dereferencing the broadcast variable
    val postGCTester = new CleanerTester(sc, broadcastIds = Seq(broadcast.id))
    broadcast = null  // Make broadcast variable out of scope
    runGC()
    postGCTester.assertCleanup()
  }

  test("automatically cleanup RDD + shuffle + broadcast") {
    val numRdds = 100
    val numBroadcasts = 4 // Broadcasts are more costly
    val rddBuffer = (1 to numRdds).map(i => randomRdd).toBuffer
    val broadcastBuffer = (1 to numBroadcasts).map(i => randomBroadcast).toBuffer
    val rddIds = sc.persistentRdds.keys.toSeq
    val shuffleIds = 0 until sc.newShuffleId
    val broadcastIds = 0L until numBroadcasts

    val preGCTester =  new CleanerTester(sc, rddIds, shuffleIds, broadcastIds)
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
  }

  def newRDD = sc.makeRDD(1 to 10)
  def newPairRDD = newRDD.map(_ -> 1)
  def newShuffleRDD = newPairRDD.reduceByKey(_ + _)
  def newBroadcast = sc.broadcast(1 to 100)
  def newRDDWithShuffleDependencies: (RDD[_], Seq[ShuffleDependency[_, _]]) = {
    def getAllDependencies(rdd: RDD[_]): Seq[Dependency[_]] = {
      rdd.dependencies ++ rdd.dependencies.flatMap { dep =>
        getAllDependencies(dep.rdd)
      }
    }
    val rdd = newShuffleRDD

    // Get all the shuffle dependencies
    val shuffleDeps = getAllDependencies(rdd)
      .filter(_.isInstanceOf[ShuffleDependency[_, _]])
      .map(_.asInstanceOf[ShuffleDependency[_, _]])
    (rdd, shuffleDeps)
  }

  def randomRdd = {
    val rdd: RDD[_] = Random.nextInt(3) match {
      case 0 => newRDD
      case 1 => newShuffleRDD
      case 2 => newPairRDD.join(newPairRDD)
    }
    if (Random.nextBoolean()) rdd.persist()
    rdd.count()
    rdd
  }

  def randomBroadcast = {
    sc.broadcast(Random.nextInt(Int.MaxValue))
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
class CleanerTester(
    sc: SparkContext,
    rddIds: Seq[Int] = Seq.empty,
    shuffleIds: Seq[Int] = Seq.empty,
    broadcastIds: Seq[Long] = Seq.empty)
  extends Logging {

  val toBeCleanedRDDIds = new HashSet[Int] with SynchronizedSet[Int] ++= rddIds
  val toBeCleanedShuffleIds = new HashSet[Int] with SynchronizedSet[Int] ++= shuffleIds
  val toBeCleanedBroadcstIds = new HashSet[Long] with SynchronizedSet[Long] ++= broadcastIds

  val cleanerListener = new CleanerListener {
    def rddCleaned(rddId: Int): Unit = {
      toBeCleanedRDDIds -= rddId
      logInfo("RDD "+ rddId + " cleaned")
    }

    def shuffleCleaned(shuffleId: Int): Unit = {
      toBeCleanedShuffleIds -= shuffleId
      logInfo("Shuffle " + shuffleId + " cleaned")
    }

    def broadcastCleaned(broadcastId: Long): Unit = {
      toBeCleanedBroadcstIds -= broadcastId
      logInfo("Broadcast" + broadcastId + " cleaned")
    }
  }

  val MAX_VALIDATION_ATTEMPTS = 10
  val VALIDATION_ATTEMPT_INTERVAL = 100

  logInfo("Attempting to validate before cleanup:\n" + uncleanedResourcesToString)
  preCleanupValidate()
  sc.cleaner.attachListener(cleanerListener)

  /** Assert that all the stuff has been cleaned up */
  def assertCleanup()(implicit waitTimeout: Eventually.Timeout) {
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
    assert(rddIds.nonEmpty || shuffleIds.nonEmpty || broadcastIds.nonEmpty, "Nothing to cleanup")

    // Verify the RDDs have been persisted and blocks are present
    assert(rddIds.forall(sc.persistentRdds.contains),
      "One or more RDDs have not been persisted, cannot start cleaner test")
    assert(rddIds.forall(rddId => blockManager.master.contains(rddBlockId(rddId))),
      "One or more RDDs' blocks cannot be found in block manager, cannot start cleaner test")

    // Verify the shuffle ids are registered and blocks are present
    assert(shuffleIds.forall(mapOutputTrackerMaster.containsShuffle),
      "One or more shuffles have not been registered cannot start cleaner test")
    assert(shuffleIds.forall(sid => diskBlockManager.containsBlock(shuffleBlockId(sid))),
      "One or more shuffles' blocks cannot be found in disk manager, cannot start cleaner test")

    // Verify that the broadcast is in the driver's block manager
    assert(broadcastIds.forall(bid => blockManager.getLevel(broadcastBlockId(bid)).isDefined),
      "One ore more broadcasts have not been persisted in the driver's block manager")
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
        // Verify all RDDs have been unpersisted
        assert(rddIds.forall(!sc.persistentRdds.contains(_)))
        assert(rddIds.forall(rddId => !blockManager.master.contains(rddBlockId(rddId))))

        // Verify all shuffles have been deregistered and cleaned up
        assert(shuffleIds.forall(!mapOutputTrackerMaster.containsShuffle(_)))
        assert(shuffleIds.forall(sid => !diskBlockManager.containsBlock(shuffleBlockId(sid))))

        // Verify all broadcasts have been unpersisted
        assert(broadcastIds.forall { bid =>
          blockManager.master.askForStorageLevels(broadcastBlockId(bid)).isEmpty
        })

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
      |\tBroadcasts = ${toBeCleanedBroadcstIds.mkString("[", ", ", "]")}
    """.stripMargin
  }

  private def isAllCleanedUp =
    toBeCleanedRDDIds.isEmpty &&
    toBeCleanedShuffleIds.isEmpty &&
    toBeCleanedBroadcstIds.isEmpty

  private def rddBlockId(rddId: Int) = RDDBlockId(rddId, 0)
  private def shuffleBlockId(shuffleId: Int) = ShuffleBlockId(shuffleId, 0, 0)
  private def broadcastBlockId(broadcastId: Long) = BroadcastBlockId(broadcastId)

  private def blockManager = sc.env.blockManager
  private def diskBlockManager = blockManager.diskBlockManager
  private def mapOutputTrackerMaster = sc.env.mapOutputTracker.asInstanceOf[MapOutputTrackerMaster]
}
