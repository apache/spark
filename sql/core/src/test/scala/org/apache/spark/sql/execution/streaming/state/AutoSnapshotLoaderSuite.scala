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

package org.apache.spark.sql.execution.streaming.state

import scala.collection.mutable.ListBuffer

import org.apache.spark.SparkFunSuite

/**
 * Suite to test [[AutoSnapshotLoader]]. Tests different behaviors including
 * when repair is enabled/disabled, when numFailuresBeforeActivating is set,
 * when maxChangeFileReplay is set.
 */
class AutoSnapshotLoaderSuite extends SparkFunSuite {
  test("successful snapshot load without auto repair") {
    // Test auto repair on or off
    Seq(true, false).foreach { enabled =>
      val loader = new TestAutoSnapshotLoader(
        autoSnapshotRepairEnabled = enabled,
        eligibleSnapshots = Seq(2, 4),
        failSnapshots = Seq.empty)

      val (versionLoaded, autoRepairCompleted) = loader.loadSnapshot(5)
      assert(!autoRepairCompleted)
      assert(versionLoaded == 4, "Should load the latest snapshot version")
      assert(loader.getRequestedSnapshotVersions == Seq(4),
        "Should have requested only the latest snapshot version")
    }
  }

  test("snapshot load failure gets repaired") {
    def createLoader(autoRepair: Boolean): TestAutoSnapshotLoader =
      new TestAutoSnapshotLoader(
        autoSnapshotRepairEnabled = autoRepair,
        eligibleSnapshots = Seq(2, 4),
        failSnapshots = Seq(4))

    // load without auto repair enabled
    var loader = createLoader(autoRepair = false)

    // This should fail to load v5 due to snapshot 4 failure, even though snapshot 2 exists
    val ex = intercept[TestLoadException] {
      loader.loadSnapshot(5)
    }
    assert(ex.snapshotVersion == 4, "Load failure should be due to version 4")

    // Now try to load with auto repair enabled
    loader = createLoader(autoRepair = true)
    val (versionLoaded, autoRepairCompleted) = loader.loadSnapshot(5)
    assert(autoRepairCompleted)
    assert(versionLoaded == 2, "Should have loaded the snapshot version before the corrupt one")
    assert(loader.getRequestedSnapshotVersions == Seq(4, 2))
  }

  test("repair works even when all snapshots are corrupt") {
    val loader = new TestAutoSnapshotLoader(
      autoSnapshotRepairEnabled = true,
      eligibleSnapshots = Seq(2, 4),
      failSnapshots = Seq(2, 4))

    val (versionLoaded, autoRepairCompleted) = loader.loadSnapshot(5)
    assert(autoRepairCompleted)
    assert(versionLoaded == 0, "Load 0 since no good snapshots available")
    assert(loader.getRequestedSnapshotVersions == Seq(4, 2, 0))
  }

  test("number of failures before activating auto repair") {
    def createLoader(numFailures: Int): TestAutoSnapshotLoader =
      new TestAutoSnapshotLoader(
        autoSnapshotRepairEnabled = true,
        numFailuresBeforeActivating = numFailures,
        eligibleSnapshots = Seq(2, 4),
        failSnapshots = Seq(4))

    (1 to 5).foreach { numFailures =>
      val loader = createLoader(numFailures)
      val (versionLoaded, autoRepairCompleted) = loader.loadSnapshot(5)
      assert(autoRepairCompleted)
      assert(versionLoaded == 2, "Should have loaded the snapshot version before the corrupt one")
      assert(loader.getRequestedSnapshotVersions == Seq.fill(numFailures)(4) :+ 2,
        s"should have tried to load version 4 $numFailures times before falling back to version 2")
    }
  }

  test("maximum change file replay") {
    def createLoader(maxChangeFileReplay: Int, fail: Seq[Long]): TestAutoSnapshotLoader =
      new TestAutoSnapshotLoader(
        autoSnapshotRepairEnabled = true,
        maxChangeFileReplay = maxChangeFileReplay,
        eligibleSnapshots = Seq(2, 4, 5),
        failSnapshots = fail)

    var loader = createLoader(maxChangeFileReplay = 1, fail = Seq(5))
    // repair with max change file replay = 1, should load snapshot 4
    val (versionLoaded, autoRepairCompleted) = loader.loadSnapshot(5)
    assert(autoRepairCompleted)
    assert(versionLoaded == 4)
    assert(loader.getRequestedSnapshotVersions == Seq(5, 4))

    // repair with max change file replay = 2, should fail since we can't use the older snapshots
    loader = createLoader(maxChangeFileReplay = 2, fail = Seq(5, 4))
    val ex = intercept[StateStoreAutoSnapshotRepairFailed] {
      loader.loadSnapshot(5)
    }

    checkError(
      exception = ex,
      condition = "CANNOT_LOAD_STATE_STORE.AUTO_SNAPSHOT_REPAIR_FAILED",
      parameters = Map(
        "latestSnapshot" -> "5",
        "stateStoreId" -> "test",
        "selectedSnapshots" -> "4", // only selected 4 due to maxChangeFileReplay = 2
        "eligibleSnapshots" -> "4,2,0")
    )
    assert(loader.getRequestedSnapshotVersions == Seq(5, 4))
    assert(ex.getCause.asInstanceOf[TestLoadException].snapshotVersion == 4)

    // repair with max change file replay = 3, should load snapshot 2
    loader = createLoader(maxChangeFileReplay = 3, fail = Seq(5, 4))
    val (versionLoaded_, autoRepairCompleted_) = loader.loadSnapshot(5)
    assert(autoRepairCompleted_)
    assert(versionLoaded_ == 2)
    assert(loader.getRequestedSnapshotVersions == Seq(5, 4, 2))
  }
}

/**
 * A test implementation of [[AutoSnapshotLoader]] for testing purposes.
 * Allows tracking of requested snapshot versions and simulating load failures.
 * */
class TestAutoSnapshotLoader(
    autoSnapshotRepairEnabled: Boolean,
    numFailuresBeforeActivating: Int = 1,
    maxChangeFileReplay: Int = 10,
    loggingId: String = "test",
    eligibleSnapshots: Seq[Long],
    failSnapshots: Seq[Long] = Seq.empty) extends AutoSnapshotLoader(
  autoSnapshotRepairEnabled, numFailuresBeforeActivating, maxChangeFileReplay, loggingId) {

  // track snapshot versions requested via loadSnapshotFromCheckpoint
  private val requestedSnapshotVersions = ListBuffer[Long]()
  def getRequestedSnapshotVersions: Seq[Long] = requestedSnapshotVersions.toSeq

  override protected def beforeLoad(): Unit = {}

  override protected def loadSnapshotFromCheckpoint(snapshotVersion: Long): Unit = {
    // Track the snapshot version
    requestedSnapshotVersions += snapshotVersion

    // throw exception if the snapshot version is in the failSnapshots list
    if (failSnapshots.contains(snapshotVersion)) {
      throw new TestLoadException(snapshotVersion)
    }
  }

  override protected def onLoadSnapshotFromCheckpointFailure(): Unit = {}

  override protected def getEligibleSnapshots(versionToLoad: Long): Seq[Long] = eligibleSnapshots
}

class TestLoadException(val snapshotVersion: Long)
  extends IllegalStateException(s"Cannot load snapshot version $snapshotVersion")
