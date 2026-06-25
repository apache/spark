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

import java.util.UUID
import java.util.concurrent.{ConcurrentLinkedQueue, CountDownLatch, TimeUnit}
import java.util.concurrent.atomic.AtomicBoolean

import scala.collection.mutable

import org.apache.hadoop.conf.Configuration
import org.apache.logging.log4j.Level
import org.scalatest.{BeforeAndAfter, PrivateMethodTester}
import org.scalatest.concurrent.Eventually._
import org.scalatest.time.SpanSugar._

import org.apache.spark._
import org.apache.spark.LocalSparkContext._
import org.apache.spark.internal.Logging
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.internal.SQLConf.STATE_STORE_PROVIDER_CLASS
import org.apache.spark.sql.test.SharedSparkSession
import org.apache.spark.sql.types._
import org.apache.spark.tags.ExtendedSQLTest

/**
 * A fake StateStoreProvider that gives tests deterministic control over
 * snapshot and cleanup maintenance timing using a latch-based handshake.
 *
 * Each operation follows a two-phase pattern:
 *   1. The op counts down its enteredLatch, telling the test "I'm running."
 *   2. The op blocks on its continueSignal until the test counts it down.
 *
 * The scheduler runs normally and submits tasks to the pools, but those
 * tasks block inside the provider's maintenance methods. This holds the
 * pool threads mid-execution, keeping downstream logic (source handling,
 * queue routing, close) from running until the test releases the latch.
 */
class BlockingMaintenanceProvider extends StateStoreProvider
    with Logging {
  private var id: StateStoreId = null

  // Per-instance state. No shared static fields, so stale scheduler
  // cycles from a previous test use the old instance's latches (already
  // counted down) and finish immediately. No cross-test interference.
  @volatile var snapshotThreadName: String = ""
  @volatile var cleanupThreadName: String = ""
  @volatile var closeThreadName: String = ""
  @volatile var snapshotShouldThrow: Boolean = false
  @volatile var cleanupShouldThrow: Boolean = false
  @volatile var closeShouldBlock: Boolean = false

  val snapshotEnteredLatch = new CountDownLatch(1)
  val cleanupEnteredLatch = new CountDownLatch(1)
  val snapshotContinueSignal = new CountDownLatch(1)
  val cleanupContinueSignal = new CountDownLatch(1)
  val closeEnteredLatch = new CountDownLatch(1)
  val closeContinueSignal = new CountDownLatch(1)

  override def init(
      stateStoreId: StateStoreId,
      keySchema: StructType,
      valueSchema: StructType,
      keyStateEncoderSpec: KeyStateEncoderSpec,
      useColumnFamilies: Boolean,
      storeConfs: StateStoreConf,
      hadoopConf: Configuration,
      useMultipleValuesPerKey: Boolean = false,
      stateSchemaProvider: Option[StateSchemaProvider] = None
  ): Unit = {
    id = stateStoreId
  }

  override def stateStoreId: StateStoreId = id

  override def close(): Unit = {
    closeThreadName = Thread.currentThread.getName
    if (closeShouldBlock) {
      closeEnteredLatch.countDown()
      closeContinueSignal.await()
    }
  }

  /** Returns null because tests using this provider do not need a real
   *  store. They only exercise the maintenance scheduler and close paths. */
  override def getStore(
      version: Long,
      uniqueId: Option[String],
      forceSnapshotOnCommit: Boolean = false,
      loadEmpty: Boolean = false): StateStore = null

  /** Signals entry, then blocks until the test releases the continue latch. */
  override def doSnapshotMaintenance(): Unit = {
    snapshotThreadName = Thread.currentThread.getName
    logInfo(s"Snapshot maintenance entered on" +
      s" ${Thread.currentThread.getName}")
    snapshotEnteredLatch.countDown()
    snapshotContinueSignal.await()
    logInfo(s"Snapshot maintenance continuing on" +
      s" ${Thread.currentThread.getName}")
    if (snapshotShouldThrow) {
      throw new RuntimeException("snapshot error")
    }
  }

  /** Same handshake as doSnapshotMaintenance but for cleanup. */
  override def doCleanupMaintenance(): Unit = {
    cleanupThreadName = Thread.currentThread.getName
    logInfo(s"Cleanup maintenance entered on" +
      s" ${Thread.currentThread.getName}")
    cleanupEnteredLatch.countDown()
    cleanupContinueSignal.await()
    logInfo(s"Cleanup maintenance continuing on" +
      s" ${Thread.currentThread.getName}")
    if (cleanupShouldThrow) {
      throw new RuntimeException("cleanup error")
    }
  }
}

class FakeStateStoreProviderTracksCloseThread extends StateStoreProvider {
  import FakeStateStoreProviderTracksCloseThread._
  private var id: StateStoreId = null

  override def init(
      stateStoreId: StateStoreId,
      keySchema: StructType,
      valueSchema: StructType,
      keyStateEncoderSpec: KeyStateEncoderSpec,
      useColumnFamilies: Boolean,
      storeConfs: StateStoreConf,
      hadoopConf: Configuration,
      useMultipleValuesPerKey: Boolean = false,
      stateSchemaProvider: Option[StateSchemaProvider] = None): Unit = {
    id = stateStoreId
  }

  override def stateStoreId: StateStoreId = id

  override def close(): Unit = {
    closeThreadNames = Thread.currentThread.getName :: closeThreadNames
  }

  override def getStore(
      version: Long,
      uniqueId: Option[String],
      forceSnapshotOnCommit: Boolean = false,
      loadEmpty: Boolean = false): StateStore = null
}

private object FakeStateStoreProviderTracksCloseThread {
  var closeThreadNames: List[String] = Nil
}

@ExtendedSQLTest
abstract class StateStoreDecoupledMaintenanceSuiteBase[
    ProviderClass <: StateStoreProvider]
    extends SparkFunSuite
    with BeforeAndAfter
    with PrivateMethodTester {

  import StateStoreCoordinatorSuite._

  before {
    StateStore.stop()
    require(!StateStore.isMaintenanceRunning)
  }

  after {
    StateStore.stop()
    require(!StateStore.isMaintenanceRunning)
  }

  private def getDefaultSQLConf(
      minDeltasForSnapshot: Int,
      numOfVersToRetainInMemory: Int): SQLConf = {
    val sqlConf = new SQLConf()
    sqlConf.setConf(SQLConf.STATE_STORE_MIN_DELTAS_FOR_SNAPSHOT,
      minDeltasForSnapshot)
    sqlConf.setConf(SQLConf.MAX_BATCHES_TO_RETAIN_IN_MEMORY,
      numOfVersToRetainInMemory)
    sqlConf.setConf(SQLConf.MIN_BATCHES_TO_RETAIN, 2)
    sqlConf
  }

  private def getUnloadQueue() = {
    val f = PrivateMethod[ConcurrentLinkedQueue[(StateStoreProviderId,
      StateStoreProvider, MaintenanceOpRequest)]](
      Symbol("unloadedProvidersToClose"))
    StateStore invokePrivate f()
  }

  private def getSnapshotPartitions() = {
    val f = PrivateMethod[mutable.HashSet[StateStoreProviderId]](
      Symbol("snapshotPartitions"))
    StateStore invokePrivate f()
  }

  private def getCleanupPartitions() = {
    val f = PrivateMethod[mutable.HashSet[StateStoreProviderId]](
      Symbol("cleanupPartitions"))
    StateStore invokePrivate f()
  }

  private def getLoadedProviders() = {
    val f = PrivateMethod[
      mutable.HashMap[StateStoreProviderId, StateStoreProvider]](
      Symbol("loadedProviders"))
    StateStore invokePrivate f()
  }

  private def getMaintenanceTask() = {
    val f = PrivateMethod[StateStore.MaintenanceTask](
      Symbol("maintenanceTask"))
    StateStore invokePrivate f()
  }

  private def getBlockingProvider(
      id: StateStoreProviderId): BlockingMaintenanceProvider = {
    val loaded = getLoadedProviders()
    loaded.synchronized { loaded.get(id).get }
      .asInstanceOf[BlockingMaintenanceProvider]
  }

  private def maintenanceStoreConf(
      providerClass: Class[_],
      interval: Long = 100L,
      numThreads: Int = 4): StateStoreConf = {
    val sqlConf = getDefaultSQLConf(
      SQLConf.STATE_STORE_MIN_DELTAS_FOR_SNAPSHOT.defaultValue.get,
      SQLConf.MAX_BATCHES_TO_RETAIN_IN_MEMORY.defaultValue.get)
    sqlConf.setConf(SQLConf.STREAMING_MAINTENANCE_INTERVAL, interval)
    sqlConf.setConf(SQLConf.NUM_STATE_STORE_MAINTENANCE_THREADS, numThreads)
    sqlConf.setConf(STATE_STORE_PROVIDER_CLASS, providerClass.getName)
    new StateStoreConf(sqlConf)
  }

  private def loadNullProvider(
      dir: String,
      storeConf: StateStoreConf,
      partition: Int = 0): StateStoreProviderId = {
    val storeId = StateStoreProviderId(
      StateStoreId(dir, 0, partition), UUID.randomUUID)
    StateStore.get(storeId, null, null, NoPrefixKeyStateEncoderSpec(null), 0,
      stateStoreCkptId = None, stateSchemaBroadcast = None,
      useColumnFamilies = false, storeConf, new Configuration())
    storeId
  }

  test("SPARK-51596: task thread unload lifecycle " +
      "from queue to close") {
    val conf = new SparkConf().setMaster("local").setAppName("test")
    withSpark(SparkContext.getOrCreate(conf)) { sc =>
      withCoordinatorRef(sc) { coordinatorRef =>
        // Long interval so close can only happen via triggerNow, not the
        // periodic scheduler tick. Without triggerNow the test fails at the
        // snapshot latch because the first cycle never fires within the 10s
        // timeout.
        val storeConf = maintenanceStoreConf(
          classOf[BlockingMaintenanceProvider], interval = 30000L)
        val id1 = loadNullProvider("lifecycle", storeConf)
        val bp = getBlockingProvider(id1)

        val queue = getUnloadQueue()
        assert(StateStore.isLoaded(id1))
        assert(queue.isEmpty, "Queue should start empty")

        // Make stale and load another provider to trigger task thread queueing.
        // Use a non-blocking provider for id2 since we don't need to
        // observe its maintenance.
        coordinatorRef.reportActiveInstance(id1, "otherhost", "otherexec", Seq.empty)
        val storeConf2 = maintenanceStoreConf(
          classOf[FakeStateStoreProviderTracksCloseThread])
        val id2 = loadNullProvider("lifecycle", storeConf2, partition = 1)

        assert(!StateStore.isLoaded(id1), "Provider1 should be removed")
        assert(StateStore.isLoaded(id2), "Provider2 should still be loaded")

        // The task thread queued id1 with All. We can't peek the queue
        // here because triggerNow fires immediately after queueing, draining
        // it before we can inspect. snapshotEnteredLatch proves the entry
        // was consumed and snapshot was submitted.

        // Step 2: Scheduler (via triggerNow) submits first op as
        // FromTaskThread. Snapshot enters latch.
        assert(bp.snapshotEnteredLatch.await(10, TimeUnit.SECONDS) &&
          bp.snapshotEnteredLatch.getCount == 0, "snapshot should have started")

        // Step 3: Release snapshot. Post-work queues remaining op (Cleanup)
        // via otherMaintenanceOpRequest.
        bp.snapshotContinueSignal.countDown()

        // Ideally we would peek the queue here to verify the entry is Cleanup
        // (via otherMaintenanceOpRequest). But triggerNow drains it before we
        // can peek. Instead, cleanupEnteredLatch being counted down proves
        // Cleanup was queued and submitted. If the entry were Snapshot,
        // doSnapshotMaintenance would have been called instead.

        // Step 4: Scheduler (via triggerNow) picks up Cleanup as
        // FromUnloadedProvidersQueue. Cleanup enters.
        assert(bp.cleanupEnteredLatch.await(10, TimeUnit.SECONDS) &&
          bp.cleanupEnteredLatch.getCount == 0, "cleanup should have started")

        // Verify intermediate queue state: snapshot's post-work queued Cleanup
        // and scheduler drained it. Cleanup is now running, queue is empty.
        assert(queue.isEmpty, "queue should be drained while cleanup runs")

        // Step 5: Release cleanup. FromUnloadedProvidersQueue calls closeProvider.
        bp.cleanupContinueSignal.countDown()

        // Verify provider was closed on cleanup pool.
        eventually(timeout(10.seconds)) {
          assert(bp.closeThreadName.contains(
            "state-store-maintenance-low-priority"),
            "close should happen on cleanup pool thread, but was on: " +
              bp.closeThreadName)
          assert(!StateStore.isLoaded(id1),
            "provider should be removed from loadedProviders")
          assert(queue.isEmpty, "Queue should be drained")
        }
      }
    }
  }

  test("tryClaimPartition returns true first call, false second, " +
      "true for different opType") {
    val id = StateStoreProviderId(
      StateStoreId("dir", 0, 0), UUID.randomUUID)
    val id2 = StateStoreProviderId(
      StateStoreId("dir", 0, 1), UUID.randomUUID)

    try {
      // First claim for snapshot succeeds
      assert(StateStore.tryClaimPartition(id, MaintenanceOpType.Snapshot))
      // Second claim for same id + opType fails
      assert(!StateStore.tryClaimPartition(id, MaintenanceOpType.Snapshot))
      // Claim for same id but different opType succeeds
      assert(StateStore.tryClaimPartition(id, MaintenanceOpType.Cleanup))
      // That one is also occupied now
      assert(!StateStore.tryClaimPartition(id, MaintenanceOpType.Cleanup))

      // Different id can still claim both
      assert(StateStore.tryClaimPartition(id2, MaintenanceOpType.Snapshot))
      assert(StateStore.tryClaimPartition(id2, MaintenanceOpType.Cleanup))
    } finally {
      getSnapshotPartitions().clear()
      getCleanupPartitions().clear()
    }
  }

  test("otherMaintenanceOpRequest maps correctly") {
    assert(StateStore.otherMaintenanceOpRequest(MaintenanceOpType.Snapshot)
      === MaintenanceOpRequest.Cleanup)
    assert(StateStore.otherMaintenanceOpRequest(MaintenanceOpType.Cleanup)
      === MaintenanceOpRequest.Snapshot)
  }

  test("closeProvider sets unloaded even if close() throws") {
    val storeId = StateStoreProviderId(StateStoreId("closeTest", 0, 0), UUID.randomUUID)
    val callOrder = new mutable.ArrayBuffer[String]()
    val provider = new FakeStateStoreProviderTracksCloseThread {
      override def close(): Unit = {
        callOrder += "close"
        throw new RuntimeException("close failed")
      }
      override def setUnloaded(): Unit = {
        callOrder += "setUnloaded"
        super.setUnloaded()
      }
    }
    provider.init(
      storeId.storeId, null, null, NoPrefixKeyStateEncoderSpec(null),
      useColumnFamilies = false, null, null)

    assert(!provider.unloaded)
    intercept[RuntimeException] {
      StateStore.closeProvider(storeId, provider)
    }
    assert(provider.unloaded,
      "setUnloaded should run even if close() throws")
    assert(callOrder === Seq("close", "setUnloaded"),
      "setUnloaded should run after close() even if it throws")
  }

  test("concurrent snapshot and cleanup on same provider " +
      "both succeed") {
    val conf = new SparkConf().setMaster("local").setAppName("test")
    withSpark(SparkContext.getOrCreate(conf)) { sc =>
      withCoordinatorRef(sc) { _ =>
        val storeConf = maintenanceStoreConf(classOf[BlockingMaintenanceProvider])
        val storeId = loadNullProvider("concurrentDir", storeConf)
        val bp = getBlockingProvider(storeId)

        // Wait for both snapshot and cleanup to enter
        assert(bp.snapshotEnteredLatch
          .await(30, TimeUnit.SECONDS), "snapshot should have started")
        assert(bp.cleanupEnteredLatch
          .await(30, TimeUnit.SECONDS), "cleanup should have started")

        // Scheduler is no longer needed. Stop and wait so no new
        // cycles interfere with assertions below.
        getMaintenanceTask().stopAndAwait()

        // Both are running on their respective pool threads.
        assert(bp.snapshotThreadName
          .startsWith("state-store-maintenance-high-priority"))
        assert(bp.cleanupThreadName
          .startsWith("state-store-maintenance-low-priority"))

        // Partition sets should be claimed while both are running.
        assert(!StateStore.tryClaimPartition(storeId, MaintenanceOpType.Snapshot),
          "snapshot partition set should be occupied")
        assert(!StateStore.tryClaimPartition(storeId, MaintenanceOpType.Cleanup),
          "cleanup partition set should be occupied")

        // Read lock should be held while maintenance ops are running.
        assert(bp.maintenanceLock.getReadLockCount == 2,
          "both pool threads should hold the read lock")

        // Release both to finish
        bp.snapshotContinueSignal.countDown()
        bp.cleanupContinueSignal.countDown()

        // Verify all ops completed by checking partition sets and read
        // lock are released. Use reflection to read the sets without
        // claiming (tryClaimPartition has side effects that break
        // eventually retries).
        eventually(timeout(10.seconds)) {
          assert(!getSnapshotPartitions().contains(storeId),
            "snapshot partition set should be released")
          assert(!getCleanupPartitions().contains(storeId),
            "cleanup partition set should be released")
          assert(bp.maintenanceLock.getReadLockCount == 0,
            "read lock should be released after maintenance completes")
        }
      }
    }
  }

  test("partition sets and locks released when maintenance throws, " +
      "write lock blocks until read lock is freed") {
    val conf = new SparkConf().setMaster("local").setAppName("test")
    withSpark(SparkContext.getOrCreate(conf)) { sc =>
      withCoordinatorRef(sc) { _ =>
        val storeConf = maintenanceStoreConf(classOf[BlockingMaintenanceProvider])
        val storeId = loadNullProvider("errorDir", storeConf)
        val bp = getBlockingProvider(storeId)
        bp.snapshotShouldThrow = true
        bp.closeShouldBlock = true

        // Wait for all ops to enter.
        assert(bp.snapshotEnteredLatch
          .await(10, TimeUnit.SECONDS), "snapshot should start")
        assert(bp.cleanupEnteredLatch
          .await(10, TimeUnit.SECONDS), "cleanup should start")

        // Scheduler is no longer needed. Stop and wait so no new
        // cycles interfere with assertions below.
        getMaintenanceTask().stopAndAwait()

        // Partition set is claimed while running.
        assert(!StateStore.tryClaimPartition(storeId, MaintenanceOpType.Snapshot),
          "snapshot set should be occupied")

        assert(bp.maintenanceLock.getReadLockCount == 2,
          "both pool threads should hold the read lock")

        // Release snapshot. It will throw. The error handler tries to
        // acquire the write lock, but cleanup still holds a read lock.
        bp.snapshotContinueSignal.countDown()

        // Wait for the error handler to be blocked on the write lock.
        eventually(timeout(5.seconds)) {
          assert(bp.maintenanceLock.getQueueLength > 0,
            "error handler should be waiting for write lock")
        }
        // Close has not been entered because the write lock is blocked.
        assert(bp.closeEnteredLatch.getCount == 1,
          "close should not be entered while write lock is blocked")

        // Release cleanup. Read lock freed. Write lock unblocks. Close called.
        bp.cleanupContinueSignal.countDown()

        assert(bp.closeEnteredLatch
          .await(10, TimeUnit.SECONDS), "close should be called")
        assert(bp.maintenanceLock.isWriteLocked,
          "write lock should be held during close")

        // Release close to let error handler finish.
        bp.closeContinueSignal.countDown()

        // Wait for the error and finally block to complete.
        eventually(timeout(10.seconds)) {
          assert(!StateStore.isLoaded(storeId),
            "provider should be unloaded after throw")
          assert(bp.closeThreadName.nonEmpty,
            "provider should be closed after throw")
          assert(!getSnapshotPartitions().contains(storeId),
            "snapshot set should be released by finally block after throw")
          assert(bp.maintenanceLock.getReadLockCount == 0,
            "read lock should be released after error handling")
          assert(!bp.maintenanceLock.isWriteLocked,
            "write lock should be released after error handling")
        }
      }
    }
  }

  private def testRequeue(opRequest: MaintenanceOpRequest): Unit = {
    val logAppender = new LogAppender("requeue-log", maxEvents = 100)
    logAppender.setThreshold(Level.INFO)
    withLogAppender(logAppender, level = Some(Level.INFO)) {
      val conf = new SparkConf().setMaster("local").setAppName("test")
      withSpark(SparkContext.getOrCreate(conf)) { sc =>
        withCoordinatorRef(sc) { _ =>
          val storeConf = maintenanceStoreConf(classOf[BlockingMaintenanceProvider])
          val storeId = loadNullProvider("requeueDir", storeConf)
          val bp = getBlockingProvider(storeId)

          // Wait for both ops to enter, occupying both partition sets.
          assert(bp.snapshotEnteredLatch.await(10, TimeUnit.SECONDS))
          assert(bp.cleanupEnteredLatch.await(10, TimeUnit.SECONDS))

          // Add an entry to the queue. The scheduler will try to drain
          // it but the partition set is occupied (held by the blocked
          // task above), so it should be requeued.
          val queue = getUnloadQueue()
          queue.add((storeId, bp, opRequest))

          // Wait for the scheduler to attempt draining and verify
          // the requeue log. Queue checks are inside eventually to
          // handle the momentary gap between the poll removing the
          // entry and offer putting the entry back.
          // Queue should still have the entry.
          eventually(timeout(10.seconds)) {
            assert(logAppender.loggingEvents.exists(
              _.getMessage.getFormattedMessage.contains("Had to requeue")),
              s"scheduler should have logged requeue for $opRequest")
            val peeked = queue.peek()
            assert(peeked != null,
              s"$opRequest entry should have been requeued")
            val (requeuedId, _, requeuedOp) = peeked
            assert(requeuedId == storeId)
            assert(requeuedOp == opRequest)
          }

          // Clean up: release latches
          bp.snapshotContinueSignal.countDown()
          bp.cleanupContinueSignal.countDown()
          queue.clear()
        }
      }
    }
  }

  test("Snapshot entry requeues when snapshot partition set is occupied") {
    testRequeue(MaintenanceOpRequest.Snapshot)
  }

  test("Cleanup entry requeues when cleanup partition set is occupied") {
    testRequeue(MaintenanceOpRequest.Cleanup)
  }

  test("All entry requeues when both partition sets are occupied") {
    testRequeue(MaintenanceOpRequest.All)
  }

  test("When MaintenanceOpRequest is All, cleanup is submitted " +
      "if snapshot partition set is occupied") {
    val conf = new SparkConf().setMaster("local").setAppName("test")
    withSpark(SparkContext.getOrCreate(conf)) { sc =>
      withCoordinatorRef(sc) { _ =>
        // Long interval so we can set up before the first
        // cycle fires (5s initial delay).
        val storeConf = maintenanceStoreConf(
          classOf[BlockingMaintenanceProvider], interval = 5000L)
        val id = loadNullProvider("shortCircuit", storeConf)
        val bp = getBlockingProvider(id)

        try {
          // Claim snapshot partition set.
          assert(StateStore.tryClaimPartition(id, MaintenanceOpType.Snapshot))

          // Remove from loadedProviders so scheduler doesn't submit
          // cleanup for this provider by iterating through it.
          // Only the queue entry should submit cleanup.
          getLoadedProviders().synchronized { getLoadedProviders().remove(id) }

          // Add All entry. Scheduler's first cycle (at 5s) drains it,
          // tries snapshot (occupied), falls through to cleanup.
          // Cleanup blocks on bp's latch, keeping the partition set.
          getUnloadQueue().add((id, bp, MaintenanceOpRequest.All))

          eventually(timeout(10.seconds)) {
            assert(getCleanupPartitions().contains(id),
              "cleanup should be claimed")
            assert(getUnloadQueue().isEmpty, "queue should be drained")
          }
        } finally {
          bp.snapshotContinueSignal.countDown()
          bp.cleanupContinueSignal.countDown()
          getSnapshotPartitions().remove(id)
        }
      }
    }
  }

  test("canProcess is false and maintenance is skipped " +
      "when provider has already been unloaded") {
    val logAppender = new LogAppender("canProcess-log", maxEvents = 100)
    logAppender.setThreshold(Level.INFO)
    withLogAppender(logAppender, level = Some(Level.INFO)) {
      val conf = new SparkConf().setMaster("local").setAppName("test")
      withSpark(SparkContext.getOrCreate(conf)) { sc =>
        withCoordinatorRef(sc) { _ =>
          val storeConf = maintenanceStoreConf(
            classOf[BlockingMaintenanceProvider], interval = 5000L)
          val id = loadNullProvider("canProcess", storeConf)
          val bp = getBlockingProvider(id)

          // Mark unloaded before the first maintenance cycle fires (5s
          // initial delay). canProcess checks !provider.unloaded and will
          // return false, skipping maintenance entirely.
          bp.setUnloaded()

          // Wait for the "Skipping maintenance" log proving canProcess
          // was false and maintenance was skipped.
          eventually(timeout(10.seconds)) {
            assert(logAppender.loggingEvents.exists(
              _.getMessage.getFormattedMessage.contains("Skipping maintenance")),
              "should log skipping maintenance for unloaded provider")
          }
          assert(bp.snapshotEnteredLatch.getCount == 1,
            "snapshot should not have entered")
          assert(bp.cleanupEnteredLatch.getCount == 1,
            "cleanup should not have entered")
        }
      }
    }
  }

  test("canProcess is false and maintenance is skipped " +
      "when provider instance differs") {
    val logAppender = new LogAppender("canProcess-stale-log", maxEvents = 100)
    logAppender.setThreshold(Level.INFO)
    withLogAppender(logAppender, level = Some(Level.INFO)) {
      val conf = new SparkConf().setMaster("local").setAppName("test")
      withSpark(SparkContext.getOrCreate(conf)) { sc =>
        withCoordinatorRef(sc) { _ =>
          // Long interval so we can block the cleanup pool before the
          // first cycle fires. numThreads=2 gives each pool 1 thread.
          val storeConf = maintenanceStoreConf(
            classOf[BlockingMaintenanceProvider],
            interval = 5000L, numThreads = 2)
          val id = loadNullProvider("canProcessStale", storeConf)
          val bp = getBlockingProvider(id)

          // Block the cleanup pool's thread with a dummy task before the
          // first cycle fires (5s away).
          val cleanupPoolField = PrivateMethod[StateStore.MaintenanceThreadPool](
            Symbol("lowPriorityThreadPool"))
          val cleanupPool = StateStore invokePrivate cleanupPoolField()
          val blockLatch = new CountDownLatch(1)
          cleanupPool.execute(() => blockLatch.await())

          // Wait for the first cycle. Snapshot runs freely.
          assert(bp.snapshotEnteredLatch.await(10, TimeUnit.SECONDS))

          // Stop the scheduler so the next cycle doesn't run the
          // replacement (which lacks thread locals).
          getMaintenanceTask().stopAndAwait()

          bp.snapshotContinueSignal.countDown()

          // Replace A with a different instance while cleanup is waiting.
          val replacement = new FakeStateStoreProviderTracksCloseThread
          replacement.init(id.storeId, null, null,
            NoPrefixKeyStateEncoderSpec(null),
            useColumnFamilies = false, null, null)
          val loaded = getLoadedProviders()
          loaded.synchronized { loaded.put(id, replacement) }

          // Release the dummy. Cleanup starts, canProcess sees
          // contains(A) is false (replacement is there), skips.
          blockLatch.countDown()

          eventually(timeout(10.seconds)) {
            assert(logAppender.loggingEvents.exists(
              _.getMessage.getFormattedMessage.contains("Skipping maintenance")),
              "should log skipping maintenance for stale instance")
          }
          assert(bp.cleanupEnteredLatch.getCount == 1,
            "cleanup should not have entered")
        }
      }
    }
  }

  test("FromLoadedProviders unload: reloaded provider is not removed nor queued") {
    val conf = new SparkConf().setMaster("local").setAppName("test")
    withSpark(SparkContext.getOrCreate(conf)) { sc =>
      withCoordinatorRef(sc) { coordinatorRef =>
        val storeConf = maintenanceStoreConf(classOf[BlockingMaintenanceProvider])
        val id = loadNullProvider("staleInstance", storeConf)
        val bp = getBlockingProvider(id)

        // Mark as needing to be closed.
        coordinatorRef.reportActiveInstance(id, "otherhost", "otherexec", Seq.empty)

        // Wait for snapshot to enter.
        assert(bp.snapshotEnteredLatch.await(10, TimeUnit.SECONDS))

        // Stop only the scheduler (not the pools) so A's threads keep
        // running but no new cycles fire after we replace.
        getMaintenanceTask().stopAndAwait()

        // Replace provider A with a different instance while A is blocked.
        // The scheduler is stopped so no maintenance runs on the
        // replacement. When A finishes, loadedProviders.get(id).contains(A)
        // is false (replacement is there), removal is skipped.
        val replacement = new FakeStateStoreProviderTracksCloseThread
        replacement.init(id.storeId, null, null,
          NoPrefixKeyStateEncoderSpec(null), useColumnFamilies = false, null, null)
        val loaded = getLoadedProviders()
        loaded.synchronized { loaded.put(id, replacement) }

        // Release A.
        bp.snapshotContinueSignal.countDown()
        bp.cleanupContinueSignal.countDown()

        // Wait for A's partition sets to be released.
        eventually(timeout(10.seconds)) {
          assert(!getSnapshotPartitions().contains(id),
            "snapshot partition should be released")
        }

        // A should NOT have removed the replacement from loadedProviders.
        assert(StateStore.isLoaded(id),
          "replacement provider should still be loaded")
        // A should NOT have queued anything (instance differs, skip).
        assert(getUnloadQueue().isEmpty,
          "queue should be empty (stale instance skipped removal)")
      }
    }
  }

  test("FromLoadedProviders unload: with concurrent ops, " +
      "only one removes and queues") {
    val conf = new SparkConf().setMaster("local").setAppName("test")
    withSpark(SparkContext.getOrCreate(conf)) { sc =>
      withCoordinatorRef(sc) { coordinatorRef =>
        val storeConf = maintenanceStoreConf(classOf[BlockingMaintenanceProvider])
        val id = loadNullProvider("concurrentUnload", storeConf)
        val bp = getBlockingProvider(id)

        // Make provider stale so both ops detect inactive in source handling.
        coordinatorRef.reportActiveInstance(id, "otherhost", "otherexec", Seq.empty)

        // Wait for both ops to enter.
        assert(bp.snapshotEnteredLatch.await(10, TimeUnit.SECONDS))
        assert(bp.cleanupEnteredLatch.await(10, TimeUnit.SECONDS))

        // Stop the scheduler so no new cycles drain the queue before
        // we can check its size.
        getMaintenanceTask().stopAndAwait()

        // Release both. Both finish, both see !verifyIfStoreInstanceActive.
        // Only one should remove from loadedProviders and queue.
        bp.snapshotContinueSignal.countDown()
        bp.cleanupContinueSignal.countDown()

        val queue = getUnloadQueue()
        eventually(timeout(10.seconds)) {
          assert(!StateStore.isLoaded(id), "provider should be removed")
          assert(queue.size() == 1,
            "only one op should queue, the other should no-op")
        }
      }
    }
  }

  test("stale provider from loadedProviders is closed properly " +
      "through the full queue routing lifecycle") {
    // Load a provider, make it stale via the coordinator, then verify:
    // 1. Snapshot and cleanup run on separate pools with partition sets claimed
    // 2. Both pool threads hold the read lock (readLockCount == 2)
    // 3. Snapshot detects inactive, queues Cleanup via otherMaintenanceOpRequest
    // 4. Cleanup runs as FromUnloadedProvidersQueue and closes the provider
    // 5. During close: write lock held, no read locks
    // 6. After close: all locks released, provider removed, queue drained
    val conf = new SparkConf().setMaster("local").setAppName("test")
    withSpark(SparkContext.getOrCreate(conf)) { sc =>
      withCoordinatorRef(sc) { coordinatorRef =>
        val storeConf = maintenanceStoreConf(classOf[BlockingMaintenanceProvider])
        val id = loadNullProvider("decoupled", storeConf)
        val bp = getBlockingProvider(id)
        bp.closeShouldBlock = true

        // Make provider stale so source handling queues it
        coordinatorRef.reportActiveInstance(id, "otherhost", "otherexec", Seq.empty)

        // Wait for both tasks to block
        assert(bp.snapshotEnteredLatch
          .await(5, TimeUnit.SECONDS), "Snapshot task did not start")
        assert(bp.cleanupEnteredLatch
          .await(5, TimeUnit.SECONDS), "Cleanup task did not start")

        // Snapshot and cleanup should run on separate pools
        assert(bp.snapshotThreadName
          .contains("state-store-maintenance-high-priority"),
          s"Snapshot should run on snapshot pool, was: " +
            s"${bp.snapshotThreadName}")
        assert(bp.cleanupThreadName
          .contains("state-store-maintenance-low-priority"),
          s"Cleanup should run on cleanup pool, was: " +
            s"${bp.cleanupThreadName}")

        // Both partition sets should be claimed
        val snap = getSnapshotPartitions()
        val clean = getCleanupPartitions()
        assert(snap.contains(id), "Snapshot partition should be claimed")
        assert(clean.contains(id), "Cleanup partition should be claimed")

        // Both pool threads hold the read lock.
        assert(bp.maintenanceLock.getReadLockCount == 2,
          "both pool threads should hold the read lock")

        val queue = getUnloadQueue()

        // Both blocked, queue should be empty, no close
        assert(queue.isEmpty, "Queue should be empty while tasks are blocked")
        assert(bp.closeThreadName.isEmpty,
          "No close while tasks are blocked")

        // Release snapshot. It will detect inactive, remove from
        // loadedProviders, and queue the other op. Cleanup stays blocked,
        // holding its partition set, so the scheduler cannot process the
        // queue entry.
        bp.snapshotContinueSignal.countDown()

        // Wait for snapshot source handling to complete. Check inside
        // eventually to handle the momentary gap between the poll
        // removing the entry and offer putting the entry back.
        // Verify the queue entry. Snapshot completed first, so it queued
        // otherMaintenanceOpRequest(Snapshot) = Cleanup.
        eventually(timeout(5.seconds)) {
          val peeked = queue.peek()
          assert(peeked != null, "Queue should have one entry")
          val (queuedId, _, opRequest) = peeked
          assert(queuedId == id)
          assert(opRequest == MaintenanceOpRequest.Cleanup,
            s"Expected Cleanup, got $opRequest")
        }

        // No close should have happened yet
        assert(bp.closeThreadName.isEmpty,
          "No close before final op runs")

        // Release cleanup. FromUnloadedProvidersQueue will release the
        // read lock, acquire the write lock, and call close().
        bp.cleanupContinueSignal.countDown()

        // close() blocks on the latch. Write lock held, no read locks.
        assert(bp.closeEnteredLatch
          .await(10, TimeUnit.SECONDS), "close should be called")
        assert(bp.maintenanceLock.isWriteLocked,
          "write lock should be held during close")
        assert(bp.maintenanceLock.getReadLockCount == 0,
          "no read locks should be held during close")

        // Release close to let the downgrade and cleanup finish.
        bp.closeContinueSignal.countDown()

        // Everything released.
        eventually(timeout(10.seconds)) {
          assert(bp.closeThreadName.nonEmpty,
            "Provider should be closed")
          assert(!StateStore.isLoaded(id),
            "Provider should be removed from loadedProviders")
          assert(queue.isEmpty, "Queue should be drained")
          assert(snap.isEmpty && clean.isEmpty,
            "Both partition sets should be released")
          assert(bp.maintenanceLock.getReadLockCount == 0,
            "read lock should be released after close")
          assert(!bp.maintenanceLock.isWriteLocked,
            "write lock should be released after close")
        }
      }
    }
  }

  test("scheduler maintenance triggerNow: at-most-one pending, no-op after stop") {
    val conf = new SparkConf().setMaster("local").setAppName("test")
    withSpark(SparkContext.getOrCreate(conf)) { sc =>
      withCoordinatorRef(sc) { _ =>
        val storeConf = maintenanceStoreConf(
          classOf[BlockingMaintenanceProvider], interval = 60000L)
        val id = loadNullProvider("triggerNow", storeConf)
        val bp = getBlockingProvider(id)

        // Reflection to extract maintenanceTask, its triggerPending flag,
        // the underlying ScheduledThreadPoolExecutor, and loadedProviders.
        val task = getMaintenanceTask()
        val loaded = getLoadedProviders()
        val pendingField = task.getClass.getDeclaredField("triggerPending")
        val executorField = task.getClass.getDeclaredField("executor")
        pendingField.setAccessible(true)
        executorField.setAccessible(true)
        val triggerPending = pendingField.get(task).asInstanceOf[AtomicBoolean]
        val executor = executorField.get(task)
          .asInstanceOf[java.util.concurrent.ScheduledThreadPoolExecutor]

        assert(!triggerPending.get(), "triggerPending should start as false")

        // Hold loadedProviders lock so doMaintenance blocks on Phase 2
        // (loadedProviders.synchronized). This keeps the scheduler executor
        // busy, allowing us to test the at-most-one guard.
        val lockHeld = new CountDownLatch(1)
        val lockRelease = new CountDownLatch(1)
        val lockThread = new Thread(() => {
          loaded.synchronized { lockHeld.countDown(); lockRelease.await() }
        })
        lockThread.start()
        lockHeld.await()

        // triggerNow submits to scheduler executor.
        // We wait until the triggered task starts executing, which will block on
        // the loadedProviders lock.
        // processUnloadedOnly=false so the triggered cycle iterates
        // loadedProviders and blocks on the lock.
        task.triggerNow(processUnloadedOnly = false)
        eventually(timeout(5.seconds)) {
          assert(!triggerPending.get(), "triggerPending reset when task started")
          // Queue has 1 entry: the periodic future. The triggered task is
          // currently executing (blocked on the loadedProviders lock).
          assert(executor.getQueue.size() == 1,
            s"expected 1 (periodic future), got ${executor.getQueue.size()}")
        }

        // First call queues one pending run
        task.triggerNow()
        assert(triggerPending.get(), "one pending run queued")
        // Queue has exactly 2: the pending triggered run + the periodic future
        assert(executor.getQueue.size() == 2,
          s"expected 2 queued tasks, got ${executor.getQueue.size()}")

        // Subsequent calls are no-ops (at-most-one pending).
        // Queue size should not increase.
        val queueSizeBefore = executor.getQueue.size()
        task.triggerNow()
        task.triggerNow()
        assert(triggerPending.get(), "still one pending")
        assert(executor.getQueue.size() == queueSizeBefore,
          "queue size should not increase from no-op triggerNow calls")

        // Release the lock so the blocked cycle can proceed. After it
        // finishes, the queued pending cycle runs automatically.
        lockRelease.countDown()

        // Wait for the first cycle's snapshot to enter
        assert(bp.snapshotEnteredLatch
          .await(10, TimeUnit.SECONDS), "triggerNow should fire a cycle")
        bp.snapshotContinueSignal.countDown()
        bp.cleanupContinueSignal.countDown()

        // Verify the queued pending task also ran: after both cycles
        // complete, the executor queue should have only the periodic future
        // left, and triggerPending should be false.
        eventually(timeout(10.seconds)) {
          assert(!triggerPending.get(), "pending task should have run")
          assert(executor.getQueue.size() == 1,
            "only the periodic future should remain in the queue")
        }

        // After stop, triggerNow catches RejectedExecutionException and logs.
        val logAppender = new LogAppender("triggerNow-warn", maxEvents = 100)
        logAppender.setThreshold(Level.WARN)
        withLogAppender(logAppender, level = Some(Level.WARN)) {
          // Use WithoutLock to avoid deadlock from stopMaintenanceTask
          // holding loadedProviders lock while awaiting pool termination, but
          // pool threads needing to acquire the same lock.
          StateStore.stopMaintenanceTaskWithoutLock()
          task.triggerNow()
          assert(!triggerPending.get(), "reset after rejection")
          assert(logAppender.loggingEvents.exists(_.getMessage.getFormattedMessage
            .contains("triggerNow called after scheduler maintenance task stopped")),
            "should log warning on rejected execution")
        }
      }
    }
  }

  test("scheduler maintenance triggerNow: only unloaded queue is processed") {
    val conf = new SparkConf().setMaster("local").setAppName("test")
    withSpark(SparkContext.getOrCreate(conf)) { sc =>
      withCoordinatorRef(sc) { coordinatorRef =>
        // Long interval so periodic cycles don't interfere.
        val storeConf = maintenanceStoreConf(
          classOf[BlockingMaintenanceProvider], interval = 60000L)
        val id1 = loadNullProvider("triggerOnly", storeConf)
        val bp1 = getBlockingProvider(id1)

        // Make id1 stale. Loading id2 calls reportActiveInstance which
        // detects id1 as stale, queues it, and calls triggerNow.
        coordinatorRef.reportActiveInstance(
          id1, "otherhost", "otherexec", Seq.empty)
        val id2 = loadNullProvider("triggerOnly", storeConf, partition = 1)
        val bp2 = getBlockingProvider(id2)

        // triggerNow fires with processUnloadedOnly=true.
        // id1 should enter (proves triggerNow processed the queue).
        assert(bp1.snapshotEnteredLatch.await(10, TimeUnit.SECONDS),
          "id1 should be processed from queue by triggerNow")

        // Give the triggered cycle time to finish. If phase 2 ran,
        // id2 would have been submitted to the pool by now.
        Thread.sleep(2000)

        // id2 should NOT have entered. The scheduler should NOT have
        // iterated loadedProviders and submitted maintenance tasks.
        assert(bp2.snapshotEnteredLatch.getCount == 1,
          "id2 snapshot should not have been submitted by triggerNow")
        assert(bp2.cleanupEnteredLatch.getCount == 1,
          "id2 cleanup should not have been submitted by triggerNow")

        bp1.snapshotContinueSignal.countDown()
        bp1.cleanupContinueSignal.countDown()
        bp2.snapshotContinueSignal.countDown()
        bp2.cleanupContinueSignal.countDown()
      }
    }
  }

  private def poolSizeConf(
      total: Int,
      ratio: Double = 0.5): StateStoreConf = {
    val sqlConf = getDefaultSQLConf(
      SQLConf.STATE_STORE_MIN_DELTAS_FOR_SNAPSHOT.defaultValue.get,
      SQLConf.MAX_BATCHES_TO_RETAIN_IN_MEMORY.defaultValue.get)
    sqlConf.setConf(SQLConf.NUM_STATE_STORE_MAINTENANCE_THREADS, total)
    sqlConf.setConf(
      SQLConf.STATE_STORE_MAINTENANCE_SNAPSHOT_THREAD_RATIO, ratio)
    new StateStoreConf(sqlConf)
  }

  test("getPoolSizes: ratio based split") {
    // Default ratio 0.5: even split.
    assert(StateStore.getPoolSizes(poolSizeConf(2)) === (1, 1))
    assert(StateStore.getPoolSizes(poolSizeConf(3)) === (2, 1))
    assert(StateStore.getPoolSizes(poolSizeConf(4)) === (2, 2))
    assert(StateStore.getPoolSizes(poolSizeConf(5)) === (3, 2))
    assert(StateStore.getPoolSizes(poolSizeConf(8)) === (4, 4))
    assert(StateStore.getPoolSizes(poolSizeConf(100)) === (50, 50))

    // Custom ratios.
    assert(StateStore.getPoolSizes(poolSizeConf(8, ratio = 0.75)) === (6, 2))
    assert(StateStore.getPoolSizes(poolSizeConf(8, ratio = 0.25)) === (2, 6))
    assert(StateStore.getPoolSizes(poolSizeConf(10, ratio = 0.8)) === (8, 2))
    assert(StateStore.getPoolSizes(poolSizeConf(10, ratio = 0.1)) === (1, 9))

    // Fractional rounding (math.round rounds 0.5 up).
    assert(StateStore.getPoolSizes(poolSizeConf(10, ratio = 1.0/3)) === (3, 7))
    assert(StateStore.getPoolSizes(poolSizeConf(11, ratio = 0.5)) === (6, 5))
    assert(StateStore.getPoolSizes(poolSizeConf(7, ratio = 0.3)) === (2, 5))

    // Each pool gets at least 1 thread. Total is never exceeded.
    assert(StateStore.getPoolSizes(poolSizeConf(2, ratio = 0.99)) === (1, 1))
    assert(StateStore.getPoolSizes(poolSizeConf(2, ratio = 0.01)) === (1, 1))
    assert(StateStore.getPoolSizes(poolSizeConf(4, ratio = 0.99)) === (3, 1))
    assert(StateStore.getPoolSizes(poolSizeConf(4, ratio = 0.01)) === (1, 3))
    assert(StateStore.getPoolSizes(poolSizeConf(10, ratio = 0.99)) === (9, 1))
    assert(StateStore.getPoolSizes(poolSizeConf(10, ratio = 0.01)) === (1, 9))

    Seq(2, 3, 6, 7, 12, 15, 20, 50).foreach { total =>
      Seq(0.05, 0.15, 0.33, 0.5, 0.67, 0.85, 0.95).foreach { ratio =>
        val (s, c) = StateStore.getPoolSizes(poolSizeConf(total, ratio))
        assert(s + c == total, s"total=$total ratio=$ratio: $s + $c != $total")
        assert(s >= 1, s"total=$total ratio=$ratio: snapshot=$s < 1")
        assert(c >= 1, s"total=$total ratio=$ratio: cleanup=$c < 1")
      }
    }
  }

  /**
   * Proves that a full pool does not starve the other. Provider A fills
   * the specified pool. Provider B on a different partition can still
   * run the other op type.
   * @param blockSnapshot if true, fill snapshot pool; if false, fill cleanup pool
   */
  private def testPoolIsolation(blockSnapshot: Boolean): Unit = {
    val conf = new SparkConf().setMaster("local").setAppName("test")
    withSpark(SparkContext.getOrCreate(conf)) { sc =>
      withCoordinatorRef(sc) { _ =>
        // numThreads=2: each pool gets exactly 1 thread.
        val storeConf = maintenanceStoreConf(
          classOf[BlockingMaintenanceProvider], numThreads = 2)
        val id1 = loadNullProvider("poolIsolation", storeConf)
        val bp1 = getBlockingProvider(id1)

        // Wait for both of A's ops to enter.
        assert(bp1.snapshotEnteredLatch.await(10, TimeUnit.SECONDS))
        assert(bp1.cleanupEnteredLatch.await(10, TimeUnit.SECONDS))

        // Release the op we DON'T want to fill, keeping the other
        // pool's only thread occupied.
        if (blockSnapshot) bp1.cleanupContinueSignal.countDown()
        else bp1.snapshotContinueSignal.countDown()

        // Load B on a different partition.
        val id2 = loadNullProvider("poolIsolation", storeConf, partition = 1)
        val bp2 = getBlockingProvider(id2)

        if (blockSnapshot) {
          assert(bp2.cleanupEnteredLatch.await(10, TimeUnit.SECONDS),
            "B's cleanup should run despite snapshot pool being full")
          assert(bp2.snapshotEnteredLatch.getCount == 1,
            "B's snapshot should not have entered")
        } else {
          assert(bp2.snapshotEnteredLatch.await(10, TimeUnit.SECONDS),
            "B's snapshot should run despite cleanup pool being full")
          assert(bp2.cleanupEnteredLatch.getCount == 1,
            "B's cleanup should not have entered")
        }

        // Release everything.
        bp1.snapshotContinueSignal.countDown()
        bp1.cleanupContinueSignal.countDown()
        bp2.snapshotContinueSignal.countDown()
        bp2.cleanupContinueSignal.countDown()
      }
    }
  }

  test("full snapshot pool does not prevent cleanup from running") {
    testPoolIsolation(blockSnapshot = true)
  }

  test("full cleanup pool does not prevent snapshot from running") {
    testPoolIsolation(blockSnapshot = false)
  }

  test("maintenance op skips when write lock is held during close") {
    val conf = new SparkConf().setMaster("local").setAppName("test")
    val logAppender = new LogAppender("tryLock-skip", maxEvents = 100)
    logAppender.setThreshold(Level.DEBUG)
    val loggerName = StateStore.getClass.getName.stripSuffix("$")
    withLogAppender(logAppender,
        loggerNames = Seq(loggerName), level = Some(Level.DEBUG)) {
      withSpark(SparkContext.getOrCreate(conf)) { sc =>
        withCoordinatorRef(sc) { coordinatorRef =>
          val storeConf = maintenanceStoreConf(
            classOf[BlockingMaintenanceProvider])
          val id = loadNullProvider("writeLockBlock", storeConf)
          val bp = getBlockingProvider(id)
          bp.closeShouldBlock = true

          // Make stale so the close path is triggered.
          coordinatorRef.reportActiveInstance(
            id, "otherhost", "otherexec", Seq.empty)

          // Wait for both ops to enter, release both. One detects
          // inactive, queues remaining op. That op runs as
          // FromUnloadedProvidersQueue, acquires write lock, calls close,
          // blocks on closeShouldBlock.
          assert(bp.snapshotEnteredLatch.await(10, TimeUnit.SECONDS))
          assert(bp.cleanupEnteredLatch.await(10, TimeUnit.SECONDS))
          bp.snapshotContinueSignal.countDown()
          bp.cleanupContinueSignal.countDown()

          // Wait for close to hold the write lock.
          assert(bp.closeEnteredLatch.await(10, TimeUnit.SECONDS))
          assert(bp.maintenanceLock.isWriteLocked,
            "write lock should be held during close")

          // Add provider back to the unload queue. Use All because we
          // don't know which op is doing close (and holding that partition
          // set). All tries both and submits whichever is free. The
          // scheduler's next cycle drains the queue and submits to pool.
          getUnloadQueue().add((id, bp, MaintenanceOpRequest.All))

          // The pool thread calls tryLock(0, SECONDS) which returns
          // false because the write lock is held. The op is skipped
          // without blocking. The debug log proves the pool thread ran.
          eventually(timeout(10.seconds)) {
            assert(getUnloadQueue().isEmpty,
              "scheduler should have consumed the queue entry")
            assert(logAppender.loggingEvents.exists(
              _.getMessage.getFormattedMessage.contains(
                "could not acquire read lock")),
              "pool thread should have logged tryLock failure")
            assert(bp.maintenanceLock.getQueueLength == 0,
              "no thread should be blocked on the lock")
            assert(bp.maintenanceLock.getReadLockCount == 0,
              "read lock should not be acquired when write lock is held")
          }
          assert(bp.maintenanceLock.isWriteLocked,
            "write lock should still be held during close")

          // Release close.
          bp.closeContinueSignal.countDown()

          eventually(timeout(10.seconds)) {
            assert(!bp.maintenanceLock.isWriteLocked,
              "write lock should be released after close")
          }
        }
      }
    }
  }
}

class StateStoreDecoupledMaintenanceSuite
    extends StateStoreDecoupledMaintenanceSuiteBase[HDFSBackedStateStoreProvider]
    with SharedSparkSession {
  override def beforeEach(): Unit = {}
  override def afterEach(): Unit = {}
}

class StateStoreDecoupledMaintenanceSuiteWithRowChecksum
    extends StateStoreDecoupledMaintenanceSuite
    with EnableStateStoreRowChecksum

class RocksDBDecoupledMaintenanceSuite
    extends StateStoreDecoupledMaintenanceSuiteBase[RocksDBStateStoreProvider]
    with AlsoTestWithEncodingTypes
    with AlsoTestWithRocksDBFeatures
    with SharedSparkSession {
  override def afterEach(): Unit = {}
}

class RocksDBDecoupledMaintenanceSuiteWithRowChecksum
    extends RocksDBDecoupledMaintenanceSuite
    with EnableStateStoreRowChecksum
