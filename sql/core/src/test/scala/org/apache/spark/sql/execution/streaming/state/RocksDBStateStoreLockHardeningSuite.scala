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
import java.util.concurrent.{CountDownLatch, TimeUnit}

import scala.concurrent.{ExecutionContext, Future}
import scala.util.Random

import org.apache.hadoop.conf.Configuration
import org.scalactic.source.Position
import org.scalatest.{BeforeAndAfter, PrivateMethodTester, Tag}
import org.scalatest.matchers.should.Matchers
import org.scalatest.time.SpanSugar._

import org.apache.spark.{SparkException, SparkFunSuite, SparkRuntimeException, TaskContext}
import org.apache.spark.sql.catalyst.plans.PlanTestBase
import org.apache.spark.sql.execution.streaming.runtime.StreamExecution
import org.apache.spark.sql.execution.streaming.state.StateStoreTestsHelper._
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.test.SharedSparkSession
import org.apache.spark.sql.types.StructType
import org.apache.spark.util.ThreadUtils
import org.apache.spark.util.ThreadUtils.awaitResult

/**
 * Comprehensive test cases for RocksDB State Store lock hardening implementation.
 * These tests verify the state machine behavior and prevent problematic concurrent executions.
 */
class RocksDBStateStoreLockHardeningSuite extends SparkFunSuite
    with PlanTestBase
    with AlsoTestWithRocksDBFeatures
    with PrivateMethodTester
    with SharedSparkSession
    with BeforeAndAfter
    with Matchers {

  before {
    StateStore.stop()
    require(!StateStore.isMaintenanceRunning)
    spark.streams.stateStoreCoordinator // initialize the lazy coordinator
  }

  after {
    StateStore.stop()
    require(!StateStore.isMaintenanceRunning)
  }

  protected def tryWithProviderResource[T](
      provider: RocksDBStateStoreProvider)(f: RocksDBStateStoreProvider => T): T = {
    try {
      f(provider)
    } finally {
      provider.close()
    }
  }

  def newStoreProvider(): RocksDBStateStoreProvider = {
    newStoreProvider(StateStoreId(newDir(), Random.nextInt(), 0))
  }

  def newStoreProvider(storeId: StateStoreId): RocksDBStateStoreProvider = {
    newStoreProvider(storeId, NoPrefixKeyStateEncoderSpec(keySchema))
  }

  def newStoreProvider(storeId: StateStoreId, useColumnFamilies: Boolean):
  RocksDBStateStoreProvider = {
    newStoreProvider(storeId, NoPrefixKeyStateEncoderSpec(keySchema),
      useColumnFamilies = useColumnFamilies)
  }

  def newStoreProvider(useColumnFamilies: Boolean): RocksDBStateStoreProvider = {
    newStoreProvider(StateStoreId(newDir(), Random.nextInt(), 0),
      NoPrefixKeyStateEncoderSpec(keySchema),
      useColumnFamilies = useColumnFamilies)
  }

  def newStoreProvider(
      useColumnFamilies: Boolean,
      useMultipleValuesPerKey: Boolean): RocksDBStateStoreProvider = {
    newStoreProvider(StateStoreId(newDir(), Random.nextInt(), 0),
      NoPrefixKeyStateEncoderSpec(keySchema),
      useColumnFamilies = useColumnFamilies,
      useMultipleValuesPerKey = useMultipleValuesPerKey
    )
  }

  def newStoreProvider(storeId: StateStoreId, conf: Configuration): RocksDBStateStoreProvider = {
    newStoreProvider(storeId, NoPrefixKeyStateEncoderSpec(keySchema), conf = conf)
  }

  def newStoreProvider(
      keySchema: StructType,
      keyStateEncoderSpec: KeyStateEncoderSpec,
      useColumnFamilies: Boolean): RocksDBStateStoreProvider = {
    newStoreProvider(StateStoreId(newDir(), Random.nextInt(), 0),
      keyStateEncoderSpec = keyStateEncoderSpec,
      keySchema = keySchema,
      useColumnFamilies = useColumnFamilies)
  }

  def newStoreProvider(
      storeId: StateStoreId,
      keyStateEncoderSpec: KeyStateEncoderSpec,
      keySchema: StructType = keySchema,
      sqlConf: Option[SQLConf] = None,
      conf: Configuration = new Configuration,
      useColumnFamilies: Boolean = false,
      useMultipleValuesPerKey: Boolean = false): RocksDBStateStoreProvider = {
    val provider = new RocksDBStateStoreProvider()
    val testStateSchemaProvider = new TestStateSchemaProvider
    conf.set(StreamExecution.RUN_ID_KEY, UUID.randomUUID().toString)
    provider.init(
      storeId,
      keySchema,
      valueSchema,
      keyStateEncoderSpec,
      useColumnFamilies,
      new StateStoreConf(sqlConf.getOrElse(SQLConf.get)),
      conf,
      useMultipleValuesPerKey,
      stateSchemaProvider = Some(testStateSchemaProvider))
    provider
  }

  override protected def test(testName: String, testTags: Tag*)(testBody: => Any)
                             (implicit pos: Position): Unit = {
    super.test(s"$testName", testTags: _*) {
      withSQLConf("spark.sql.streaming.stateStore.rocksdb.lockAcquireTimeoutMs" -> "2000") {
        testBody
      }
    }
  }

  // Custom ExecutionContext for concurrent testing
  implicit val ec: ExecutionContext = ExecutionContext.fromExecutor(
    ThreadUtils.newDaemonFixedThreadPool(5, "lock-hardening-test-pool"))

  val timeout = 10.seconds

  test("lock hardening: metrics atomicity - prevent cross-thread metric contamination") {
    import scala.concurrent.ExecutionContext

    // Create separate execution contexts to simulate different threads
    implicit val ec1: ExecutionContext = ExecutionContext.fromExecutor(
      ThreadUtils.newDaemonSingleThreadExecutor("thread-1"))
    implicit val ec2: ExecutionContext = ExecutionContext.fromExecutor(
      ThreadUtils.newDaemonSingleThreadExecutor("thread-2"))

    tryWithProviderResource(newStoreProvider(useColumnFamilies = false)) { provider =>
      @volatile var thread1Metrics: StateStoreMetrics = null
      @volatile var thread2Metrics: StateStoreMetrics = null

      // Thread 1: Complete transaction and store metrics
      val future1 = Future {
        val taskContext = TaskContext.empty()
        TaskContext.setTaskContext(taskContext)

        val store1 = provider.getStore(0)
        put(store1, "a", 0, 1, StateStore.DEFAULT_COL_FAMILY_NAME)
        assert(store1.commit() === 1)

        // Store metrics from thread 1's perspective
        thread1Metrics = store1.metrics
        taskContext.markTaskCompleted(None)
      }(ec1)

      // Wait for thread 1 to complete
      ThreadUtils.awaitResult(future1, 5.seconds)

      // Thread 2: Start new transaction and access metrics
      val future2 = Future {
        val taskContext = TaskContext.empty()
        TaskContext.setTaskContext(taskContext)

        val store2 = provider.getStore(1)
        put(store2, "b", 0, 2, StateStore.DEFAULT_COL_FAMILY_NAME)
        assert(store2.commit() === 2)

        // Store metrics from thread 2's perspective
        thread2Metrics = store2.metrics
        taskContext.markTaskCompleted(None)
      }(ec2)

      ThreadUtils.awaitResult(future2, 5.seconds)

      // Verify each thread gets its own correct metrics
      // Thread 1 should see metrics reflecting its commit (1 key)
      assert(thread1Metrics.numKeys === 1,
        s"Thread 1 should see 1 key, but saw ${thread1Metrics.numKeys}")

      // Thread 2 should see metrics reflecting its commit (2 keys total)
      assert(thread2Metrics.numKeys === 2,
        s"Thread 2 should see 2 keys, but saw ${thread2Metrics.numKeys}")

      // This test verifies that:
      // 1. Metrics are stored locally during commit (no cross-thread contamination)
      // 2. Each thread gets the correct metrics for its transaction
      // 3. No thread gets metrics from another thread's transaction
    }
  }

  test("lock hardening: abort after commit prevention") {
    tryWithProviderResource(newStoreProvider(useColumnFamilies = false)) { provider =>
      val store = provider.getStore(0)
      put(store, "key", 0, 1, StateStore.DEFAULT_COL_FAMILY_NAME)

      // Commit the store
      store.commit()

      // Attempting to abort after commit should throw StateStoreOperationOutOfOrder
      val exception = intercept[SparkRuntimeException] {
        store.abort()
      }

      checkError(
        exception,
        condition = "STATE_STORE_OPERATION_OUT_OF_ORDER",
        parameters = Map("errorMsg" ->
          ("Expected possible states (" +
            "UPDATING, ABORTED) but found COMMITTED"))
      )
    }
  }

  test("lock hardening: access after close prevention") {
    val provider = newStoreProvider(useColumnFamilies = false)
    val store = provider.getStore(0)
    put(store, "key", 0, 1, StateStore.DEFAULT_COL_FAMILY_NAME)
    store.commit()

    // Manually trigger state machine close to ensure proper state transition
    val stateMachine = PrivateMethod[Any](Symbol("stateMachine"))
    val stateMachineObj = provider invokePrivate stateMachine()
    stateMachineObj.asInstanceOf[RocksDBStateMachine].close()

    // Attempting to get a new store after close should fail
    // with StateStoreInvalidStateMachineTransition
    val exception = intercept[StateStoreInvalidStateMachineTransition] {
      provider.getStore(1)
    }

    assert(exception.getMessage.contains("Old state: CLOSED"))
  }

  test("lock hardening: state machine operation ordering after commit") {
    tryWithProviderResource(newStoreProvider(useColumnFamilies = false)) { provider =>
      val store = provider.getStore(0)
      put(store, "key", 0, 1, StateStore.DEFAULT_COL_FAMILY_NAME)
      store.commit()

      // All update operations should fail after commit due to invalid stamp
      val putException = intercept[StateStoreInvalidStamp] {
        put(store, "key2", 0, 2, StateStore.DEFAULT_COL_FAMILY_NAME)
      }
      assert(putException.getMessage.contains("Invalid stamp"))

      val removeException = intercept[StateStoreInvalidStamp] {
        remove(store, { case (key, _) => key == "key" }, StateStore.DEFAULT_COL_FAMILY_NAME)
      }
      assert(removeException.getMessage.contains("Invalid stamp"))

      // Get operations should also fail with invalid stamp
      val getException = intercept[StateStoreInvalidStamp] {
        get(store, "key", 0, StateStore.DEFAULT_COL_FAMILY_NAME)
      }
      assert(getException.getMessage.contains("Invalid stamp"))
    }
  }

  test("lock hardening: state machine operation ordering after abort") {
    tryWithProviderResource(newStoreProvider(useColumnFamilies = false)) { provider =>
      val store = provider.getStore(0)
      put(store, "key", 0, 1, StateStore.DEFAULT_COL_FAMILY_NAME)
      store.abort()

      // All operations should fail after abort due to invalid stamp
      val putException = intercept[StateStoreInvalidStamp] {
        put(store, "key2", 0, 2, StateStore.DEFAULT_COL_FAMILY_NAME)
      }
      assert(putException.getMessage.contains("Invalid stamp"))

      val getException = intercept[StateStoreInvalidStamp] {
        get(store, "key", 0, StateStore.DEFAULT_COL_FAMILY_NAME)
      }
      assert(getException.getMessage.contains("Invalid stamp"))
    }
  }

  test("lock hardening: concurrent state store instances prevention") {
    tryWithProviderResource(newStoreProvider(useColumnFamilies = false)) { provider =>
      val store1 = provider.getStore(0)
      assertAcquiredThreadIsCurrentThread(provider)

      // Latches to coordinate timing between threads
      val threadStarted = new CountDownLatch(1)
      val proceedToAcquire = new CountDownLatch(1)
      val lockAttempted = new CountDownLatch(1)

      // Start concurrent thread that will timeout waiting
      val concurrentFuture = Future {
        val taskContext = TaskContext.empty()
        TaskContext.setTaskContext(taskContext)

        try {
          threadStarted.countDown()

          // Wait for signal to proceed with lock acquisition
          proceedToAcquire.await(5, TimeUnit.SECONDS)

          lockAttempted.countDown()

          // This should block and eventually timeout/fail
          provider.getStore(0)
          false // Should not reach here
        } catch {
          case ex: Exception if ex.getMessage.contains("could not be acquired") => true
          case ex: Exception if ex.getMessage.contains("not released") => true
          case ex: Exception if ex.getMessage.contains("Waiting to acquire lock") => true
          case ex: Exception =>
            // Log the actual exception for debugging
            logInfo(s"Unexpected exception: ${ex.getClass.getName}: ${ex.getMessage}")
            false
        }
      }

      // Wait for concurrent thread to start
      assert(threadStarted.await(5, TimeUnit.SECONDS), "Concurrent thread should start")

      // Signal the concurrent thread to proceed with lock acquisition
      proceedToAcquire.countDown()

      // Wait for the concurrent thread to attempt lock acquisition
      assert(lockAttempted.await(5, TimeUnit.SECONDS), "Concurrent thread should attempt lock")

      // Give the concurrent thread time to hit the blocking code and timeout
      Thread.sleep(2500) // Wait longer than the 2 second timeout in awaitNotLocked

      // The concurrent thread should have timed out by now, so commit to release lock
      store1.commit()

      // Now the concurrent future should return with the expected error
      val result = awaitResult(concurrentFuture, 5.seconds)
      assert(result, "Concurrent access should be prevented with proper error")

      // After commit, new access should work
      val secondStore = provider.getStore(1)
      assertAcquiredThreadIsCurrentThread(provider)
      secondStore.abort()
    }
  }

  test("lock hardening: task completion listener releases ownership") {
    tryWithProviderResource(newStoreProvider(useColumnFamilies = false)) { provider =>
      var taskCompleted = false
      var storeStamp: Long = -1

      val taskFuture = Future {
        val taskContext = TaskContext.empty()
        TaskContext.setTaskContext(taskContext)

        val store = provider.getStore(0)
        val stateMachine = PrivateMethod[Any](Symbol("stateMachine"))
        val stateMachineObj = provider invokePrivate stateMachine()
        storeStamp = stateMachineObj.asInstanceOf[RocksDBStateMachine].currentValidStamp.get()
        put(store, "key", 0, 1, StateStore.DEFAULT_COL_FAMILY_NAME)

        // Simulate task failure without explicit abort
        taskContext.markTaskCompleted(Some(new SparkException("Task failure injection")))
        taskCompleted = true

        // Don't explicitly abort - let TaskCompletionListener handle it
      }

      awaitResult(taskFuture, timeout)
      assert(taskCompleted)

      // Wait a bit for TaskCompletionListener to execute
      Thread.sleep(100)

      // Verify that ownership was released by the TaskCompletionListener
      val stateMachine = PrivateMethod[Any](Symbol("stateMachine"))
      val stateMachineObj = provider invokePrivate stateMachine()
      val currentStamp = stateMachineObj.asInstanceOf[RocksDBStateMachine].currentValidStamp.get()
      assert(currentStamp == -1,
        s"State machine should be unlocked (stamp = -1) but was $currentStamp")
    }
  }

  test("lock hardening: concurrent access serialization without deadlocks") {
    tryWithProviderResource(newStoreProvider(useColumnFamilies = false)) { provider =>
      val numThreads = 3
      val latch = new CountDownLatch(numThreads)
      var completedThreads = 0
      val results = new Array[Boolean](numThreads)
      var exceptions = List.empty[Throwable]

      val futures = (0 until numThreads).map { threadId =>
        Future {
          val taskContext = TaskContext.empty()
          TaskContext.setTaskContext(taskContext)

          try {
            latch.countDown()
            latch.await(5, TimeUnit.SECONDS) // Wait for all threads to be ready

            val store = provider.getStore(0)
            put(store, s"key$threadId",
              threadId, threadId * 100, StateStore.DEFAULT_COL_FAMILY_NAME)

            // Verify this thread has ownership
            assertAcquiredThreadIsCurrentThread(provider)

            store.commit()

            synchronized {
              completedThreads += 1
              results(threadId) = true
            }
            true

          } catch {
            case ex: Throwable =>
              synchronized {
                exceptions = ex :: exceptions
              }
              false
          }
        }
      }

      // Wait for all futures to complete
      futures.foreach(f => awaitResult(f, timeout))

      // Verify results
      assert(exceptions.isEmpty, s"Unexpected exceptions: ${exceptions.mkString(", ")}")
      assert(completedThreads == numThreads,
        s"Expected $numThreads threads to complete, got $completedThreads")
      assert(results.forall(identity), "All threads should have completed successfully")
    }
  }

  test("lock hardening: read-to-write store upgrade stamp consistency") {
    tryWithProviderResource(newStoreProvider(useColumnFamilies = false)) { provider =>
      // Get a read-only store first
      val readStore = provider.getReadStore(0)

      // Upgrade to write store
      val writeStore = provider.upgradeReadStoreToWriteStore(readStore, 0)

      // Verify the upgrade maintains stamp consistency
      assertAcquiredThreadIsCurrentThread(provider)

      // Should be able to perform write operations
      put(writeStore, "key", 0, 1, StateStore.DEFAULT_COL_FAMILY_NAME)
      writeStore.commit()

      // Verify read store operations now fail with invalid stamp
      val exception = intercept[StateStoreInvalidStamp] {
        readStore.get(dataToKeyRow("key", 0), StateStore.DEFAULT_COL_FAMILY_NAME)
      }
      assert(exception.getMessage.contains("Invalid stamp"))
    }
  }

  test("lock hardening: provider state machine transitions") {
    tryWithProviderResource(newStoreProvider(useColumnFamilies = false)) { provider =>
      // Initially should be in RELEASED state (no acquired thread info)
      val stateMachine = PrivateMethod[Any](Symbol("stateMachine"))
      val stateMachineObj = provider invokePrivate stateMachine()
      val initialThreadInfo =
        stateMachineObj.asInstanceOf[RocksDBStateMachine].getAcquiredThreadInfo
      assert(initialThreadInfo.isEmpty, "Initial state should have no acquired thread info")

      // Acquire a store - should transition to ACQUIRED
      val store = provider.getStore(0)
      assertAcquiredThreadIsCurrentThread(provider)

      // Verify stamp is valid
      val stateMachine2 = PrivateMethod[Any](Symbol("stateMachine"))
      val stateMachineObj2 = provider invokePrivate stateMachine2()
      val stamp = stateMachineObj2.asInstanceOf[RocksDBStateMachine].currentValidStamp.get()
      assert(stamp != -1, "Valid stamp should not be -1")

      // Commit and verify transition back to RELEASED
      store.commit()

      val stateMachine3 = PrivateMethod[Any](Symbol("stateMachine"))
      val stateMachineObj3 = provider invokePrivate stateMachine3()
      val finalStamp = stateMachineObj3.asInstanceOf[RocksDBStateMachine].currentValidStamp.get()
      assert(finalStamp == -1, "After commit, stamp should be -1 (released)")

      // Note: Thread info may still be present after commit as it's only cleared when
      // the provider is accessed again or explicitly released
    }
  }

  test("lock hardening: metrics access control during UPDATING state") {
    tryWithProviderResource(newStoreProvider(useColumnFamilies = false)) { provider =>
      val store = provider.getStore(0)
      put(store, "key", 0, 1, StateStore.DEFAULT_COL_FAMILY_NAME)

      // Metrics should not be accessible during UPDATING state
      val exception = intercept[SparkRuntimeException] {
        store.metrics
      }

      checkError(
        exception,
        condition = "STATE_STORE_OPERATION_OUT_OF_ORDER",
        parameters = Map("errorMsg" ->
          "Cannot get metrics in UPDATING state")
      )

      // After commit, metrics should be accessible
      store.commit()
      val metrics = store.metrics
      assert(metrics.numKeys == 1)
    }
  }

  test("lock hardening: checkpoint info access control during UPDATING state") {
    tryWithProviderResource(newStoreProvider(useColumnFamilies = false)) { provider =>
      val store = provider.getStore(0)
      put(store, "key", 0, 1, StateStore.DEFAULT_COL_FAMILY_NAME)

      // Checkpoint info should not be accessible during UPDATING state
      val exception = intercept[SparkRuntimeException] {
        store.getStateStoreCheckpointInfo()
      }

      checkError(
        exception,
        condition = "STATE_STORE_OPERATION_OUT_OF_ORDER",
        parameters = Map("errorMsg" ->
          "Cannot get metrics in UPDATING state")
      )

      // After commit, checkpoint info should be accessible
      store.commit()
      val checkpointInfo = store.getStateStoreCheckpointInfo()
      assert(checkpointInfo != null)
    }
  }

  test("lock hardening: multiple instance prevention with detailed error") {
    tryWithProviderResource(newStoreProvider(useColumnFamilies = false)) { provider =>
      val store1 = provider.getStore(0)

      // Try to get another instance from a different thread
      val concurrentFuture = Future {
        val taskContext = TaskContext.empty()
        TaskContext.setTaskContext(taskContext)

        val startTime = System.currentTimeMillis()
        val exception = intercept[SparkException] {
          provider.getStore(0)
        }
        val endTime = System.currentTimeMillis()

        // Verify error message contains expected details
        val message = exception.getMessage
        (message.contains("UNRELEASED_THREAD_ERROR"),
          endTime - startTime)
      }

      val (hasCorrectError, duration) = awaitResult(concurrentFuture, timeout)
      assert(hasCorrectError, "Should get unreleased thread error or timeout waiting for lock")

      // Verify it actually waited (didn't fail immediately)
      assert(duration >= 2000,
        s"Should have waited at least 2 seconds but only waited $duration ms")

      store1.commit()
    }
  }

  test("lock hardening: stamp verification prevents unauthorized access") {
    tryWithProviderResource(newStoreProvider(useColumnFamilies = false)) { provider =>
      val store = provider.getStore(0)
      val stateMachine = PrivateMethod[Any](Symbol("stateMachine"))
      val stateMachineObj = provider invokePrivate stateMachine()
      val validStamp = stateMachineObj.asInstanceOf[RocksDBStateMachine].currentValidStamp.get()

      // Simulate stamp verification with correct stamp
      stateMachineObj.asInstanceOf[RocksDBStateMachine].verifyStamp(validStamp) // Should not throw

      // Simulate stamp verification with incorrect stamp
      val incorrectStamp = validStamp + 1
      val exception = intercept[StateStoreInvalidStamp] {
        stateMachineObj.asInstanceOf[RocksDBStateMachine].verifyStamp(incorrectStamp)
      }
      assert(exception.getMessage.contains("Invalid stamp"))

      store.abort()

      // After abort, even the originally valid stamp should be invalid
      val postAbortException = intercept[StateStoreInvalidStamp] {
        val stateMachine2 = PrivateMethod[Any](Symbol("stateMachine"))
        val stateMachineObj2 = provider invokePrivate stateMachine2()
        stateMachineObj2.asInstanceOf[RocksDBStateMachine].verifyStamp(validStamp)
      }
      assert(postAbortException.getMessage.contains("Invalid stamp"))
    }
  }

  test("SPARK-53276: safe provider close during concurrent commit operations") {
    // This test simulates the scenario fixed by SPARK-53276:
    // - Multiple partitions are processing, some hit errors while others are committing
    // - StateStore.stop() is called, which tries to close all providers
    // - The fix ensures RocksDB is only closed when no other thread is using it

    tryWithProviderResource(newStoreProvider(useColumnFamilies = false)) { provider =>
      val commitInProgressLatch = new CountDownLatch(1)
      val proceedWithCloseLatch = new CountDownLatch(1)
      val commitCompletedLatch = new CountDownLatch(1)

      @volatile var commitThreadException: Option[Throwable] = None
      @volatile var closeResult: Option[Boolean] = None

      // Thread 1: Simulate partition that's in the middle of commit (partition 1)
      val commitFuture = Future {
        val taskContext = TaskContext.empty()
        TaskContext.setTaskContext(taskContext)

        try {
          val store = provider.getStore(0)
          put(store, "partition1_key", 0, 1, StateStore.DEFAULT_COL_FAMILY_NAME)

          // Signal that we're about to start commit
          commitInProgressLatch.countDown()

          // Wait for close thread to start attempting close
          proceedWithCloseLatch.await(5, TimeUnit.SECONDS)

          // Simulate some commit processing time
          Thread.sleep(500)

          // This commit should succeed even while close() is being called
          val version = store.commit()
          assert(version === 1)

          commitCompletedLatch.countDown()
        } catch {
          case ex: Throwable =>
            commitThreadException = Some(ex)
            commitCompletedLatch.countDown()
        }
      }

      // Thread 2: Simulate StateStore.stop() being called (from partition 0 error)
      val closeFuture = Future {
        try {
          // Wait for commit thread to start its transaction
          commitInProgressLatch.await(5, TimeUnit.SECONDS)

          // Signal commit thread to proceed with its work
          proceedWithCloseLatch.countDown()

          // Attempt to close the provider (this simulates StateStore.stop())
          // With the fix, this should wait until the commit thread releases the RocksDB
          provider.close()
          closeResult = Some(true)

        } catch {
          case ex: Throwable =>
            // Log unexpected exceptions for debugging
            logInfo(s"Close thread exception: ${ex.getClass.getName}: ${ex.getMessage}")
        }
      }

      // Wait for both operations to complete
      awaitResult(commitFuture, 10.seconds)
      awaitResult(closeFuture, 10.seconds)

      // Verify that commit completed successfully without exceptions
      assert(commitCompletedLatch.await(1, TimeUnit.SECONDS),
        "Commit should have completed")
      assert(commitThreadException.isEmpty,
        s"Commit should not have thrown exception: ${commitThreadException.map(_.getMessage)}")

      // The state machine should have coordinated the close properly:
      // - Either close() returned true (indicating it was safe to close)
      // - Or close() waited until the commit completed before proceeding

      // After everything completes, the provider should be properly closed
      val stateMachine = PrivateMethod[Any](Symbol("stateMachine"))
      val stateMachineObj = provider invokePrivate stateMachine()

      // Attempting to get a new store should fail since provider is closed
      val exception = intercept[StateStoreInvalidStateMachineTransition] {
        provider.getStore(1)
      }
      assert(exception.getMessage.contains("Old state: CLOSED"))
    }
  }

  test("SPARK-53276: close waits for concurrent operations to complete") {
    // This test verifies that close() properly waits for ongoing operations
    // to complete before actually closing the RocksDB instance

    tryWithProviderResource(newStoreProvider(useColumnFamilies = false)) { provider =>
      val operationStarted = new CountDownLatch(1)
      val proceedWithClose = new CountDownLatch(1)
      val operationCompleted = new CountDownLatch(1)

      @volatile var operationException: Option[Throwable] = None
      @volatile var closeStartTime: Long = 0
      @volatile var closeEndTime: Long = 0

      // Thread 1: Long-running operation
      val operationFuture = Future {
        val taskContext = TaskContext.empty()
        TaskContext.setTaskContext(taskContext)

        try {
          val store = provider.getStore(0)
          put(store, "key", 0, 1, StateStore.DEFAULT_COL_FAMILY_NAME)

          operationStarted.countDown()

          // Wait for close to be initiated
          proceedWithClose.await(5, TimeUnit.SECONDS)

          // Simulate some processing time
          Thread.sleep(1000)

          // This should complete successfully
          store.commit()
          operationCompleted.countDown()

        } catch {
          case ex: Throwable =>
            operationException = Some(ex)
            operationCompleted.countDown()
        }
      }

      // Thread 2: Close operation
      val closeFuture = Future {
        // Wait for operation to start
        operationStarted.await(5, TimeUnit.SECONDS)

        // Signal operation to proceed
        proceedWithClose.countDown()

        // Start close - this should wait for the operation to complete
        closeStartTime = System.currentTimeMillis()
        provider.close()
        closeEndTime = System.currentTimeMillis()
      }

      // Wait for both to complete
      awaitResult(operationFuture, 10.seconds)
      awaitResult(closeFuture, 10.seconds)

      // Verify operation completed successfully
      assert(operationCompleted.await(1, TimeUnit.SECONDS),
        "Operation should have completed")
      assert(operationException.isEmpty,
        s"Operation should not have failed: ${operationException.map(_.getMessage)}")

      // Verify close waited for operation (should take at least 1 second)
      val closeDuration = closeEndTime - closeStartTime
      assert(closeDuration >= 900, // Allow some margin for timing
        s"Close should have waited for operation, but took only $closeDuration ms")

      // Verify provider is properly closed
      val exception = intercept[StateStoreInvalidStateMachineTransition] {
        provider.getStore(1)
      }
      assert(exception.getMessage.contains("Old state: CLOSED"))
    }
  }

  // Helper method to assert current thread has ownership
  def assertAcquiredThreadIsCurrentThread(provider: RocksDBStateStoreProvider): Unit = {
    val stateMachine = PrivateMethod[Any](Symbol("stateMachine"))
    val stateMachineObj = provider invokePrivate stateMachine()
    val threadInfo = stateMachineObj.asInstanceOf[RocksDBStateMachine].getAcquiredThreadInfo
    assert(threadInfo.isDefined,
      "acquired thread info should not be null after load")
    val threadId = threadInfo.get.threadRef.get.get.getId
    assert(
      threadId == Thread.currentThread().getId,
      s"acquired thread should be current thread ${Thread.currentThread().getId} " +
        s"after load but was $threadId")
  }
}

/**
 * Test suite that runs all RocksDBStateStoreLockHardeningSuite tests with row checksum enabled.
 */
class RocksDBStateStoreLockHardeningSuiteWithRowChecksum
  extends RocksDBStateStoreLockHardeningSuite with EnableStateStoreRowChecksum
