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

import java.util.concurrent.TimeUnit
import java.util.concurrent.atomic.AtomicLong
import javax.annotation.concurrent.GuardedBy

import scala.ref.WeakReference

import org.apache.spark.TaskContext
import org.apache.spark.internal.{Logging, LogKeys}
import org.apache.spark.sql.errors.QueryExecutionErrors

/**
 * A state machine that manages the lifecycle of a RocksDB instance
 *
 * This class enforces proper state transitions and ensures thread-safety for accessing
 * RocksDB instances.
 * It prevents concurrent modifications to the same native RocksDB instance by using
 * a stamp-based locking mechanism.
 *
 * State Lifecycle:
 * - RELEASED: The RocksDB instance is not being accessed by any thread
 * - ACQUIRED: The RocksDB instance is currently being accessed by a thread
 * - CLOSED: The RocksDB instance has been closed and can no longer be used
 *
 * Valid Transitions:
 * - RELEASED -> ACQUIRED: When a thread acquires the RocksDB instance
 * - ACQUIRED -> RELEASED: When a thread releases the RocksDB instance
 * - RELEASED -> CLOSED: When the RocksDB instance is shut down
 * - ACQUIRED -> MAINTENANCE: Maintenance can be performed on an acquired RocksDB instance
 * - RELEASED -> MAINTENANCE: Maintenance can be performed on a released RocksDB instance
 *
 * Stamps:
 * Each time a RocksDB instance is acquired, a unique stamp is generated. This stamp must be
 * presented when performing operations on the RocksDB instance and when releasing it. This ensures
 * that only the stamp owner that acquired the RocksDB instance can release it or perform
 * operations.
 */
class RocksDBStateMachine(
    stateStoreId: StateStoreId,
    rocksDBConf: RocksDBConf) extends Logging {

  private sealed trait STATE
  private case object RELEASED extends STATE
  private case object ACQUIRED extends STATE
  private case object CLOSED extends STATE

  private sealed abstract class OPERATION(name: String) {
    override def toString: String = name
  }
  private case object LOAD extends OPERATION("load")
  private case object RELEASE extends OPERATION("release")
  private case object CLOSE extends OPERATION("close")
  private case object MAINTENANCE extends OPERATION("maintenance")

  private val stateMachineLock = new Object()
  @GuardedBy("stateMachineLock")
  private var state: STATE = RELEASED

  // This is only maintained for logging purposes
  @GuardedBy("stateMachineLock")
  private var acquiredThreadInfo: AcquiredThreadInfo = _

  private val RELEASED_STATE_MACHINE_STAMP: Long = -1L

  /**
   * Map defining all valid state transitions in the state machine.
   * Key: (currentState, operation) -> Value: nextState
   *
   * Valid transitions:
   * - (RELEASED, LOAD) -> ACQUIRED: Acquire exclusive access to the RocksDB instance
   * - (ACQUIRED, RELEASE) -> RELEASED: Release exclusive access
   * - (RELEASED, CLOSE) -> CLOSED: Permanently close the RocksDB instance
   * - (CLOSED, CLOSE) -> CLOSED: Close is idempotent
   * - (RELEASED, MAINTENANCE) -> RELEASED: Maintenance on released RocksDB instance
   * - (ACQUIRED, MAINTENANCE) -> ACQUIRED: Maintenance on acquired RocksDB instance
   */
  private val allowedStateTransitions: Map[(STATE, OPERATION), STATE] = Map(
    (RELEASED, LOAD) -> ACQUIRED,
    (ACQUIRED, RELEASE) -> RELEASED,
    (RELEASED, CLOSE) -> CLOSED,
    (CLOSED, CLOSE) -> CLOSED,  // Idempotent close operation
    (RELEASED, MAINTENANCE) -> RELEASED,
    (ACQUIRED, MAINTENANCE) -> ACQUIRED
  )

  /**
   * Returns information about the thread that currently has the RocksDB instance acquired.
   * This method is exposed for testing purposes only.
   *
   * @return Some(AcquiredThreadInfo) if a thread currently has the RocksDB instance acquired,
   *         None if the RocksDB instance is in RELEASED state
   */
  private[spark] def getAcquiredThreadInfo: Option[AcquiredThreadInfo] =
    stateMachineLock.synchronized {
      Option(acquiredThreadInfo).map(_.copy())
    }

  // Can be read without holding any locks, but should only be updated when
  // stateMachineLock is held.
  private[state] val currentValidStamp = new AtomicLong(RELEASED_STATE_MACHINE_STAMP)
  @GuardedBy("stateMachineLock")
  private var lastValidStamp: Long = 0L

  /**
   * This method is marked "WithLock" because it MUST only be called when the caller
   * already holds the stateMachineLock. Calling this method without holding the lock
   * will result in race conditions and data corruption.
   *
   * @return A new unique stamp value
   */
  @GuardedBy("stateMachineLock")
  private def incAndGetStampWithLock: Long = {
    assert(Thread.holdsLock(stateMachineLock), "Instance lock must be held")
    lastValidStamp += 1
    currentValidStamp.set(lastValidStamp)
    logInfo(log"New stamp: ${MDC(LogKeys.STAMP, currentValidStamp.get())} issued for " +
      log"${MDC(LogKeys.STATE_STORE_ID, stateStoreId)}")
    lastValidStamp
  }

  /**
   * This method is marked "WithLock" because it MUST only be called when the caller
   * already holds the stateMachineLock. The method uses stateMachineLock.wait() which
   * requires the calling stamp owner to own the monitor. Calling this without holding the
   * lock will throw IllegalMonitorStateException.
   *
   * @param operation The operation being attempted (used for error reporting)
   * @throws QueryExecutionErrors.unreleasedThreadError if timeout occurs
   */
  @GuardedBy("stateMachineLock")
  private def awaitNotAcquiredWithLock(operation: OPERATION): Unit = {
    assert(Thread.holdsLock(stateMachineLock), "Instance lock must be held")
    val waitStartTime = System.nanoTime()
    def timeWaitedMs = {
      val elapsedNanos = System.nanoTime() - waitStartTime
      // Convert from nanoseconds to milliseconds
      TimeUnit.MILLISECONDS.convert(elapsedNanos, TimeUnit.NANOSECONDS)
    }
    while (state == ACQUIRED && timeWaitedMs < rocksDBConf.lockAcquireTimeoutMs) {
      stateMachineLock.wait(10)
      // log every 30 seconds
      if (timeWaitedMs % (30 * 1000) == 0) {
        logInfo(log"Waiting to acquire lock for ${MDC(LogKeys.STATE_STORE_ID, stateStoreId)}")
      }
    }
    if (state == ACQUIRED) {
      val newAcquiredThreadInfo = AcquiredThreadInfo()
      val stackTraceOutput = acquiredThreadInfo.threadRef.get.get.getStackTrace.mkString("\n")
      val loggingId = s"StateStoreId(opId=${stateStoreId.operatorId}," +
        s"partId=${stateStoreId.partitionId},name=${stateStoreId.storeName})"
      throw QueryExecutionErrors.unreleasedThreadError(loggingId, operation.toString,
        newAcquiredThreadInfo.toString(), acquiredThreadInfo.toString(), timeWaitedMs,
        stackTraceOutput)
    }
  }

  /**
   * Return the task ID from the active TaskContext if exists for logging purposes.
   * Returns "undefined" if no TaskContext is active.
   */
  private def taskID: String = {
    val taskContext = TaskContext.get()
    if (taskContext != null) {
      taskContext.taskAttemptId().toString
    } else {
      "undefined"
    }
  }

  /**
   * Validates a state operation and updates the internal state if the transition is legal.
   *
   * This method is the core of the state machine that ensures thread-safe access to RocksDB
   * instances. It uses a map-based approach to define valid state transitions,
   * making the state machine logic cleaner and more maintainable.
   *
   * Thread Safety Requirements:
   * - Caller MUST hold the stateMachineLock before calling this method
   * - This is enforced by the synchronized blocks in all public methods
   *
   * Side Effects:
   * - Updates the internal state variable
   * - Sets acquiredThreadInfo when transitioning to ACQUIRED state
   * - Logs state transitions for debugging
   *
   * @param operation The requested state operation (LOAD, RELEASE, CLOSE, or MAINTENANCE)
   * @return A tuple of (oldState, newState) representing the state before and after operation
   * @throws StateStoreInvalidStateMachineTransition if the requested operation is not allowed
   *         from the current state
   */
  @GuardedBy("stateMachineLock")
  private def validateAndTransitionState(operation: OPERATION): (STATE, STATE) = {
    assert(Thread.holdsLock(stateMachineLock), "Instance lock must be held")
    val oldState = state
    val newState = allowedStateTransitions.get((oldState, operation)) match {
      case Some(nextState) => nextState
      case None =>
        // Determine expected state for better error message
        val expectedState = operation match {
          case LOAD => "ACQUIRED"
          case RELEASE => "RELEASED"
          case CLOSE => "CLOSED"
          case MAINTENANCE => oldState.toString
        }
        throw StateStoreErrors.invalidStateMachineTransition(
          oldState.toString, expectedState, operation.toString, stateStoreId)
    }
    state = newState
    if (newState == ACQUIRED) {
      acquiredThreadInfo = AcquiredThreadInfo()
    }
    logInfo(log"Transitioned state from ${MDC(LogKeys.STATE_STORE_STATE, oldState)} " +
      log"to ${MDC(LogKeys.STATE_STORE_STATE, newState)} " +
      log"with operation ${MDC(LogKeys.OPERATION, operation.toString)} " +
      log"for StateStoreId ${MDC(LogKeys.STATE_STORE_ID, stateStoreId)}" +
      log"by TaskID ${MDC(LogKeys.TASK_ID, taskID)}")
    (oldState, newState)
  }

  /**
   * Verifies that the provided stamp matches the current valid stamp.
   * This ensures that operations are performed by the task that acquired the RocksDB instance.
   *
   * @param stamp The stamp to verify against the current valid stamp
   * @throws StateStoreInvalidStamp if the stamp does not match the current valid stamp
   */
  def verifyStamp(stamp: Long): Unit = {
    val currentStamp = currentValidStamp.get()
    if (stamp != currentStamp) {
      logWarning(log"Invalid stamp=${MDC(LogKeys.STAMP, stamp)} " +
        log"used by TaskID=${MDC(LogKeys.TASK_ID, taskID)}")
      throw StateStoreErrors.invalidStamp(stamp, currentStamp)
    }
  }

  /**
   * Releases the RocksDB instance, transitioning it from ACQUIRED to RELEASED state.
   * This can only be called by the stamp owner that acquired the RocksDB instance.
   *
   * @param stamp The stamp that was returned when the RocksDB instance was acquired
   * @param throwEx Whether to throw an exception if the stamp is invalid (default: true)
   * @return true if the RocksDB instance was successfully released, false if stamp was invalid
   *         and throwEx=false
   * @throws StateStoreInvalidStamp if stamp is invalid and throwEx=true
   * @throws StateStoreInvalidStateMachineTransition if the current state doesn't allow release
   */
  def releaseStamp(stamp: Long, throwEx: Boolean = true): Boolean = stateMachineLock.synchronized {
    currentValidStamp.compareAndSet(stamp, RELEASED_STATE_MACHINE_STAMP) match {
      case true =>
        validateAndTransitionState(RELEASE)
        true
      case false =>
        throwEx match {
          case true =>
            val actualStamp = currentValidStamp.get()
            throw StateStoreErrors.invalidStamp(stamp, actualStamp)
          case false =>
            false
        }
    }
  }

  /**
   * Acquires the RocksDB instance for exclusive use by the calling task.
   * Transitions the state from RELEASED to ACQUIRED.
   *
   * This method will block if another task currently has a stamp for the RocksDB instance,
   * waiting up to the configured timeout before throwing an exception.
   *
   * @return A unique stamp that must be used for subsequent operations and release
   * @throws StateStoreInvalidStateMachineTransition if the RocksDB instance is in CLOSED state
   * @throws QueryExecutionErrors.unreleasedThreadError if timeout occurs waiting for another thread
   */
  def acquireStamp(): Long = stateMachineLock.synchronized {
    awaitNotAcquiredWithLock(LOAD)
    validateAndTransitionState(LOAD)
    incAndGetStampWithLock
  }

  /**
   * This verifies that it is in a state that allows maintenance to be performed.
   * This operation is allowed in both RELEASED and ACQUIRED states.
   *
   * @throws StateStoreInvalidStateMachineTransition if the RocksDB instance is in CLOSED state
   */
  def verifyForMaintenance(): Unit = stateMachineLock.synchronized {
    validateAndTransitionState(MAINTENANCE)
  }

  /**
   * Closes the RocksDB instance permanently, transitioning it to CLOSED state.
   * Once closed, the RocksDB instance cannot be used again and all future operations will fail.
   *
   * This method will block if another task currently has a stamp for the RocksDB instance,
   * waiting up to the configured timeout before throwing an exception.
   *
   * @throws StateStoreInvalidStateMachineTransition if called multiple times (idempotent)
   * @throws QueryExecutionErrors.unreleasedThreadError if timeout occurs waiting for another thread
   */
  def close(): Boolean = stateMachineLock.synchronized {
    // return boolean as to whether we need to close or not
    if (state == CLOSED) {
      false
    } else {
      logInfo(log"Trying to close store ${MDC(LogKeys.STATE_STORE_ID, stateStoreId)}")
      awaitNotAcquiredWithLock(CLOSE)
      logInfo(log"Finished waiting to acquire lock," +
        log" transitioning to close store ${MDC(LogKeys.STATE_STORE_ID, stateStoreId)}")
      validateAndTransitionState(CLOSE)
      true
    }
  }
}

case class AcquiredThreadInfo(
    threadRef: WeakReference[Thread] = new WeakReference[Thread](Thread.currentThread()),
    tc: TaskContext = TaskContext.get()) {
  override def toString(): String = {
    val taskStr = if (tc != null) {
      val taskDetails =
        s"partition ${tc.partitionId()}.${tc.attemptNumber()} in stage " +
          s"${tc.stageId()}.${tc.stageAttemptNumber()}, TID ${tc.taskAttemptId()}"
      s", task: $taskDetails"
    } else ""

    s"[ThreadId: ${threadRef.get.map(_.getId)}$taskStr]"
  }
}
