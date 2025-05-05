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

import org.apache.spark.internal.{Logging, LogKeys, MDC}
import org.apache.spark.sql.errors.QueryExecutionErrors

/**
 * A state machine that manages the lifecycle of RocksDB state store instances.
 *
 * This class enforces proper state transitions and ensures thread-safety for accessing
 * state stores. It prevents concurrent modifications to the same state store by using
 * a stamp-based locking mechanism.
 *
 * State Lifecycle:
 * - RELEASED: The store is not being accessed by any thread
 * - ACQUIRED: The store is currently being accessed by a thread
 * - CLOSED: The store has been closed and can no longer be used
 *
 * Valid Transitions:
 * - RELEASED -> ACQUIRED: When a thread acquires the store
 * - ACQUIRED -> RELEASED: When a thread releases the store
 * - RELEASED -> CLOSED: When the store is shut down
 * - ACQUIRED -> MAINTENANCE: Maintenance can be performed on an acquired store
 * - RELEASED -> MAINTENANCE: Maintenance can be performed on a released store
 *
 * Stamps:
 * Each time a store is acquired, a unique stamp is generated. This stamp must be presented
 * when performing operations on the store and when releasing it. This ensures that only
 * the thread that acquired the store can release it or perform operations on it.
 */
class RocksDBStateStoreProviderStateMachine(
    stateStoreId: StateStoreId,
    rocksDBConf: RocksDBConf) extends Logging {

  private sealed trait STATE
  private case object RELEASED extends STATE
  private case object ACQUIRED extends STATE
  private case object CLOSED extends STATE

  private sealed abstract class TRANSITION(name: String) {
    override def toString: String = name
  }
  private case object LOAD extends TRANSITION("load")
  private case object RELEASE extends TRANSITION("release")
  private case object CLOSE extends TRANSITION("close")
  private case object MAINTENANCE extends TRANSITION("maintenance")

  private val instanceLock = new Object()
  @GuardedBy("instanceLock")
  private var state: STATE = RELEASED
  @GuardedBy("instanceLock")
  private var acquiredThreadInfo: AcquiredThreadInfo = _

  // Can be read without holding any locks, but should only be updated when
  // instanceLock is held.
  // -1 indicates that the store is not locked.
  private[sql] val currentValidStamp = new AtomicLong(-1L)
  @GuardedBy("instanceLock")
  private var lastValidStamp: Long = 0L

  // Instance lock must be held.
  private def incAndGetStamp: Long = {
    lastValidStamp += 1
    currentValidStamp.set(lastValidStamp)
    lastValidStamp
  }

  // Instance lock must be held.
  private def awaitNotLocked(transition: TRANSITION): Unit = {
    val waitStartTime = System.nanoTime()
    def timeWaitedMs = {
      val elapsedNanos = System.nanoTime() - waitStartTime
      // Convert from nanoseconds to milliseconds
      TimeUnit.MILLISECONDS.convert(elapsedNanos, TimeUnit.NANOSECONDS)
    }
    while (state == ACQUIRED && timeWaitedMs < rocksDBConf.lockAcquireTimeoutMs) {
      instanceLock.wait(10)
    }
    if (state == ACQUIRED) {
      val newAcquiredThreadInfo = AcquiredThreadInfo()
      val stackTraceOutput = acquiredThreadInfo.threadRef.get.get.getStackTrace.mkString("\n")
      val loggingId = s"StateStoreId(opId=${stateStoreId.operatorId}," +
        s"partId=${stateStoreId.partitionId},name=${stateStoreId.storeName})"
      throw QueryExecutionErrors.unreleasedThreadError(loggingId, transition.toString,
        newAcquiredThreadInfo.toString(), acquiredThreadInfo.toString(), timeWaitedMs,
        stackTraceOutput)
    }
  }

  /**
   * Returns oldState, newState.
   * Throws error if transition is illegal.
   * MUST be called for every StateStoreProvider method.
   * Caller MUST hold instance lock.
   */
  private def validateAndTransitionState(transition: TRANSITION): (STATE, STATE) = {
    val oldState = state
    val newState = transition match {
      case LOAD =>
        oldState match {
          case RELEASED => ACQUIRED
          case ACQUIRED => throw new IllegalStateException("Cannot lock when state is LOCKED")
          case CLOSED => throw new IllegalStateException("Cannot lock when state is CLOSED")
        }
      case RELEASE =>
        oldState match {
          case RELEASED => throw new IllegalStateException("Cannot unlock when state is UNLOCKED")
          case ACQUIRED => RELEASED
          case CLOSED => throw new IllegalStateException("Cannot unlock when state is CLOSED")
        }
      case CLOSE =>
        oldState match {
          case RELEASED => CLOSED
          case ACQUIRED => throw new IllegalStateException("Cannot closed when state is LOCKED")
          case CLOSED => CLOSED
        }
      case MAINTENANCE =>
        oldState match {
          case RELEASED => RELEASED
          case ACQUIRED => ACQUIRED
          case CLOSED => throw new IllegalStateException("Cannot do maintenance when state is" +
            "CLOSED")
        }
    }
    state = newState
    if (newState == ACQUIRED) {
      acquiredThreadInfo = AcquiredThreadInfo()
    }
    logInfo(log"Transitioned state from ${MDC(LogKeys.STATE_STORE_STATE, oldState)} " +
      log"to ${MDC(LogKeys.STATE_STORE_STATE, newState)} " +
      log"for StateStoreId ${MDC(LogKeys.STATE_STORE_ID, stateStoreId)}")
    (oldState, newState)
  }

  def verifyStamp(stamp: Long): Unit = {
    if (stamp != currentValidStamp.get()) {
      throw new IllegalStateException(s"Invalid stamp $stamp, " +
        s"currentStamp: ${currentValidStamp.get()}")
    }
  }

  // Returns whether store successfully released
  def releaseStore(stamp: Long, throwEx: Boolean = true): Boolean = instanceLock.synchronized {
    if (!currentValidStamp.compareAndSet(stamp, -1L)) {
      if (throwEx) {
        throw new IllegalStateException("Invalid stamp for release")
      } else {
        return false
      }
    }
    validateAndTransitionState(RELEASE)
    true
  }

  def acquireStore(): Long = instanceLock.synchronized {
    awaitNotLocked(LOAD)
    validateAndTransitionState(LOAD)
    incAndGetStamp
  }

  def maintenanceStore(): Unit = instanceLock.synchronized {
    validateAndTransitionState(MAINTENANCE)
  }

  def closeStore(): Unit = instanceLock.synchronized {
    awaitNotLocked(CLOSE)
    validateAndTransitionState(CLOSE)
  }
}
