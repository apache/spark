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

package org.apache.spark.sql.connect.service

import java.util.UUID
import java.util.concurrent.{ConcurrentHashMap, ConcurrentMap, Executor, Executors, Semaphore}
import java.util.concurrent.{ScheduledExecutorService, TimeUnit}
import java.util.concurrent.atomic.{AtomicBoolean, AtomicLong, AtomicReference}

import scala.concurrent.duration.FiniteDuration
import scala.jdk.CollectionConverters._
import scala.util.control.NonFatal

import com.google.common.cache.CacheBuilder
import io.grpc.{Context, Status}
import io.grpc.stub.StreamObserver

import org.apache.spark.{SparkEnv, SparkSQLException}
import org.apache.spark.connect.proto
import org.apache.spark.internal.{Logging, LogKeys}
import org.apache.spark.sql.catalyst.util.DateTimeConstants.NANOS_PER_MILLIS
import org.apache.spark.sql.connect.IllegalStateErrors
import org.apache.spark.sql.connect.config.Connect.{CONNECT_EXECUTE_MANAGER_ABANDONED_TOMBSTONES_SIZE, CONNECT_EXECUTE_MANAGER_DETACHED_TIMEOUT, CONNECT_EXECUTE_MANAGER_MAINTENANCE_INTERVAL, CONNECT_EXECUTE_MAX_CONCURRENT_QUERIES, CONNECT_EXECUTE_MAX_CONCURRENT_QUERIES_TIMEOUT}
import org.apache.spark.sql.connect.execution.ExecuteGrpcResponseSender
import org.apache.spark.sql.connect.planner.InvalidInputErrors
import org.apache.spark.util.ThreadUtils

// Unique key identifying execution by combination of user, session and operation id
case class ExecuteKey(userId: String, sessionId: String, operationId: String)

object ExecuteKey {
  def apply(request: proto.ExecutePlanRequest, sessionHolder: SessionHolder): ExecuteKey = {
    val operationId = if (request.hasOperationId) {
      try {
        UUID.fromString(request.getOperationId).toString
      } catch {
        case _: IllegalArgumentException =>
          throw new SparkSQLException(
            errorClass = "INVALID_HANDLE.FORMAT",
            messageParameters = Map("handle" -> request.getOperationId))
      }
    } else {
      UUID.randomUUID().toString
    }
    ExecuteKey(sessionHolder.userId, sessionHolder.sessionId, operationId)
  }
}

/**
 * Marker trait for exceptions that should be surfaced to the client as retryable. When an error
 * carrying this trait is converted to a gRPC status, a `google.rpc.RetryInfo` detail is attached
 * so that Spark Connect clients transparently re-submit the request. See
 * `ErrorUtils.buildStatusFromThrowable`.
 */
private[connect] trait RetryableGrpcError { self: Throwable => }

/**
 * Thrown when a query cannot acquire an execution slot within
 * `spark.connect.execute.maxConcurrentQueries.timeout` while the concurrency limit is reached. It
 * is retryable: the client re-submits the request, re-joining the FIFO wait queue.
 */
private[connect] class SparkConnectConcurrentExecutionsTimeoutException(
    limit: Int,
    timeout: String)
    extends SparkSQLException(
      errorClass = "CONNECT.EXECUTE_CONCURRENT_LIMIT_TIMEOUT",
      messageParameters = Map("limit" -> limit.toString, "timeout" -> timeout))
    with RetryableGrpcError

/** A semaphore permit that can be released at most once. */
private[connect] class ExecutionPermit(private val semaphore: Semaphore) {
  private val released = new AtomicBoolean(false)

  def release(): Unit = {
    if (released.compareAndSet(false, true)) {
      semaphore.release()
    }
  }
}

/**
 * Global tracker of all ExecuteHolder executions.
 *
 * All ExecuteHolders are created, and removed through it. It keeps track of all the executions,
 * and removes executions that have been abandoned.
 */
private[connect] class SparkConnectExecutionManager() extends Logging {

  private var acceptingExecutions = true

  /** Concurrent hash table containing all the current executions. */
  private val executions: ConcurrentMap[ExecuteKey, ExecuteHolder] =
    new ConcurrentHashMap[ExecuteKey, ExecuteHolder]()

  /** Graveyard of tombstones of executions that were abandoned and removed. */
  private val abandonedTombstones = CacheBuilder
    .newBuilder()
    .maximumSize(SparkEnv.get.conf.get(CONNECT_EXECUTE_MANAGER_ABANDONED_TOMBSTONES_SIZE))
    .build[ExecuteKey, ExecuteInfo]()

  /** The time when the last execution was removed. */
  private val lastExecutionTimeNs: AtomicLong = new AtomicLong(System.nanoTime())

  /** Executor for the periodic maintenance */
  private val scheduledExecutor: AtomicReference[ScheduledExecutorService] =
    new AtomicReference[ScheduledExecutorService]()

  /** Configured maximum number of concurrent executions (<= 0 means unlimited). */
  private val maxConcurrentQueries: Int =
    SparkEnv.get.conf.get(CONNECT_EXECUTE_MAX_CONCURRENT_QUERIES)

  /**
   * Time in milliseconds a query waits for an execution slot before giving up with a retryable
   * error. 0 means wait indefinitely. Only relevant when the concurrency limit is enabled.
   */
  private val acquireTimeoutMs: Long =
    SparkEnv.get.conf.get(CONNECT_EXECUTE_MAX_CONCURRENT_QUERIES_TIMEOUT)

  /**
   * Semaphore to control the maximum number of concurrent executions. When maxConcurrentQueries >
   * 0, this semaphore limits concurrent executions. Acquiring a permit means an execution slot is
   * available. The semaphore is fair so that, up to the acquire timeout, waiters are served in
   * FIFO order.
   */
  private val executionSemaphore =
    new Semaphore(if (maxConcurrentQueries > 0) maxConcurrentQueries else Int.MaxValue, true)

  private val directExecutor: Executor = (command: Runnable) => command.run()

  locally {
    logInfo(
      log"Spark Connect execution semaphore initialized with maxConcurrentQueries=" +
        log"${MDC(LogKeys.MAX_SLOTS, maxConcurrentQueries)} " +
        log"(${MDC(LogKeys.NUM_SLOTS, getAvailableExecutionSlots)} permits)")
  }

  /**
   * Get the current number of permits available in the execution semaphore. Exposed for testing.
   */
  private[connect] def getAvailableExecutionSlots: Int = {
    executionSemaphore.availablePermits()
  }

  /** Get the approximate number of queries waiting for an execution slot. Exposed for testing. */
  private[connect] def getExecutionQueueLength: Int = {
    executionSemaphore.getQueueLength
  }

  /**
   * Acquire a permit from the execution semaphore before starting a new execution. Waiters are
   * served in FIFO order. If `acquireTimeoutMs` is 0, this blocks until a slot is available.
   * Otherwise it waits at most that long and, on timeout, throws a retryable
   * [[SparkConnectConcurrentExecutionsTimeoutException]] so the client re-submits the request.
   */
  private[connect] def acquireExecutionSlot(): ExecutionPermit = {
    val availableBefore = executionSemaphore.availablePermits()
    if (availableBefore == 0) {
      val queueLength = executionSemaphore.getQueueLength
      logInfo(
        log"All execution slots are in use. Query will wait in queue. " +
          log"Queue length: ${MDC(LogKeys.THREAD_POOL_WAIT_QUEUE_SIZE, queueLength)}")
    }

    val grpcContext = Context.current()
    if (grpcContext.isCancelled) {
      throw cancelledException()
    }

    val waitingThread = Thread.currentThread()
    val cancellationListener: Context.CancellationListener =
      (_: Context) => waitingThread.interrupt()
    grpcContext.addListener(cancellationListener, directExecutor)

    var acquired = false
    try {
      acquired = if (acquireTimeoutMs > 0) {
        executionSemaphore.tryAcquire(acquireTimeoutMs, TimeUnit.MILLISECONDS)
      } else {
        executionSemaphore.acquire()
        true
      }
    } catch {
      case _: InterruptedException if grpcContext.isCancelled =>
        Thread.interrupted()
        throw cancelledException()
      case e: InterruptedException =>
        Thread.currentThread().interrupt()
        throw e
    } finally {
      grpcContext.removeListener(cancellationListener)
    }

    if (grpcContext.isCancelled) {
      if (acquired) {
        executionSemaphore.release()
      }
      Thread.interrupted()
      throw cancelledException()
    }

    if (!acquired) {
      logInfo(
        log"Query gave up waiting for an execution slot after " +
          log"${MDC(LogKeys.TIMEOUT, acquireTimeoutMs)} ms and will be asked to retry.")
      throw new SparkConnectConcurrentExecutionsTimeoutException(
        maxConcurrentQueries,
        s"$acquireTimeoutMs ms")
    }

    if (availableBefore == 0) {
      logInfo(log"Query acquired execution slot and will start")
    }
    new ExecutionPermit(executionSemaphore)
  }

  private def cancelledException() = {
    Status.CANCELLED
      .withDescription("ExecutePlan RPC was cancelled while queued")
      .asRuntimeException()
  }

  private def serviceUnavailableException() =
    Status.UNAVAILABLE.withDescription("Spark Connect service is stopping").asRuntimeException()

  /**
   * Create a new ExecuteHolder and register it with this global manager and with its session.
   */
  private[connect] def createExecuteHolder(
      executeKey: ExecuteKey,
      request: proto.ExecutePlanRequest,
      sessionHolder: SessionHolder,
      executionPermit: Option[ExecutionPermit] = None): ExecuteHolder = {
    val opId = executeKey.operationId
    val executeHolder = executions.compute(
      executeKey,
      (executeKey, oldExecuteHolder) => {

        // Check if the operation already exists, either in the active execution map
        if (oldExecuteHolder != null) {
          throw new SparkSQLException(
            errorClass = "INVALID_HANDLE.OPERATION_ALREADY_EXISTS",
            messageParameters = Map("handle" -> opId))
        }
        // Check if the operation is already in the graveyard of abandoned executions, or was
        // recently completed. Prevents double execution when client retries on a lost response.
        if (getAbandonedTombstone(executeKey).isDefined ||
          sessionHolder.getOperationStatus(opId).isDefined) {

          logInfo(
            log"Operation ${MDC(LogKeys.EXECUTE_KEY, executeKey)}: Already tombstoned: " +
              log"${MDC(LogKeys.STATUS, getAbandonedTombstone(executeKey).isDefined)}.")
          logInfo(
            log"Operation ${MDC(LogKeys.EXECUTE_KEY, executeKey)}: Seen previously: " +
              log"${MDC(LogKeys.STATUS, sessionHolder.getOperationStatus(opId).isDefined)}.")

          throw new SparkSQLException(
            errorClass = "INVALID_HANDLE.OPERATION_ABANDONED",
            messageParameters = Map("handle" -> opId))
        }
        new ExecuteHolder(executeKey, request, sessionHolder, executionPermit)
      })

    sessionHolder.addOperationId(opId)

    logInfo(log"ExecuteHolder ${MDC(LogKeys.EXECUTE_KEY, executeHolder.key)} is created.")

    schedulePeriodicChecks() // Starts the maintenance thread if it hasn't started.

    executeHolder
  }

  /**
   * Create a new ExecuteHolder and register it with this global manager and with its session.
   */
  private[connect] def createExecuteHolder(v: proto.ExecutePlanRequest): ExecuteHolder = {
    val previousSessionId = v.hasClientObservedServerSideSessionId match {
      case true => Some(v.getClientObservedServerSideSessionId)
      case false => None
    }
    val sessionHolder = SparkConnectService
      .getOrCreateIsolatedSession(v.getUserContext.getUserId, v.getSessionId, previousSessionId)
    val executeKey = ExecuteKey(v, sessionHolder)
    createExecuteHolder(executeKey, v, sessionHolder)
  }

  /**
   * Remove an ExecuteHolder from this global manager and from its session. Interrupt the
   * execution if still running, free all resources.
   */
  private[connect] def removeExecuteHolder(key: ExecuteKey, abandoned: Boolean = false): Unit = {
    var removedHolder: ExecuteHolder = null
    executions.computeIfPresent(
      key,
      (_, executeHolder) => {
        // Put into abandonedTombstones before removing it from executions, so that the client ends
        // up getting an INVALID_HANDLE.OPERATION_ABANDONED error on a retry.
        if (abandoned) {
          abandonedTombstones.put(key, executeHolder.getExecuteInfo)
          executeHolder.sessionHolder.closeOperation(executeHolder)
        }
        removedHolder = executeHolder
        null
      })

    if (removedHolder == null) {
      return
    }
    val executeHolder = removedHolder

    try {
      executeHolder.sessionHolder.closeOperation(executeHolder)
      updateLastExecutionTime()
      logInfo(log"ExecuteHolder ${MDC(LogKeys.EXECUTE_KEY, key)} is removed.")
      executeHolder.close()

      if (abandoned) {
        // Update in abandonedTombstones: above it wasn't yet updated with closedTime etc.
        abandonedTombstones.put(key, executeHolder.getExecuteInfo)
      }
    } finally {
      executeHolder.releaseExecutionPermit()
      logDebug(
        log"Execution slot released. Available slots: " +
          log"${MDC(LogKeys.NUM_SLOTS, getAvailableExecutionSlots)}")
    }
  }

  private[connect] def getExecuteHolder(key: ExecuteKey): Option[ExecuteHolder] = {
    Option(executions.get(key))
  }

  /**
   * Create a new ExecuteHolder, register it with this global manager and with its session, and
   * attach the given response observer to it.
   */
  private[connect] def createExecuteHolderAndAttach(
      executeKey: ExecuteKey,
      request: proto.ExecutePlanRequest,
      sessionHolder: SessionHolder,
      responseObserver: StreamObserver[proto.ExecutePlanResponse]): ExecuteHolder = {
    // Acquire a slot from the semaphore before starting execution.
    // This blocks if max concurrent queries limit is reached.
    val executionPermit = acquireExecutionSlot()

    val executeHolder =
      try {
        synchronized {
          if (!acceptingExecutions) {
            throw serviceUnavailableException()
          }
          createExecuteHolder(executeKey, request, sessionHolder, Some(executionPermit))
        }
      } catch {
        case t: Throwable =>
          executionPermit.release()
          throw t
      }

    try {
      // SPARK-53339: Validate the plan before starting the execution thread.
      // postStarted() was moved into executeInternal(), so invalid plans that previously
      // caused postStarted() to throw (and thus triggered removeExecuteHolder in this
      // catch block) now fail asynchronously inside the execution thread. This early
      // validation ensures that invalid plans are still caught synchronously here.
      request.getPlan.getOpTypeCase match {
        case proto.Plan.OpTypeCase.ROOT | proto.Plan.OpTypeCase.COMMAND => // valid
        case other =>
          throw InvalidInputErrors.invalidOneOfField(other, request.getPlan.getDescriptorForType)
      }
      executeHolder.start()
    } catch {
      // Errors raised before the execution holder has finished spawning a thread are considered
      // plan execution failure, and the client should not try reattaching it afterwards.
      case t: Throwable =>
        removeExecuteHolder(executeHolder.key)
        throw t
    }

    try {
      val responseSender =
        new ExecuteGrpcResponseSender[proto.ExecutePlanResponse](executeHolder, responseObserver)
      executeHolder.runGrpcResponseSender(responseSender)
    } finally {
      executeHolder.afterInitialRPC()
    }
    executeHolder
  }

  /**
   * Reattach the given response observer to the given ExecuteHolder.
   */
  private[connect] def reattachExecuteHolder(
      executeHolder: ExecuteHolder,
      responseObserver: StreamObserver[proto.ExecutePlanResponse],
      lastConsumedResponseId: Option[String]): Unit = {
    if (!executeHolder.reattachable) {
      logWarning(log"Reattach to not reattachable operation.")
      throw new SparkSQLException(
        errorClass = "INVALID_CURSOR.NOT_REATTACHABLE",
        messageParameters = Map.empty)
    } else if (executeHolder.isOrphan()) {
      logWarning(log"Reattach to an orphan operation.")
      removeExecuteHolder(executeHolder.key)
      throw IllegalStateErrors.operationOrphaned(executeHolder.key.toString)
    }

    val responseSender =
      new ExecuteGrpcResponseSender[proto.ExecutePlanResponse](executeHolder, responseObserver)
    lastConsumedResponseId match {
      case Some(lastResponseId) =>
        // start from response after lastResponseId
        executeHolder.runGrpcResponseSender(responseSender, lastResponseId)
      case None =>
        // start from the start of the stream.
        executeHolder.runGrpcResponseSender(responseSender)
    }
  }

  private[connect] def removeAllExecutionsForSession(key: SessionKey): Unit = {
    executions.forEach((_, executeHolder) => {
      if (executeHolder.sessionHolder.key == key) {
        val info = executeHolder.getExecuteInfo
        logInfo(
          log"Execution ${MDC(LogKeys.EXECUTE_INFO, info)} removed in removeSessionExecutions.")
        removeExecuteHolder(executeHolder.key, abandoned = true)
      }
    })
  }

  /** Get info about abandoned execution, if there is one. */
  private[connect] def getAbandonedTombstone(key: ExecuteKey): Option[ExecuteInfo] = {
    Option(abandonedTombstones.getIfPresent(key))
  }

  /**
   * If there are no executions, return Left with System.nanoTime of last active execution.
   * Otherwise return Right with list of ExecuteInfo of all executions.
   */
  def listActiveExecutions: Either[Long, Seq[ExecuteInfo]] = {
    if (executions.isEmpty) {
      Left(lastExecutionTimeNs.getAcquire())
    } else {
      Right(executions.values().asScala.map(_.getExecuteInfo).toBuffer.toSeq)
    }
  }

  /**
   * Return list of executions that got abandoned and removed by periodic maintenance. This is a
   * cache, and the tombstones will be eventually removed.
   */
  def listAbandonedExecutions: Seq[ExecuteInfo] = {
    abandonedTombstones.asMap.asScala.values.toSeq
  }

  private[connect] def shutdown(): Unit = {
    synchronized {
      acceptingExecutions = false
    }

    val executor = scheduledExecutor.getAndSet(null)
    if (executor != null) {
      ThreadUtils.shutdown(executor, FiniteDuration(1, TimeUnit.MINUTES))
    }

    executions.keySet().asScala.foreach(key => removeExecuteHolder(key))
    abandonedTombstones.invalidateAll()

    updateLastExecutionTime()
  }

  private[connect] def start(): Unit = synchronized {
    acceptingExecutions = true
  }

  /**
   * Updates the last execution time after the last execution has been removed.
   */
  private def updateLastExecutionTime(): Unit = {
    lastExecutionTimeNs.getAndUpdate(prev => prev.max(System.nanoTime()))
  }

  /**
   * Schedules periodic maintenance checks if it is not already scheduled. The checks are looking
   * for executions that have not been closed, but are left with no RPC attached to them, and
   * removes them after a timeout.
   */
  private def schedulePeriodicChecks(): Unit = {
    var executor = scheduledExecutor.getAcquire()
    if (executor == null) {
      executor = Executors.newSingleThreadScheduledExecutor()
      if (scheduledExecutor.compareAndExchangeRelease(null, executor) == null) {
        val interval = SparkEnv.get.conf.get(CONNECT_EXECUTE_MANAGER_MAINTENANCE_INTERVAL)
        logInfo(
          log"Starting thread for cleanup of abandoned executions every " +
            log"${MDC(LogKeys.INTERVAL, interval)} ms")
        executor.scheduleAtFixedRate(
          () => {
            try {
              val timeoutNs =
                SparkEnv.get.conf.get(CONNECT_EXECUTE_MANAGER_DETACHED_TIMEOUT) * NANOS_PER_MILLIS
              periodicMaintenance(timeoutNs)
            } catch {
              case NonFatal(ex) => logWarning("Unexpected exception in periodic task", ex)
            }
          },
          interval,
          interval,
          TimeUnit.MILLISECONDS)
      }
    }
  }

  // Visible for testing.
  private[connect] def periodicMaintenance(timeoutNs: Long): Unit = {
    // Find any detached executions that expired and should be removed.
    logDebug("Started periodic run of SparkConnectExecutionManager maintenance.")

    val nowNs = System.nanoTime()
    executions.forEach((_, executeHolder) => {
      executeHolder.lastAttachedRpcTimeNs match {
        case Some(detachedNs) =>
          if (detachedNs + timeoutNs <= nowNs) {
            val info = executeHolder.getExecuteInfo
            logInfo(
              log"Found execution ${MDC(LogKeys.EXECUTE_INFO, info)} that was abandoned " +
                log"and expired and will be removed.")
            removeExecuteHolder(executeHolder.key, abandoned = true)
          }
        case _ => // execution is active
      }
    })

    logDebug("Finished periodic run of SparkConnectExecutionManager maintenance.")
  }

  // For testing.
  private[connect] def setAllRPCsDeadline(deadlineNs: Long) = {
    executions.values().asScala.foreach(_.setGrpcResponseSendersDeadline(deadlineNs))
  }

  // For testing.
  private[connect] def interruptAllRPCs() = {
    executions.values().asScala.foreach(_.interruptGrpcResponseSenders())
  }

  private[connect] def listExecuteHolders: Seq[ExecuteHolder] = {
    executions.values().asScala.toSeq
  }
}
