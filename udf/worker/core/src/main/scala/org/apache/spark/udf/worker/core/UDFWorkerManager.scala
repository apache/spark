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
package org.apache.spark.udf.worker.core

import java.util.{ArrayList, HashMap}

import org.apache.spark.annotation.Experimental
import org.apache.spark.udf.worker.UDFWorkerSpecification

/**
 * :: Experimental :: Creates [[WorkerSession]] instances for a given
 * [[UDFWorkerSpecification]], managing [[WorkerDispatcher]] instances and
 * their lifecycle internally.
 *
 * Dispatchers are cached by spec (protobuf value equality) and reused across
 * sessions. The manager tracks the number of active sessions per dispatcher
 * via [[WorkerSession#addSessionCompletionListener]]. When the last session
 * for a dispatcher is closed, the entry is removed and
 * [[onAllDispatcherSessionsClosed]] is called.
 *
 * You might be wondering why the Dispatcher does not track the number of
 * active sessions itself. The reason is that this would create a
 * unavoidable race condition: Clients can provide different worker
 * specs. Therefore, different dispatchers may be required, which cannot all
 * exist for the whole Spark lifetime -> Dispatchers need to be removed/terminated
 * at some point. If Dispatchers were to track their active sessions themselves
 * and we would use this to decide on the dispatcher lifetime, it can always
 * happen that there are concurrent [[createSession]] requests while
 * the Dispatcher is being disposed off - which would create session
 * initialization errors and may cause Spark task/query failures.
 * Instead, we track the active sessions per Dispatcher globally
 * in this manager.
 *
 * Thread safety: a single lock guards all state -- dispatchers, active
 * sessions, and the stopping flag.
 *
 * Subclasses must implement [[doCreateDispatcher]] to provide a concrete
 * dispatcher and [[onAllDispatcherSessionsClosed]] to control dispatcher
 * teardown policy. Keeping a dispatcher alive after
 * [[onAllDispatcherSessionsClosed]] should be a conscious decision
 * and requires additional clean-up logic in the subclasses implemented
 * via [[doStop]].
 */
@Experimental
abstract class UDFWorkerManager(
    workerLogger: WorkerLogger = WorkerLogger.NoOp
) {

  protected val logger: WorkerLogger =
    workerLogger.forClass(getClass)

  /*
   * Why do we need an [[activeSessionCount]] and an [[activeSessions]]
   * list? [[activeSessionCount]] is per dispatcher. [[activeSessions]]
   * is globally and allows us to perform session cleanup on [[stop]].
   * Moreover, this distinction allows us to create sessions without
   * requiring a lock on [[lock]].
   */
  private class DispatcherEntry(val dispatcher: WorkerDispatcher) {
    var activeSessionCount: Int = 0
  }

  // All fields below are guarded by `lock`.
  private val lock = new Object
  private val dispatchers =
    new HashMap[UDFWorkerSpecification, DispatcherEntry]()
  private val activeSessions = new ArrayList[WorkerSession]()
  private var stopped = false

  /**
   * Creates a [[WorkerSession]] for the given worker specification and
   * optional security scope.
   *
   * If a dispatcher for this spec already exists it is reused; otherwise
   * [[doCreateDispatcher]] is called to create one. A completion listener
   * is registered on the session to track when it closes.
   */
  final def createSession(
      workerSpec: UDFWorkerSpecification,
      securityScope: Option[WorkerSecurityScope] = None
  ): WorkerSession = {
    // Get the dispatcher
    val entry = lock.synchronized {
      if (stopped) {
        throwStopped()
      }
      getOrCreateDispatcherEntry(workerSpec)
    }

    // Create a new session (potentially slow -> outside the lock).
    // Note: This might fail if Spark is concurrently being stopped
    // and the dispatcher is cleaned up. As Spark is stopping,
    // this failure is acceptable. On the happy path, no sessions
    // should try to be created while Spark is shutting down.
    val session = entry.dispatcher.createSession(securityScope)
    lock.synchronized {
      if (stopped) {
        session.close()
        throwStopped()
      }
      activeSessions.add(session)
    }

    logger.info(s"Created session ${session.sessionId}" +
      s" on dispatcher ${entry.dispatcher.dispatcherId}" +
      s" (active: ${entry.activeSessionCount})")

    // Register a completion listener that updates the
    // state when the session is canceled or closed
    session.addSessionCompletionListener { session =>
      logger.info(s"Session ${session.sessionId} terminated")
      lock.synchronized {
        if (!stopped) {
          activeSessions.remove(session)
          handleSessionTermination(workerSpec)
        }
      }
    }

    session
  }

  /**
   * Called on driver/executor shutdown. Cancels any active sessions,
   * closes all cached dispatchers, and resets internal state.
   *
   * Safety net -- in normal operation, sessions are closed by task
   * completion listeners and dispatchers are cleaned up via
   * [[onAllDispatcherSessionsClosed]] when their last session closes.
   */
  final def stop(): Unit = {
    logger.info("UDFWorkerManager stopping" +
      s" (${activeSessions.size()} active sessions," +
      s" ${dispatchers.size()} dispatchers)")

    lock.synchronized {
      stopped = true

      // Cancel any sessions that are still active. Cancel is a
      // no-op if the session was already closed/cancelled.
      activeSessions.forEach { s =>
        logger.debug(s"Cancelling session ${s.sessionId}" +
          " during stop")
        s.cancel()
      }
      activeSessions.clear()

      // Close all dispatchers we control.
      // When spark is stopped in a clean state
      // (only finished tasks), it is expected
      // that all dispatchers have been terminated
      // already. This is a safety-net.
      dispatchers.forEach { (_, entry) =>
        logger.debug(s"Closing dispatcher" +
          s" ${entry.dispatcher.dispatcherId} during stop")
        entry.dispatcher.close()
      }
      dispatchers.clear()
    }

    // Perform cleanup in the sub classes
    doStop()
    logger.info("UDFWorkerManager stopped")
  }

  private def throwStopped(): Nothing =
    throw new IllegalStateException(
      "UDFWorkerManager is stopped")

  // Must be called while holding `lock`.
  private def handleSessionTermination(
      workerSpec: UDFWorkerSpecification
  ): Unit = {
    val entry = dispatchers.get(workerSpec)
    // Note: entry == null is unexpected and should
    // throw here.
    entry.activeSessionCount -= 1
    if (entry.activeSessionCount == 0) {
      logger.info("All sessions closed for dispatcher " +
        s"${entry.dispatcher.dispatcherId}, removing from cache")
      dispatchers.remove(workerSpec)
      onAllDispatcherSessionsClosed(entry.dispatcher)
    }
  }

  // Must be called while holding `lock`.
  private def getOrCreateDispatcherEntry(
      workerSpec: UDFWorkerSpecification
  ): DispatcherEntry = {
    var entry = dispatchers.get(workerSpec)
    if (entry == null) {
      val dispatcher = doCreateDispatcher(workerSpec, logger)
      logger.info(s"Created dispatcher ${dispatcher.dispatcherId}")
      entry = new DispatcherEntry(dispatcher)
      dispatchers.put(workerSpec, entry)
    }
    entry.activeSessionCount += 1
    entry
  }

  /**
   * Creates a new [[WorkerDispatcher]] for the given specification.
   * It is expected that creating the dispatcher
   * itself is not slow while creating a session might be.
   */
  protected def doCreateDispatcher(
      workerSpec: UDFWorkerSpecification,
      logger: WorkerLogger
  ): WorkerDispatcher

  /**
   * Called when the last active session for a dispatcher is closed.
   * Subclasses must decide what to do with the now-idle dispatcher.
   * Not called during [[stop]] -- the manager cleans up dispatchers
   * it holds directly in that case. Cleanup of dispatchers not
   * provided to the manager is the responsibility of the subclass
   * via [[doStop]].
   */
  protected def onAllDispatcherSessionsClosed(
      dispatcher: WorkerDispatcher
  ): Unit

  /**
   * Called when the executor/driver stops. Subclasses should clean
   * up any dispatchers/resources they hold beyond what the parent
   * class manages.
   */
  protected def doStop(): Unit
}
