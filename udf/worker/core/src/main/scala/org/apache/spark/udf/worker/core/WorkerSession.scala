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

import java.util.ArrayList
import java.util.concurrent.atomic.AtomicBoolean

import org.apache.spark.annotation.Experimental

/**
 * :: Experimental ::
 * Carries all information needed to initialize a UDF execution on a worker.
 *
 * This message is passed to [[WorkerSession#init]] and contains the function
 * definition, schemas, and any additional configuration.
 *
 * Placeholder: will be replaced by a generated proto message once the
 * UDF wire protocol lands. Do not rely on case-class equality --
 * `Array[Byte]` fields compare by reference.
 *
 * @param functionPayload serialized function (e.g., pickled Python, JVM bytes)
 * @param inputSchema     serialized input schema (e.g., Arrow schema bytes)
 * @param outputSchema    serialized output schema (e.g., Arrow schema bytes)
 * @param properties      additional key-value configuration. Can carry
 *                        protocol-specific or engine-specific metadata that
 *                        does not yet have a dedicated field.
 */
@Experimental
case class InitMessage(
    functionPayload: Array[Byte],
    inputSchema: Array[Byte],
    outputSchema: Array[Byte],
    properties: Map[String, String] = Map.empty)

/**
 * :: Experimental ::
 * One UDF execution on a worker -- the main interface Spark uses to run UDFs.
 *
 * A [[WorkerSession]] is the '''per-UDF-invocation''' handle that Spark
 * obtains from [[WorkerDispatcher#createSession]]. It carries the full
 * init / data-stream / finish lifecycle for a single UDF evaluation.
 *
 * A [[WorkerSession]] does ''not'' own the underlying worker or its
 * transport channel -- those are managed by the [[WorkerDispatcher]].
 * Multiple sessions may share the same worker when the worker supports
 * concurrency.
 *
 * '''Usage:'''
 * {{{
 *   val session = dispatcher.createSession(securityScope = None)
 *   try {
 *     session.init(InitMessage(functionPayload, inputSchema, outputSchema))
 *     val results = session.process(inputBatches)
 *     results.foreach(handleBatch)
 *   } finally {
 *     session.close()
 *   }
 * }}}
 *
 * '''Lifecycle:'''
 *  - [[init]] must be called exactly once before [[process]].
 *  - [[process]] must be called at most once per session.
 *  - [[close]] must always be called (use try-finally).
 *  - [[cancel]] may be called at any time to abort execution.
 *
 * The lifecycle is enforced here: [[init]], [[process]], [[cancel]],
 * and [[close]] are `final` and delegate to [[doInit]] / [[doProcess]] /
 * [[doCancel]], and [[doClose]] after AtomicBoolean guards.
 * Subclasses implement the protocol-specific work and do not re-check
 * the contract.
 *
 * Completion listeners registered via [[addSessionCompletionListener]]
 * are fired exactly once, after [[doClose]] or [[doCancel]]
 * (whichever runs first). Note that the completion listener can
 * be executed in a completely separate thread from the thread who
 * registered the listener.
 */
@Experimental
abstract class WorkerSession(
    workerLogger: WorkerLogger
) extends AutoCloseable {

  protected val logger: WorkerLogger =
    workerLogger.forClass(getClass)

  /** Unique identifier for this session. */
  val sessionId: String = java.util.UUID.randomUUID().toString

  private val initialized = new AtomicBoolean(false)
  private val processed = new AtomicBoolean(false)
  private val closed = new AtomicBoolean(false)

  // Guards `completionListeners`, and `completionListenersFired`
  // to ensure that a listener added after close is fired
  // immediately and exactly once.
  private val listenerLock = new Object
  private var completionListenersFired = false
  private val completionListeners =
    new ArrayList[WorkerSession => Unit]()

  /**
   * Registers a closure to be invoked when this session completes
   * (via [[close]] or [[cancel]]). Listeners fire exactly once, in
   * registration order. If the session is already closed when
   * registering, the listener is fired immediately.
   */
  final def addSessionCompletionListener(
      f: WorkerSession => Unit): Unit = {
    listenerLock.synchronized {
      if (completionListenersFired) {
        // Listeners from the list were already fired
        // -> Invoke immediately.
        f(this)
      } else {
        completionListeners.add(f)
      }
    }
  }

  private def fireCompletionListeners(): Unit = {
    listenerLock.synchronized {
      completionListenersFired = true
      completionListeners.forEach(_(this))
    }
  }

  /**
   * Initializes the UDF execution. Must be called exactly once before
   * [[process]].
   *
   * Throws `IllegalStateException` if called more than once.
   *
   * @param message the initialization parameters including the serialized
   *                function, input/output schemas, and configuration.
   */
  final def init(message: InitMessage): Unit = {
    if (!initialized.compareAndSet(false, true)) {
      throw new IllegalStateException("init has already been called on this session")
    }
    logger.info(s"Session $sessionId: init")
    doInit(message)
  }

  /**
   * Processes input data through the worker and returns results.
   *
   * Follows Spark's Iterator-to-Iterator pattern: input batches are streamed
   * to the worker, and result batches are lazily pulled from the returned
   * iterator. The session sends a Finish signal to the worker when the input
   * iterator is exhausted.
   *
   * Must be called after [[init]] and at most once per session.
   * Throws `IllegalStateException` if called before [[init]] or more than once.
   *
   * @param input iterator of raw input data batches (e.g., Arrow IPC)
   * @return iterator of raw result data batches
   */
  final def process(
      input: Iterator[Array[Byte]]): Iterator[Array[Byte]] = {
    if (!initialized.get()) {
      throw new IllegalStateException("process called before init")
    }
    if (!processed.compareAndSet(false, true)) {
      throw new IllegalStateException("process has already been called on this session")
    }
    logger.info(s"Session $sessionId: process started")
    doProcess(input)
  }

  /**
   * Requests cancellation of the current UDF execution.
   *
   * '''Thread-safety:''' implementations must allow [[cancel]] to be called
   * from a thread different from the one driving [[process]] (typically a
   * task interruption thread). It may be invoked at any point after
   * [[init]] and should be a no-op if execution has already finished.
   */
  final def cancel(): Unit = {
    // TODO [SPARK-55278]: Implement correct cancellation/finish semantics
    //          according to the worker_spec.proto.
    if (closed.compareAndSet(false, true)) {
      logger.info(s"Session $sessionId: cancel")
      doCancel()
      fireCompletionListeners()
    }
  }

  /** Closes this session and releases resources. */
  override final def close(): Unit = {
    if (closed.compareAndSet(false, true)) {
      logger.info(s"Session $sessionId: close")
      doClose()
      fireCompletionListeners()
    }
  }

  /** Subclass hook for [[init]]. Called once, after the guard. */
  protected def doInit(message: InitMessage): Unit

  /** Subclass hook for [[process]]. Called at most once, after the guard. */
  protected def doProcess(input: Iterator[Array[Byte]]): Iterator[Array[Byte]]

  /** Subclass hook for [[cancel]]. Called once, after the guard. */
  protected def doCancel(): Unit

  /** Subclass hook for [[close]]. Called at most once, after the guard. */
  protected def doClose(): Unit
}
