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

import java.util.concurrent.atomic.AtomicBoolean

import org.apache.spark.annotation.Experimental
import org.apache.spark.udf.worker.Init

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
 *     session.init(Init.newBuilder()
 *       .setProtocolVersion(1)
 *       .setUdf(UdfPayload.newBuilder().setPayload(callable).setFormat(fmt).build())
 *       .setDataFormat(UDFWorkerDataFormat.ARROW)
 *       .build())
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
 *  - [[cancel]] may be called at any time from any execution context.
 *    See [[cancel]] for the full contract.
 *
 * The lifecycle is enforced here: [[init]] and [[process]] are `final`
 * and delegate to [[doInit]] / [[doProcess]] after AtomicBoolean guards.
 * Subclasses implement the protocol-specific work and do not re-check
 * the contract.
 */
@Experimental
abstract class WorkerSession extends AutoCloseable {

  private val initialized = new AtomicBoolean(false)
  private val processed = new AtomicBoolean(false)

  /**
   * Initializes the UDF execution. Must be called exactly once before
   * [[process]].
   *
   * Throws `IllegalStateException` if called more than once.
   *
   * @param message the [[Init]] message carrying the UDF body, data
   *                format, optional schemas, and any session context
   *                the worker needs to start processing.
   */
  final def init(message: Init): Unit = {
    if (!initialized.compareAndSet(false, true)) {
      throw new IllegalStateException("init has already been called on this session")
    }
    doInit(message)
  }

  /**
   * Processes input data through the worker and returns results.
   *
   * Follows Spark's Iterator-to-Iterator pattern: input batches are streamed
   * to the worker, and result batches are lazily pulled from the returned
   * iterator. The session sends a finish signal to the worker when the input
   * iterator is exhausted.
   *
   * Must be called after [[init]] and at most once per session.
   * Throws `IllegalStateException` if called before [[init]] or more than once.
   *
   * @param input iterator of raw input data batches (e.g., Arrow IPC)
   * @return iterator of raw result data batches
   */
  final def process(input: Iterator[Array[Byte]]): Iterator[Array[Byte]] = {
    if (!initialized.get()) {
      throw new IllegalStateException("process called before init")
    }
    if (!processed.compareAndSet(false, true)) {
      throw new IllegalStateException("process has already been called on this session")
    }
    doProcess(input)
  }

  /**
   * Subclass hook for [[init]]. Called once, after the guard.
   * Implementations MUST establish the worker connection here, not
   * earlier. This ensures [[cancel]] before [[init]] is a no-op.
   */
  protected def doInit(message: Init): Unit

  /** Subclass hook for [[process]]. Called at most once, after the guard. */
  protected def doProcess(input: Iterator[Array[Byte]]): Iterator[Array[Byte]]

  /**
   * Requests cancellation of the current UDF execution.
   *
   * '''Thread-safety:''' [[cancel]] may be called concurrently with
   * [[process]] from any execution context.
   *
   * '''Lifecycle:''' [[cancel]] is idempotent and safe at any point in
   * the session's life:
   *  - before [[init]] -- a no-op; the session may still be closed
   *    normally via [[close]].
   *  - between [[init]] and [[process]] -- signals that the session
   *    should be terminated; the caller should not invoke [[process]]
   *    and should call [[close]] to release resources.
   *    Implementations SHOULD surface this as an error if [[process]]
   *    is subsequently invoked despite the cancellation.
   *  - during [[process]] (data flowing or awaiting completion)
   *    -- requests the worker to abort on a best-effort basis.
   *  - after [[process]] has returned (session already terminated)
   *    -- a no-op.
   *
   * Implementations are responsible for the lifecycle-aware behavior
   * described above so that the caller does not need to coordinate
   * with the execution context driving [[process]].
   */
  def cancel(): Unit

  /**
   * Closes this session and releases resources. Idempotent; safe to
   * call from a `finally` block regardless of whether [[init]],
   * [[process]], or [[cancel]] have been invoked.
   *
   * If [[init]] was called but [[process]] was not (e.g. an exception
   * was thrown between the two), [[close]] signals cancellation to the
   * worker before releasing resources so it can clean up
   * deterministically. Subclasses implement [[doClose]] for resource
   * teardown; the base class handles the cancel-before-close guarantee
   * automatically.
   */
  final override def close(): Unit = {
    if (initialized.get() && !processed.get()) {
      cancel()
    }
    doClose()
  }

  /** Subclass hook for [[close]]. The base class guarantees that
   *  [[cancel]] has already been called if [[init]] was invoked but
   *  [[process]] was not.
   */
  protected def doClose(): Unit
}
