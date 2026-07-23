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

import org.apache.spark.annotation.Experimental
import org.apache.spark.udf.worker.{CancelResponse, ExecutionError, FinishResponse}

/**
 * :: Experimental ::
 * The terminal outcome a [[WorkerSession]] settles on, returned by
 * [[WorkerSession#close]]. Mirrors the four terminal `WorkerSession.SessionState`s,
 * so close() reports the outcome faithfully rather than collapsing failures into
 * a clean cancel.
 *
 * '''Clean outcomes''' ([[Finished]] / [[Cancelled]]) wrap the worker's
 * `FinishResponse` / `CancelResponse` -- per-execution metrics, an optional
 * inline `data` payload, and an optional finish/cancel '''callback error''' --
 * which the engine surfaces to the UDF's client-side finish/cancel callbacks.
 * The proto allows `Cancel` to follow `Finish` on the same stream, so close()
 * may observe either depending on arrival timing (see `udf_message.proto`): if
 * `FinishResponse` was already produced when a `Cancel` arrives the engine still
 * receives [[Finished]], otherwise [[Cancelled]].
 *
 * '''Failure outcomes''' ([[Failed]] / [[TransportFailed]]) carry the cause
 * instead of a proto terminator (none arrived, so they have no metrics). They
 * exist so a failure is not reported as a benign cancel -- in particular an
 * error raised during finish/close, '''after all data has been drained''',
 * reaches the caller only through this value, never through the result iterator.
 */
@Experimental
sealed trait Termination

object Termination {
  /** The stream ended with a `FinishResponse` (normal completion). */
  final case class Finished(response: FinishResponse) extends Termination

  /** The stream ended with a `CancelResponse` (cooperative cancellation). */
  final case class Cancelled(response: CancelResponse) extends Termination

  /**
   * An execution error settled the session without a proto terminator (e.g. an
   * `ErrorResponse`, or an error before init completed). Carries the structured
   * [[ExecutionError]].
   */
  final case class Failed(error: ExecutionError) extends Termination

  /**
   * A transport failure, timeout, or interrupt tore the stream down before any
   * terminator arrived. Carries the underlying cause.
   */
  final case class TransportFailed(cause: Throwable) extends Termination
}
