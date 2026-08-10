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

/**
 * :: Experimental ::
 * The lifecycle-side handle the engine uses to release a worker back to
 * its dispatcher.
 *
 * Sessions hold a [[WorkerHandle]] -- not the concrete worker type --
 * so the engine-facing layer (the `core` package) is decoupled from any
 * specific worker provisioning model (direct local spawn, indirect
 * daemon-provided, etc.). Each [[WorkerDispatcher]] supplies its own
 * implementation; the session calls back into it on close.
 *
 * Contract:
 *  - [[releaseSession]] decrements the dispatcher-side reference count.
 *    The caller ([[WorkerSession.close]]) invokes it exactly once per
 *    session, so implementations need not be idempotent and MAY fail fast
 *    on an unbalanced (negative) count. Sessions sharing the same worker
 *    call it independently (each exactly once).
 *  - [[markInvalid]] is sticky: once set, the dispatcher MUST NOT return
 *    this worker to any reuse pool. Sessions set this when the worker
 *    is observed in an unsafe state (transport error, hung worker,
 *    protocol violation).
 *  - [[id]] is a stable string for diagnostics. The session uses it in
 *    error messages; the dispatcher uses it to key the worker in its
 *    own tracking map. Implementations must keep it constant for the
 *    lifetime of the handle.
 */
@Experimental
trait WorkerHandle {

  /** Stable identifier for logs and diagnostic messages. */
  def id: String

  /**
   * Marks the worker as unsafe to recycle. Idempotent and sticky.
   * Sessions call this in their close path when the protocol layer
   * reports an unsalvageable termination.
   */
  def markInvalid(): Unit

  /**
   * Decrements the dispatcher-side session ref count. The caller
   * ([[WorkerSession.close]]) invokes this exactly once per session, so
   * implementations need not be idempotent.
   */
  def releaseSession(): Unit
}
