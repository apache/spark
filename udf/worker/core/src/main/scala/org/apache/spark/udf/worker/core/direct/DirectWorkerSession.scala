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
package org.apache.spark.udf.worker.core.direct

import java.util.concurrent.atomic.AtomicBoolean

import org.apache.spark.annotation.Experimental
import org.apache.spark.udf.worker.core.{WorkerConnection, WorkerSession}

/**
 * :: Experimental ::
 * A [[WorkerSession]] backed by a locally-spawned [[DirectWorkerProcess]].
 *
 * This is the session type returned by [[DirectWorkerDispatcher]]. It ties
 * the session lifecycle to the worker's ref-count: the dispatcher increments
 * the count before construction, and [[close]] decrements it, so the
 * dispatcher knows when a worker process is idle and can be terminated or
 * reused.
 *
 * Subclasses implement the protocol-specific data transmission
 * ([[init]], [[process]], [[cancel]]).
 *
 * @param workerProcess the direct worker process backing this session.
 *                      Internal to the `core` package and test code -- the
 *                      worker handle is a dispatcher implementation detail,
 *                      not part of the public WorkerSession API.
 */
@Experimental
abstract class DirectWorkerSession(
    private[core] val workerProcess: DirectWorkerProcess) extends WorkerSession {

  private val released = new AtomicBoolean(false)

  /** The connection to the worker for this session. */
  def connection: WorkerConnection = workerProcess.connection

  override def close(): Unit = {
    if (released.compareAndSet(false, true)) {
      workerProcess.releaseSession()
    }
  }
}
