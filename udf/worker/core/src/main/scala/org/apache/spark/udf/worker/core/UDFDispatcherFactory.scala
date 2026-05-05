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
import org.apache.spark.udf.worker.UDFWorkerSpecification

/**
 * :: Experimental ::
 * Creates [[WorkerDispatcher]] instances and controls their
 * lifecycle after all sessions have closed.
 *
 * Implementations are passed to [[UDFDispatcherManager]] which
 * handles caching, session tracking, and shutdown.
 */
@Experimental
trait UDFDispatcherFactory {

  /**
   * Creates a new [[WorkerDispatcher]] for the given specification.
   * It is expected that creating the dispatcher
   * itself is not slow while creating a session might be.
   */
  def createDispatcher(
      workerSpec: UDFWorkerSpecification,
      logger: WorkerLogger): WorkerDispatcher

  /**
   * Called when the last active session for a dispatcher is closed.
   * Implementations must decide what to do with the now-idle
   * dispatcher: close it immediately, schedule idle-timeout
   * eviction, etc.
   * Not called during [[UDFDispatcherManager#stop]] -- the manager
   * cleans up dispatchers it holds directly in that case.
   */
  def onAllDispatcherSessionsClosed(
      dispatcher: WorkerDispatcher): Unit

  /**
   * Called when the executor/driver stops. Implementations should
   * clean up any dispatchers/resources they hold beyond what the
   * [[UDFDispatcherManager]] manages.
   */
  def onStop(): Unit
}
