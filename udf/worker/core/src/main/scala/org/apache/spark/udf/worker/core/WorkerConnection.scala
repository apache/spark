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
 * A transport-level connection to a running UDF worker process.
 *
 * A [[WorkerConnection]] represents the communication channel between the
 * Spark engine and a single worker process (e.g., a gRPC channel over a
 * Unix domain socket, or a raw TCP socket). It is owned by a worker
 * process wrapper (e.g., [[direct.DirectWorkerProcess]]) and shared
 * across all [[WorkerSession]]s that use that process.
 *
 * One connection, many sessions: the worker exposes a single server-side
 * endpoint that all sessions share. For gRPC, per-session work lives on
 * multiplexed streams over this channel.
 *
 * Implementations expose only lifecycle. Data transmission happens at
 * the [[WorkerSession]] level -- this class is solely about whether the
 * channel is open.
 *
 * '''Relationship to other classes (direct creation mode):'''
 * {{{
 *   DirectWorkerProcess  1 --- 1  WorkerConnection   (transport over UDS)
 *   DirectWorkerProcess  1 --- *  WorkerSession      (UDF executions)
 * }}}
 */
@Experimental
abstract class WorkerConnection extends AutoCloseable {
  /** Returns true if the underlying transport channel is still usable. */
  def isActive: Boolean
}
