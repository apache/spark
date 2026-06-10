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
package org.apache.spark.udf.worker.grpc.testing

import org.apache.spark.udf.worker.UDFWorkerSpecification
import org.apache.spark.udf.worker.core.{WorkerConnection, WorkerHandle, WorkerSession}
import org.apache.spark.udf.worker.grpc.DirectGrpcDispatcher

/**
 * A [[DirectGrpcDispatcher]] convenience for tests: overrides the
 * transport and session hooks to yield [[SocketFileConnection]]s and
 * [[NoOpWorkerSession]]s, so lifecycle tests exercise the dispatcher's
 * spawn / wait-for-ready / cleanup machinery without driving the gRPC
 * protocol.
 *
 * Reusable across modules: callers in `sql/core` (or anywhere with a
 * test-jar dependency on `udf-worker-core`) can drop this in for tests
 * that only need to verify a worker spec produces a spawnable worker.
 */
class TestDirectGrpcDispatcher(spec: UDFWorkerSpecification)
    extends DirectGrpcDispatcher(spec) {

  override protected def newConnection(address: String): WorkerConnection =
    new SocketFileConnection(address)

  override protected def newSession(
      workerHandle: WorkerHandle,
      connection: WorkerConnection): WorkerSession =
    new NoOpWorkerSession(workerHandle, logger)
}
