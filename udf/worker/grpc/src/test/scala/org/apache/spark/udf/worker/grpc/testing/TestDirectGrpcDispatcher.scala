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

import java.io.File
import java.util.concurrent.TimeUnit

import org.apache.spark.udf.worker.UDFWorkerSpecification
import org.apache.spark.udf.worker.core.{WorkerConnection, WorkerHandle, WorkerSession}
import org.apache.spark.udf.worker.core.direct.{DirectWorkerException, DirectWorkerTimeoutException}
import org.apache.spark.udf.worker.core.direct.DirectWorkerDispatcher.READY_POLL_INTERVAL_MS
import org.apache.spark.udf.worker.grpc.DirectGrpcDispatcher

/**
 * A [[DirectGrpcDispatcher]] convenience for tests: overrides the
 * transport and session hooks to yield [[SocketFileConnection]]s and
 * [[NoOpWorkerSession]]s, so lifecycle tests exercise the dispatcher's
 * spawn / wait-for-ready / cleanup machinery without driving the gRPC
 * protocol.
 *
 * Reusable across modules: callers in `sql/core` (or anywhere with a
 * test-jar dependency on `udf-worker-grpc`) can drop this in for tests
 * that only need to verify a worker spec produces a spawnable worker.
 */
class TestDirectGrpcDispatcher(spec: UDFWorkerSpecification)
    extends DirectGrpcDispatcher(spec) {

  override protected def newConnection(address: String): WorkerConnection =
    new SocketFileConnection(address)

  override protected def connectWorker(
      address: String,
      process: Process,
      outputFile: File): WorkerConnection = {
    val deadlineNanos = System.nanoTime() + TimeUnit.MILLISECONDS.toNanos(initTimeoutMs)
    val socket = new File(address)
    while (!socket.exists() && System.nanoTime() < deadlineNanos) {
      if (!process.isAlive) throwWorkerExited(process, address, outputFile)
      Thread.sleep(READY_POLL_INTERVAL_MS)
    }
    if (!socket.exists()) {
      if (!process.isAlive) throwWorkerExited(process, address, outputFile)
      throw new DirectWorkerTimeoutException(
        s"Worker did not create socket at $address within ${initTimeoutMs}ms\n" +
          readOutputTail(outputFile))
    }
    newConnection(address)
  }

  override protected def newSession(workerHandle: WorkerHandle): WorkerSession =
    new NoOpWorkerSession(workerHandle, logger)

  private def throwWorkerExited(
      process: Process,
      address: String,
      outputFile: File): Nothing = {
    throw new DirectWorkerException(
      s"Worker exited with code ${process.exitValue()} before creating socket at $address\n" +
        readOutputTail(outputFile))
  }
}
