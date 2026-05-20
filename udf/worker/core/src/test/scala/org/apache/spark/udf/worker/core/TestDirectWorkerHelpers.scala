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

import java.io.File

import org.apache.spark.udf.worker.UDFWorkerSpecification
import org.apache.spark.udf.worker.core.direct.{
  DirectUnixSocketWorkerDispatcher, DirectWorkerProcess,
  DirectWorkerSession}
import org.apache.spark.udf.worker.Init

/** A [[WorkerConnection]] test implementation that considers the connection
  * active as long as the socket file exists on disk. Inherits socket-file
  * deletion from [[UnixSocketWorkerConnection.close]].
  */
class SocketFileConnection(socketPath: String)
    extends UnixSocketWorkerConnection(socketPath) {
  override def isActive: Boolean = new File(socketPath).exists()
}

/** A stub [[DirectWorkerSession]] for process-lifecycle tests that don't need
  * actual data transmission.
  *
  * TODO: [[cancel]] is a no-op here. Once a concrete [[DirectWorkerSession]]
  * with real data-plane wiring lands, add tests exercising cancel() in
  * particular: cancel from a different thread than process(), cancel after
  * process() has returned, and cancel before init (should be a no-op). See the
  * thread-safety contract in the docstring on
  * [[org.apache.spark.udf.worker.core.WorkerSession.cancel]].
  */
class StubWorkerSession(workerProcess: DirectWorkerProcess)
    extends DirectWorkerSession(workerProcess) {

  override protected def doInit(message: Init): Unit = {}

  override protected def doProcess(
      input: Iterator[Array[Byte]]
  ): Iterator[Array[Byte]] =
    Iterator.empty

  override def cancel(): Unit = {}
}

/** A [[DirectUnixSocketWorkerDispatcher]] subclass for testing that uses a
  * socket-file connection and stub sessions instead of a real protocol
  * implementation.
  */
class TestDirectWorkerDispatcher(spec: UDFWorkerSpecification)
    extends DirectUnixSocketWorkerDispatcher(spec) {

  override protected def createConnection(
      socketPath: String
  ): UnixSocketWorkerConnection =
    new SocketFileConnection(socketPath)

  override protected def createSessionForWorker(
      worker: DirectWorkerProcess
  ): WorkerSession =
    new StubWorkerSession(worker)
}
