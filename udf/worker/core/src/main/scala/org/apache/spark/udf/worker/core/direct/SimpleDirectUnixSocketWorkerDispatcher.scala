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

import java.net.{StandardProtocolFamily, UnixDomainSocketAddress}
import java.nio.channels.SocketChannel

import org.apache.spark.annotation.Experimental
import org.apache.spark.udf.worker.UDFWorkerSpecification
import org.apache.spark.udf.worker.core.{
  InitMessage,
  UnixSocketWorkerConnection,
  WorkerLogger,
  WorkerSession
}

// All implementations in this file are temporary and will be removed
// once the gRPC protocol lands with
// https://github.com/apache/spark/pull/55657

@Experimental
private[direct] class SimpleWorkerConnection(
    socketPath: String,
    private val channel: SocketChannel
) extends UnixSocketWorkerConnection(socketPath) {

  override def isActive: Boolean = channel.isOpen

  override def close(): Unit = {
    channel.close()
    super.close()
  }
}

private[direct] object SimpleWorkerConnection {
  def connect(
      socketPath: String,
      logger: WorkerLogger
  ): SimpleWorkerConnection = {
    val address = UnixDomainSocketAddress.of(socketPath)
    val channel = SocketChannel.open(StandardProtocolFamily.UNIX)
    channel.connect(address)
    logger.info(s"Connected to worker via UDS at $socketPath")
    new SimpleWorkerConnection(socketPath, channel)
  }
}

@Experimental
private[direct] class SimpleWorkerSession(
    worker: DirectWorkerProcess,
    private val workerLogger: WorkerLogger
) extends DirectWorkerSession(worker, workerLogger) {

  override protected[core] def doInit(message: InitMessage): Unit = {
    val conn = connection
      .asInstanceOf[SimpleWorkerConnection]
    logger.info(
      s"Session initialized on worker ${worker.id}, " +
        s"connection active: ${conn.isActive}"
    )
  }

  override protected[core] def doProcess(
      input: Iterator[Array[Byte]]
  ): Iterator[Array[Byte]] = {
    input
  }

  override def doCancel(): Unit = {}

  override def doClose(): Unit = {
    worker.releaseSession()
  }
}

@Experimental
class SimpleDirectWorkerDispatcher(
    workerSpec: UDFWorkerSpecification,
    logger: WorkerLogger
) extends DirectUnixSocketWorkerDispatcher(workerSpec, logger) {

  override protected def createConnection(
      socketPath: String
  ): UnixSocketWorkerConnection =
    SimpleWorkerConnection.connect(socketPath, logger)

  override protected def createSessionForWorker(
      worker: DirectWorkerProcess
  ): WorkerSession =
    new SimpleWorkerSession(worker, logger)
}
