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

import org.apache.spark.udf.worker.core.WorkerConnection

/**
 * A [[WorkerConnection]] test implementation that treats the connection
 * as active as long as the worker's UDS file exists on disk. The socket
 * file is removed on close.
 *
 * Suitable for dispatcher-lifecycle tests that don't need to drive the
 * gRPC protocol -- e.g. verifying that a worker spec spawns a real
 * worker process that creates the expected socket.
 */
class SocketFileConnection(val socketPath: String) extends WorkerConnection {
  override def isActive: Boolean = new File(socketPath).exists()
  override def close(): Unit = {
    val f = new File(socketPath)
    if (f.exists()) f.delete()
  }
}
