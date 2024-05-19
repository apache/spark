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

package org.apache.spark.deploy.worker

import java.util.concurrent.atomic.AtomicBoolean

import org.apache.spark.internal.{Logging, MDC}
import org.apache.spark.internal.LogKeys.WORKER_URL
import org.apache.spark.rpc._

/**
 * Endpoint which connects to a worker process and terminates the JVM if the
 * connection is severed.
 * Provides fate sharing between a worker and its associated child processes.
 */
private[spark] class WorkerWatcher(
    override val rpcEnv: RpcEnv,
    workerUrl: String,
    isTesting: Boolean = false,
    isChildProcessStopping: AtomicBoolean = new AtomicBoolean(false))
  extends RpcEndpoint with Logging {

  logInfo(log"Connecting to worker ${MDC(WORKER_URL, workerUrl)}")
  if (!isTesting) {
    rpcEnv.asyncSetupEndpointRefByURI(workerUrl)
  }

  // Used to avoid shutting down JVM during tests
  // In the normal case, exitNonZero will call `System.exit(-1)` to shutdown the JVM. In the unit
  // test, the user should call `setTesting(true)` so that `exitNonZero` will set `isShutDown` to
  // true rather than calling `System.exit`. The user can check `isShutDown` to know if
  // `exitNonZero` is called.
  private[deploy] var isShutDown = false

  // Lets filter events only from the worker's rpc system
  private val expectedAddress = RpcAddress.fromUrlString(workerUrl)
  private def isWorker(address: RpcAddress) = expectedAddress == address

  private def exitNonZero() =
    if (isTesting) {
      isShutDown = true
    } else if (isChildProcessStopping.compareAndSet(false, true)) {
      // SPARK-35714: avoid the duplicate call of `System.exit` to avoid the dead lock.
      // Same as SPARK-14180, we should run `System.exit` in a separate thread to avoid
      // dead lock since `System.exit` will trigger the shutdown hook of `executor.stop`.
      new Thread("WorkerWatcher-exit-executor") {
        override def run(): Unit = System.exit(-1)
      }.start()
    }

  override def receive: PartialFunction[Any, Unit] = {
    case e => logWarning(s"Received unexpected message: $e")
  }

  override def onConnected(remoteAddress: RpcAddress): Unit = {
    if (isWorker(remoteAddress)) {
      logInfo(log"Successfully connected to ${MDC(WORKER_URL, workerUrl)}")
    }
  }

  override def onDisconnected(remoteAddress: RpcAddress): Unit = {
    if (isWorker(remoteAddress)) {
      // This log message will never be seen
      logError(log"Lost connection to worker rpc endpoint ${MDC(WORKER_URL, workerUrl)}. Exiting.")
      exitNonZero()
    }
  }

  override def onNetworkError(cause: Throwable, remoteAddress: RpcAddress): Unit = {
    if (isWorker(remoteAddress)) {
      // These logs may not be seen if the worker (and associated pipe) has died
      logError(
        log"Could not initialize connection to worker ${MDC(WORKER_URL, workerUrl)}. Exiting.",
        cause)
      exitNonZero()
    }
  }
}
