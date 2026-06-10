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

import org.apache.spark.udf.worker.{Cancel, DataRequest, DataResponse, Finish, FinishResponse,
  Init, InitResponse}
import org.apache.spark.udf.worker.core.{Termination, WorkerHandle, WorkerLogger, WorkerSession}

/**
 * No-op [[WorkerSession]] for lifecycle-only tests. All protocol methods
 * are inert (init/finish report empty responses); tests that exercise the
 * actual wire protocol use `GrpcWorkerSession` against a real or in-process
 * echo worker.
 */
class NoOpWorkerSession(
    workerHandle: WorkerHandle,
    logger: WorkerLogger = WorkerLogger.NoOp)
  extends WorkerSession(workerHandle, logger) {

  override protected def doInit(message: Init): InitResponse = InitResponse.getDefaultInstance
  override protected def doProcess(
      input: Iterator[DataRequest],
      finish: () => Finish): Iterator[DataResponse] =
    Iterator.empty[DataResponse]
  override protected def doClose(cancel: () => Cancel): Termination = {
    // Settle the clean terminal so close() does not fall through to its
    // contract-violation recovery path. A no-op session has no in-flight work,
    // so the cancel thunk is never needed.
    completeTerminal(Termination.Finished(FinishResponse.getDefaultInstance))
    settledTermination
  }
}
