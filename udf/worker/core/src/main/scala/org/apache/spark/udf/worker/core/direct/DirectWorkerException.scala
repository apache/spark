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

import org.apache.spark.annotation.Experimental

/**
 * :: Experimental ::
 * Thrown by [[DirectWorkerDispatcher]] for runtime failures: worker
 * spawn problems, environment setup or cleanup failures, callable
 * timeouts, and socket-establishment timeouts.
 *
 * Distinguished from `IllegalArgumentException` (bad spec) and
 * `IllegalStateException` (using a closed dispatcher), which indicate
 * programming errors. Catching this type lets callers handle runtime
 * failures specifically without catching every `RuntimeException`.
 */
@Experimental
class DirectWorkerException(message: String, cause: Throwable = null)
  extends RuntimeException(message, cause)

/**
 * :: Experimental ::
 * A [[DirectWorkerException]] caused specifically by a timeout: a worker
 * that did not bind its socket within `initialization_timeout_ms`, or a
 * setup callable (verify / install / cleanup) that exceeded
 * `callableTimeoutMs`. Exposed as a distinct type so callers can choose
 * different retry / escalation paths for timeouts vs other failures.
 */
@Experimental
class DirectWorkerTimeoutException(message: String, cause: Throwable = null)
  extends DirectWorkerException(message, cause)
