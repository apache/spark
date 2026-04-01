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
 * Represents one single UDF execution.
 *
 * A [[WorkerSession]] is obtained from [[WorkerDispatcher#createSession]] and
 * can carry per-execution state for that UDF invocation. Implementations may
 * add concrete data-processing methods and lifecycle hooks as needed.
 *
 * A WorkerSession is not 1-to-1 mapped to an actual worker process. Multiple
 * WorkerSessions may be backed by the same worker when the worker is reused.
 * Worker reuse and pooling are managed by each [[WorkerDispatcher]]
 * implementation based on the [[WorkerSpecification]].
 */
@Experimental
abstract class WorkerSession extends AutoCloseable {
  override def close(): Unit = {}
}
