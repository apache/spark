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
import org.apache.spark.udf.worker.WorkerSpecification

/**
 * :: Experimental ::
 * Manages workers for a single [[WorkerSpecification]] and hides worker details from Spark.
 *
 * A [[WorkerDispatcher]] is created from a worker specification (plus context such
 * as security scope). It owns the underlying worker processes and connections,
 * handling pooling, reuse, and lifecycle behind the scenes. Spark interacts with
 * workers exclusively through the [[WorkerSession]]s returned by [[createSession]].
 */
@Experimental
trait WorkerDispatcher extends AutoCloseable {

  def workerSpec: WorkerSpecification

  /**
   * Creates a [[WorkerSession]] that maps to one single UDF execution.
   *
   * @param securityScope identifies which pool of workers may be reused for this
   *                      session. Dispatcher implementations use the scope to
   *                      decide whether an existing worker can be shared or a new
   *                      one must be created.
   */
  def createSession(securityScope: Option[WorkerSecurityScope]): WorkerSession

  override def close(): Unit
}
