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
import org.apache.spark.udf.worker.UDFWorkerSpecification

/**
 * :: Experimental ::
 * Creates [[WorkerDispatcher]] instances.
 *
 * Implementations are passed to [[UDFDispatcherManager]] which
 * handles caching and shutdown.
 */
@Experimental
trait UDFDispatcherFactory {

  /**
   * Creates a new [[WorkerDispatcher]] for the given specification.
   * At most one dispatcher will be requested per unique
   * [[UDFWorkerSpecification]]
   */
  def createDispatcher(
      workerSpec: UDFWorkerSpecification,
      logger: WorkerLogger): WorkerDispatcher

}
