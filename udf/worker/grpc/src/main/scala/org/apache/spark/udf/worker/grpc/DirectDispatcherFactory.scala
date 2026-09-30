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
package org.apache.spark.udf.worker.grpc

import org.apache.spark.annotation.Experimental
import org.apache.spark.udf.worker.UDFWorkerSpecification
import org.apache.spark.udf.worker.core.{UDFDispatcherFactory, WorkerDispatcher, WorkerLogger}

/**
 * :: Experimental :: Spark-owned factory for the `DIRECT` dispatch mode.
 *
 * `core` cannot name this class at compile time, because that would put the gRPC runtime on the
 * engine classpath. [[org.apache.spark.SparkEnv]] loads this no-arg class for a `DirectWorker`
 * specification. It is not a user extension point: worker-specific behavior belongs in
 * [[UDFWorkerSpecification]].
 */
@Experimental
class DirectDispatcherFactory extends UDFDispatcherFactory {

  override def createDispatcher(
      workerSpec: UDFWorkerSpecification,
      logger: WorkerLogger): WorkerDispatcher = {
    new DirectGrpcDispatcher(workerSpec, logger)
  }
}
