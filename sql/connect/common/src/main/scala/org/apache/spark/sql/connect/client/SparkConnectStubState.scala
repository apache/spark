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
package org.apache.spark.sql.connect.client

import io.grpc.ManagedChannel

import org.apache.spark.internal.Logging

// This is common state shared between the blocking and non-blocking stubs.
//
// The common logic is responsible to verify the integrity of the response. The invariant is
// that the same stub instance is used for all requests from the same client. In addition,
// this class provides access to the commonly configured retry policy and exception conversion
// logic.
class SparkConnectStubState(channel: ManagedChannel, retryPolicies: Seq[RetryPolicy])
    extends Logging {

  // Manages the retry handler logic used by the stubs.
  lazy val retryHandler = new GrpcRetryHandler(retryPolicies)

  // Responsible to convert the GRPC Status exceptions into Spark exceptions.
  lazy val exceptionConverter: GrpcExceptionConverter = new GrpcExceptionConverter(channel)

  // Provides a helper for validating the responses processed by the stub.
  lazy val responseValidator = new ResponseValidator()

}
