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

package org.apache.spark.sql.connect.service

import io.grpc.{Metadata, ServerCall, ServerCallHandler, ServerInterceptor, Status}

class PreSharedKeyAuthenticationInterceptor(token: String) extends ServerInterceptor {

  val authorizationMetadataKey =
    Metadata.Key.of("Authorization", Metadata.ASCII_STRING_MARSHALLER)

  val expectedValue = s"Bearer $token"

  override def interceptCall[ReqT, RespT](
      call: ServerCall[ReqT, RespT],
      metadata: Metadata,
      next: ServerCallHandler[ReqT, RespT]): ServerCall.Listener[ReqT] = {
    val authHeaderValue = metadata.get(authorizationMetadataKey)

    if (authHeaderValue == null) {
      val status = Status.UNAUTHENTICATED.withDescription("No authentication token provided")
      call.close(status, new Metadata())
      new ServerCall.Listener[ReqT]() {}
    } else if (authHeaderValue != expectedValue) {
      val status = Status.UNAUTHENTICATED.withDescription("Invalid authentication token")
      call.close(status, new Metadata())
      new ServerCall.Listener[ReqT]() {}
    } else {
      next.startCall(call, metadata)
    }
  }
}
