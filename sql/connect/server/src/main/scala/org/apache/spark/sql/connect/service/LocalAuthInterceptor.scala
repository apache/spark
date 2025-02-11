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

import io.grpc.{Metadata, ServerCall, ServerCallHandler, ServerInterceptor}

import org.apache.spark.SparkSecurityException
import org.apache.spark.sql.connect.common.config.ConnectCommon

/**
 * A gRPC interceptor to check if the header contains token for local authentication.
 */
class LocalAuthInterceptor(localToken: String) extends ServerInterceptor {
  override def interceptCall[ReqT, RespT](
      call: ServerCall[ReqT, RespT],
      headers: Metadata,
      next: ServerCallHandler[ReqT, RespT]): ServerCall.Listener[ReqT] = {
    val t = Option(
      headers.get(Metadata.Key
        .of(ConnectCommon.CONNECT_LOCAL_AUTH_TOKEN_PARAM_NAME, Metadata.ASCII_STRING_MARSHALLER)))
      .map(_.substring("Bearer ".length))
    if (t.isEmpty || t.get != localToken) {
      throw new SparkSecurityException(
        errorClass = "_LEGACY_ERROR_TEMP_3303",
        messageParameters = Map.empty)
    }
    next.startCall(call, headers)
  }
}
