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
import io.grpc.ForwardingServerCallListener.SimpleForwardingServerCallListener

import org.apache.spark.SparkContext

/**
 * Interceptor for cleaning up local properties in the SparkContext after gRPC server calls.
 */
class LocalPropertiesCleanupInterceptor extends ServerInterceptor {

  override def interceptCall[ReqT, RespT](
      serverCall: ServerCall[ReqT, RespT],
      metadata: Metadata,
      serverCallHandler: ServerCallHandler[ReqT, RespT]): ServerCall.Listener[ReqT] = {
    new SimpleForwardingServerCallListener[ReqT](
      serverCallHandler.startCall(serverCall, metadata)) {
      override def onComplete(): Unit = {
        cleanupLocalProperties()
        super.onComplete()
      }

      override def onCancel(): Unit = {
        cleanupLocalProperties()
        super.onCancel()
      }

      private def cleanupLocalProperties(): Unit = {
        SparkContext.getActive.foreach(_.getLocalProperties.clear())
      }
    }
  }
}
