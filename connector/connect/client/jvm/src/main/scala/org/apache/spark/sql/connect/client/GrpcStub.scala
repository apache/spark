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

import org.apache.spark.connect.proto.{AnalyzePlanRequest, AnalyzePlanResponse, ConfigRequest, ConfigResponse, ExecutePlanRequest, ExecutePlanResponse, InterruptRequest, InterruptResponse}
import org.apache.spark.connect.proto

private[client] class CustomSparkConnectBlockingStub(channel: ManagedChannel) {

  private val stub = proto.SparkConnectServiceGrpc.newBlockingStub(channel)

  def executePlan(request: ExecutePlanRequest): java.util.Iterator[ExecutePlanResponse] = {
    GrpcExceptionConverter.convert {
      GrpcExceptionConverter.convertIterator[ExecutePlanResponse](stub.executePlan(request))
    }
  }

  def analyzePlan(request: AnalyzePlanRequest): AnalyzePlanResponse = {
    GrpcExceptionConverter.convert {
      stub.analyzePlan(request)
    }
  }

  def config(request: ConfigRequest): ConfigResponse = {
    GrpcExceptionConverter.convert {
      stub.config(request)
    }
  }

  def interrupt(request: InterruptRequest): InterruptResponse = {
    GrpcExceptionConverter.convert {
      stub.interrupt(request)
    }
  }
}
