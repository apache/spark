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

package org.apache.spark.sql.connect.execution

import io.grpc.stub.StreamObserver

import org.apache.spark.connect.proto.ExecutePlanResponse

/**
 * Container for responses to Execution.
 *
 * TODO: At this moment, it simply forwards response to the underlying GRPC StreamObserver.
 * Detaching execution from the RPC handler, this can be used to store the responses, and then
 * send them to different GRPC StreamObservers.
 *
 * @param responseObserver
 */
class ExecutePlanResponseObserver(responseObserver: StreamObserver[ExecutePlanResponse])
    extends StreamObserver[ExecutePlanResponse] {

  def onNext(r: ExecutePlanResponse): Unit = {
    responseObserver.onNext(r)
  }

  def onError(t: Throwable): Unit = {
    responseObserver.onError(t)
  }

  def onCompleted(): Unit = {
    responseObserver.onCompleted()
  }
}
