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

import io.grpc.stub.StreamObserver

import org.apache.spark.connect.proto
import org.apache.spark.internal.Logging

class SparkConnectInterruptHandler(responseObserver: StreamObserver[proto.InterruptResponse])
  extends Logging {

  def handle(v: proto.InterruptRequest): Unit = {
    val session =
      SparkConnectService
        .getOrCreateIsolatedSession(v.getUserContext.getUserId, v.getSessionId)
        .session

    // todo handle unset
    val jobGroupId =
      s"User_${v.getUserContext.getUserId}_Session_${v.getSessionId}_Request_${v.getRequestId}"

    session.sparkContext.cancelJobGroup(jobGroupId)

    val builder = proto.InterruptResponse.newBuilder()
    responseObserver.onNext(builder.build())
    responseObserver.onCompleted()
  }
}
