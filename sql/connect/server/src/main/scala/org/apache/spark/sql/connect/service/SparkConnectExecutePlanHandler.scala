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

import org.apache.spark.SparkSQLException
import org.apache.spark.connect.proto
import org.apache.spark.internal.Logging

class SparkConnectExecutePlanHandler(responseObserver: StreamObserver[proto.ExecutePlanResponse])
    extends Logging {

  def handle(v: proto.ExecutePlanRequest): Unit = {
    val previousSessionId = v.hasClientObservedServerSideSessionId match {
      case true => Some(v.getClientObservedServerSideSessionId)
      case false => None
    }
    val sessionHolder = SparkConnectService
      .getOrCreateIsolatedSession(v.getUserContext.getUserId, v.getSessionId, previousSessionId)
    val executeKey = ExecuteKey(v, sessionHolder)

    SparkConnectService.executionManager.getExecuteHolder(executeKey) match {
      case None =>
        // Create a new execute holder and attach to it.
        SparkConnectService.executionManager
          .createExecuteHolderAndAttach(executeKey, v, sessionHolder, responseObserver)
      case Some(executeHolder) if executeHolder.request.getPlan.equals(v.getPlan) =>
        // If the execute holder already exists with the same plan, reattach to it.
        SparkConnectService.executionManager
          .reattachExecuteHolder(executeHolder, responseObserver, None)
      case Some(_) =>
        throw new SparkSQLException(
          errorClass = "INVALID_HANDLE.OPERATION_ALREADY_EXISTS",
          messageParameters = Map("handle" -> executeKey.operationId))
    }
  }
}
