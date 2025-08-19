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
package org.apache.spark.sql.connect.execution.command

import io.grpc.stub.StreamObserver

import org.apache.spark.connect.proto
import org.apache.spark.connect.proto.ExecutePlanResponse
import org.apache.spark.sql.{DataFrame, Row}
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.classic.{Dataset, SparkSession}
import org.apache.spark.storage.StorageLevel

case class CheckpointCommand(
    query: LogicalPlan,
    isLocal: Boolean,
    storageLevelOpt: Option[StorageLevel],
    eager: Boolean,
    dfId: String,
    dfCacher: (String, DataFrame) => Unit)
    extends ConnectLeafRunnableCommand {

  override def run(session: SparkSession): Seq[Row] = {
    val target = Dataset.ofRows(session, query)
    val checkpointed = if (isLocal) {
      storageLevelOpt match {
        case Some(storageLevel) =>
          target.localCheckpoint(eager = eager, storageLevel = storageLevel)
        case None =>
          target.localCheckpoint(eager = eager)
      }
    } else {
      target.checkpoint(eager = eager)
    }
    dfCacher(dfId, checkpointed)
    Seq.empty
  }

  override def handleConnectResponse(
      responseObserver: StreamObserver[ExecutePlanResponse],
      sessionId: String,
      serverSessionId: String): Unit = {
    responseObserver.onNext(
      proto.ExecutePlanResponse
        .newBuilder()
        .setSessionId(sessionId)
        .setServerSideSessionId(serverSessionId)
        .setCheckpointCommandResult(
          proto.CheckpointCommandResult
            .newBuilder()
            .setRelation(proto.CachedRemoteRelation.newBuilder().setRelationId(dfId).build())
            .build())
        .build())
  }
}
