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
package org.apache.spark.sql.connect.planner

import java.util.UUID

import scala.collection.mutable

import io.grpc.stub.StreamObserver

import org.apache.spark.SparkException
import org.apache.spark.connect.proto
import org.apache.spark.sql.{QueryTest, Row}
import org.apache.spark.sql.classic.Dataset
import org.apache.spark.sql.connect.SparkConnectTestUtils
import org.apache.spark.sql.connect.common.InvalidPlanInput
import org.apache.spark.sql.connect.service.{SessionHolder, SparkConnectService}
import org.apache.spark.sql.test.SharedSparkSession

class SparkConnectCheckpointSuite extends QueryTest with SharedSparkSession {

  override def afterEach(): Unit = {
    try {
      SparkConnectService.sessionManager.invalidateAllSessions()
    } finally {
      super.afterEach()
    }
  }

  private def process(
      sessionHolder: SessionHolder,
      command: proto.Command): Seq[proto.ExecutePlanResponse] = {
    val executeHolder = SparkConnectTestUtils.createDummyExecuteHolder(sessionHolder, command)
    val responses = mutable.ArrayBuffer.empty[proto.ExecutePlanResponse]
    try {
      new SparkConnectPlanner(executeHolder).process(
        command,
        new StreamObserver[proto.ExecutePlanResponse] {
          override def onNext(response: proto.ExecutePlanResponse): Unit = responses += response
          override def onError(t: Throwable): Unit = throw t
          override def onCompleted(): Unit = {}
        })
      responses.toSeq
    } finally {
      SparkConnectService.executionManager.removeExecuteHolder(executeHolder.key)
    }
  }

  private def checkpointCommand(
      local: Boolean,
      eager: Boolean,
      relationId: Option[String],
      end: Long = 3): proto.Command = {
    val checkpoint = proto.CheckpointCommand
      .newBuilder()
      .setRelation(
        proto.Relation.newBuilder().setRange(proto.Range.newBuilder().setEnd(end).setStep(1)))
      .setLocal(local)
      .setEager(eager)
    relationId.foreach(checkpoint.setRelationId)
    proto.Command.newBuilder().setCheckpointCommand(checkpoint).build()
  }

  for {
    local <- Seq(false, true)
    eager <- Seq(false, true)
    clientGeneratedId <- Seq(false, true)
  } {
    test(s"Checkpoint relation ID: local=$local, eager=$eager, client=$clientGeneratedId") {
      withTempDir { dir =>
        spark.sparkContext.setCheckpointDir(dir.getCanonicalPath)
        val sessionHolder = SparkConnectTestUtils.createDummySessionHolder(spark)
        val requestedId = if (clientGeneratedId) Some(UUID.randomUUID().toString) else None
        val responses = process(sessionHolder, checkpointCommand(local, eager, requestedId))
        assert(responses.size == 1)
        val relation = responses.head.getCheckpointCommandResult.getRelation
        val relationId = relation.getRelationId
        requestedId match {
          case Some(id) => assert(relationId == id)
          case None => assert(UUID.fromString(relationId).toString == relationId)
        }
        val expected = Seq(Row(0L), Row(1L), Row(2L))
        checkAnswer(sessionHolder.getDataFrameOrThrow(relationId), expected)
        val plan = new SparkConnectPlanner(sessionHolder).transformRelation(
          proto.Relation.newBuilder().setCachedRemoteRelation(relation).build())
        checkAnswer(Dataset.ofRows(spark, plan), expected)

        process(
          sessionHolder,
          proto.Command
            .newBuilder()
            .setRemoveCachedRemoteRelationCommand(
              proto.RemoveCachedRemoteRelationCommand.newBuilder().setRelation(relation))
            .build())
        checkError(
          exception = intercept[InvalidPlanInput] {
            sessionHolder.getDataFrameOrThrow(relationId)
          },
          condition = "CONNECT_INVALID_PLAN.DATAFRAME_NOT_FOUND",
          parameters = Map("dfId" -> relationId, "sessionId" -> sessionHolder.sessionId))
      }
    }
  }

  test("Checkpoint rejects an empty relation ID before planning the input") {
    val sessionHolder = SparkConnectTestUtils.createDummySessionHolder(spark)
    val command = proto.Command
      .newBuilder()
      .setCheckpointCommand(proto.CheckpointCommand.newBuilder().setRelationId(""))
      .build()
    checkError(
      exception = intercept[InvalidPlanInput] { process(sessionHolder, command) },
      condition = "CONNECT_INVALID_PLAN.CHECKPOINT_RELATION_ID_EMPTY",
      parameters = Map.empty)
  }

  test("Checkpoint rejects a duplicate relation ID without replacing the DataFrame") {
    val sessionHolder = SparkConnectTestUtils.createDummySessionHolder(spark)
    val relationId = "graphframes-checkpoint"
    val command = checkpointCommand(local = true, eager = false, relationId = Some(relationId))
    process(sessionHolder, command)
    val original = sessionHolder.getDataFrameOrThrow(relationId)
    checkError(
      exception = intercept[SparkException] {
        process(
          sessionHolder,
          checkpointCommand(local = true, eager = false, relationId = Some(relationId), end = 5))
      },
      condition = "INTERNAL_ERROR",
      parameters = Map("message" -> s"A dataframe is already associated with id $relationId"))
    assert(sessionHolder.getDataFrameOrThrow(relationId) eq original)
    checkAnswer(original, Seq(Row(0L), Row(1L), Row(2L)))
  }

  test("Checkpoint relation IDs are scoped to the session") {
    val first = SparkConnectTestUtils.createDummySessionHolder(spark)
    val second = SparkConnectTestUtils.createDummySessionHolder(spark.newSession())
    val relationId = Some("graphframes-checkpoint")
    process(first, checkpointCommand(local = true, eager = false, relationId = relationId, end = 3))
    checkError(
      exception = intercept[InvalidPlanInput] {
        second.getDataFrameOrThrow(relationId.get)
      },
      condition = "CONNECT_INVALID_PLAN.DATAFRAME_NOT_FOUND",
      parameters = Map("dfId" -> relationId.get, "sessionId" -> second.sessionId))
    process(
      second,
      checkpointCommand(local = true, eager = false, relationId = relationId, end = 5))
    checkAnswer(first.getDataFrameOrThrow(relationId.get), Seq(Row(0L), Row(1L), Row(2L)))
    checkAnswer(second.getDataFrameOrThrow(relationId.get), (0L until 5L).map(Row(_)))
  }
}
