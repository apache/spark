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

import com.fasterxml.jackson.annotation.JsonIgnore
import com.google.protobuf.Message

import org.apache.spark.SparkContext
import org.apache.spark.connect.proto
import org.apache.spark.connect.proto.ExecutePlanRequest
import org.apache.spark.scheduler.SparkListenerEvent
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.catalyst.plans.logical.{Command, LogicalPlan}
import org.apache.spark.sql.connect.common.ProtoUtils
import org.apache.spark.util.{Clock, Utils}

object Events {
  // TODO: Make this configurable
  val MAX_STATEMENT_TEXT_SIZE = 65535
}

case class Events(sessionHolder: SessionHolder, clock: Clock) {
  def postStarted(planHolder: ExecutePlanHolder, v: ExecutePlanRequest): Unit = {
    val sc = sessionHolder.session.sparkContext
    val plan: Message =
      v.getPlan.getOpTypeCase match {
        case proto.Plan.OpTypeCase.COMMAND => v.getPlan.getCommand
        case proto.Plan.OpTypeCase.ROOT => v.getPlan.getRoot
        case _ =>
          throw new UnsupportedOperationException(s"${v.getPlan.getOpTypeCase} not supported.")
      }

    sc.listenerBus.post(
      SparkListenerConnectOperationStarted(
        planHolder.jobGroupId,
        planHolder.operationId,
        clock.getTimeMillis(),
        v.getSessionId,
        v.getUserContext.getUserId,
        v.getUserContext.getUserName,
        Utils.redact(
          sessionHolder.session.sessionState.conf.stringRedactionPattern,
          ProtoUtils.abbreviate(plan, Events.MAX_STATEMENT_TEXT_SIZE).toString),
        v.getClientType))
  }

  private def assertExecutedPlanPrepared(dataFrameOpt: Option[DataFrame]): Unit = {
    dataFrameOpt.foreach { dataFrame =>
      val isEagerlyExecuted = dataFrame.queryExecution.analyzed.find {
        case _: Command => true
        case _ => false
      }.isDefined
      val isStreaming = dataFrame.queryExecution.analyzed.isStreaming

      if (!isEagerlyExecuted && !isStreaming) {
        dataFrame.queryExecution.executedPlan
      }
    }
  }

  /**
   * When plan has been optimized prior to execution.
   */
  def postParsed(dataFrameOpt: Option[DataFrame]): Unit = {
    assertExecutedPlanPrepared(dataFrameOpt)
    for {
      jobGroupId <- getJobGroupId()
      queryId <- ExecutePlanHolder.getQueryOperationId(jobGroupId)
    } {
      val event = SparkListenerConnectOperationParsed(jobGroupId, queryId, clock.getTimeMillis())
      event.analyzedPlan = dataFrameOpt.map(_.queryExecution.analyzed)
      sessionHolder.session.sparkContext.listenerBus.post(event)
    }
  }
  def postCanceled(jobGroupId: String): Unit = {
    for {
      queryId <- ExecutePlanHolder.getQueryOperationId(jobGroupId)
    } {
      sessionHolder.session.sparkContext.listenerBus
        .post(SparkListenerConnectOperationCanceled(jobGroupId, queryId, clock.getTimeMillis()))
    }
  }
  def postFailed(errorMessage: String): Unit = {
    for {
      jobGroupId <- getJobGroupId()
      queryId <- ExecutePlanHolder.getQueryOperationId(jobGroupId)
    } {
      sessionHolder.session.sparkContext.listenerBus.post(
        SparkListenerConnectOperationFailed(
          jobGroupId,
          queryId,
          clock.getTimeMillis(),
          errorMessage))
    }
  }
  def postParsedAndFinished(dataFrameOpt: Option[DataFrame]): Unit = {
    postParsed(dataFrameOpt)
    postFinished()
  }
  def postFinished(): Unit = {
    for {
      jobGroupId <- getJobGroupId()
      queryId <- ExecutePlanHolder.getQueryOperationId(jobGroupId)
    } {
      sessionHolder.session.sparkContext.listenerBus
        .post(SparkListenerConnectOperationFinished(jobGroupId, queryId, clock.getTimeMillis()))
    }
  }
  def postClosed(): Unit = {
    for {
      jobGroupId <- getJobGroupId()
      queryId <- ExecutePlanHolder.getQueryOperationId(jobGroupId)
    } {
      sessionHolder.session.sparkContext.listenerBus
        .post(SparkListenerConnectOperationClosed(jobGroupId, queryId, clock.getTimeMillis()))
    }
  }
  def postSessionClosed(): Unit = {
    sessionHolder.session.sparkContext.listenerBus
      .post(SparkListenerConnectSessionClosed(sessionHolder.sessionId, clock.getTimeMillis()))
  }
  def getJobGroupId(): Option[String] = {
    Option(
      sessionHolder.session.sparkContext
        .getLocalProperty(SparkContext.SPARK_JOB_GROUP_ID))
  }
}

case class SparkListenerConnectOperationStarted(
    jobGroupId: String,
    operationId: String,
    eventTime: Long,
    sessionId: String,
    userId: String,
    userName: String,
    statementText: String,
    clientType: String,
    extraTags: Map[String, String] = Map.empty)
    extends SparkListenerEvent

/**
 * When plan has been optimized prior to execution.
 */
case class SparkListenerConnectOperationParsed(
    jobGroupId: String,
    operationId: String,
    eventTime: Long,
    extraTags: Map[String, String] = Map.empty)
    extends SparkListenerEvent {
  @JsonIgnore var analyzedPlan: Option[LogicalPlan] = None
}

case class SparkListenerConnectOperationCanceled(
    jobGroupId: String,
    operationId: String,
    eventTime: Long,
    extraTags: Map[String, String] = Map.empty)
    extends SparkListenerEvent

case class SparkListenerConnectOperationFailed(
    jobGroupId: String,
    operationId: String,
    eventTime: Long,
    errorMessage: String,
    extraTags: Map[String, String] = Map.empty)
    extends SparkListenerEvent

case class SparkListenerConnectOperationFinished(
    jobGroupId: String,
    operationId: String,
    eventTime: Long,
    extraTags: Map[String, String] = Map.empty)
    extends SparkListenerEvent

case class SparkListenerConnectOperationClosed(
    jobGroupId: String,
    operationId: String,
    eventTime: Long,
    extraTags: Map[String, String] = Map.empty)
    extends SparkListenerEvent

case class SparkListenerConnectSessionClosed(
    sessionId: String,
    eventTime: Long,
    extraTags: Map[String, String] = Map.empty)
    extends SparkListenerEvent
