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

import org.apache.spark.connect.proto
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.connect.SparkConnectTestUtils
import org.apache.spark.sql.connect.service.{ExecuteHolder, ExecuteStatus, SessionStatus, SparkConnectService}

object SparkConnectPlannerTestUtils {
  def transform(spark: SparkSession, relation: proto.Relation): LogicalPlan = {
    new SparkConnectPlanner(SparkConnectTestUtils.createDummySessionHolder(spark))
      .transformRelation(relation)
  }

  def transform(spark: SparkSession, command: proto.Command): Unit = {
    val executeHolder = buildExecutePlanHolder(spark, command)
    new SparkConnectPlanner(executeHolder).process(command, new MockObserver())
  }

  private def buildExecutePlanHolder(
      spark: SparkSession,
      command: proto.Command): ExecuteHolder = {
    val sessionHolder = SparkConnectTestUtils.createDummySessionHolder(spark)
    sessionHolder.eventManager.status_(SessionStatus.Started)

    val context = proto.UserContext
      .newBuilder()
      .setUserId(sessionHolder.userId)
      .build()
    val plan = proto.Plan
      .newBuilder()
      .setCommand(command)
      .build()
    val request = proto.ExecutePlanRequest
      .newBuilder()
      .setPlan(plan)
      .setSessionId(sessionHolder.sessionId)
      .setUserContext(context)
      .build()

    val executeHolder = SparkConnectService.executionManager.createExecuteHolder(request)
    executeHolder.eventsManager.status_(ExecuteStatus.Started)
    executeHolder
  }
}
