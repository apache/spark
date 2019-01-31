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

package org.apache.spark.sql.execution.adaptive

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.execution.SparkPlan
import org.apache.spark.sql.execution.command.ExecutedCommandExec

/**
 * This rule wraps the query plan with an [[AdaptiveSparkPlan]], which executes the query plan
 * adaptively with runtime data statistics. Note that this rule must be run after
 * [[org.apache.spark.sql.execution.exchange.EnsureRequirements]], so that the exchange nodes are
 * already inserted.
 */
case class InsertAdaptiveSparkPlan(session: SparkSession) extends Rule[SparkPlan] {

  override def apply(plan: SparkPlan): SparkPlan = plan match {
    case _: ExecutedCommandExec => plan
    case _ if session.sessionState.conf.adaptiveExecutionEnabled =>
      AdaptiveSparkPlan(plan, session.cloneSession())
  }
}
