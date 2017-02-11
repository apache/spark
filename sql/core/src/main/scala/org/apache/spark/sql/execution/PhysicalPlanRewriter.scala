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

package org.apache.spark.sql.execution

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.rules.RuleExecutor
import org.apache.spark.sql.execution.aggregate.MergePartialAggregate
import org.apache.spark.sql.execution.exchange.{EnsureRequirements, ReuseExchange}

/**
 * A sequence of rules that will be applied in order to the physical plan before execution.
 */
class PhysicalPlanRewriter(sparkSession: SparkSession) extends RuleExecutor[SparkPlan] {

  private val fixedPoint = FixedPoint(sparkSession.sessionState.conf.optimizerMaxIterations)

  override def batches: Seq[Batch] = Seq(
    Batch("ExtractPythonUDFs", Once,
      python.ExtractPythonUDFs),
    Batch("PlanSubqueries", Once,
      PlanSubqueries(sparkSession)),
    Batch("EnsureRequirements", Once,
      EnsureRequirements(sparkSession.sessionState.conf)),
    Batch("MergePartialAggregate", fixedPoint,
      MergePartialAggregate),
    Batch("CollapseCodegenStages", Once,
      CollapseCodegenStages(sparkSession.sessionState.conf)),
    Batch("ReuseResources", Once,
      ReuseExchange(sparkSession.sessionState.conf),
      ReuseSubquery(sparkSession.sessionState.conf)
    )
  )
}
