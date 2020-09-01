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
import org.apache.spark.sql.catalyst.dsl.expressions._
import org.apache.spark.sql.catalyst.expressions.{IsNull, Literal}
import org.apache.spark.sql.catalyst.expressions.aggregate.{Count, CountIf}
import org.apache.spark.sql.catalyst.optimizer.BuildRight
import org.apache.spark.sql.catalyst.planning.ExtractSingleColumnNullAwareAntiJoin
import org.apache.spark.sql.catalyst.plans.LeftAnti
import org.apache.spark.sql.catalyst.plans.logical.{Aggregate, LocalRelation}
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.execution.adaptive.AdaptiveSparkPlanExec
import org.apache.spark.sql.execution.exchange.Exchange
import org.apache.spark.sql.execution.joins.ShuffledHashJoinExec
import org.apache.spark.sql.internal.SQLConf

/**
 * This optimization rule detects and eliminate Shuffled NAAJ with following scenarios.
 * 1. Join is single column shuffled NAAJ and buildSide is not [[Exchange]],
 * when buildSide is Empty, the Join will be rewritten into its streamedSide logical plan.
 * 2. Join is single column shuffled NAAJ and buildSide is not [[Exchange]],
 * when buildSide contains record with all the partition keys to be null,
 * the Join will be rewritten into an Empty LocalRelation.
 */
case class RemoveShuffledNaaJIfBuildSideNotExchange(
    sparkSession: SparkSession,
    conf: SQLConf) extends Rule[SparkPlan] {
  def apply(plan: SparkPlan): SparkPlan = {
    if (!conf.adaptiveExecutionEnabled || !plan.isInstanceOf[AdaptiveSparkPlanExec]) {
      plan
    } else {
      val updatedInputPlan = plan.asInstanceOf[AdaptiveSparkPlanExec].inputPlan.transformDown {
        case shj @ ShuffledHashJoinExec(_, _, LeftAnti, BuildRight, _, left, right, true)
          if !right.isInstanceOf[Exchange] =>
          // When BuildSide is not an Exchange, pre-compute information
          // whether the buildSide is empty or contains record with all the partition.
          shj.logicalLink.get match {
            case j @ ExtractSingleColumnNullAwareAntiJoin(_, _) =>
              val executedPlan = new QueryExecution(sparkSession,
                Aggregate(Seq.empty, Seq(
                  Count(Literal(1)).toAggregateExpression().as("outputNumRows"),
                  CountIf(IsNull(j.right.output.head))
                    .toAggregateExpression().as("nullPartitionKeyNumRows")), j.right))
                .executedPlan

              val statsRow = executedPlan.executeTake(1).head

              val isEmpty = statsRow.getLong(0) == 0L
              val containsNullPartitionKeys = statsRow.getLong(1) > 0L

              if (isEmpty) {
                left
              } else if (containsNullPartitionKeys) {
                val newPlan = LocalTableScanExec(shj.output, Seq.empty)
                newPlan.setLogicalLink(
                  LocalRelation(j.output, data = Seq.empty, isStreaming = j.isStreaming)
                )
                newPlan
              } else {
                shj
              }
          }
      }
      plan.asInstanceOf[AdaptiveSparkPlanExec].copy(inputPlan = updatedInputPlan)
    }
  }
}
