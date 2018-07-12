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

package org.apache.spark.sql.execution.joins

import org.apache.spark.sql.catalyst.expressions.Expression
import org.apache.spark.sql.catalyst.plans.physical.{ClusteredDistribution, Distribution, HashClusteredDistribution, HashPartitioning}
import org.apache.spark.sql.execution.{FileSourceScanExec, SparkPlan}
import org.apache.spark.sql.execution.exchange.ShuffleExchangeExec

object JoinUtils {
  private def avoidShuffleIfPossible(
      joinKeys: Seq[Expression],
      expressions: Seq[Expression],
      leftKeys: Seq[Expression],
      rightKeys: Seq[Expression]): Seq[Distribution] = {
    val indices = expressions.map(x => joinKeys.indexWhere(_.semanticEquals(x)))
    HashClusteredDistribution(indices.map(leftKeys(_))) ::
      HashClusteredDistribution(indices.map(rightKeys(_))) :: Nil
  }

  private def checkBucketTable(plan: SparkPlan) = {
    // plan has HashPartitioning already
    if (plan.collect { case _: ShuffleExchangeExec => true }.nonEmpty) {
      false
    } else {
      val leaves = plan.collectLeaves()
      leaves.length == 1 && leaves.head.isInstanceOf[FileSourceScanExec]
    }
  }

  def requiredChildDistributionForShuffledJoin(
      partitioningDetection: Boolean,
      leftKeys: Seq[Expression],
      rightKeys: Seq[Expression],
      left: SparkPlan,
      right: SparkPlan): Seq[Distribution] = {
    if (!partitioningDetection) {
      return HashClusteredDistribution(leftKeys) :: HashClusteredDistribution(rightKeys) :: Nil
    }

    val leftPartitioning = left.outputPartitioning
    val rightPartitioning = right.outputPartitioning
    leftPartitioning match {
      case HashPartitioning(leftExpressions, _)
        if leftPartitioning.satisfies(ClusteredDistribution(leftKeys)) &&
          checkBucketTable(left) =>
        avoidShuffleIfPossible(leftKeys, leftExpressions, leftKeys, rightKeys)

      case _ => rightPartitioning match {
        case HashPartitioning(rightExpressions, _)
          if rightPartitioning.satisfies(ClusteredDistribution(rightKeys)) &&
            checkBucketTable(right) =>
          avoidShuffleIfPossible(rightKeys, rightExpressions, leftKeys, rightKeys)

        case _ =>
          HashClusteredDistribution(leftKeys) :: HashClusteredDistribution(rightKeys) :: Nil
      }
    }
  }
}
