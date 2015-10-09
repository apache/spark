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

import org.apache.spark.annotation.DeveloperApi
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.Expression
import org.apache.spark.sql.catalyst.plans.physical._
import org.apache.spark.sql.execution.{BinaryNode, SparkPlan}
import org.apache.spark.sql.execution.metric.SQLMetrics

/**
 * :: DeveloperApi ::
 * Performs an inner hash join of two child relations by first shuffling the data using the join
 * keys.
 */
@DeveloperApi
case class ShuffledHashJoin(
    leftKeys: Seq[Expression],
    rightKeys: Seq[Expression],
    buildSide: BuildSide,
    left: SparkPlan,
    right: SparkPlan)
  extends BinaryNode with HashJoin {

  override private[sql] lazy val metrics = Map(
    "numLeftRows" -> SQLMetrics.createLongMetric(sparkContext, "number of left rows"),
    "numRightRows" -> SQLMetrics.createLongMetric(sparkContext, "number of right rows"),
    "numOutputRows" -> SQLMetrics.createLongMetric(sparkContext, "number of output rows"))

  override def outputPartitioning: Partitioning =
    PartitioningCollection(Seq(left.outputPartitioning, right.outputPartitioning))

  override def requiredChildDistribution: Seq[Distribution] =
    ClusteredDistribution(leftKeys) :: ClusteredDistribution(rightKeys) :: Nil

  protected override def doExecute(): RDD[InternalRow] = {
    val (numBuildRows, numStreamedRows) = buildSide match {
      case BuildLeft => (longMetric("numLeftRows"), longMetric("numRightRows"))
      case BuildRight => (longMetric("numRightRows"), longMetric("numLeftRows"))
    }
    val numOutputRows = longMetric("numOutputRows")

    buildPlan.execute().zipPartitions(streamedPlan.execute()) { (buildIter, streamIter) =>
      val hashed = HashedRelation(buildIter, numBuildRows, buildSideKeyGenerator)
      hashJoin(streamIter, numStreamedRows, hashed, numOutputRows)
    }
  }
}
