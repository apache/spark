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
package org.apache.spark.sql.streaming.util

import org.apache.spark.SparkFunSuite
import org.apache.spark.sql.catalyst.expressions.{AttributeReference, Expression}
import org.apache.spark.sql.catalyst.plans.physical.{ClusteredDistribution, HashPartitioning, StatefulOpClusteredDistribution}
import org.apache.spark.sql.execution.SparkPlan

trait StatefulOpClusteredDistributionTestHelper extends SparkFunSuite {
  protected def requireClusteredDistribution(
      plan: SparkPlan,
      desiredClusterColumns: Seq[Seq[String]],
      desiredNumPartitions: Option[Int]): Boolean = {
    assert(plan.requiredChildDistribution.length === desiredClusterColumns.length)
    plan.requiredChildDistribution.zip(desiredClusterColumns).forall {
      case (d: ClusteredDistribution, clusterColumns: Seq[String])
        if partitionExpressionsColumns(d.clustering) == clusterColumns &&
          d.requiredNumPartitions == desiredNumPartitions => true

      case _ => false
    }
  }

  protected def requireStatefulOpClusteredDistribution(
      plan: SparkPlan,
      desiredClusterColumns: Seq[Seq[String]],
      desiredNumPartitions: Int): Boolean = {
    assert(plan.requiredChildDistribution.length === desiredClusterColumns.length)
    plan.requiredChildDistribution.zip(desiredClusterColumns).forall {
      case (d: StatefulOpClusteredDistribution, clusterColumns: Seq[String])
        if partitionExpressionsColumns(d.expressions) == clusterColumns &&
          d._requiredNumPartitions == desiredNumPartitions => true

      case _ => false
    }
  }

  protected def hasDesiredHashPartitioning(
      plan: SparkPlan,
      desiredClusterColumns: Seq[String],
      desiredNumPartitions: Int): Boolean = {
    plan.outputPartitioning match {
      case HashPartitioning(expressions, numPartitions)
        if partitionExpressionsColumns(expressions) == desiredClusterColumns &&
          numPartitions == desiredNumPartitions => true

      case _ => false
    }
  }

  protected def hasDesiredHashPartitioningInChildren(
      plan: SparkPlan,
      desiredClusterColumns: Seq[Seq[String]],
      desiredNumPartitions: Int): Boolean = {
    plan.children.zip(desiredClusterColumns).forall { case (child, clusterColumns) =>
      hasDesiredHashPartitioning(child, clusterColumns, desiredNumPartitions)
    }
  }

  private def partitionExpressionsColumns(expressions: Seq[Expression]): Seq[String] = {
    expressions.flatMap {
      case ref: AttributeReference => Some(ref.name)
    }
  }
}
