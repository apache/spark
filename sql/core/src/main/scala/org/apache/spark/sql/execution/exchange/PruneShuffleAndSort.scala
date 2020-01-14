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

package org.apache.spark.sql.execution.exchange

import org.apache.spark.sql.catalyst.expressions.SortOrder
import org.apache.spark.sql.catalyst.plans.physical.{HashPartitioning, PartitioningCollection}
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.execution.{SortExec, SparkPlan}

case class PruneShuffleAndSort() extends Rule[SparkPlan] {

  override def apply(plan: SparkPlan): SparkPlan = {
    plan.transformUp {
      case operator @ ShuffleExchangeExec(upper: HashPartitioning, child, _) =>
        child.outputPartitioning match {
          case lower: HashPartitioning if upper.semanticEquals(lower) => child
          case _ @ PartitioningCollection(partitionings) =>
            if (partitionings.exists{case lower: HashPartitioning =>
              upper.semanticEquals(lower)
            }) {
              child
            } else {
              operator
            }
          case _ => operator
        }
      case SortExec(upper, false, child, _)
        if SortOrder.orderingSatisfies(child.outputOrdering, upper) => child
      case subPlan: SparkPlan => subPlan
    }
  }
}
