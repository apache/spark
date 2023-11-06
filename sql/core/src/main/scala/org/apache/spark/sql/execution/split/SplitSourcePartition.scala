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

package org.apache.spark.sql.execution.split

import scala.annotation.tailrec

import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.execution._
import org.apache.spark.sql.execution.exchange.BroadcastExchangeExec

object SplitSourcePartition extends Rule[SparkPlan] {

  override def apply(plan: SparkPlan): SparkPlan = {
    if (!plan.conf.splitSourcePartitionEnabled ||
      plan.find {
        case _: DataSourceScanExec => true
        case _ => false
      }.isEmpty) {
      return plan
    }

    val r = plan.transformOnceWithPruning(shouldBreak) {
      case d: DataSourceScanExec =>
        SplitExchangeExec(
          plan.conf.splitSourcePartitionMaxExpandNum,
          plan.conf.splitSourcePartitionThreshold,
          d)
      case o => o
    }
    r
  }

  private def shouldBreak(plan: SparkPlan): Boolean =
    plan match {
      case BroadcastExchangeExec(_, c) => askChild(c)
      /* case p if !p.requiredChildDistribution.forall(_ == UnspecifiedDistribution) =>
        p.children.exists(supportSplit) */
      case _ => false
    }

  @tailrec
  private def askChild(plan: SparkPlan): Boolean =
    plan match {
      case n if n == null => false
      case l: LeafExecNode => l.isInstanceOf[DataSourceScanExec]
      case u: UnaryExecNode => askChild(u.child)
      case _ => false
    }

}
