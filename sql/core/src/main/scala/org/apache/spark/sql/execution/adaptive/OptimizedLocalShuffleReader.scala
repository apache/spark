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

import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.execution.SparkPlan
import org.apache.spark.sql.execution.adaptive.rule.CoalescedShuffleReaderExec
import org.apache.spark.sql.execution.exchange.{EnsureRequirements, ShuffleExchangeExec}
import org.apache.spark.sql.execution.joins.{BroadcastHashJoinExec, BuildLeft, BuildRight}
import org.apache.spark.sql.internal.SQLConf

case class OptimizedLocalShuffleReader(conf: SQLConf) extends Rule[SparkPlan] {
  override def apply(plan: SparkPlan): SparkPlan = {
    if (!conf.optimizedLocalShuffleReaderEnabled) {
      return plan
    }

    plan.transformUp {
      case bhj: BroadcastHashJoinExec =>
        bhj.buildSide match {
          case BuildLeft if (bhj.right.isInstanceOf[CoalescedShuffleReaderExec]) =>
            bhj.right.asInstanceOf[CoalescedShuffleReaderExec].isLocal = true
          case BuildRight if (bhj.left.isInstanceOf[CoalescedShuffleReaderExec]) =>
            bhj.left.asInstanceOf[CoalescedShuffleReaderExec].isLocal = true
          case _ => None
        }
        bhj
    }

    val afterEnsureRequirements = EnsureRequirements(conf).apply(plan)
    val numExchanges = afterEnsureRequirements.collect {
      case e: ShuffleExchangeExec => e
    }.length
    if (numExchanges > 0) {
      logWarning("Local shuffle reader optimization is not applied due" +
        " to additional shuffles will be introduced.")
      revertLocalShuffleReader(plan)
    } else {
      plan
    }
  }
  private def revertLocalShuffleReader(plan: SparkPlan): SparkPlan = {
    plan.transformUp {
      case bhj: BroadcastHashJoinExec =>
        bhj.buildSide match {
          case BuildLeft if (bhj.right.isInstanceOf[CoalescedShuffleReaderExec]) =>
            bhj.right.asInstanceOf[CoalescedShuffleReaderExec].isLocal = false
          case BuildRight if (bhj.left.isInstanceOf[CoalescedShuffleReaderExec]) =>
            bhj.left.asInstanceOf[CoalescedShuffleReaderExec].isLocal = false
          case _ => None
        }
        bhj
    }
    plan
  }
}
