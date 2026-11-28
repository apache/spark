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

package org.apache.spark.sql.execution.streaming.runtime

import org.apache.spark.sql.catalyst.plans.physical.{HashPartitioning, RoundRobinPartitioning}
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.execution.SparkPlan
import org.apache.spark.sql.execution.exchange.{Exchange, ShuffleExchangeExec}
import org.apache.spark.sql.execution.joins.CartesianProductExec
import org.apache.spark.sql.execution.streaming.operators.stateful.StateStoreWriter
import org.apache.spark.sql.internal.SQLConf

/**
 * A Cartesian product runs one task per (left partition, right partition) pair. When one side
 * reaches a stateful operator in the same stage, every state partition is therefore computed by
 * one task per partition of the other side, and those sibling tasks load the same state store at
 * once: they wait on each other for the store, and each one reloads its state. State stores assume
 * one task per state partition per stage.
 *
 * This rule ends the stage after the stateful operator with a shuffle that keeps the stateful
 * side's partition count, so each state partition is computed by exactly one task and the
 * Cartesian product reads the shuffle output instead.
 *
 * Must run after `EnsureRequirements`: an exchange it inserts between the stateful operator and the
 * Cartesian product already ends the stage, and this rule must not add a second one.
 */
object InsertShuffleBeforeStatefulCartesianProduct extends Rule[SparkPlan] {

  override def apply(plan: SparkPlan): SparkPlan = {
    if (!conf.getConf(SQLConf.STATEFUL_OPERATOR_SHUFFLE_BEFORE_CARTESIAN_PRODUCT_ENABLED)) {
      return plan
    }
    plan.transformUp {
      case j: CartesianProductExec =>
        (hasStatefulOperatorInStage(j.left), hasStatefulOperatorInStage(j.right)) match {
          case (true, false) => j.withNewChildren(Seq(shuffle(j.left), j.right))
          case (false, true) => j.withNewChildren(Seq(j.left, shuffle(j.right)))
          case _ => j
        }
    }
  }

  /**
   * Keeps the child's partition count rather than the state partition count, which can differ: a
   * streaming global limit has one partition. Reuses the child's hash partitioning, so every row
   * stays in its partition and no sort is needed; round-robin otherwise. Some operators, such as
   * GenerateExec, forward a partitioning over attributes they no longer output, so the partitioning
   * is reused only when the child still outputs every attribute it references.
   */
  private def shuffle(child: SparkPlan): SparkPlan = {
    val partitioning = child.outputPartitioning match {
      case h: HashPartitioning if h.references.subsetOf(child.outputSet) => h
      case p => RoundRobinPartitioning(p.numPartitions)
    }
    ShuffleExchangeExec(partitioning, child)
  }

  /** Whether a stateful operator is reachable without crossing a stage boundary. */
  private def hasStatefulOperatorInStage(plan: SparkPlan): Boolean = plan match {
    case _: Exchange => false
    case s: StateStoreWriter => s.stateInfo.isDefined
    case _ => plan.children.exists(hasStatefulOperatorInStage)
  }
}
