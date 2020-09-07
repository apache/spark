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

import scala.collection.mutable

import org.apache.spark.sql.catalyst.expressions.Expression
import org.apache.spark.sql.catalyst.plans.physical.{HashPartitioning, Partitioning}
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.execution.{SortExec, SparkPlan}
import org.apache.spark.sql.execution.joins.SortMergeJoinExec
import org.apache.spark.sql.internal.SQLConf

/**
 * This rule removes shuffle for the sort merge join if the following conditions are met:
 * - The child of ShuffleExchangeExec has HashPartitioning with the same number of partitions
 *   as the other side of join.
 * - The child of ShuffleExchangeExec has output partitioning which has the subset of
 *   join keys on the respective join side.
 *
 * If the above conditions are met, shuffle can be eliminated for the sort merge join
 * because rows are sorted before join logic is applied.
 */
case class OptimizeSortMergeJoinWithPartialHashDistribution(conf: SQLConf) extends Rule[SparkPlan] {
  def apply(plan: SparkPlan): SparkPlan = {
    if (!conf.optimizeSortMergeJoinWithPartialHashDistribution) {
      return plan
    }

    plan.transformUp {
      case s @ SortMergeJoinExec(_, _, _, _,
        lSort @ SortExec(_, _,
          ExtractShuffleExchangeExecChild(
            lChild,
            lChildOutputPartitioning: HashPartitioning),
          _),
        rSort @ SortExec(_, _,
          ExtractShuffleExchangeExecChild(
            rChild,
            rChildOutputPartitioning: HashPartitioning),
          _),
        false) if isPartialHashDistribution(
          s.leftKeys, lChildOutputPartitioning, s.rightKeys, rChildOutputPartitioning) =>
        // Remove ShuffleExchangeExec.
        s.copy(left = lSort.copy(child = lChild), right = rSort.copy(child = rChild))
      case other => other
    }
  }

  /*
   * Returns true if both HashPartitioning have the same number of partitions and
   * their partitioning expressions are a subset of their respective join keys.
   */
  private def isPartialHashDistribution(
      leftKeys: Seq[Expression],
      leftPartitioning: HashPartitioning,
      rightKeys: Seq[Expression],
      rightPartitioning: HashPartitioning): Boolean = {
    val mapping = leftKeyToRightKeyMapping(leftKeys, rightKeys)
    (leftPartitioning.numPartitions == rightPartitioning.numPartitions) &&
      leftPartitioning.expressions.zip(rightPartitioning.expressions)
        .forall {
          case (le, re) => mapping.get(le.canonicalized)
            .map(_.exists(_.semanticEquals(re)))
            .getOrElse(false)
        }
  }

  /*
   * Builds a mapping from a left key to right key(s).
   */
  private def leftKeyToRightKeyMapping(
      leftKeys: Seq[Expression],
      rightKeys: Seq[Expression]): Map[Expression, Seq[Expression]] = {
    assert(leftKeys.length == rightKeys.length)
    val mapping = mutable.Map.empty[Expression, Seq[Expression]]
    leftKeys.zip(rightKeys).foreach {
      case (leftKey, rightKey) =>
        val key = leftKey.canonicalized
        mapping.get(key) match {
          case Some(v) => mapping.put(key, v :+ rightKey)
          case None => mapping.put(key, Seq(rightKey))
        }
    }
    mapping.toMap
  }
}

/**
 * An extractor that extracts the child of ShuffleExchangeExec and the child's
 * output partitioning.
 */
object ExtractShuffleExchangeExecChild {
  def unapply(plan: SparkPlan): Option[(SparkPlan, Partitioning)] = {
    plan match {
      case s: ShuffleExchangeExec => Some(s.child, s.child.outputPartitioning)
      case _ => None
    }
  }
}
