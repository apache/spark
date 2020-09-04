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

case class RemoveShuffleExchangeForSortMergeJoin(conf: SQLConf) extends Rule[SparkPlan] {
  def apply(plan: SparkPlan): SparkPlan = plan.transformUp {
    case s @ SortMergeJoinExec(
          _,
          _,
          _,
          _,
          lSort @ SortExec(
            _,
            _,
            ExtractShuffleExchangeExecChild(
              leftChild,
              leftChildOutputPartitioning: HashPartitioning),
            _),
          rSort @ SortExec(
            _,
            _,
            ExtractShuffleExchangeExecChild(
              rightChild,
              rightChildOutputPartitioning: HashPartitioning),
            _),
          _) =>
      // Need to make sure left.child.outputPartitioning and right.child.outputPartitioning
      // consist only of left and right keys respectively, and the order should match up.
      val leftKeyToRightKeyMapping = mutable.Map.empty[Expression, Expression]
      s.leftKeys.zip(s.rightKeys).foreach {
        case (leftKey, rightKey) =>
          val key = leftKey.canonicalized
          leftKeyToRightKeyMapping.get(key) match {
            case Some(_) => () // mapping.put(key, _ :+ rightKey) SHOULD FAIL
            case None => leftKeyToRightKeyMapping.put(key, rightKey)
          }
      }

      // check the length
      val satisfied = leftChildOutputPartitioning.expressions
        .zip(rightChildOutputPartitioning.expressions)
        .forall {
          case (le, re) => leftKeyToRightKeyMapping.get(le.canonicalized)
            .map(_.semanticEquals(re))
            .getOrElse(false)
        }

      val result = if (satisfied) {
        s.copy(left = lSort.copy(child = leftChild), right = rSort.copy(child = rightChild))
      } else {
        s
      }
      result
    case other => other
  }
}

object ExtractShuffleExchangeExecChild {
  def unapply(plan: SparkPlan): Option[(SparkPlan, Partitioning)] = {
    plan match {
      case s: ShuffleExchangeExec => Some(s.child, s.child.outputPartitioning)
      case _ => None
    }
  }
}
