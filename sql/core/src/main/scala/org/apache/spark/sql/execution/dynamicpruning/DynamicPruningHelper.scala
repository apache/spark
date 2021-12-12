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

package org.apache.spark.sql.execution.dynamicpruning

import org.apache.spark.sql.catalyst.catalog.HiveTableRelation
import org.apache.spark.sql.catalyst.expressions.{And, Attribute, AttributeSet, BinaryComparison, Expression, In, InSet, MultiLikeBase, Not, Or, PredicateHelper, StringPredicate, StringRegexExpression, V2ExpressionUtils}
import org.apache.spark.sql.catalyst.plans._
import org.apache.spark.sql.catalyst.plans.logical.{Filter, LogicalPlan}
import org.apache.spark.sql.connector.read.SupportsRuntimeFiltering
import org.apache.spark.sql.execution.datasources.{HadoopFsRelation, LogicalRelation}
import org.apache.spark.sql.execution.datasources.v2.DataSourceV2ScanRelation

trait DynamicPruningHelper extends PredicateHelper {

  /**
   * Searches for a table scan that can be filtered for a given column in a logical plan.
   *
   * This methods tries to find either a v1 or Hive serde partitioned scan for a given
   * partition column or a v2 scan that support runtime filtering on a given attribute.
   */
  def getFilterableTableScan(a: Expression, plan: LogicalPlan): Option[LogicalPlan] = {
    val srcInfo: Option[(Expression, LogicalPlan)] = findExpressionAndTrackLineageDown(a, plan)
    srcInfo.flatMap {
      case (resExp, l: LogicalRelation) =>
        l.relation match {
          case fs: HadoopFsRelation =>
            val partitionColumns = AttributeSet(
              l.resolve(fs.partitionSchema, fs.sparkSession.sessionState.analyzer.resolver))
            if (resExp.references.subsetOf(partitionColumns)) {
              return Some(l)
            } else {
              None
            }
          case _ => None
        }
      case (resExp, l: HiveTableRelation) =>
        if (resExp.references.subsetOf(AttributeSet(l.partitionCols))) {
          return Some(l)
        } else {
          None
        }
      case (resExp, r @ DataSourceV2ScanRelation(_, scan: SupportsRuntimeFiltering, _)) =>
        val filterAttrs = V2ExpressionUtils.resolveRefs[Attribute](scan.filterAttributes, r)
        if (resExp.references.subsetOf(AttributeSet(filterAttrs))) {
          Some(r)
        } else {
          None
        }
      case _ => None
    }
  }

  // checks if two expressions are on opposite sides of the join
  def fromDifferentSides(
    left: LogicalPlan,
    right: LogicalPlan,
    x: Expression,
    y: Expression): Boolean = {
    def fromLeftRight(x: Expression, y: Expression) =
      !x.references.isEmpty && x.references.subsetOf(left.outputSet) &&
        !y.references.isEmpty && y.references.subsetOf(right.outputSet)
    fromLeftRight(x, y) || fromLeftRight(y, x)
  }

  /**
   * Returns whether an expression is likely to be selective
   */
  private def isLikelySelective(e: Expression): Boolean = e match {
    case Not(expr) => isLikelySelective(expr)
    case And(l, r) => isLikelySelective(l) || isLikelySelective(r)
    case Or(l, r) => isLikelySelective(l) && isLikelySelective(r)
    case _: StringRegexExpression => true
    case _: BinaryComparison => true
    case _: In | _: InSet => true
    case _: StringPredicate => true
    case _: MultiLikeBase => true
    case _ => false
  }

  /**
   * Search a filtering predicate in a given logical plan
   */
  private def hasSelectivePredicate(plan: LogicalPlan): Boolean = {
    plan.find {
      case f: Filter => isLikelySelective(f.condition)
      case _ => false
    }.isDefined
  }

  /**
   * To be able to prune partitions on a join key, the filtering side needs to
   * meet the following requirements:
   *   (1) it can not be a stream
   *   (2) it needs to contain a selective predicate used for filtering
   */
  def hasPartitionPruningFilter(plan: LogicalPlan): Boolean = {
    !plan.isStreaming && hasSelectivePredicate(plan)
  }

  def canPruneLeft(joinType: JoinType): Boolean = joinType match {
    case Inner | LeftSemi | RightOuter => true
    case _ => false
  }

  def canPruneRight(joinType: JoinType): Boolean = joinType match {
    case Inner | LeftSemi | LeftOuter => true
    case _ => false
  }
}
