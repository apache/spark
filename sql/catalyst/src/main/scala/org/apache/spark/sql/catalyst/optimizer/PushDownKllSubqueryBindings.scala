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

package org.apache.spark.sql.catalyst.optimizer

import scala.collection.mutable

import org.apache.spark.sql.catalyst.analysis.{KllSubqueryBindingPlacement, PullOutNondeterministic, ValidateSubqueryExpression}
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.plans.InnerLike
import org.apache.spark.sql.catalyst.plans.logical._
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.catalyst.trees.TreePattern.NONDETERMINISTIC_SUBQUERY_BINDING

/** Revisit compiler bindings after pruning removes an earlier projection placement barrier. */
object PushDownKllSubqueryBindings extends Rule[LogicalPlan] with PredicateHelper {
  override def apply(plan: LogicalPlan): LogicalPlan = plan.transformUpWithPruning(
    _.containsPattern(NONDETERMINISTIC_SUBQUERY_BINDING), ruleId) {
    case join: Join if join.joinType.isInstanceOf[InnerLike] && join.hint == JoinHint.NONE =>
      val independent = new ValidateSubqueryExpression.KllSubqueryIndependence
      def eligible(alias: Alias): Boolean = alias.isNondeterministicSubqueryBinding &&
        (alias.child match {
          case scalar: ScalarSubquery => independent(scalar)
          case getter: KllSketchGetNBase => PullOutNondeterministic.safeKllCount(getter)
          case _ => false
        })
      def lower(input: LogicalPlan, consumers: Seq[Expression]): LogicalPlan = input match {
        case filter: Filter => filter.copy(child = lower(filter.child,
          consumers :+ filter.condition))
        case alias: SubqueryAlias => alias.copy(child = lower(alias.child, consumers))
        case projection: Project =>
          val bindings = projection.projectList.collect { case a: Alias if eligible(a) => a }
          val bindingIds = bindings.map(_.exprId).toSet
          val aliases = AttributeMap(getAliasMap(projection).toSeq.filterNot {
            case (attribute, _) => bindingIds.contains(attribute.exprId)
          })
          val expanded = consumers.map(replaceAlias(_, aliases))
          val uses = mutable.HashMap.empty[ExprId, AttributeSet]
          def collect(expression: Expression, references: AttributeSet): Unit = expression match {
            case attribute: Attribute if bindingIds.contains(attribute.exprId) =>
              uses(attribute.exprId) = uses.getOrElse(attribute.exprId, AttributeSet.empty) ++
                AttributeSet(references.filterNot(a => bindingIds.contains(a.exprId)))
            case comparison: BinaryComparison => comparison.children.foreach { operand =>
              val local = AttributeSet(
                operand.references.filterNot(a => bindingIds.contains(a.exprId)))
              collect(operand, if (local.nonEmpty) local else references)
            }
            case other => other.children.foreach(collect(_, references))
          }
          expanded.foreach { expression =>
            splitConjunctivePredicates(expression).foreach(e => collect(e, e.references))
          }
          val placement = new KllSubqueryBindingPlacement
          val movable = bindings.filter { binding =>
            uses.contains(binding.exprId) &&
              !(placement.destination(projection.child, uses(binding.exprId)) eq projection.child)
          }
          if (movable.isEmpty) projection.copy(child = lower(projection.child, expanded)) else {
            val moved = movable.map(_.exprId).toSet
            val outputs = projection.projectList.map {
              case alias: Alias if moved.contains(alias.exprId) => alias.toAttribute
              case other => other
            }
            Project(outputs, placement.project(projection.child, movable, uses.toMap))
          }
        case other => other
      }
      join.copy(left = lower(join.left, join.condition.toSeq),
        right = lower(join.right, join.condition.toSeq))
  }
}
