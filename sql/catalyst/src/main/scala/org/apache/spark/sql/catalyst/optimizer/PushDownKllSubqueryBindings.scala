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
      val reachable = new java.util.IdentityHashMap[LogicalPlan, java.lang.Boolean]
      def reachesBinding(input: LogicalPlan): Boolean = reachable.computeIfAbsent(input, {
        case filter: Filter => reachesBinding(filter.child)
        case alias: SubqueryAlias => reachesBinding(alias.child)
        case project: Project =>
          project.projectList.exists {
            case alias: Alias => eligible(alias)
            case _ => false
          } || reachesBinding(project.child)
        case _ => false
      }).booleanValue()
      def lower(input: LogicalPlan, consumers: => Vector[Expression]): LogicalPlan = {
        if (!input.containsPattern(NONDETERMINISTIC_SUBQUERY_BINDING) ||
            !reachesBinding(input)) {
          return input
        }
        input match {
          case filter: Filter => filter.copy(child = lower(filter.child,
            consumers :+ trimAliases(filter.condition)))
          case alias: SubqueryAlias => alias.copy(child = lower(alias.child, consumers))
          case projection: Project =>
            val bindings = projection.projectList.collect { case a: Alias if eligible(a) => a }
            val bindingIds = bindings.map(_.exprId).toSet
            val aliases = AttributeMap(getAliasMap(projection).toSeq.filterNot {
              case (attribute, _) => bindingIds.contains(attribute.exprId)
            })
            val expanded = if (aliases.isEmpty) consumers
              else consumers.map(replaceAlias(_, aliases))
            if (bindings.isEmpty) {
              return projection.copy(child = lower(projection.child, expanded))
            }
            val contributions = mutable.HashMap.empty[ExprId, mutable.ArrayBuffer[AttributeSet]]
            val contexts = new java.util.IdentityHashMap[AttributeSet, AttributeSet]
            val seen = mutable.HashMap.empty[
              ExprId, java.util.IdentityHashMap[AttributeSet, java.lang.Boolean]]
            def withoutBindings(references: AttributeSet): AttributeSet = {
              val cached = contexts.get(references)
              if (cached != null) cached else {
                val filtered = references.filter(a => !bindingIds.contains(a.exprId))
                contexts.put(references, filtered)
                contexts.put(filtered, filtered)
                filtered
              }
            }
            def collect(expression: Expression, references: AttributeSet): Unit = expression match {
              case attribute: Attribute if bindingIds.contains(attribute.exprId) =>
                val filtered = withoutBindings(references)
                val contexts = seen.getOrElseUpdate(attribute.exprId,
                  new java.util.IdentityHashMap[AttributeSet, java.lang.Boolean])
                if (contexts.put(filtered, true) == null) {
                  contributions.getOrElseUpdate(attribute.exprId, mutable.ArrayBuffer.empty) +=
                    filtered
                }
              case comparison: BinaryComparison => comparison.children.foreach { operand =>
                val local = withoutBindings(operand.references)
                collect(operand, if (local.nonEmpty) local else references)
              }
              case other => other.children.foreach(collect(_, references))
            }
            def collectConjuncts(expression: Expression): Unit = expression match {
              case And(left, right) => collectConjuncts(left); collectConjuncts(right)
              case other => collect(other, other.references)
            }
            expanded.foreach(collectConjuncts)
            val uses = contributions.iterator.map { case (id, sets) =>
              id -> AttributeSet.fromAttributeSets(sets)
            }.toMap
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
      }
      lazy val consumers = join.condition.toVector.map(trimAliases)
      join.copy(left = lower(join.left, consumers), right = lower(join.right, consumers))
  }
}
