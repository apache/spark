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

package org.apache.spark.sql.catalyst.analysis

import org.apache.spark.sql.catalyst.expressions.{Attribute, AttributeReference, ExprId}
import org.apache.spark.sql.catalyst.plans.logical.{LeafNode, LogicalPlan}
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.types.StructType

/**
 * Shared helpers for the stateful-operator nullability fix. The fix has three
 * independent components, all gated by
 * [[SQLConf.STATEFUL_OPERATOR_ALWAYS_NULLABLE_OUTPUT]] (pinned per-query via the
 * offset log so existing queries keep their pre-fix behavior on restart):
 *
 *   - (a) `widenStateSchema`: explicit `asNullable` at every state-schema construction
 *         site in each stateful physical exec.
 *   - (b) `widenOutputForStatefulOp`: a per-op `output` override on every stateful logical
 *         and physical operator, used by the operator's `output` definition.
 *   - (c) [[WidenStatefulOperatorAttributeNullability]] (defined below in this file): a
 *         custom optimizer rule that widens `AttributeReference`s inside stateful ops'
 *         internal expressions and propagates upward to ancestor expressions.
 */
object WidenStatefulOpNullability {

  def isEnabled: Boolean =
    SQLConf.get.getConf(SQLConf.STATEFUL_OPERATOR_ALWAYS_NULLABLE_OUTPUT)

  /**
   * Recursively widens an attribute to be fully nullable: outer `nullable = true` plus
   * every nested `StructField.nullable`, `ArrayType.containsNull`, and
   * `MapType.valueContainsNull` flipped to `true` via
   * [[org.apache.spark.sql.types.DataType#asNullable]].
   */
  def deepWidenAttribute(a: Attribute): Attribute = a match {
    case ref: AttributeReference =>
      AttributeReference(
        ref.name, ref.dataType.asNullable, nullable = true, ref.metadata)(
        ref.exprId, ref.qualifier)
    case other => other.withNullability(true)
  }

  /**
   * Component (a): widens a state schema to fully nullable. Stateful physical execs apply
   * this at every `validateAndMaybeEvolveStateSchema(...)` call site and every
   * `mapPartitionsWith*StateStore(...)` call site. When the conf is off, returns the
   * schema unchanged.
   */
  def widenStateSchema(schema: StructType): StructType =
    if (isEnabled) schema.asNullable else schema

  /**
   * Component (b): wraps a stateful operator's `output` to be fully nullable. The caller
   * is responsible for only calling this from within an `output` definition on a stateful
   * operator; gating is handled here via [[isEnabled]].
   */
  def widenOutputForStatefulOp(base: Seq[Attribute]): Seq[Attribute] =
    if (isEnabled) base.map(deepWidenAttribute) else base
}

/**
 * Component (c) of the stateful-operator nullability fix: a custom optimizer rule that
 * widens `AttributeReference`s inside streaming-stateful operators' internal expressions
 * and propagates the widening upward to ancestor operators' expressions.
 *
 * The rule does NOT introduce any new logical or physical node. It is purely an
 * attribute-rewrite pass:
 *
 *   1. At a stateful operator: rewrite every `AttributeReference` inside the operator's
 *      internal expressions via [[WidenStatefulOpNullability#deepWidenAttribute]] whenever
 *      the attribute's `exprId` matches one in the operator's own (already widened via
 *      component (b)) `output`.
 *
 *   2. At non-stateful ancestor operators: rewrite `AttributeReference`s whose `exprId` is
 *      in `children.flatMap(_.output)` (already widened thanks to component (b)).
 *
 * '''Scope.''' The walk only fires on nodes whose subtree contains a stateful operator.
 *
 * '''Ordering constraint.''' This rule must run AFTER every `UpdateAttributeNullability`
 * invocation in both the main optimizer and AQE.
 *
 * '''Idempotence.''' [[WidenStatefulOpNullability#deepWidenAttribute]] is idempotent.
 */
object WidenStatefulOperatorAttributeNullability extends Rule[LogicalPlan] {

  override def apply(plan: LogicalPlan): LogicalPlan = {
    if (!conf.getConf(SQLConf.STATEFUL_OPERATOR_ALWAYS_NULLABLE_OUTPUT) ||
        !plan.containsStatefulOperator) {
      return plan
    }
    plan.resolveOperatorsUp {
      case p if !p.resolved => p
      case p: LeafNode => p
      case p if !p.containsStatefulOperator => p
      case p =>
        val widenableExprIds: Set[ExprId] = (p.output ++ p.children.flatMap(_.output))
          .iterator.collect { case ar: AttributeReference => ar.exprId }.toSet
        if (widenableExprIds.isEmpty) {
          p
        } else {
          p.transformExpressions {
            case ar: AttributeReference if widenableExprIds.contains(ar.exprId) =>
              val widened = WidenStatefulOpNullability.deepWidenAttribute(ar)
              if (ar.dataType == widened.dataType && ar.nullable == widened.nullable) {
                ar
              } else {
                widened
              }
          }
        }
    }
  }
}
