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

package org.apache.spark.sql.catalyst.plans

import java.util.HashMap

import org.apache.spark.sql.catalyst.analysis.GetViewColumnByNameAndOrdinal
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.expressions.aggregate.AggregateExpression
import org.apache.spark.sql.catalyst.optimizer.ReplaceExpressions
import org.apache.spark.sql.catalyst.plans.logical._

object NormalizePlan extends PredicateHelper {
  def apply(plan: LogicalPlan): LogicalPlan = {
    val withNormalizedExpressions = normalizeExpressions(plan)
    val withNormalizedExprIds = normalizeExprIds(withNormalizedExpressions)
    normalizePlan(withNormalizedExprIds)
  }

  /**
   * Normalizes expressions in a plan, that either produces non-deterministic results or
   * will be different between fixed-point and single-pass analyzer, due to the nature
   * of bottom-up resolution. Before normalization, pre-process the plan by replacing all
   * [[RuntimeReplaceable]] nodes with their replacements.
   */
  def normalizeExpressions(plan: LogicalPlan): LogicalPlan = {
    val withNormalizedRuntimeReplaceable = normalizeRuntimeReplaceable(plan)
    withNormalizedRuntimeReplaceable transformAllExpressions {
      case commonExpressionDef: CommonExpressionDef =>
        commonExpressionDef.copy(id = new CommonExpressionId(id = 0))
      case commonExpressionRef: CommonExpressionRef =>
        commonExpressionRef.copy(id = new CommonExpressionId(id = 0))
      case expressionWithRandomSeed: ExpressionWithRandomSeed =>
        expressionWithRandomSeed.withNewSeed(0)
    }
  }

  /**
   * Normalize [[RuntimeReplaceable]] nodes by replacing them with their replacement expressions.
   * This is necessary because fixed-point analyzer may produce non-deterministic results when
   * resolving original expressions. For example, in a query like:
   *
   * {{{ SELECT assert_true(1) }}}
   *
   * Before resolution, we have [[UnresolvedFunction]] whose child is Literal(1). This child will
   * first be converted to Cast(Literal(1), BooleanType) by type coercion. Because in this case
   * [[Cast]] doesn't require timezone, the expression will be implicitly resolved. Because the
   * child of initially unresolved function is resolved, the function can be converted to
   * [[AssertTrue]], which is of type [[InheritAnalysisRules]]. However, because the only child of
   * [[InheritAnalysisRules]] is the replacement expression, the original expression will be lost
   * and timezone will never be applied. This causes inconsistencies, because fixed-point semantic
   * is to ALWAYS apply timezone, regardless of whether the Cast actually needs it.
   */
  def normalizeRuntimeReplaceable(plan: LogicalPlan): LogicalPlan = ReplaceExpressions(plan)

  /**
   * Since attribute references are given globally unique ids during analysis,
   * we must normalize them to check if two different queries are identical.
   */
  def normalizeExprIds(plan: LogicalPlan): LogicalPlan = {
    plan transformAllExpressions {
      case s: ScalarSubquery =>
        s.copy(plan = normalizeExprIds(s.plan), exprId = ExprId(0))
      case s: LateralSubquery =>
        s.copy(plan = normalizeExprIds(s.plan), exprId = ExprId(0))
      case e: Exists =>
        e.copy(plan = normalizeExprIds(e.plan), exprId = ExprId(0))
      case l: ListQuery =>
        l.copy(plan = normalizeExprIds(l.plan), exprId = ExprId(0))
      case a: AttributeReference =>
        AttributeReference(a.name, a.dataType, a.nullable)(exprId = ExprId(0))
      case OuterReference(a: AttributeReference) =>
        OuterReference(AttributeReference(a.name, a.dataType, a.nullable)(exprId = ExprId(0)))
      case a: Alias =>
        Alias(a.child, a.name)(exprId = ExprId(0))
      case OuterReference(a: Alias) =>
        OuterReference(Alias(a.child, a.name)(exprId = ExprId(0)))
      case ae: AggregateExpression =>
        ae.copy(resultId = ExprId(0))
      case lv: NamedLambdaVariable =>
        lv.copy(exprId = ExprId(0), value = null)
      case udf: PythonUDF =>
        udf.copy(resultId = ExprId(0))
      case udaf: PythonUDAF =>
        udaf.copy(resultId = ExprId(0))
      case a: FunctionTableSubqueryArgumentExpression =>
        a.copy(plan = normalizeExprIds(a.plan), exprId = ExprId(0))
    }
  }

  /**
   * Normalizes plans:
   * - Filter the filter conditions that appear in a plan. For instance,
   *   ((expr 1 && expr 2) && expr 3), (expr 1 && expr 2 && expr 3), (expr 3 && (expr 1 && expr 2)
   *   etc., will all now be equivalent.
   * - Sample the seed will replaced by 0L.
   * - Join conditions will be resorted by hashCode.
   * - CTERelationDef ids will be rewritten using a monitonically increasing counter from 0.
   * - CTERelationRef ids will be remapped based on the new CTERelationDef IDs. This is possible,
   *   because WithCTE returns cteDefs as first children, and the defs will be traversed before the
   *   refs.
   */
  def normalizePlan(plan: LogicalPlan): LogicalPlan = {
    val cteIdNormalizer = new CteIdNormalizer
    plan transform {
      case Filter(condition: Expression, child: LogicalPlan) =>
        Filter(
          splitConjunctivePredicates(condition)
            .map(rewriteBinaryComparison)
            .sortBy(_.hashCode())
            .reduce(And),
          child
        )
      case sample: Sample =>
        sample.copy(seed = 0L)
      case Join(left, right, joinType, condition, hint) if condition.isDefined =>
        val newJoinType = joinType match {
          case ExistenceJoin(a: Attribute) =>
            val newAttr = AttributeReference(a.name, a.dataType, a.nullable)(exprId = ExprId(0))
            ExistenceJoin(newAttr)
          case other => other
        }

        val newCondition =
          splitConjunctivePredicates(condition.get)
            .map(rewriteBinaryComparison)
            .sortBy(_.hashCode())
            .reduce(And)
        Join(left, right, newJoinType, Some(newCondition), hint)
      case Project(projectList, child) =>
        val projList = projectList
          .map { e =>
            e.transformUp {
              case g: GetViewColumnByNameAndOrdinal => g.copy(viewDDL = None)
            }
          }
          .asInstanceOf[Seq[NamedExpression]]
        Project(projList, child)
      case c: KeepAnalyzedQuery => c.storeAnalyzedQuery()
      case localRelation: LocalRelation if !localRelation.data.isEmpty =>
        /**
         * A substitute for the [[LocalRelation.data]]. [[GenericInternalRow]] is incomparable for
         * maps, because [[ArrayBasedMapData]] doesn't define [[equals]].
         */
        val unsafeProjection = UnsafeProjection.create(localRelation.schema)
        localRelation.copy(data = localRelation.data.map { row =>
          unsafeProjection(row)
        })
      case cteRelationDef: CTERelationDef =>
        cteIdNormalizer.normalizeDef(cteRelationDef)
      case cteRelationRef: CTERelationRef =>
        cteIdNormalizer.normalizeRef(cteRelationRef)
    }
  }

  /**
   * Rewrite [[BinaryComparison]] operator to keep order. The following cases will be
   * equivalent:
   * 1. (a = b), (b = a);
   * 2. (a <=> b), (b <=> a).
   * 3. (a > b), (b < a)
   */
  private def rewriteBinaryComparison(condition: Expression): Expression = condition match {
    case EqualTo(l, r) => Seq(l, r).sortBy(_.hashCode()).reduce(EqualTo)
    case EqualNullSafe(l, r) => Seq(l, r).sortBy(_.hashCode()).reduce(EqualNullSafe)
    case GreaterThan(l, r) if l.hashCode() > r.hashCode() => LessThan(r, l)
    case LessThan(l, r) if l.hashCode() > r.hashCode() => GreaterThan(r, l)
    case GreaterThanOrEqual(l, r) if l.hashCode() > r.hashCode() => LessThanOrEqual(r, l)
    case LessThanOrEqual(l, r) if l.hashCode() > r.hashCode() => GreaterThanOrEqual(r, l)
    case _ => condition // Don't reorder.
  }
}

class CteIdNormalizer {
  private var cteIdCounter: Long = 0
  private val oldToNewIdMapping = new HashMap[Long, Long]

  def normalizeDef(cteRelationDef: CTERelationDef): CTERelationDef = {
    try {
      oldToNewIdMapping.put(cteRelationDef.id, cteIdCounter)
      cteRelationDef.copy(id = cteIdCounter)
    } finally {
      cteIdCounter += 1
    }
  }

  def normalizeRef(cteRelationRef: CTERelationRef): CTERelationRef = {
    if (oldToNewIdMapping.containsKey(cteRelationRef.cteId)) {
      cteRelationRef.copy(cteId = oldToNewIdMapping.get(cteRelationRef.cteId))
    } else {
      cteRelationRef
    }
  }
}
