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

import org.apache.spark.sql.AnalysisException
import org.apache.spark.sql.catalyst.expressions.SubqueryExpression
import org.apache.spark.sql.catalyst.plans.logical.{LogicalPlan, With}
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.internal.SQLConf.{LEGACY_CTE_PRECEDENCE_POLICY, LegacyBehaviorPolicy}

/**
 * Analyze WITH nodes and substitute child plan with CTE definitions.
 */
object CTESubstitution extends Rule[LogicalPlan] {
  def apply(plan: LogicalPlan): LogicalPlan = {
    LegacyBehaviorPolicy.withName(SQLConf.get.getConf(LEGACY_CTE_PRECEDENCE_POLICY)) match {
      case LegacyBehaviorPolicy.EXCEPTION =>
        assertNoNameConflictsInCTE(plan, inTraverse = false)
        traverseAndSubstituteCTE(plan, inTraverse = false)
      case LegacyBehaviorPolicy.LEGACY =>
        legacyTraverseAndSubstituteCTE(plan)
      case LegacyBehaviorPolicy.CORRECTED =>
        traverseAndSubstituteCTE(plan, inTraverse = false)
    }
  }

  /**
   * Check the plan to be traversed has naming conflicts in nested CTE or not, traverse through
   * child, innerChildren and subquery for the current plan.
   */
  private def assertNoNameConflictsInCTE(
      plan: LogicalPlan,
      inTraverse: Boolean,
      cteNames: Set[String] = Set.empty): Unit = {
    plan.foreach {
      case w @ With(child, relations) =>
        val newNames = relations.map {
          case (cteName, _) =>
            if (cteNames.contains(cteName)) {
              throw new AnalysisException(s"Name $cteName is ambiguous in nested CTE. " +
                s"Please set ${LEGACY_CTE_PRECEDENCE_POLICY.key} to CORRECTED so that name " +
                "defined in inner CTE takes precedence. If set it to LEGACY, outer CTE " +
                "definitions will take precedence. See more details in SPARK-28228.")
            } else {
              cteName
            }
        }.toSet
        child.transformExpressions {
          case e: SubqueryExpression =>
            assertNoNameConflictsInCTE(e.plan, inTraverse = true, cteNames ++ newNames)
            e
        }
        w.innerChildren.foreach { p =>
          assertNoNameConflictsInCTE(p, inTraverse = true, cteNames ++ newNames)
        }

      case other if inTraverse =>
        other.transformExpressions {
          case e: SubqueryExpression =>
            assertNoNameConflictsInCTE(e.plan, inTraverse = true, cteNames)
            e
        }

      case _ =>
    }
  }

  private def legacyTraverseAndSubstituteCTE(plan: LogicalPlan): LogicalPlan = {
    plan.resolveOperatorsUp {
      case With(child, relations) =>
        // substitute CTE expressions right-to-left to resolve references to previous CTEs:
        // with a as (select * from t), b as (select * from a) select * from b
        relations.foldRight(child) {
          case ((cteName, ctePlan), currentPlan) => substituteCTE(currentPlan, cteName, ctePlan)
        }
    }
  }

  /**
   * Traverse the plan and expression nodes as a tree and replace matching references to CTE
   * definitions.
   * - If the rule encounters a WITH node then it substitutes the child of the node with CTE
   *   definitions of the node right-to-left order as a definition can reference to a previous
   *   one.
   *   For example the following query is valid:
   *   WITH
   *     t AS (SELECT 1),
   *     t2 AS (SELECT * FROM t)
   *   SELECT * FROM t2
   * - If a CTE definition contains an inner WITH node then substitution of inner should take
   *   precedence because it can shadow an outer CTE definition.
   *   For example the following query should return 2:
   *   WITH
   *     t AS (SELECT 1),
   *     t2 AS (
   *       WITH t AS (SELECT 2)
   *       SELECT * FROM t
   *     )
   *   SELECT * FROM t2
   * - If a CTE definition contains a subquery that contains an inner WITH node then substitution
   *   of inner should take precedence because it can shadow an outer CTE definition.
   *   For example the following query should return 2:
   *   WITH t AS (SELECT 1 AS c)
   *   SELECT max(c) FROM (
   *     WITH t AS (SELECT 2 AS c)
   *     SELECT * FROM t
   *   )
   * - If a CTE definition contains a subquery expression that contains an inner WITH node then
   *   substitution of inner should take precedence because it can shadow an outer CTE
   *   definition.
   *   For example the following query should return 2:
   *   WITH t AS (SELECT 1)
   *   SELECT (
   *     WITH t AS (SELECT 2)
   *     SELECT * FROM t
   *   )
   * @param plan the plan to be traversed
   * @param inTraverse whether the current traverse is called from another traverse, only in this
   *                   case name collision can occur
   * @return the plan where CTE substitution is applied
   */
  private def traverseAndSubstituteCTE(plan: LogicalPlan, inTraverse: Boolean): LogicalPlan = {
    plan.resolveOperatorsUp {
      case With(child: LogicalPlan, relations) =>
        // child might contain an inner CTE that has priority so traverse and substitute inner CTEs
        // in child first
        val traversedChild: LogicalPlan = child transformExpressions {
          case e: SubqueryExpression => e.withNewPlan(traverseAndSubstituteCTE(e.plan, true))
        }

        // Substitute CTE definitions from last to first as a CTE definition can reference a
        // previous one
        relations.foldRight(traversedChild) {
          case ((cteName, ctePlan), currentPlan) =>
            // A CTE definition might contain an inner CTE that has priority, so traverse and
            // substitute CTE defined in ctePlan.
            // A CTE definition might not be used at all or might be used multiple times. To avoid
            // computation if it is not used and to avoid multiple recomputation if it is used
            // multiple times we use a lazy construct with call-by-name parameter passing.
            lazy val substitutedCTEPlan = traverseAndSubstituteCTE(ctePlan, true)
            substituteCTE(currentPlan, cteName, substitutedCTEPlan)
        }

      // CTE name collision can occur only when inTraverse is true, it helps to avoid eager CTE
      // substitution in a subquery expression.
      case other if inTraverse =>
        other.transformExpressions {
          case e: SubqueryExpression => e.withNewPlan(traverseAndSubstituteCTE(e.plan, true))
        }
    }
  }

  private def substituteCTE(
      plan: LogicalPlan,
      cteName: String,
      ctePlan: => LogicalPlan): LogicalPlan =
    plan resolveOperatorsUp {
      case UnresolvedRelation(Seq(table)) if plan.conf.resolver(cteName, table) => ctePlan

      case other =>
        // This cannot be done in ResolveSubquery because ResolveSubquery does not know the CTE.
        other transformExpressions {
          case e: SubqueryExpression => e.withNewPlan(substituteCTE(e.plan, cteName, ctePlan))
        }
    }
}
