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

import scala.collection.mutable

import org.apache.spark.sql.AnalysisException
import org.apache.spark.sql.catalyst.expressions.SubqueryExpression
import org.apache.spark.sql.catalyst.plans.logical.{Distinct, Except, LogicalPlan, RecursiveRelation, SubqueryAlias, Union, With}
import org.apache.spark.sql.catalyst.plans.logical.{LogicalPlan, SubqueryAlias, With}
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
        assertNoNameConflictsInCTE(plan)
        traverseAndSubstituteCTE(plan)
      case LegacyBehaviorPolicy.LEGACY =>
        legacyTraverseAndSubstituteCTE(plan)
      case LegacyBehaviorPolicy.CORRECTED =>
        traverseAndSubstituteCTE(plan)
    }
  }

  /**
   * Spark 3.0 changes the CTE relations resolution, and inner relations take precedence. This is
   * correct but we need to warn users about this behavior change under EXCEPTION mode, when we see
   * CTE relations with conflicting names.
   *
   * Note that, before Spark 3.0 the parser didn't support CTE in the FROM clause. For example,
   * `WITH ... SELECT * FROM (WITH ... SELECT ...)` was not supported. We should not fail for this
   * case, as Spark versions before 3.0 can't run it anyway. The parameter `startOfQuery` is used
   * to indicate where we can define CTE relations before Spark 3.0, and we should only check
   * name conflicts when `startOfQuery` is true.
   */
  private def assertNoNameConflictsInCTE(
      plan: LogicalPlan,
      outerCTERelationNames: Seq[String] = Nil,
      startOfQuery: Boolean = true): Unit = {
    val resolver = SQLConf.get.resolver
    plan match {
      case With(child, relations, _) =>
        val newNames = mutable.ArrayBuffer.empty[String]
        newNames ++= outerCTERelationNames
        relations.foreach {
          case (name, relation) =>
            if (startOfQuery && outerCTERelationNames.exists(resolver(_, name))) {
              throw new AnalysisException(s"Name $name is ambiguous in nested CTE. " +
                s"Please set ${LEGACY_CTE_PRECEDENCE_POLICY.key} to CORRECTED so that name " +
                "defined in inner CTE takes precedence. If set it to LEGACY, outer CTE " +
                "definitions will take precedence. See more details in SPARK-28228.")
            }
            // CTE relation is defined as `SubqueryAlias`. Here we skip it and check the child
            // directly, so that `startOfQuery` is set correctly.
            assertNoNameConflictsInCTE(relation.child, newNames)
            newNames += name
        }
        assertNoNameConflictsInCTE(child, newNames, startOfQuery = false)

      case other =>
        other.subqueries.foreach(assertNoNameConflictsInCTE(_, outerCTERelationNames))
        other.children.foreach(
          assertNoNameConflictsInCTE(_, outerCTERelationNames, startOfQuery = false))
    }
  }

  private def legacyTraverseAndSubstituteCTE(plan: LogicalPlan): LogicalPlan = {
    plan.resolveOperatorsUp {
      case With(child, relations, _) =>
        val resolvedCTERelations = resolveCTERelations(relations, isLegacy = true)
        substituteCTE(child, resolvedCTERelations)
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
   * @return the plan where CTE substitution is applied
   */
  private def traverseAndSubstituteCTE(plan: LogicalPlan): LogicalPlan = {
    plan.resolveOperatorsUp {
      case With(child: LogicalPlan, relations, allowRecursion) =>
        val resolvedCTERelations = resolveCTERelations(relations, isLegacy = false, allowRecursion)
        substituteCTE(child, resolvedCTERelations)

      case other =>
        other.transformExpressions {
          case e: SubqueryExpression =>
            e.withNewPlan(traverseAndSubstituteCTE(e.plan))
        }
    }
  }

  private def resolveCTERelations(
      relations: Seq[(String, SubqueryAlias)],
      isLegacy: Boolean,
      allowRecursion: Boolean = false): Seq[(String, LogicalPlan)] = {
    val resolvedCTERelations = new mutable.ArrayBuffer[(String, LogicalPlan)](relations.size)
    for ((name, relation) <- relations) {
      val innerCTEResolved = if (isLegacy) {
        // In legacy mode, outer CTE relations take precedence. Here we don't resolve the inner
        // `With` nodes, later we will substitute `UnresolvedRelation`s with outer CTE relations.
        // Analyzer will run this rule multiple times until all `With` nodes are resolved.
        relation
      } else {
        // A CTE definition might contain an inner CTE that has a higher priority, so traverse and
        // substitute CTE defined in `relation` first.
        traverseAndSubstituteCTE(relation)
      }
      val recursionHandled = if (allowRecursion) {
        handleRecursion(innerCTEResolved, name)
      } else {
        innerCTEResolved
      }
      // CTE definition can reference a previous one
      resolvedCTERelations += (name -> substituteCTE(recursionHandled, resolvedCTERelations))
    }
    resolvedCTERelations
  }

  private def substituteCTE(
      plan: LogicalPlan,
      cteRelations: Seq[(String, LogicalPlan)]): LogicalPlan =
    plan resolveOperatorsUp {
      case u @ UnresolvedRelation(Seq(table)) =>
        cteRelations.find(r => SQLConf.get.resolver(r._1, table)).map(_._2).getOrElse(u)

      case other =>
        // This cannot be done in ResolveSubquery because ResolveSubquery does not know the CTE.
        other transformExpressions {
          case e: SubqueryExpression => e.withNewPlan(substituteCTE(e.plan, cteRelations))
        }
    }

  /**
   * If recursion is allowed, recursion handling starts with inserting unresolved self-references
   * ([[UnresolvedRecursiveReference]]) to places where a reference to the CTE definition itself is
   * found.
   * If there is a self-reference then we need to check if structure of the query satisfies the SQL
   * recursion rules and insert a [[RecursiveRelation]] finally.
   */
  private def handleRecursion(plan: LogicalPlan, cteName: String) = {
    // check if there is any reference to the CTE and if there is then treat the CTE as recursive
    val (recursiveReferencesPlan, recursiveReferenceCount) =
      insertRecursiveReferences(plan, cteName)
    if (recursiveReferenceCount > 0) {
      // if there is a reference then the CTE needs to follow one of these structures
      recursiveReferencesPlan match {
        case SubqueryAlias(_, u: Union) =>
          insertRecursiveRelation(cteName, Seq.empty, false, u)
        case SubqueryAlias(_, Distinct(u: Union)) =>
          insertRecursiveRelation(cteName, Seq.empty, true, u)
        case SubqueryAlias(_, UnresolvedSubqueryColumnAliases(columnNames, u: Union)) =>
          insertRecursiveRelation(cteName, columnNames, false, u)
        case SubqueryAlias(_, UnresolvedSubqueryColumnAliases(columnNames, Distinct(u: Union))) =>
          insertRecursiveRelation(cteName, columnNames, true, u)
        case _ =>
          throw new AnalysisException(s"Recursive query $cteName should contain UNION or UNION " +
            "ALL statements only. This error can also be caused by ORDER BY or LIMIT keywords " +
            "used on result of UNION or UNION ALL.")
      }
    } else {
      plan
    }
  }

  /**
   * If we encounter a relation that matches the recursive CTE then the relation is replaced to an
   * [[UnresolvedRecursiveReference]]. The replacement process also checks possible references in
   * subqueries and reports them as errors.
   */
  private def insertRecursiveReferences(plan: LogicalPlan, cteName: String): (LogicalPlan, Int) = {
    val resolver = SQLConf.get.resolver

    var recursiveReferenceCount = 0
    val newPlan = plan resolveOperators {
      case UnresolvedRelation(Seq(table)) if (resolver(cteName, table)) =>
        recursiveReferenceCount += 1
        UnresolvedRecursiveReference(cteName, false)

      case other =>
        other.subqueries.foreach(checkAndTraverse(_, {
          case UnresolvedRelation(Seq(table)) if resolver(cteName, table) =>
            throw new AnalysisException(s"Recursive query $cteName should not contain recursive " +
              "references in its subquery.")
          case _ => true
        }))
        other
    }

    (newPlan, recursiveReferenceCount)
  }

  private def insertRecursiveRelation(
      cteName: String,
      columnNames: Seq[String],
      distinct: Boolean,
      union: Union) = {
    if (union.children.size != 2) {
      throw new AnalysisException(s"Recursive query ${cteName} should contain one anchor term " +
        "and one recursive term connected with UNION or UNION ALL.")
    }

    val anchorTerm :: recursiveTerm :: Nil = union.children

    // The anchor term shouldn't contain a recursive reference that matches the name of the CTE,
    // except if it is nested under an other RecursiveRelation with the same name.
    checkAndTraverse(anchorTerm, {
      case UnresolvedRecursiveReference(name, _) if name == cteName =>
        throw new AnalysisException(s"Recursive query $cteName should not contain recursive " +
          "references in its anchor (first) term.")
      case RecursiveRelation(name, _, _) if name == cteName => false
      case _ => true
    })

    // The anchor term has a special role, its output column are aliased if required.
    val aliasedAnchorTerm = SubqueryAlias(cteName,
      if (columnNames.nonEmpty) {
        UnresolvedSubqueryColumnAliases(columnNames, anchorTerm)
      } else {
        anchorTerm
      }
    )

    // If UNION combinator is used between the terms we extend the anchor with a DISTINCT and the
    // recursive term with an EXCEPT clause and a reference to the so far accumulated result.
    if (distinct) {
      RecursiveRelation(cteName, Distinct(aliasedAnchorTerm),
        Except(recursiveTerm, UnresolvedRecursiveReference(cteName, true), false))
    } else {
      RecursiveRelation(cteName, aliasedAnchorTerm, recursiveTerm)
    }
  }

  /**
   * Taverses the plan including subqueries and run the check while it returns true.
   */
  private def checkAndTraverse(plan: LogicalPlan, check: LogicalPlan => Boolean): Unit = {
    if (check(plan)) {
      plan.children.foreach(checkAndTraverse(_, check))
      plan.subqueries.foreach(checkAndTraverse(_, check))
    }
  }
}
