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

import scala.collection.mutable.ArrayBuffer

import org.apache.spark.sql.catalyst.expressions.SubqueryExpression
import org.apache.spark.sql.catalyst.plans.logical.{Command, CTEInChildren, CTERelationDef, CTERelationRef, InsertIntoDir, LogicalPlan, ParsedStatement, SubqueryAlias, UnresolvedWith, WithCTE}
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.catalyst.trees.TreePattern._
import org.apache.spark.sql.errors.DataTypeErrors.toSQLId
import org.apache.spark.sql.errors.QueryCompilationErrors
import org.apache.spark.sql.internal.{LegacyBehaviorPolicy, SQLConf}
import org.apache.spark.sql.internal.SQLConf.LEGACY_CTE_PRECEDENCE_POLICY

/**
 * Analyze WITH nodes and substitute child plan with CTE references or CTE definitions depending
 * on the conditions below:
 * 1. If in legacy mode, replace with CTE definitions, i.e., inline CTEs.
 * 2. Otherwise, replace with CTE references `CTERelationRef`s. The decision to inline or not
 *    inline will be made later by the rule `InlineCTE` after query analysis.
 *
 * All the CTE definitions that are not inlined after this substitution will be grouped together
 * under one `WithCTE` node for each of the main query and the subqueries. Any of the main query
 * or the subqueries that do not contain CTEs or have had all CTEs inlined will obviously not have
 * any `WithCTE` nodes. If any though, the `WithCTE` node will be in the same place as where the
 * outermost `With` node once was.
 *
 * The CTE definitions in a `WithCTE` node are kept in the order of how they have been resolved.
 * That means the CTE definitions are guaranteed to be in topological order base on their
 * dependency for any valid CTE query (i.e., given CTE definitions A and B with B referencing A,
 * A is guaranteed to appear before B). Otherwise, it must be an invalid user query, and an
 * analysis exception will be thrown later by relation resolving rules.
 *
 * If the query is a SQL command or DML statement (extends `CTEInChildren`),
 * place `WithCTE` into their children.
 */
object CTESubstitution extends Rule[LogicalPlan] {
  def apply(plan: LogicalPlan): LogicalPlan = {
    if (!plan.containsPattern(UNRESOLVED_WITH)) {
      return plan
    }

    val commands = plan.collect {
      case c @ (_: Command | _: ParsedStatement | _: InsertIntoDir) => c
    }
    val forceInline = if (commands.length == 1) {
      if (conf.getConf(SQLConf.LEGACY_INLINE_CTE_IN_COMMANDS)) {
        // The legacy behavior always inlines the CTE relations for queries in commands.
        true
      } else {
        // If there is only one command and it's `CTEInChildren`, we can resolve
        // CTE normally and don't need to force inline.
        !commands.head.isInstanceOf[CTEInChildren]
      }
    } else if (commands.length > 1) {
      // This can happen with the multi-insert statement. We should fall back to
      // the legacy behavior.
      true
    } else {
      false
    }

    val cteDefs = ArrayBuffer.empty[CTERelationDef]
    val (substituted, firstSubstituted) =
      LegacyBehaviorPolicy.withName(conf.getConf(LEGACY_CTE_PRECEDENCE_POLICY)) match {
        case LegacyBehaviorPolicy.EXCEPTION =>
          assertNoNameConflictsInCTE(plan)
          traverseAndSubstituteCTE(plan, forceInline, Seq.empty, cteDefs)
        case LegacyBehaviorPolicy.LEGACY =>
          (legacyTraverseAndSubstituteCTE(plan, cteDefs), None)
        case LegacyBehaviorPolicy.CORRECTED =>
          traverseAndSubstituteCTE(plan, forceInline, Seq.empty, cteDefs)
    }
    if (cteDefs.isEmpty) {
      substituted
    } else if (substituted eq firstSubstituted.get) {
      withCTEDefs(substituted, cteDefs.toSeq)
    } else {
      var done = false
      substituted.resolveOperatorsWithPruning(_ => !done) {
        case p if p eq firstSubstituted.get =>
          // `firstSubstituted` is the parent of all other CTEs (if any).
          done = true
          withCTEDefs(p, cteDefs.toSeq)
        case p if p.children.count(_.containsPattern(CTE)) > 1 =>
          // This is the first common parent of all CTEs.
          done = true
          withCTEDefs(p, cteDefs.toSeq)
      }
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
    val resolver = conf.resolver
    plan match {
      case UnresolvedWith(child, relations, _) =>
        val newNames = ArrayBuffer.empty[String]
        newNames ++= outerCTERelationNames
        relations.foreach {
          case (name, relation) =>
            if (startOfQuery && outerCTERelationNames.exists(resolver(_, name))) {
              throw QueryCompilationErrors.ambiguousRelationAliasNameInNestedCTEError(name)
            }
            // CTE relation is defined as `SubqueryAlias`. Here we skip it and check the child
            // directly, so that `startOfQuery` is set correctly.
            assertNoNameConflictsInCTE(relation.child, newNames.toSeq)
            newNames += name
        }
        assertNoNameConflictsInCTE(child, newNames.toSeq, startOfQuery = false)

      case other =>
        other.subqueries.foreach(assertNoNameConflictsInCTE(_, outerCTERelationNames))
        other.children.foreach(
          assertNoNameConflictsInCTE(_, outerCTERelationNames, startOfQuery = false))
    }
  }

  private def legacyTraverseAndSubstituteCTE(
      plan: LogicalPlan,
      cteDefs: ArrayBuffer[CTERelationDef]): LogicalPlan = {
    plan.resolveOperatorsUp {
      case cte @ UnresolvedWith(child, relations, allowRecursion) =>
        if (allowRecursion) {
          cte.failAnalysis(
            errorClass = "RECURSIVE_CTE_IN_LEGACY_MODE",
            messageParameters = Map.empty)
        }
        val resolvedCTERelations = resolveCTERelations(relations, isLegacy = true,
          forceInline = false, Seq.empty, cteDefs, allowRecursion)
        substituteCTE(child, alwaysInline = true, resolvedCTERelations, None)
    }
  }

  /**
   * Traverse the plan and expression nodes as a tree and replace matching references with CTE
   * references if `isCommand` is false, otherwise with the query plans of the corresponding
   * CTE definitions.
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
   * @param forceInline always inline the CTE relations if this is true
   * @param outerCTEDefs already resolved outer CTE definitions with names
   * @param cteDefs all accumulated CTE definitions
   * @return the plan where CTE substitution is applied and optionally the last substituted `With`
   *         where CTE definitions will be gathered to
   */
  private def traverseAndSubstituteCTE(
      plan: LogicalPlan,
      forceInline: Boolean,
      outerCTEDefs: Seq[(String, CTERelationDef)],
      cteDefs: ArrayBuffer[CTERelationDef]): (LogicalPlan, Option[LogicalPlan]) = {
    var firstSubstituted: Option[LogicalPlan] = None
    val newPlan = plan.resolveOperatorsDownWithPruning(
        _.containsAnyPattern(UNRESOLVED_WITH, PLAN_EXPRESSION)) {
      // allowRecursion flag is set to `True` by the parser if the `RECURSIVE` keyword is used.
      case cte @ UnresolvedWith(child: LogicalPlan, relations, allowRecursion) =>
        if (allowRecursion && forceInline) {
          cte.failAnalysis(
            errorClass = "RECURSIVE_CTE_WHEN_INLINING_IS_FORCED",
            messageParameters = Map.empty)
        }
        val resolvedCTERelations =
          resolveCTERelations(relations, isLegacy = false, forceInline, outerCTEDefs, cteDefs,
            allowRecursion) ++ outerCTEDefs
        val substituted = substituteCTE(
          traverseAndSubstituteCTE(child, forceInline, resolvedCTERelations, cteDefs)._1,
          forceInline,
          resolvedCTERelations,
          None)
        if (firstSubstituted.isEmpty) {
          firstSubstituted = Some(substituted)
        }
        substituted

      case other =>
        other.transformExpressionsWithPruning(_.containsPattern(PLAN_EXPRESSION)) {
          case e: SubqueryExpression => e.withNewPlan(apply(e.plan))
        }
    }
    (newPlan, firstSubstituted)
  }

  private def resolveCTERelations(
      relations: Seq[(String, SubqueryAlias)],
      isLegacy: Boolean,
      forceInline: Boolean,
      outerCTEDefs: Seq[(String, CTERelationDef)],
      cteDefs: ArrayBuffer[CTERelationDef],
      allowRecursion: Boolean): Seq[(String, CTERelationDef)] = {
    val alwaysInline = isLegacy || forceInline
    var resolvedCTERelations = if (alwaysInline) {
      Seq.empty
    } else {
      outerCTEDefs
    }
    for ((name, relation) <- relations) {
      val innerCTEResolved = if (isLegacy) {
        // In legacy mode, outer CTE relations take precedence. Here we don't resolve the inner
        // `With` nodes, later we will substitute `UnresolvedRelation`s with outer CTE relations.
        // Analyzer will run this rule multiple times until all `With` nodes are resolved.
        relation
      } else {
        // A CTE definition might contain an inner CTE that has a higher priority, so traverse and
        // substitute CTE defined in `relation` first.
        // NOTE: we must call `traverseAndSubstituteCTE` before `substituteCTE`, as the relations
        // in the inner CTE have higher priority over the relations in the outer CTE when resolving
        // inner CTE relations. For example:
        // WITH
        //   t1 AS (SELECT 1),
        //   t2 AS (
        //     WITH
        //       t1 AS (SELECT 2),
        //       t3 AS (SELECT * FROM t1)
        //     SELECT * FROM t1
        //   )
        // SELECT * FROM t2
        // t3 should resolve the t1 to `SELECT 2` ("inner" t1) instead of `SELECT 1`.
        //
        // When recursion allowed (RECURSIVE keyword used):
        // Consider following example:
        //  WITH
        //    t1 AS (SELECT 1),
        //    t2 AS (
        //      WITH RECURSIVE
        //        t1 AS (
        //          SELECT 1 AS level
        //          UNION (
        //            WITH t3 AS (SELECT level + 1 FROM t1 WHERE level < 10)
        //            SELECT * FROM t3
        //          )
        //        )
        //      SELECT * FROM t1
        //    )
        //  SELECT * FROM t2
        // t1 reference within t3 would initially resolve to outer `t1` (SELECT 1), as the inner t1
        // is not yet known. Therefore, we need to remove definitions that conflict with current
        // relation `name` from the list of `outerCTEDefs` entering `traverseAndSubstituteCTE()`.
        // NOTE: It will be recognized later in the code that this is actually a self-reference
        // (reference to the inner t1).
        val nonConflictingCTERelations = if (allowRecursion) {
          resolvedCTERelations.filterNot {
            case (cteName, cteDef) => cteDef.conf.resolver(cteName, name)
          }
        } else {
          resolvedCTERelations
        }
        traverseAndSubstituteCTE(relation, forceInline, nonConflictingCTERelations, cteDefs)._1
      }

      // If recursion is allowed (RECURSIVE keyword specified)
      // then it has higher priority than outer or previous relations.
      // Therefore, we construct a `CTERelationDef` for the current relation.
      // Later if we encounter unresolved relation which we need to find which CTE Def it is
      // referencing to, we first check if it is a reference to this one. If yes, then we set the
      // reference as being recursive.
      val recursiveCTERelation = if (allowRecursion) {
        Some(name -> CTERelationDef(relation))
      } else {
        None
      }
      // CTE definition can reference a previous one or itself if recursion allowed.
      val substituted = substituteCTE(innerCTEResolved, alwaysInline,
        resolvedCTERelations, recursiveCTERelation)
      val cteRelation = CTERelationDef(substituted)
      if (!alwaysInline) {
        cteDefs += cteRelation
      }

      // Prepending new CTEs makes sure that those have higher priority over outer ones.
      resolvedCTERelations +:= (name -> cteRelation)
    }
    resolvedCTERelations
  }

  /**
   * This function is called from `substituteCTE` to actually substitute unresolved relations
   * with CTE references.
   */
  private def resolveWithCTERelations(
      table: String,
      alwaysInline: Boolean,
      cteRelations: Seq[(String, CTERelationDef)],
      recursiveCTERelation: Option[(String, CTERelationDef)],
      unresolvedRelation: UnresolvedRelation): LogicalPlan = {
    if (recursiveCTERelation.isDefined && conf.resolver(recursiveCTERelation.get._1, table)) {
      // self-reference is found
      recursiveCTERelation.map {
        case (_, d) =>
          SubqueryAlias(table,
            CTERelationRef(d.id, d.resolved, d.output, d.isStreaming, recursive = true))
      }.get
    } else {
      cteRelations
        .find(r => conf.resolver(r._1, table))
        .map {
          case (_, d) =>
            if (alwaysInline) {
              d.child
            } else {
              // Add a `SubqueryAlias` for hint-resolving rules to match relation names.
              // This is a non-recursive reference, recursive parameter is by default set to false
              SubqueryAlias(table,
                CTERelationRef(d.id, d.resolved, d.output, d.isStreaming))
            }
        }
        .getOrElse(unresolvedRelation)
    }
  }

  /**
   * Substitute unresolved relations in the plan with CTE references (CTERelationRef).
   */
  private def substituteCTE(
      plan: LogicalPlan,
      alwaysInline: Boolean,
      cteRelations: Seq[(String, CTERelationDef)],
      recursiveCTERelation: Option[(String, CTERelationDef)]): LogicalPlan = {
    plan.resolveOperatorsUpWithPruning(
        _.containsAnyPattern(RELATION_TIME_TRAVEL, UNRESOLVED_RELATION, PLAN_EXPRESSION,
          UNRESOLVED_IDENTIFIER)) {
      case RelationTimeTravel(UnresolvedRelation(Seq(table), _, _), _, _)
        if cteRelations.exists(r => plan.conf.resolver(r._1, table)) =>
        throw QueryCompilationErrors.timeTravelUnsupportedError(toSQLId(table))

      case u @ UnresolvedRelation(Seq(table), _, _) =>
        resolveWithCTERelations(table, alwaysInline, cteRelations,
          recursiveCTERelation, u)

      case p: PlanWithUnresolvedIdentifier =>
        // We must look up CTE relations first when resolving `UnresolvedRelation`s,
        // but we can't do it here as `PlanWithUnresolvedIdentifier` is a leaf node
        // and may produce `UnresolvedRelation` later. Instead, we delay CTE resolution
        // by moving it to the planBuilder of the corresponding `PlanWithUnresolvedIdentifier`.
        p.copy(planBuilder = (nameParts, children) => {
          p.planBuilder.apply(nameParts, children) match {
            case u @ UnresolvedRelation(Seq(table), _, _) =>
              resolveWithCTERelations(table, alwaysInline, cteRelations,
                recursiveCTERelation, u)
            case other => other
          }
        })

      case other =>
        // This cannot be done in ResolveSubquery because ResolveSubquery does not know the CTE.
        other.transformExpressionsWithPruning(_.containsPattern(PLAN_EXPRESSION)) {
          case e: SubqueryExpression =>
            e.withNewPlan(
              apply(substituteCTE(e.plan, alwaysInline, cteRelations, None)))
        }
    }
  }

  /**
   * For commands which extend `CTEInChildren`, we should place the `WithCTE` node on its
   * children. There are two reasons:
   *  1. Some rules will pattern match the root command nodes, and we should keep command
   *     as the root node to not break them.
   *  2. `Dataset` eagerly executes the commands inside a query plan. For example,
   *     sql("WITH v ... CREATE TABLE t AS SELECT * FROM v") will create the table instead of just
   *     analyzing the command. However, the CTE references inside commands will be invalid if we
   *     execute the command alone, as the CTE definitions are outside of the command.
   */
  private def withCTEDefs(p: LogicalPlan, cteDefs: Seq[CTERelationDef]): LogicalPlan = {
    p match {
      case c: CTEInChildren => c.withCTEDefs(cteDefs)
      case _ => WithCTE(p, cteDefs)
    }
  }
}
