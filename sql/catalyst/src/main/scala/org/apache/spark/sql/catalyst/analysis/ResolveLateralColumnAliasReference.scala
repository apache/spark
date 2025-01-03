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

import java.util.LinkedHashSet

import scala.jdk.CollectionConverters._

import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.expressions.WindowExpression.hasWindowExpression
import org.apache.spark.sql.catalyst.expressions.aggregate.AggregateExpression
import org.apache.spark.sql.catalyst.plans.logical.{Aggregate, LogicalPlan, Project}
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.catalyst.trees.TreePattern.{LATERAL_COLUMN_ALIAS_REFERENCE, TEMP_RESOLVED_COLUMN}
import org.apache.spark.sql.catalyst.util.toPrettySQL
import org.apache.spark.sql.errors.QueryCompilationErrors
import org.apache.spark.sql.internal.SQLConf

// scalastyle:off line.size.limit
/**
 * This rule is the second phase to resolve lateral column alias.
 *
 * Resolve lateral column alias, which references the alias defined previously in the SELECT list.
 * Plan-wise, it handles two types of operators: Project and Aggregate.
 * - in Project, pushing down the referenced lateral alias into a newly created Project, resolve
 *   the attributes referencing these aliases
 * - in Aggregate, inserting the Project node above and falling back to the resolution of Project.
 *
 * The whole process is generally divided into two phases:
 * 1) recognize resolved lateral alias, wrap the attributes referencing them with
 *    [[LateralColumnAliasReference]]
 * 2) when the whole operator is resolved, or contains [[Window]] but have all other resolved,
 *    For Project, it unwrap [[LateralColumnAliasReference]], further resolves the attributes and
 *    push down the referenced lateral aliases.
 *    For Aggregate, it goes through the whole aggregation list, extracts the aggregation
 *    expressions and grouping expressions to keep them in this Aggregate node, and add a Project
 *    above with the original output. It doesn't do anything on [[LateralColumnAliasReference]], but
 *    completely leave it to the Project in the future turns of this rule.
 *
 * ** Example for Project:
 * Before rewrite:
 * Project [age AS a, 'a + 1]
 * +- Child
 *
 * After phase 1:
 * Project [age AS a, lca(a) + 1]
 * +- Child
 *
 * After phase 2:
 * Project [a, a + 1]
 * +- Project [child output, age AS a]
 *    +- Child
 *
 *
 * ** Example for Aggregate:
 * Before rewrite:
 * Aggregate [dept#14] [dept#14 AS a#12, 'a + 1, avg(salary#16) AS b#13, 'b + avg(bonus#17)]
 * +- Child [dept#14,name#15,salary#16,bonus#17]
 *
 * After phase 1:
 * Aggregate [dept#14] [dept#14 AS a#12, lca(a) + 1, avg(salary#16) AS b#13, lca(b) + avg(bonus#17)]
 * +- Child [dept#14,name#15,salary#16,bonus#17]
 *
 * After phase 2:
 * Project [dept#14 AS a#12, lca(a) + 1, avg(salary)#26 AS b#13, lca(b) + avg(bonus)#27]
 * +- Aggregate [dept#14] [avg(salary#16) AS avg(salary)#26, avg(bonus#17) AS avg(bonus)#27,dept#14]
 *    +- Child [dept#14,name#15,salary#16,bonus#17]
 *
 * Now the problem falls back to the lateral alias resolution in Project.
 * After future rounds of this rule:
 * Project [a#12, a#12 + 1, b#13, b#13 + avg(bonus)#27]
 * +- Project [dept#14 AS a#12, avg(salary)#26 AS b#13]
 *    +- Aggregate [dept#14] [avg(salary#16) AS avg(salary)#26, avg(bonus#17) AS avg(bonus)#27, dept#14]
 *       +- Child [dept#14,name#15,salary#16,bonus#17]
 *
 *
 * ** Example for Window:
 * Query:
 * select dept as d, sum(salary) as s, avg(s) over (partition by s order by d) as avg from employee group by dept
 *
 * After phase 1:
 * 'Aggregate [dept#17], [dept#17 AS d#15, sum(salary#19) AS s#16L, avg(lca(s#16L)) windowspecdefinition(lca(s#16L), lca(d#15) ASC NULLS FIRST, specifiedwindowframe(..)) AS avg#25]
 *  +- Relation spark_catalog.default.employee[dept#17,name#18,salary#19,bonus#20,properties#21]
 * It is similar to a regular Aggregate. All expressions in it are resolved, but itself is
 * unresolved due to the Window expression. The rule allows appliction on this case.
 *
 * After phase 2:
 * 'Project [dept#17 AS d#15, sum(salary)#26L AS s#16L, avg(lca(s#16L)) windowspecdefinition(lca(s#16L), lca(d#15) ASC NULLS FIRST, specifiedwindowframe(..)) AS avg#25]
 * +- Aggregate [dept#17], [dept#17, sum(salary#19) AS sum(salary)#26L]
 *    +- Relation spark_catalog.default.employee[dept#17,name#18,salary#19,bonus#20,properties#21]
 * Same as Aggregate, it extracts grouping expressions and aggregate functions. Window expressions
 * are completely lifted up to upper Project, free from the current Aggregate.
 *
 * Then this rule will apply on the Project, adding another Project below.
 * Till this phase, all lateral column alias references have been resolved and removed.
 * Finally, rule [[ExtractWindowExpressions]] will apply on the top Project with window expressions.
 * It is guaranteed that [[ResolveLateralColumnAliasReference]] is applied before
 * [[ExtractWindowExpressions]].
 */
// scalastyle:on line.size.limit
object ResolveLateralColumnAliasReference extends Rule[LogicalPlan] {
  case class AliasEntry(alias: Alias, index: Int)

  private def assignAlias(expr: Expression): NamedExpression = {
    expr match {
      case ne: NamedExpression => ne
      case e => Alias(e, toPrettySQL(e))()
    }
  }

  /**
   * Check if the rule is applicable on operator [[p]].
   * It should satisfy one of the following conditions:
   * - operator [[p]] is resolved
   * - [[pList]] of operator [[p]] contains WindowExpression, but all expressions of [[p]] are
   *   resolved, and the children of [[p]] are resolved.
   */
  private def ruleApplicableOnOperator(p: LogicalPlan, pList: Seq[NamedExpression]): Boolean = {
    p.resolved ||
      (pList.exists(hasWindowExpression) && p.expressions.forall(_.resolved) && p.childrenResolved)
  }

  /** Internal application method. A hand-written bottom-up recursive traverse. */
  private def apply0(plan: LogicalPlan): LogicalPlan = {
    plan match {
      case p: LogicalPlan if !p.containsPattern(LATERAL_COLUMN_ALIAS_REFERENCE) =>
        p

      // It should not change the Aggregate (and thus the plan shape) if its parent is an
      // UnresolvedHaving, to avoid breaking the shape pattern `UnresolvedHaving - Aggregate`
      // matched by ResolveAggregateFunctions. See SPARK-42936 and SPARK-44714 for more details.
      case u @ UnresolvedHaving(_, agg: Aggregate) =>
        u.copy(child = agg.mapChildren(apply0))

      case pOriginal: Project if ruleApplicableOnOperator(pOriginal, pOriginal.projectList)
          && pOriginal.projectList.exists(_.containsPattern(LATERAL_COLUMN_ALIAS_REFERENCE)) =>
        val p @ Project(projectList, child) = pOriginal.mapChildren(apply0)
        var aliasMap = AttributeMap.empty[AliasEntry]
        val referencedAliases = new LinkedHashSet[AliasEntry]
        def unwrapLCAReference(e: NamedExpression): NamedExpression = {
          e.transformWithPruning(_.containsPattern(LATERAL_COLUMN_ALIAS_REFERENCE)) {
            case lcaRef: LateralColumnAliasReference if aliasMap.contains(lcaRef.a) =>
              val aliasEntry = aliasMap.get(lcaRef.a).get
              // If there is no chaining of lateral column alias reference, push down the alias
              // and unwrap the LateralColumnAliasReference to the NamedExpression inside
              // If there is chaining, don't resolve and save to future rounds
              if (!aliasEntry.alias.containsPattern(LATERAL_COLUMN_ALIAS_REFERENCE)) {
                referencedAliases.add(aliasEntry)
                lcaRef.ne
              } else {
                lcaRef
              }
            case lcaRef: LateralColumnAliasReference if !aliasMap.contains(lcaRef.a) =>
              // It shouldn't happen, but restore to unresolved attribute to be safe.
              UnresolvedAttribute(lcaRef.nameParts)
          }.asInstanceOf[NamedExpression]
        }
        val newProjectList = projectList.zipWithIndex.map {
          case (a: Alias, idx) =>
            val lcaResolved = unwrapLCAReference(a)
            // Insert the original alias instead of rewritten one to detect chained LCA
            aliasMap += (a.toAttribute -> AliasEntry(a, idx))
            lcaResolved
          case (e, _) =>
            unwrapLCAReference(e)
        }

        if (referencedAliases.isEmpty) {
          p
        } else {
          val outerProjectList = collection.mutable.Seq(newProjectList: _*)
          val innerProjectList =
            collection.mutable.ArrayBuffer(child.output.map(_.asInstanceOf[NamedExpression]): _*)
          referencedAliases.forEach { case AliasEntry(alias: Alias, idx) =>
            outerProjectList.update(idx, alias.toAttribute)
            innerProjectList += alias
          }
          p.copy(
            projectList = outerProjectList.toSeq,
            child = Project(innerProjectList.toSeq, child)
          )
        }

      case aggOriginal: Aggregate
        if ruleApplicableOnOperator(aggOriginal, aggOriginal.aggregateExpressions)
          && aggOriginal.aggregateExpressions.exists(
            _.containsPattern(LATERAL_COLUMN_ALIAS_REFERENCE)) =>
        val agg @ Aggregate(groupingExpressions, aggregateExpressions, _, _) =
          aggOriginal.mapChildren(apply0)

        // Check if current Aggregate is eligible to lift up with Project: the aggregate
        // expression only contains: 1) aggregate functions, 2) grouping expressions, 3) leaf
        // expressions excluding attributes not in grouping expressions
        // This check is to prevent unnecessary transformation on invalid plan, to guarantee it
        // throws the same exception. For example, cases like non-aggregate expressions not
        // in group by, once transformed, will throw a different exception: missing input.
        def eligibleToLiftUp(exp: Expression): Boolean = {
          exp match {
            case _: AggregateExpression => true
            case e if groupingExpressions.exists(_.semanticEquals(e)) => true
            case a: Attribute => false
            case s: ScalarSubquery if s.children.nonEmpty
              && !groupingExpressions.exists(_.semanticEquals(s)) => false
            // Manually skip detection on function itself because it can be an aggregate function.
            // This is to avoid expressions like sum(salary) over () eligible to lift up.
            case WindowExpression(function, spec) =>
              function.children.forall(eligibleToLiftUp) && eligibleToLiftUp(spec)
            case e => e.children.forall(eligibleToLiftUp)
          }
        }
        if (!aggregateExpressions.forall(eligibleToLiftUp)) {
          agg
        } else {
          val newAggExprs = new LinkedHashSet[NamedExpression]
          val expressionMap = collection.mutable.LinkedHashMap.empty[Expression, NamedExpression]
          // Extract the expressions to keep in the Aggregate. Return the transformed expression
          // fully substituted with the attribute reference to the extracted expressions.
          def extractExpressions(expr: Expression): Expression = {
            expr match {
              case w @ WindowExpression(function, spec) =>
                // Manually skip the handling on the function itself, iterate on its children
                // instead. This is because WindowExpression.windowFunction can be an
                // [[AggregateExpression]], but we don't want to extract it to the below Aggregate.
                // For example, for WindowExpression
                // `sum(sum(col1)) over (partition by .. order by ..)`, we want to avoid extracting
                // the whole windowFunction `sum(sum(col1))`, but to extract its child `sum(col1)`
                // instead.
                w.copy(
                  windowFunction = function.mapChildren(extractExpressions),
                  windowSpec = extractExpressions(spec).asInstanceOf[WindowSpecDefinition])
              case aggExpr: AggregateExpression =>
                // Doesn't support referencing a lateral alias in aggregate function
                if (aggExpr.containsPattern(LATERAL_COLUMN_ALIAS_REFERENCE)) {
                  aggExpr.collectFirst {
                    case lcaRef: LateralColumnAliasReference =>
                      throw QueryCompilationErrors.lateralColumnAliasInAggFuncUnsupportedError(
                        lcaRef.nameParts, aggExpr)
                  }
                }
                val ne = expressionMap.getOrElseUpdate(aggExpr.canonicalized, assignAlias(aggExpr))
                newAggExprs.add(ne)
                ne.toAttribute
              case e if groupingExpressions.exists(_.semanticEquals(e)) =>
                val ne = expressionMap.getOrElseUpdate(e.canonicalized, assignAlias(e))
                newAggExprs.add(ne)
                ne.toAttribute
              case e => e.mapChildren(extractExpressions)
            }
          }
          val projectExprs = aggregateExpressions.map(
            extractExpressions(_).asInstanceOf[NamedExpression])
          Project(
            projectList = projectExprs,
            child = agg.copy(aggregateExpressions = newAggExprs.asScala.toSeq)
          )
        }

      case p: LogicalPlan =>
        p.mapChildren(apply0)
    }
  }

  override def apply(plan: LogicalPlan): LogicalPlan = {
    if (!conf.getConf(SQLConf.LATERAL_COLUMN_ALIAS_IMPLICIT_ENABLED)) {
      plan
    } else if (plan.containsAnyPattern(TEMP_RESOLVED_COLUMN)) {
      // It should not change the plan if `TempResolvedColumn` is present in the query plan. These
      // plans need certain plan shape to get recognized and resolved by other rules, such as
      // Filter/Sort + Aggregate to be matched by ResolveAggregateFunctions. LCA resolution can
      // break the plan shape, like adding Project above Aggregate.
      // TODO: this condition only guarantees to keep the shape after the plan has
      //  `TempResolvedColumn`. However, it does not consider the case of breaking the shape even
      //  before `TempResolvedColumn` is generated by matching Filter/Sort - Aggregate in
      //  ResolveReferences. Currently the correctness of this case now relies on the rule
      //  application order, that ResolveReference is right before the application of
      //  ResolveLateralColumnAliasReference. The condition in the two rules guarantees that the
      //  case can never happen. We should consider to remove this order dependency but still assure
      //  correctness in the future.
      plan
    } else {
      // phase 2: unwrap
      apply0(plan)
    }
  }
}
