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

import java.util.Locale

import scala.collection.mutable

import org.apache.spark.internal.Logging
import org.apache.spark.sql.AnalysisException
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.expressions.SubExprUtils.wrapOuterReference
import org.apache.spark.sql.catalyst.plans.logical._
import org.apache.spark.sql.catalyst.trees.CurrentOrigin.withOrigin
import org.apache.spark.sql.catalyst.trees.TreePattern._
import org.apache.spark.sql.catalyst.util.toPrettySQL
import org.apache.spark.sql.errors.{DataTypeErrorsBase, QueryCompilationErrors}
import org.apache.spark.sql.internal.SQLConf

trait ColumnResolutionHelper extends Logging with DataTypeErrorsBase {

  def conf: SQLConf

  /**
   * This method tries to resolve expressions and find missing attributes recursively.
   * Specifically, when the expressions used in `Sort` or `Filter` contain unresolved attributes
   * or resolved attributes which are missing from child output. This method tries to find the
   * missing attributes and add them into the projection.
   */
  protected def resolveExprsAndAddMissingAttrs(
      exprs: Seq[Expression], plan: LogicalPlan): (Seq[Expression], LogicalPlan) = {
    // Missing attributes can be unresolved attributes or resolved attributes which are not in
    // the output attributes of the plan.
    if (exprs.forall(e => e.resolved && e.references.subsetOf(plan.outputSet))) {
      (exprs, plan)
    } else {
      plan match {
        // For `Distinct` and `SubqueryAlias`, we can't recursively resolve and add attributes
        // via its children.
        case u: UnaryNode if !u.isInstanceOf[Distinct] && !u.isInstanceOf[SubqueryAlias] =>
          val (newExprs, newChild) = {
            // Resolving expressions against current plan.
            val maybeResolvedExprs = exprs.map(resolveExpressionByPlanOutput(_, u))
            // Recursively resolving expressions on the child of current plan.
            resolveExprsAndAddMissingAttrs(maybeResolvedExprs, u.child)
          }
          // If some attributes used by expressions are resolvable only on the rewritten child
          // plan, we need to add them into original projection.
          lazy val missingAttrs =
            (AttributeSet(newExprs) -- u.outputSet).intersect(newChild.outputSet)
          u match {
            case p: Project =>
              val newProject = Project(p.projectList ++ missingAttrs, newChild)
              newProject.copyTagsFrom(p)
              (newExprs, newProject)

            case a @ Aggregate(groupExprs, aggExprs, child) =>
              if (missingAttrs.forall(attr => groupExprs.exists(_.semanticEquals(attr)))) {
                // All the missing attributes are grouping expressions, valid case.
                (newExprs,
                  a.copy(aggregateExpressions = aggExprs ++ missingAttrs, child = newChild))
              } else {
                // Need to add non-grouping attributes, invalid case.
                (exprs, a)
              }

            case g: Generate =>
              (newExprs, g.copy(unrequiredChildIndex = Nil, child = newChild))

            case _ =>
              (newExprs, u.withNewChildren(Seq(newChild)))
          }
        // For other operators, we can't recursively resolve and add attributes via its children.
        case other =>
          (exprs.map(resolveExpressionByPlanOutput(_, other)), other)
      }
    }
  }

    // support CURRENT_DATE, CURRENT_TIMESTAMP, and grouping__id
  private val literalFunctions: Seq[(String, () => Expression, Expression => String)] = Seq(
    (CurrentDate().prettyName, () => CurrentDate(), toPrettySQL(_)),
    (CurrentTimestamp().prettyName, () => CurrentTimestamp(), toPrettySQL(_)),
    (CurrentUser().prettyName, () => CurrentUser(), toPrettySQL),
    ("user", () => CurrentUser(), toPrettySQL),
    (VirtualColumn.hiveGroupingIdName, () => GroupingID(Nil), _ => VirtualColumn.hiveGroupingIdName)
  )

  /**
   * Literal functions do not require the user to specify braces when calling them
   * When an attributes is not resolvable, we try to resolve it as a literal function.
   */
  private def resolveLiteralFunction(nameParts: Seq[String]): Option[NamedExpression] = {
    if (nameParts.length != 1) return None
    val name = nameParts.head
    literalFunctions.find(func => caseInsensitiveResolution(func._1, name)).map {
      case (_, getFuncExpr, getAliasName) =>
        val funcExpr = getFuncExpr()
        Alias(funcExpr, getAliasName(funcExpr))()
    }
  }

  /**
   * Resolves `UnresolvedAttribute`, `GetColumnByOrdinal` and extract value expressions(s) by
   * traversing the input expression in top-down manner. It must be top-down because we need to
   * skip over unbound lambda function expression. The lambda expressions are resolved in a
   * different place [[ResolveLambdaVariables]].
   *
   * Example :
   * SELECT transform(array(1, 2, 3), (x, i) -> x + i)"
   *
   * In the case above, x and i are resolved as lambda variables in [[ResolveLambdaVariables]].
   */
  private def resolveExpression(
      expr: Expression,
      resolveColumnByName: Seq[String] => Option[Expression],
      getAttrCandidates: () => Seq[Attribute],
      throws: Boolean,
      allowOuter: Boolean): Expression = {
    def innerResolve(e: Expression, isTopLevel: Boolean): Expression = withOrigin(e.origin) {
      if (e.resolved) return e
      val resolved = e match {
        case f: LambdaFunction if !f.bound => f

        case GetColumnByOrdinal(ordinal, _) =>
          val attrCandidates = getAttrCandidates()
          assert(ordinal >= 0 && ordinal < attrCandidates.length)
          attrCandidates(ordinal)

        case GetViewColumnByNameAndOrdinal(
            viewName, colName, ordinal, expectedNumCandidates, viewDDL) =>
          val attrCandidates = getAttrCandidates()
          val matched = attrCandidates.filter(a => conf.resolver(a.name, colName))
          if (matched.length != expectedNumCandidates) {
            throw QueryCompilationErrors.incompatibleViewSchemaChangeError(
              viewName, colName, expectedNumCandidates, matched, viewDDL)
          }
          matched(ordinal)

        case u @ UnresolvedAttribute(nameParts) =>
          val result = withPosition(u) {
            resolveColumnByName(nameParts).orElse(resolveLiteralFunction(nameParts)).map {
              // We trim unnecessary alias here. Note that, we cannot trim the alias at top-level,
              // as we should resolve `UnresolvedAttribute` to a named expression. The caller side
              // can trim the top-level alias if it's safe to do so. Since we will call
              // CleanupAliases later in Analyzer, trim non top-level unnecessary alias is safe.
              case Alias(child, _) if !isTopLevel => child
              case other => other
            }.getOrElse(u)
          }
          logDebug(s"Resolving $u to $result")
          result

        // Re-resolves `TempResolvedColumn` if it has tried to be resolved with Aggregate
        // but failed. If we still can't resolve it, we should keep it as `TempResolvedColumn`,
        // so that it won't become a fresh `TempResolvedColumn` again.
        case t: TempResolvedColumn if t.hasTried => withPosition(t) {
          innerResolve(UnresolvedAttribute(t.nameParts), isTopLevel) match {
            case _: UnresolvedAttribute => t
            case other => other
          }
        }

        case u @ UnresolvedExtractValue(child, fieldName) =>
          val newChild = innerResolve(child, isTopLevel = false)
          if (newChild.resolved) {
            ExtractValue(newChild, fieldName, conf.resolver)
          } else {
            u.copy(child = newChild)
          }

        case _ => e.mapChildren(innerResolve(_, isTopLevel = false))
      }
      resolved.copyTagsFrom(e)
      resolved
    }

    try {
      val resolved = innerResolve(expr, isTopLevel = true)
      if (allowOuter) resolveOuterRef(resolved) else resolved
    } catch {
      case ae: AnalysisException if !throws =>
        logDebug(ae.getMessage)
        expr
    }
  }

  // Resolves `UnresolvedAttribute` to `OuterReference`.
  protected def resolveOuterRef(e: Expression): Expression = {
    val outerPlan = AnalysisContext.get.outerPlan
    if (outerPlan.isEmpty) return e

    def resolve(nameParts: Seq[String]): Option[Expression] = try {
      outerPlan.get match {
        // Subqueries in UnresolvedHaving can host grouping expressions and aggregate functions.
        // We should resolve columns with `agg.output` and the rule `ResolveAggregateFunctions` will
        // push them down to Aggregate later. This is similar to what we do in `resolveColumns`.
        case u @ UnresolvedHaving(_, agg: Aggregate) =>
          agg.resolveChildren(nameParts, conf.resolver)
            .orElse(u.resolveChildren(nameParts, conf.resolver))
            .map(wrapOuterReference)
        case other =>
          other.resolveChildren(nameParts, conf.resolver).map(wrapOuterReference)
      }
    } catch {
      case ae: AnalysisException =>
        logDebug(ae.getMessage)
        None
    }

    e.transformWithPruning(_.containsAnyPattern(UNRESOLVED_ATTRIBUTE, TEMP_RESOLVED_COLUMN)) {
      case u: UnresolvedAttribute =>
        resolve(u.nameParts).getOrElse(u)
      // Re-resolves `TempResolvedColumn` as outer references if it has tried to be resolved with
      // Aggregate but failed.
      case t: TempResolvedColumn if t.hasTried =>
        resolve(t.nameParts).getOrElse(t)
    }
  }

  // Resolves `UnresolvedAttribute` to `TempResolvedColumn` via `plan.child.output` if plan is an
  // `Aggregate`. If `TempResolvedColumn` doesn't end up as aggregate function input or grouping
  // column, we will undo the column resolution later to avoid confusing error message. E,g,, if
  // a table `t` has columns `c1` and `c2`, for query `SELECT ... FROM t GROUP BY c1 HAVING c2 = 0`,
  // even though we can resolve column `c2` here, we should undo it and fail with
  // "Column c2 not found".
  protected def resolveColWithAgg(e: Expression, plan: LogicalPlan): Expression = plan match {
    case agg: Aggregate =>
      e.transformWithPruning(_.containsAnyPattern(UNRESOLVED_ATTRIBUTE)) {
        case u: UnresolvedAttribute =>
          try {
            agg.child.resolve(u.nameParts, conf.resolver).map({
              case a: Alias => TempResolvedColumn(a.child, u.nameParts)
              case o => TempResolvedColumn(o, u.nameParts)
            }).getOrElse(u)
          } catch {
            case ae: AnalysisException =>
              logDebug(ae.getMessage)
              u
          }
      }
    case _ => e
  }

  protected def resolveLateralColumnAlias(selectList: Seq[Expression]): Seq[Expression] = {
    if (!conf.getConf(SQLConf.LATERAL_COLUMN_ALIAS_IMPLICIT_ENABLED)) return selectList

    // A mapping from lower-cased alias name to either the Alias itself, or the count of aliases
    // that have the same lower-cased name. If the count is larger than 1, we won't use it to
    // resolve lateral column aliases.
    val aliasMap = mutable.HashMap.empty[String, Either[Alias, Int]]

    def resolve(e: Expression): Expression = {
      e.transformUpWithPruning(
        _.containsAnyPattern(UNRESOLVED_ATTRIBUTE, LATERAL_COLUMN_ALIAS_REFERENCE)) {
        case w: WindowExpression if w.containsPattern(LATERAL_COLUMN_ALIAS_REFERENCE) =>
          w.transformDownWithPruning(_.containsPattern(LATERAL_COLUMN_ALIAS_REFERENCE)) {
            case lcaRef: LateralColumnAliasReference =>
              throw QueryCompilationErrors.lateralColumnAliasInWindowUnsupportedError(
                lcaRef.nameParts, w)
          }

        case u: UnresolvedAttribute =>
          // Lateral column alias does not have qualifiers. We always use the first name part to
          // look up lateral column aliases.
          val lowerCasedName = u.nameParts.head.toLowerCase(Locale.ROOT)
          aliasMap.get(lowerCasedName).map {
            case scala.util.Left(alias) =>
              if (alias.resolved) {
                val resolvedAttr = resolveExpressionByPlanOutput(
                  u, LocalRelation(Seq(alias.toAttribute)), throws = true
                ).asInstanceOf[NamedExpression]
                assert(resolvedAttr.resolved)
                LateralColumnAliasReference(resolvedAttr, u.nameParts, alias.toAttribute)
              } else {
                // Still returns a `LateralColumnAliasReference` even if the lateral column alias
                // is not resolved yet. This is to make sure we won't mistakenly resolve it to
                // outer references.
                LateralColumnAliasReference(u, u.nameParts, alias.toAttribute)
              }
            case scala.util.Right(count) =>
              throw QueryCompilationErrors.ambiguousLateralColumnAliasError(u.name, count)
          }.getOrElse(u)

        case LateralColumnAliasReference(u: UnresolvedAttribute, _, _) =>
          resolve(u)
      }
    }

    selectList.map {
      case a: Alias =>
        val result = resolve(a)
        val lowerCasedName = a.name.toLowerCase(Locale.ROOT)
        aliasMap.get(lowerCasedName) match {
          case Some(scala.util.Left(_)) =>
            aliasMap(lowerCasedName) = scala.util.Right(2)
          case Some(scala.util.Right(count)) =>
            aliasMap(lowerCasedName) = scala.util.Right(count + 1)
          case None =>
            aliasMap += lowerCasedName -> scala.util.Left(a)
        }
        result
      case other => resolve(other)
    }
  }

  /**
   * Resolves `UnresolvedAttribute`, `GetColumnByOrdinal` and extract value expressions(s) by the
   * input plan's output attributes. In order to resolve the nested fields correctly, this function
   * makes use of `throws` parameter to control when to raise an AnalysisException.
   *
   * Example :
   * SELECT * FROM t ORDER BY a.b
   *
   * In the above example, after `a` is resolved to a struct-type column, we may fail to resolve `b`
   * if there is no such nested field named "b". We should not fail and wait for other rules to
   * resolve it if possible.
   */
  def resolveExpressionByPlanOutput(
      expr: Expression,
      plan: LogicalPlan,
      throws: Boolean = false,
      allowOuter: Boolean = false): Expression = {
    resolveExpression(
      tryResolveColumnByPlanId(expr, plan),
      resolveColumnByName = nameParts => {
        plan.resolve(nameParts, conf.resolver)
      },
      getAttrCandidates = () => plan.output,
      throws = throws,
      allowOuter = allowOuter)
  }

  /**
   * Resolves `UnresolvedAttribute`, `GetColumnByOrdinal` and extract value expressions(s) by the
   * input plan's children output attributes.
   *
   * @param e The expression need to be resolved.
   * @param q The LogicalPlan whose children are used to resolve expression's attribute.
   * @return resolved Expression.
   */
  def resolveExpressionByPlanChildren(
      e: Expression,
      q: LogicalPlan,
      allowOuter: Boolean = false): Expression = {
    resolveExpression(
      tryResolveColumnByPlanId(e, q),
      resolveColumnByName = nameParts => {
        q.resolveChildren(nameParts, conf.resolver)
      },
      getAttrCandidates = () => {
        assert(q.children.length == 1)
        q.children.head.output
      },
      throws = true,
      allowOuter = allowOuter)
  }

  def resolveExprInAssignment(expr: Expression, hostPlan: LogicalPlan): Expression = {
    resolveExpressionByPlanChildren(expr, hostPlan) match {
      // Assignment key and value does not need the alias when resolving nested columns.
      case Alias(child: ExtractValue, _) => child
      case other => other
    }
  }

  // If the TreeNodeTag 'LogicalPlan.PLAN_ID_TAG' is attached, it means that the plan and
  // expression are from Spark Connect, and need to be resolved in this way:
  //    1. extract the attached plan id from UnresolvedAttribute;
  //    2. top-down traverse the query plan to find the plan node that matches the plan id;
  //    3. if can not find the matching node, fail the analysis due to illegal references;
  //    4. if more than one matching nodes are found, fail due to ambiguous column reference;
  //    5. resolve the expression with the matching node, if any error occurs here, return the
  //       original expression as it is.
  private def tryResolveColumnByPlanId(
      e: Expression,
      q: LogicalPlan,
      idToPlan: mutable.HashMap[Long, LogicalPlan] = mutable.HashMap.empty): Expression = e match {
    case u: UnresolvedAttribute =>
      resolveUnresolvedAttributeByPlanId(
        u, q, idToPlan: mutable.HashMap[Long, LogicalPlan]
      ).getOrElse(u)
    case _ if e.containsPattern(UNRESOLVED_ATTRIBUTE) =>
      e.mapChildren(c => tryResolveColumnByPlanId(c, q, idToPlan))
    case _ => e
  }

  private def resolveUnresolvedAttributeByPlanId(
      u: UnresolvedAttribute,
      q: LogicalPlan,
      idToPlan: mutable.HashMap[Long, LogicalPlan]): Option[NamedExpression] = {
    val planIdOpt = u.getTagValue(LogicalPlan.PLAN_ID_TAG)
    if (planIdOpt.isEmpty) return None
    val planId = planIdOpt.get
    logDebug(s"Extract plan_id $planId from $u")

    val plan = idToPlan.getOrElseUpdate(planId, {
      findPlanById(u, planId, q).getOrElse {
        // For example:
        //  df1 = spark.createDataFrame([Row(a = 1, b = 2, c = 3)]])
        //  df2 = spark.createDataFrame([Row(a = 1, b = 2)]])
        //  df1.select(df2.a)   <-   illegal reference df2.a
        throw new AnalysisException(s"When resolving $u, " +
          s"fail to find subplan with plan_id=$planId in $q")
      }
    })

    try {
      plan.resolve(u.nameParts, conf.resolver)
    } catch {
      case e: AnalysisException =>
        logDebug(s"Fail to resolve $u with $plan due to $e")
        None
    }
  }

  private def findPlanById(
      u: UnresolvedAttribute,
      id: Long,
      plan: LogicalPlan): Option[LogicalPlan] = {
    if (plan.getTagValue(LogicalPlan.PLAN_ID_TAG).contains(id)) {
      Some(plan)
    } else if (plan.children.length == 1) {
      findPlanById(u, id, plan.children.head)
    } else if (plan.children.length > 1) {
      val matched = plan.children.flatMap(findPlanById(u, id, _))
      if (matched.length > 1) {
        throw new AnalysisException(
          errorClass = "AMBIGUOUS_COLUMN_REFERENCE",
          messageParameters = Map("name" -> toSQLId(u.nameParts)),
          origin = u.origin
        )
      } else {
        matched.headOption
      }
    } else {
      None
    }
  }
}
