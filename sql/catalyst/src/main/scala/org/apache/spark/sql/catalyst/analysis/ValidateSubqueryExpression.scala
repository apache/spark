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

import org.apache.spark.internal.{Logging, LogKeys, MDC}
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.expressions.SubExprUtils._
import org.apache.spark.sql.catalyst.expressions.aggregate.AggregateExpression
import org.apache.spark.sql.catalyst.optimizer.{BooleanSimplification, DecorrelateInnerQuery}
import org.apache.spark.sql.catalyst.plans._
import org.apache.spark.sql.catalyst.plans.logical._
import org.apache.spark.sql.errors.{QueryCompilationErrors, QueryErrorsBase}
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.util.Utils

object ValidateSubqueryExpression
    extends PredicateHelper with QueryErrorsBase with PlanToString with Logging {

  /**
   * Validates subquery expressions in the plan. Upon failure, returns an user facing error.
   */
  def apply(plan: LogicalPlan, expr: SubqueryExpression): Unit = {
    def checkAggregateInScalarSubquery(
        conditions: Seq[Expression],
        query: LogicalPlan, agg: Aggregate): Unit = {
      // Make sure correlated scalar subqueries contain one row for every outer row by
      // enforcing that they are aggregates containing exactly one aggregate expression.
      val aggregates = agg.expressions.flatMap(_.collect {
        case a: AggregateExpression => a
      })
      if (aggregates.isEmpty) {
        expr.failAnalysis(
          errorClass = "UNSUPPORTED_SUBQUERY_EXPRESSION_CATEGORY." +
            "MUST_AGGREGATE_CORRELATED_SCALAR_SUBQUERY",
          messageParameters = Map.empty)
      }

      val nonEquivalentGroupByExprs = nonEquivalentGroupbyCols(query, agg)
      val invalidCols = if (!SQLConf.get.getConf(
        SQLConf.LEGACY_SCALAR_SUBQUERY_ALLOW_GROUP_BY_NON_EQUALITY_CORRELATED_PREDICATE)) {
        nonEquivalentGroupByExprs
      } else {
        // Legacy incorrect logic for checking for invalid group-by columns (see SPARK-48503).
        // Allows any inner attribute that appears in a correlated predicate, even if it is a
        // non-equality predicate or under an operator that can change the values of the attribute
        // (see comments on getCorrelatedEquivalentInnerColumns for examples).
        // Note: groupByCols does not contain outer refs - grouping by an outer ref is always ok
        val groupByCols = AttributeSet(agg.groupingExpressions.flatMap(_.references))
        val subqueryColumns = getCorrelatedPredicates(query).flatMap(_.references)
          .filterNot(conditions.flatMap(_.references).contains)
        val correlatedCols = AttributeSet(subqueryColumns)
        val invalidColsLegacy = groupByCols -- correlatedCols
        if (!nonEquivalentGroupByExprs.isEmpty && invalidColsLegacy.isEmpty) {
          logWarning(log"Using legacy behavior for " +
            log"${MDC(LogKeys.CONFIG, SQLConf
            .LEGACY_SCALAR_SUBQUERY_ALLOW_GROUP_BY_NON_EQUALITY_CORRELATED_PREDICATE.key)}. " +
            log"Query would be rejected with non-legacy behavior but is allowed by " +
            log"legacy behavior. Query may be invalid and return wrong results if the scalar " +
            log"subquery's group-by outputs multiple rows.")
        }
        invalidColsLegacy
      }

      if (invalidCols.nonEmpty) {
        val names = invalidCols.map { el =>
          el match {
            case attr: Attribute => attr.name
            case expr: Expression => expr.toString
          }
        }
        expr.failAnalysis(
          errorClass = "UNSUPPORTED_SUBQUERY_EXPRESSION_CATEGORY." +
            "NON_CORRELATED_COLUMNS_IN_GROUP_BY",
          messageParameters = Map("value" -> names.mkString(",")))
      }
    }

    // Skip subquery aliases added by the Analyzer as well as hints.
    // For projects, do the necessary mapping and skip to its child.
    @scala.annotation.tailrec
    def cleanQueryInScalarSubquery(p: LogicalPlan): LogicalPlan = p match {
      case s: SubqueryAlias => cleanQueryInScalarSubquery(s.child)
      // Skip SQL function node added by the Analyzer
      case s: SQLFunctionNode => cleanQueryInScalarSubquery(s.child)
      case p: Project => cleanQueryInScalarSubquery(p.child)
      case h: ResolvedHint => cleanQueryInScalarSubquery(h.child)
      case child => child
    }

    // Check whether the given expressions contains the subquery expression.
    def containsExpr(expressions: Seq[Expression]): Boolean = {
      expressions.exists(_.exists(_.semanticEquals(expr)))
    }

    def checkOuterReference(p: LogicalPlan, expr: SubqueryExpression): Unit = p match {
      case f: Filter =>
        if (hasOuterReferences(expr.plan)) {
          expr.plan.expressions.foreach(_.foreachUp {
            case o: OuterReference =>
              p.children.foreach(e =>
                if (!e.output.exists(_.exprId == o.exprId)) {
                  o.failAnalysis(
                    errorClass = "UNSUPPORTED_SUBQUERY_EXPRESSION_CATEGORY." +
                      "CORRELATED_COLUMN_NOT_FOUND",
                    messageParameters = Map("value" -> o.name))
                })
            case _ =>
          })
        }
      case _ =>
    }

    // Check if there is outer attribute that cannot be found from the plan.
    checkOuterReference(plan, expr)

    expr match {
      case ScalarSubquery(query, outerAttrs, _, _, _, _, _) =>
        // Scalar subquery must return one column as output.
        if (query.output.size != 1) {
          throw QueryCompilationErrors.subqueryReturnMoreThanOneColumn(query.output.size,
            expr.origin)
        }

        if (outerAttrs.nonEmpty) {
          if (!SQLConf.get.getConf(SQLConf.SCALAR_SUBQUERY_USE_SINGLE_JOIN)) {
            cleanQueryInScalarSubquery(query) match {
              case a: Aggregate => checkAggregateInScalarSubquery(outerAttrs, query, a)
              case Filter(_, a: Aggregate) => checkAggregateInScalarSubquery(outerAttrs, query, a)
              case p: LogicalPlan if p.maxRows.exists(_ <= 1) => // Ok
              case other =>
                expr.failAnalysis(
                  errorClass = "UNSUPPORTED_SUBQUERY_EXPRESSION_CATEGORY." +
                    "MUST_AGGREGATE_CORRELATED_SCALAR_SUBQUERY",
                  messageParameters = Map.empty)
            }
          }

          // Only certain operators are allowed to host subquery expression containing
          // outer references.
          plan match {
            case _: Filter | _: Project | _: SupportsSubquery => // Ok
            case a: Aggregate =>
              // If the correlated scalar subquery is in the grouping expressions of an Aggregate,
              // it must also be in the aggregate expressions to be rewritten in the optimization
              // phase.
              if (containsExpr(a.groupingExpressions) && !containsExpr(a.aggregateExpressions)) {
                a.failAnalysis(
                  errorClass = "UNSUPPORTED_SUBQUERY_EXPRESSION_CATEGORY." +
                    "MUST_AGGREGATE_CORRELATED_SCALAR_SUBQUERY",
                  messageParameters = Map.empty)
              }
            case other =>
              other.failAnalysis(
                errorClass = "UNSUPPORTED_SUBQUERY_EXPRESSION_CATEGORY." +
                  "UNSUPPORTED_CORRELATED_SCALAR_SUBQUERY",
                messageParameters = Map("treeNode" -> planToString(other)))
          }
        }
        // Validate to make sure the correlations appearing in the query are valid and
        // allowed by spark.
        checkCorrelationsInSubquery(expr.plan, isScalar = true)

      case _: LateralSubquery =>
        assert(plan.isInstanceOf[LateralJoin])
        val join = plan.asInstanceOf[LateralJoin]
        // A lateral join with a multi-row outer query and a non-deterministic lateral subquery
        // cannot be decorrelated. Otherwise it may produce incorrect results.
        if (!expr.deterministic && !join.left.maxRows.exists(_ <= 1)) {
          cleanQueryInScalarSubquery(join.right.plan) match {
            // Python UDTFs are by default non-deterministic. They are constructed as a
            // OneRowRelation subquery and can be rewritten by the optimizer without
            // any decorrelation.
            case Generate(_: PythonUDTF, _, _, _, _, _: OneRowRelation)
              if SQLConf.get.getConf(SQLConf.OPTIMIZE_ONE_ROW_RELATION_SUBQUERY) =>  // Ok
            case _ =>
              expr.failAnalysis(
                errorClass = "UNSUPPORTED_SUBQUERY_EXPRESSION_CATEGORY." +
                  "NON_DETERMINISTIC_LATERAL_SUBQUERIES",
                messageParameters = Map("treeNode" -> planToString(plan)))
          }
        }
        // Check if the lateral join's join condition is deterministic.
        if (join.condition.exists(!_.deterministic)) {
          join.condition.get.failAnalysis(
            errorClass = "UNSUPPORTED_SUBQUERY_EXPRESSION_CATEGORY." +
              "LATERAL_JOIN_CONDITION_NON_DETERMINISTIC",
            messageParameters = Map("condition" -> join.condition.get.sql))
        }
        // Validate to make sure the correlations appearing in the query are valid and
        // allowed by spark.
        checkCorrelationsInSubquery(expr.plan, isLateral = true)

      case _: FunctionTableSubqueryArgumentExpression =>
        // Do nothing here, since we will check for this pattern later.

      case inSubqueryOrExistsSubquery =>
        plan match {
          case _: Filter | _: SupportsSubquery | _: Join |
            _: Project | _: Aggregate | _: Window => // Ok
          case _ =>
            expr.failAnalysis(
              errorClass =
                "UNSUPPORTED_SUBQUERY_EXPRESSION_CATEGORY.UNSUPPORTED_IN_EXISTS_SUBQUERY",
              messageParameters = Map("treeNode" -> planToString(plan)))
        }
        // Validate to make sure the correlations appearing in the query are valid and
        // allowed by spark.
        checkCorrelationsInSubquery(expr.plan)
    }
  }

  /**
   * Validates to make sure the outer references appearing inside the subquery
   * are allowed.
   */
  private def checkCorrelationsInSubquery(
      sub: LogicalPlan,
      isScalar: Boolean = false,
      isLateral: Boolean = false): Unit = {
    // Some query shapes are only supported with the DecorrelateInnerQuery framework.
    // Support for Exists and IN subqueries is subject to a separate config flag
    // 'decorrelateInnerQueryEnabledForExistsIn'.
    val usingDecorrelateInnerQueryFramework =
      (SQLConf.get.decorrelateInnerQueryEnabledForExistsIn || isScalar || isLateral) &&
        SQLConf.get.decorrelateInnerQueryEnabled

    // Validate that correlated aggregate expression do not contain a mixture
    // of outer and local references.
    def checkMixedReferencesInsideAggregateExpr(expr: Expression): Unit = {
      expr.foreach {
        case a: AggregateExpression if containsOuter(a) =>
          if (a.references.nonEmpty) {
            a.failAnalysis(
              errorClass = "UNSUPPORTED_SUBQUERY_EXPRESSION_CATEGORY." +
                "AGGREGATE_FUNCTION_MIXED_OUTER_LOCAL_REFERENCES",
              messageParameters = Map("function" -> a.sql))
          }
        case _ =>
      }
    }

    // Make sure expressions of a plan do not contain outer references.
    def failOnOuterReferenceInPlan(p: LogicalPlan): Unit = {
      if (p.expressions.exists(containsOuter)) {
        p.failAnalysis(
          errorClass = "UNSUPPORTED_SUBQUERY_EXPRESSION_CATEGORY." +
            "ACCESSING_OUTER_QUERY_COLUMN_IS_NOT_ALLOWED",
          messageParameters = Map("treeNode" -> planToString(p)))
      }
    }

    // Check whether the logical plan node can host outer references.
    // A `Project` can host outer references if it is inside a scalar or a lateral subquery and
    // DecorrelateInnerQuery is enabled. Otherwise, only Filter can only outer references.
    def canHostOuter(plan: LogicalPlan): Boolean = plan match {
      case _: Filter => true
      case _: Project => usingDecorrelateInnerQueryFramework
      case _: Join => usingDecorrelateInnerQueryFramework
      case _ => false
    }

    // Make sure a plan's expressions do not contain :
    // 1. Aggregate expressions that have mixture of outer and local references.
    // 2. Expressions containing outer references on plan nodes other than allowed operators.
    def failOnInvalidOuterReference(p: LogicalPlan): Unit = {
      p.expressions.foreach(checkMixedReferencesInsideAggregateExpr)
      val exprs = stripOuterReferences(p.expressions.filter(expr => containsOuter(expr)))
      if (!canHostOuter(p) && !exprs.isEmpty) {
        p.failAnalysis(
          errorClass =
            "UNSUPPORTED_SUBQUERY_EXPRESSION_CATEGORY.CORRELATED_REFERENCE",
          messageParameters = Map("sqlExprs" -> exprs.map(toSQLExpr).mkString(",")))
      }
    }

    // SPARK-17348: A potential incorrect result case.
    // When a correlated predicate is a non-equality predicate,
    // certain operators are not permitted from the operator
    // hosting the correlated predicate up to the operator on the outer table.
    // Otherwise, the pull up of the correlated predicate
    // will generate a plan with a different semantics
    // which could return incorrect result.
    // Currently we check for Aggregate and Window operators
    //
    // Below shows an example of a Logical Plan during Analyzer phase that
    // show this problem. Pulling the correlated predicate [outer(c2#77) >= ..]
    // through the Aggregate (or Window) operator could alter the result of
    // the Aggregate.
    //
    // Project [c1#76]
    // +- Project [c1#87, c2#88]
    // :  (Aggregate or Window operator)
    // :  +- Filter [outer(c2#77) >= c2#88)]
    // :     +- SubqueryAlias t2, `t2`
    // :        +- Project [_1#84 AS c1#87, _2#85 AS c2#88]
    // :           +- LocalRelation [_1#84, _2#85]
    // +- SubqueryAlias t1, `t1`
    // +- Project [_1#73 AS c1#76, _2#74 AS c2#77]
    // +- LocalRelation [_1#73, _2#74]
    // SPARK-35080: The same issue can happen to correlated equality predicates when
    // they do not guarantee one-to-one mapping between inner and outer attributes.
    // For example:
    // Table:
    //   t1(a, b): [(0, 6), (1, 5), (2, 4)]
    //   t2(c): [(6)]
    //
    // Query:
    //   SELECT c, (SELECT COUNT(*) FROM t1 WHERE a + b = c) FROM t2
    //
    // Original subquery plan:
    //   Aggregate [count(1)]
    //   +- Filter ((a + b) = outer(c))
    //      +- LocalRelation [a, b]
    //
    // Plan after pulling up correlated predicates:
    //   Aggregate [a, b] [count(1), a, b]
    //   +- LocalRelation [a, b]
    //
    // Plan after rewrite:
    //   Project [c1, count(1)]
    //   +- Join LeftOuter ((a + b) = c)
    //      :- LocalRelation [c]
    //      +- Aggregate [a, b] [count(1), a, b]
    //         +- LocalRelation [a, b]
    //
    // The right hand side of the join transformed from the subquery will output
    //   count(1) | a | b
    //      1     | 0 | 6
    //      1     | 1 | 5
    //      1     | 2 | 4
    // and the plan after rewrite will give the original query incorrect results.
    def failOnUnsupportedCorrelatedPredicate(predicates: Seq[Expression], p: LogicalPlan): Unit = {
      // Correlated non-equality predicates are only supported with the decorrelate
      // inner query framework. Currently we only use this new framework for scalar
      // and lateral subqueries.
      val allowNonEqualityPredicates = usingDecorrelateInnerQueryFramework
      if (!allowNonEqualityPredicates && predicates.nonEmpty) {
        // Report a non-supported case as an exception
        p.failAnalysis(
          errorClass = "UNSUPPORTED_SUBQUERY_EXPRESSION_CATEGORY." +
            "CORRELATED_COLUMN_IS_NOT_ALLOWED_IN_PREDICATE",
          messageParameters =
            Map("treeNode" -> s"${exprsToString(predicates)}\n${planToString(p)}"))
      }
    }

    // Recursively check invalid outer references in the plan.
    def checkPlan(
        plan: LogicalPlan,
        aggregated: Boolean = false,
        canContainOuter: Boolean = true): Unit = {

      if (!canContainOuter) {
        failOnOuterReferenceInPlan(plan)
      }

      // Approve operators allowed in a correlated subquery
      // There are 4 categories:
      // 1. Operators that are allowed anywhere in a correlated subquery, and,
      //    by definition of the operators, they either do not contain
      //    any columns or cannot host outer references.
      // 2. Operators that are allowed anywhere in a correlated subquery
      //    so long as they do not host outer references.
      // 3. Operators that need special handling. These operators are
      //    Filter, Join, Aggregate, and Generate.
      //
      // Any operators that are not in the above list are allowed
      // in a correlated subquery only if they are not on a correlation path.
      // In other word, these operators are allowed only under a correlation point.
      //
      // A correlation path is defined as the sub-tree of all the operators that
      // are on the path from the operator hosting the correlated expressions
      // up to the operator producing the correlated values.
      plan match {
        // Category 1:
        // ResolvedHint, LeafNode, Repartition, and SubqueryAlias
        case p @ (_: ResolvedHint | _: LeafNode | _: Repartition | _: SubqueryAlias) =>
          p.children.foreach(child => checkPlan(child, aggregated, canContainOuter))

        case p @ (_ : Union | _: SetOperation) =>
          // Set operations (e.g. UNION) containing correlated values are only supported
          // with DecorrelateInnerQuery framework.
          val childCanContainOuter = (canContainOuter
            && usingDecorrelateInnerQueryFramework
            && SQLConf.get.getConf(SQLConf.DECORRELATE_SET_OPS_ENABLED))
          p.children.foreach(child => checkPlan(child, aggregated, childCanContainOuter))

        // SQLFunctionNode serves as a container for the underlying SQL function plan.
        case s: SQLFunctionNode =>
          s.children.foreach(child => checkPlan(child, aggregated, canContainOuter))

        // Category 2:
        // These operators can be anywhere in a correlated subquery.
        // so long as they do not host outer references in the operators.
        case p: Project =>
          failOnInvalidOuterReference(p)
          checkPlan(p.child, aggregated, canContainOuter)

        case s: Sort =>
          failOnInvalidOuterReference(s)
          checkPlan(s.child, aggregated, canContainOuter)

        case r: RepartitionByExpression =>
          failOnInvalidOuterReference(r)
          checkPlan(r.child, aggregated, canContainOuter)

        case l: LateralJoin =>
          failOnInvalidOuterReference(l)
          checkPlan(l.child, aggregated, canContainOuter)

        // Category 3:
        // Filter is one of the two operators allowed to host correlated expressions.
        // The other operator is Join. Filter can be anywhere in a correlated subquery.
        case f: Filter =>
          failOnInvalidOuterReference(f)
          val (correlated, _) = splitConjunctivePredicates(f.condition).partition(containsOuter)
          val unsupportedPredicates = correlated.filterNot(DecorrelateInnerQuery.canPullUpOverAgg)
          if (aggregated) {
            failOnUnsupportedCorrelatedPredicate(unsupportedPredicates, f)
          }
          checkPlan(f.child, aggregated, canContainOuter)

        // Aggregate cannot host any correlated expressions
        // It can be on a correlation path if the correlation contains
        // only supported correlated equality predicates.
        // It cannot be on a correlation path if the correlation has
        // non-equality correlated predicates.
        case a: Aggregate =>
          failOnInvalidOuterReference(a)
          checkPlan(a.child, aggregated = true, canContainOuter)

        // Same as Aggregate above.
        case w: Window =>
          failOnInvalidOuterReference(w)
          checkPlan(w.child, aggregated = true, canContainOuter)

        // Distinct does not host any correlated expressions, but during the optimization phase
        // it will be rewritten as Aggregate, which can only be on a correlation path if the
        // correlation contains only the supported correlated equality predicates.
        // Only block it for lateral subqueries because scalar subqueries must be aggregated
        // and it does not impact the results for IN/EXISTS subqueries.
        case d: Distinct =>
          checkPlan(d.child, aggregated = isLateral, canContainOuter)

        // Join can host correlated expressions.
        case j @ Join(left, right, joinType, _, _) =>
          failOnInvalidOuterReference(j)
          joinType match {
            // Inner join, like Filter, can be anywhere.
            case _: InnerLike =>
              j.children.foreach(child => checkPlan(child, aggregated, canContainOuter))

            // Left outer join's right operand cannot be on a correlation path.
            // LeftAnti and ExistenceJoin are special cases of LeftOuter.
            // Note that ExistenceJoin cannot be expressed externally in both SQL and DataFrame
            // so it should not show up here in Analysis phase. This is just a safety net.
            //
            // LeftSemi does not allow output from the right operand.
            // Any correlated references in the subplan
            // of the right operand cannot be pulled up.
            case LeftOuter | LeftSemi | LeftAnti | ExistenceJoin(_) =>
              checkPlan(left, aggregated, canContainOuter)
              checkPlan(right, aggregated, canContainOuter = false)

            // Likewise, Right outer join's left operand cannot be on a correlation path.
            case RightOuter =>
              checkPlan(left, aggregated, canContainOuter = false)
              checkPlan(right, aggregated, canContainOuter)

            // Any other join types not explicitly listed above,
            // including Full outer join, are treated as Category 4.
            case _ =>
              j.children.foreach(child => checkPlan(child, aggregated, canContainOuter = false))
          }

        // Generator with join=true, i.e., expressed with
        // LATERAL VIEW [OUTER], similar to inner join,
        // allows to have correlation under it
        // but must not host any outer references.
        // Note:
        // Generator with requiredChildOutput.isEmpty is treated as Category 4.
        case g: Generate if g.requiredChildOutput.nonEmpty =>
          failOnInvalidOuterReference(g)
          checkPlan(g.child, aggregated, canContainOuter)

        // Correlated subquery can have a LIMIT clause
        case l @ Limit(_, input) =>
          failOnInvalidOuterReference(l)
          checkPlan(
            input,
            aggregated,
            canContainOuter && SQLConf.get.getConf(SQLConf.DECORRELATE_LIMIT_ENABLED))

        case o @ Offset(_, input) =>
          failOnInvalidOuterReference(o)
          checkPlan(
            input,
            aggregated,
            canContainOuter && SQLConf.get.getConf(SQLConf.DECORRELATE_OFFSET_ENABLED))

        // We always inline CTE relations before analysis check, and only un-referenced CTE
        // relations will be kept in the plan. Here we should simply skip them and check the
        // children, as un-referenced CTE relations won't be executed anyway and doesn't need to
        // be restricted by the current subquery correlation limitations.
        case _: WithCTE | _: CTERelationDef =>
          plan.children.foreach(p => checkPlan(p, aggregated, canContainOuter))

        // Category 4: Any other operators not in the above 3 categories
        // cannot be on a correlation path, that is they are allowed only
        // under a correlation point but they and their descendant operators
        // are not allowed to have any correlated expressions.
        case p =>
          p.children.foreach(p => checkPlan(p, aggregated, canContainOuter = false))
      }
    }

    // Simplify the predicates before validating any unsupported correlation patterns in the plan.
    AnalysisHelper.allowInvokingTransformsInAnalyzer {
      checkPlan(BooleanSimplification(sub))
    }
  }

  private def exprsToString(exprs: Seq[Expression]): String = {
    val result = exprs.map(_.toString).mkString("\n")
    if (Utils.isTesting) scrubOutIds(result) else result
  }
}
