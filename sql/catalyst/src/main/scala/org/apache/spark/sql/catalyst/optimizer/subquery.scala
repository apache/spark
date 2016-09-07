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

import scala.collection.mutable.ArrayBuffer

import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.expressions.aggregate._
import org.apache.spark.sql.catalyst.plans._
import org.apache.spark.sql.catalyst.plans.logical._
import org.apache.spark.sql.catalyst.rules._
import org.apache.spark.sql.types._

/*
 * This file defines optimization rules related to subqueries.
 */


/**
 * This rule rewrites predicate sub-queries into left semi/anti joins. The following predicates
 * are supported:
 * a. EXISTS/NOT EXISTS will be rewritten as semi/anti join, unresolved conditions in Filter
 *    will be pulled out as the join conditions.
 * b. IN/NOT IN will be rewritten as semi/anti join, unresolved conditions in the Filter will
 *    be pulled out as join conditions, value = selected column will also be used as join
 *    condition.
 */
object RewritePredicateSubquery extends Rule[LogicalPlan] with PredicateHelper {
  def apply(plan: LogicalPlan): LogicalPlan = plan transform {
    case Filter(condition, child) =>
      val (withSubquery, withoutSubquery) =
        splitConjunctivePredicates(condition).partition(PredicateSubquery.hasPredicateSubquery)

      // Construct the pruned filter condition.
      val newFilter: LogicalPlan = withoutSubquery match {
        case Nil => child
        case conditions => Filter(conditions.reduce(And), child)
      }

      // Filter the plan by applying left semi and left anti joins.
      rewritePredicateSubquery(withSubquery, newFilter)

    case Scanner(projectList, filters, child) if filters.exists(
      PredicateSubquery.hasPredicateSubquery(_)) =>
      val (withSubquery, withoutSubquery) =
        filters.partition(PredicateSubquery.hasPredicateSubquery)

      val newFilter: LogicalPlan = withoutSubquery match {
        case Nil => Scanner(child)
        case conditions => Scanner(conditions.reduce(And), child)
      }

      val newPlan = rewritePredicateSubquery(withSubquery, newFilter)

      if (newPlan.output.forall(projectList.contains(_))) {
        newPlan
      } else {
        Project(projectList, newPlan)
      }
  }

  /**
   * Re-construct the plan by applying left semi and left anti joins instead of predicate
   * subquerys.
   */
  private def rewritePredicateSubquery(
      predicateSubquerys: Seq[Expression],
      filter: LogicalPlan): LogicalPlan = {
    predicateSubquerys.foldLeft(filter) {
      case (p, PredicateSubquery(sub, conditions, _, _)) =>
        val (joinCond, outerPlan) = rewriteExistentialExpr(conditions, p)
        Join(outerPlan, sub, LeftSemi, joinCond)
      case (p, Not(PredicateSubquery(sub, conditions, false, _))) =>
        val (joinCond, outerPlan) = rewriteExistentialExpr(conditions, p)
        Join(outerPlan, sub, LeftAnti, joinCond)
      case (p, Not(PredicateSubquery(sub, conditions, true, _))) =>
        // This is a NULL-aware (left) anti join (NAAJ) e.g. col NOT IN expr
        // Construct the condition. A NULL in one of the conditions is regarded as a positive
        // result; such a row will be filtered out by the Anti-Join operator.

        // Note that will almost certainly be planned as a Broadcast Nested Loop join.
        // Use EXISTS if performance matters to you.
        val (joinCond, outerPlan) = rewriteExistentialExpr(conditions, p)
        val anyNull = splitConjunctivePredicates(joinCond.get).map(IsNull).reduceLeft(Or)
        Join(outerPlan, sub, LeftAnti, Option(Or(anyNull, joinCond.get)))
      case (p, predicate) =>
        val (newCond, inputPlan) = rewriteExistentialExpr(Seq(predicate), p)
        Project(p.output, Filter(newCond.get, inputPlan))
    }
  }

  /**
   * Given a predicate expression and an input plan, it rewrites
   * any embedded existential sub-query into an existential join.
   * It returns the rewritten expression together with the updated plan.
   * Currently, it does not support null-aware joins. Embedded NOT IN predicates
   * are blocked in the Analyzer.
   */
  private def rewriteExistentialExpr(
      exprs: Seq[Expression],
      plan: LogicalPlan): (Option[Expression], LogicalPlan) = {
    var newPlan = plan
    val newExprs = exprs.map { e =>
      e transformUp {
        case PredicateSubquery(sub, conditions, nullAware, _) =>
          // TODO: support null-aware join
          val exists = AttributeReference("exists", BooleanType, nullable = false)()
          newPlan = Join(newPlan, sub, ExistenceJoin(exists), conditions.reduceLeftOption(And))
          exists
        }
    }
    (newExprs.reduceOption(And), newPlan)
  }
}


/**
 * This rule rewrites correlated [[ScalarSubquery]] expressions into LEFT OUTER joins.
 */
object RewriteCorrelatedScalarSubquery extends Rule[LogicalPlan] {
  /**
   * Extract all correlated scalar subqueries from an expression. The subqueries are collected using
   * the given collector. The expression is rewritten and returned.
   */
  private def extractCorrelatedScalarSubqueries[E <: Expression](
      expression: E,
      subqueries: ArrayBuffer[ScalarSubquery]): E = {
    val newExpression = expression transform {
      case s: ScalarSubquery if s.children.nonEmpty =>
        subqueries += s
        s.plan.output.head
    }
    newExpression.asInstanceOf[E]
  }

  /**
   * Statically evaluate an expression containing zero or more placeholders, given a set
   * of bindings for placeholder values.
   */
  private def evalExpr(expr: Expression, bindings: Map[ExprId, Option[Any]]) : Option[Any] = {
    val rewrittenExpr = expr transform {
      case r: AttributeReference =>
        bindings(r.exprId) match {
          case Some(v) => Literal.create(v, r.dataType)
          case None => Literal.default(NullType)
        }
    }
    Option(rewrittenExpr.eval())
  }

  /**
   * Statically evaluate an expression containing one or more aggregates on an empty input.
   */
  private def evalAggOnZeroTups(expr: Expression) : Option[Any] = {
    // AggregateExpressions are Unevaluable, so we need to replace all aggregates
    // in the expression with the value they would return for zero input tuples.
    // Also replace attribute refs (for example, for grouping columns) with NULL.
    val rewrittenExpr = expr transform {
      case a @ AggregateExpression(aggFunc, _, _, resultId) =>
        aggFunc.defaultResult.getOrElse(Literal.default(NullType))

      case _: AttributeReference => Literal.default(NullType)
    }
    Option(rewrittenExpr.eval())
  }

  /**
   * Statically evaluate a scalar subquery on an empty input.
   *
   * <b>WARNING:</b> This method only covers subqueries that pass the checks under
   * [[org.apache.spark.sql.catalyst.analysis.CheckAnalysis]]. If the checks in
   * CheckAnalysis become less restrictive, this method will need to change.
   */
  private def evalSubqueryOnZeroTups(plan: LogicalPlan) : Option[Any] = {
    // Inputs to this method will start with a chain of zero or more SubqueryAlias
    // and Project operators, followed by an optional Filter, followed by an
    // Aggregate. Traverse the operators recursively.
    def evalPlan(lp : LogicalPlan) : Map[ExprId, Option[Any]] = lp match {
      case SubqueryAlias(_, child, _) => evalPlan(child)
      case Filter(condition, child) =>
        val bindings = evalPlan(child)
        if (bindings.isEmpty) bindings
        else {
          val exprResult = evalExpr(condition, bindings).getOrElse(false)
            .asInstanceOf[Boolean]
          if (exprResult) bindings else Map.empty
        }

      case Project(projectList, child) =>
        val bindings = evalPlan(child)
        if (bindings.isEmpty) {
          bindings
        } else {
          projectList.map(ne => (ne.exprId, evalExpr(ne, bindings))).toMap
        }

      case Aggregate(_, aggExprs, _) =>
        // Some of the expressions under the Aggregate node are the join columns
        // for joining with the outer query block. Fill those expressions in with
        // nulls and statically evaluate the remainder.
        aggExprs.map {
          case ref: AttributeReference => (ref.exprId, None)
          case alias @ Alias(_: AttributeReference, _) => (alias.exprId, None)
          case ne => (ne.exprId, evalAggOnZeroTups(ne))
        }.toMap

      case _ => sys.error(s"Unexpected operator in scalar subquery: $lp")
    }

    val resultMap = evalPlan(plan)

    // By convention, the scalar subquery result is the leftmost field.
    resultMap(plan.output.head.exprId)
  }

  /**
   * Split the plan for a scalar subquery into the parts above the innermost query block
   * (first part of returned value), the HAVING clause of the innermost query block
   * (optional second part) and the parts below the HAVING CLAUSE (third part).
   */
  private def splitSubquery(plan: LogicalPlan) : (Seq[LogicalPlan], Option[Filter], Aggregate) = {
    val topPart = ArrayBuffer.empty[LogicalPlan]
    var bottomPart: LogicalPlan = plan
    while (true) {
      bottomPart match {
        case havingPart @ Filter(_, aggPart: Aggregate) =>
          return (topPart, Option(havingPart), aggPart)

        case aggPart: Aggregate =>
          // No HAVING clause
          return (topPart, None, aggPart)

        case p @ Project(_, child) =>
          topPart += p
          bottomPart = child

        case s @ SubqueryAlias(_, child, _) =>
          topPart += s
          bottomPart = child

        case Filter(_, op) =>
          sys.error(s"Correlated subquery has unexpected operator $op below filter")

        case op @ _ => sys.error(s"Unexpected operator $op in correlated subquery")
      }
    }

    sys.error("This line should be unreachable")
  }

  // Name of generated column used in rewrite below
  val ALWAYS_TRUE_COLNAME = "alwaysTrue"

  /**
   * Construct a new child plan by left joining the given subqueries to a base plan.
   */
  private def constructLeftJoins(
      child: LogicalPlan,
      subqueries: ArrayBuffer[ScalarSubquery]): LogicalPlan = {
    subqueries.foldLeft(child) {
      case (currentChild, ScalarSubquery(query, conditions, _)) =>
        val origOutput = query.output.head

        val resultWithZeroTups = evalSubqueryOnZeroTups(query)
        if (resultWithZeroTups.isEmpty) {
          // CASE 1: Subquery guaranteed not to have the COUNT bug
          Project(
            currentChild.output :+ origOutput,
            Join(currentChild, query, LeftOuter, conditions.reduceOption(And)))
        } else {
          // Subquery might have the COUNT bug. Add appropriate corrections.
          val (topPart, havingNode, aggNode) = splitSubquery(query)

          // The next two cases add a leading column to the outer join input to make it
          // possible to distinguish between the case when no tuples join and the case
          // when the tuple that joins contains null values.
          // The leading column always has the value TRUE.
          val alwaysTrueExprId = NamedExpression.newExprId
          val alwaysTrueExpr = Alias(Literal.TrueLiteral,
            ALWAYS_TRUE_COLNAME)(exprId = alwaysTrueExprId)
          val alwaysTrueRef = AttributeReference(ALWAYS_TRUE_COLNAME,
            BooleanType)(exprId = alwaysTrueExprId)

          val aggValRef = query.output.head

          if (havingNode.isEmpty) {
            // CASE 2: Subquery with no HAVING clause
            Project(
              currentChild.output :+
                Alias(
                  If(IsNull(alwaysTrueRef),
                    Literal.create(resultWithZeroTups.get, origOutput.dataType),
                    aggValRef), origOutput.name)(exprId = origOutput.exprId),
              Join(currentChild,
                Project(query.output :+ alwaysTrueExpr, query),
                LeftOuter, conditions.reduceOption(And)))

          } else {
            // CASE 3: Subquery with HAVING clause. Pull the HAVING clause above the join.
            // Need to modify any operators below the join to pass through all columns
            // referenced in the HAVING clause.
            var subqueryRoot: UnaryNode = aggNode
            val havingInputs: Seq[NamedExpression] = aggNode.output

            topPart.reverse.foreach {
              case Project(projList, _) =>
                subqueryRoot = Project(projList ++ havingInputs, subqueryRoot)
              case s @ SubqueryAlias(alias, _, None) =>
                subqueryRoot = SubqueryAlias(alias, subqueryRoot, None)
              case op => sys.error(s"Unexpected operator $op in corelated subquery")
            }

            // CASE WHEN alwayTrue IS NULL THEN resultOnZeroTups
            //      WHEN NOT (original HAVING clause expr) THEN CAST(null AS <type of aggVal>)
            //      ELSE (aggregate value) END AS (original column name)
            val caseExpr = Alias(CaseWhen(Seq(
              (IsNull(alwaysTrueRef), Literal.create(resultWithZeroTups.get, origOutput.dataType)),
              (Not(havingNode.get.condition), Literal.create(null, aggValRef.dataType))),
              aggValRef),
              origOutput.name)(exprId = origOutput.exprId)

            Project(
              currentChild.output :+ caseExpr,
              Join(currentChild,
                Project(subqueryRoot.output :+ alwaysTrueExpr, subqueryRoot),
                LeftOuter, conditions.reduceOption(And)))

          }
        }
    }
  }

  /**
   * Rewrite [[Filter]], [[Project]] and [[Aggregate]] plans containing correlated scalar
   * subqueries.
   */
  def apply(plan: LogicalPlan): LogicalPlan = plan transform {
    case a @ Aggregate(grouping, expressions, child) =>
      val subqueries = ArrayBuffer.empty[ScalarSubquery]
      val newExpressions = expressions.map(extractCorrelatedScalarSubqueries(_, subqueries))
      if (subqueries.nonEmpty) {
        // We currently only allow correlated subqueries in an aggregate if they are part of the
        // grouping expressions. As a result we need to replace all the scalar subqueries in the
        // grouping expressions by their result.
        val newGrouping = grouping.map { e =>
          subqueries.find(_.semanticEquals(e)).map(_.plan.output.head).getOrElse(e)
        }
        Aggregate(newGrouping, newExpressions, constructLeftJoins(child, subqueries))
      } else {
        a
      }
    case p @ Project(expressions, child) =>
      val subqueries = ArrayBuffer.empty[ScalarSubquery]
      val newExpressions = expressions.map(extractCorrelatedScalarSubqueries(_, subqueries))
      if (subqueries.nonEmpty) {
        Project(newExpressions, constructLeftJoins(child, subqueries))
      } else {
        p
      }
    case f @ Filter(condition, child) =>
      val subqueries = ArrayBuffer.empty[ScalarSubquery]
      val newCondition = extractCorrelatedScalarSubqueries(condition, subqueries)
      if (subqueries.nonEmpty) {
        Project(f.output, Filter(newCondition, constructLeftJoins(child, subqueries)))
      } else {
        f
      }
  }
}
