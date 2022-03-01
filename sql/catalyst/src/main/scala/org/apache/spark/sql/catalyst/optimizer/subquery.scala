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
import scala.collection.mutable.ArrayBuffer

import org.apache.spark.sql.catalyst.analysis.EliminateSubqueryAliases
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.expressions.ScalarSubquery._
import org.apache.spark.sql.catalyst.expressions.SubExprUtils._
import org.apache.spark.sql.catalyst.expressions.aggregate._
import org.apache.spark.sql.catalyst.plans._
import org.apache.spark.sql.catalyst.plans.logical._
import org.apache.spark.sql.catalyst.rules._
import org.apache.spark.sql.catalyst.trees.TreePattern.{EXISTS_SUBQUERY, FILTER, IN_SUBQUERY, LATERAL_JOIN, LIST_SUBQUERY, PLAN_EXPRESSION, SCALAR_SUBQUERY}
import org.apache.spark.sql.errors.{QueryCompilationErrors, QueryExecutionErrors}
import org.apache.spark.sql.internal.SQLConf
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

  private def buildJoin(
      outerPlan: LogicalPlan,
      subplan: LogicalPlan,
      joinType: JoinType,
      condition: Option[Expression]): Join = {
    // Deduplicate conflicting attributes if any.
    val dedupSubplan = dedupSubqueryOnSelfJoin(outerPlan, subplan, None, condition)
    Join(outerPlan, dedupSubplan, joinType, condition, JoinHint.NONE)
  }

  private def dedupSubqueryOnSelfJoin(
      outerPlan: LogicalPlan,
      subplan: LogicalPlan,
      valuesOpt: Option[Seq[Expression]],
      condition: Option[Expression] = None): LogicalPlan = {
    // SPARK-21835: It is possibly that the two sides of the join have conflicting attributes,
    // the produced join then becomes unresolved and break structural integrity. We should
    // de-duplicate conflicting attributes.
    // SPARK-26078: it may also happen that the subquery has conflicting attributes with the outer
    // values. In this case, the resulting join would contain trivially true conditions (e.g.
    // id#3 = id#3) which cannot be de-duplicated after. In this method, if there are conflicting
    // attributes in the join condition, the subquery's conflicting attributes are changed using
    // a projection which aliases them and resolves the problem.
    val outerReferences = valuesOpt.map(values =>
      AttributeSet.fromAttributeSets(values.map(_.references))).getOrElse(AttributeSet.empty)
    val outerRefs = outerPlan.outputSet ++ outerReferences
    val duplicates = outerRefs.intersect(subplan.outputSet)
    if (duplicates.nonEmpty) {
      condition.foreach { e =>
          val conflictingAttrs = e.references.intersect(duplicates)
          if (conflictingAttrs.nonEmpty) {
            throw QueryCompilationErrors.conflictingAttributesInJoinConditionError(
              conflictingAttrs, outerPlan, subplan)
          }
      }
      val rewrites = AttributeMap(duplicates.map { dup =>
        dup -> Alias(dup, dup.toString)()
      }.toSeq)
      val aliasedExpressions = subplan.output.map { ref =>
        rewrites.getOrElse(ref, ref)
      }
      Project(aliasedExpressions, subplan)
    } else {
      subplan
    }
  }

  def apply(plan: LogicalPlan): LogicalPlan = plan.transformWithPruning(
    t => t.containsAnyPattern(EXISTS_SUBQUERY, LIST_SUBQUERY) && t.containsPattern(FILTER)) {
    case Filter(condition, child)
      if SubqueryExpression.hasInOrCorrelatedExistsSubquery(condition) =>
      val (withSubquery, withoutSubquery) =
        splitConjunctivePredicates(condition)
          .partition(SubqueryExpression.hasInOrCorrelatedExistsSubquery)

      // Construct the pruned filter condition.
      val newFilter: LogicalPlan = withoutSubquery match {
        case Nil => child
        case conditions => Filter(conditions.reduce(And), child)
      }

      // Filter the plan by applying left semi and left anti joins.
      withSubquery.foldLeft(newFilter) {
        case (p, Exists(sub, _, _, conditions)) =>
          val (joinCond, outerPlan) = rewriteExistentialExpr(conditions, p)
          buildJoin(outerPlan, sub, LeftSemi, joinCond)
        case (p, Not(Exists(sub, _, _, conditions))) =>
          val (joinCond, outerPlan) = rewriteExistentialExpr(conditions, p)
          buildJoin(outerPlan, sub, LeftAnti, joinCond)
        case (p, InSubquery(values, ListQuery(sub, _, _, _, conditions))) =>
          // Deduplicate conflicting attributes if any.
          val newSub = dedupSubqueryOnSelfJoin(p, sub, Some(values))
          val inConditions = values.zip(newSub.output).map(EqualTo.tupled)
          val (joinCond, outerPlan) = rewriteExistentialExpr(inConditions ++ conditions, p)
          Join(outerPlan, newSub, LeftSemi, joinCond, JoinHint.NONE)
        case (p, Not(InSubquery(values, ListQuery(sub, _, _, _, conditions)))) =>
          // This is a NULL-aware (left) anti join (NAAJ) e.g. col NOT IN expr
          // Construct the condition. A NULL in one of the conditions is regarded as a positive
          // result; such a row will be filtered out by the Anti-Join operator.

          // Note that will almost certainly be planned as a Broadcast Nested Loop join.
          // Use EXISTS if performance matters to you.

          // Deduplicate conflicting attributes if any.
          val newSub = dedupSubqueryOnSelfJoin(p, sub, Some(values))
          val inConditions = values.zip(newSub.output).map(EqualTo.tupled)
          val (joinCond, outerPlan) = rewriteExistentialExpr(inConditions, p)
          // Expand the NOT IN expression with the NULL-aware semantic
          // to its full form. That is from:
          //   (a1,a2,...) = (b1,b2,...)
          // to
          //   (a1=b1 OR isnull(a1=b1)) AND (a2=b2 OR isnull(a2=b2)) AND ...
          val baseJoinConds = splitConjunctivePredicates(joinCond.get)
          val nullAwareJoinConds = baseJoinConds.map(c => Or(c, IsNull(c)))
          // After that, add back the correlated join predicate(s) in the subquery
          // Example:
          // SELECT ... FROM A WHERE A.A1 NOT IN (SELECT B.B1 FROM B WHERE B.B2 = A.A2 AND B.B3 > 1)
          // will have the final conditions in the LEFT ANTI as
          // (A.A1 = B.B1 OR ISNULL(A.A1 = B.B1)) AND (B.B2 = A.A2) AND B.B3 > 1
          val finalJoinCond = (nullAwareJoinConds ++ conditions).reduceLeft(And)
          Join(outerPlan, newSub, LeftAnti, Option(finalJoinCond), JoinHint.NONE)
        case (p, predicate) =>
          val (newCond, inputPlan) = rewriteExistentialExpr(Seq(predicate), p)
          Project(p.output, Filter(newCond.get, inputPlan))
      }
  }

  /**
   * Given a predicate expression and an input plan, it rewrites any embedded existential sub-query
   * into an existential join. It returns the rewritten expression together with the updated plan.
   * Currently, it does not support NOT IN nested inside a NOT expression. This case is blocked in
   * the Analyzer.
   */
  private def rewriteExistentialExpr(
      exprs: Seq[Expression],
      plan: LogicalPlan): (Option[Expression], LogicalPlan) = {
    var newPlan = plan
    val newExprs = exprs.map { e =>
      e.transformDownWithPruning(_.containsAnyPattern(EXISTS_SUBQUERY, IN_SUBQUERY)) {
        case Exists(sub, _, _, conditions) =>
          val exists = AttributeReference("exists", BooleanType, nullable = false)()
          newPlan =
            buildJoin(newPlan, sub, ExistenceJoin(exists), conditions.reduceLeftOption(And))
          exists
        case Not(InSubquery(values, ListQuery(sub, _, _, _, conditions))) =>
          val exists = AttributeReference("exists", BooleanType, nullable = false)()
          // Deduplicate conflicting attributes if any.
          val newSub = dedupSubqueryOnSelfJoin(newPlan, sub, Some(values))
          val inConditions = values.zip(sub.output).map(EqualTo.tupled)
          // To handle a null-aware predicate not-in-subquery in nested conditions
          // (e.g., `v > 0 OR t1.id NOT IN (SELECT id FROM t2)`), we transform
          // `inCondition` (t1.id=t2.id) into `(inCondition) OR ISNULL(inCondition)`.
          //
          // For example, `SELECT * FROM t1 WHERE v > 0 OR t1.id NOT IN (SELECT id FROM t2)`
          // is transformed into a plan below;
          // == Optimized Logical Plan ==
          // Project [id#78, v#79]
          // +- Filter ((v#79 > 0) OR NOT exists#83)
          //   +- Join ExistenceJoin(exists#83), ((id#78 = id#80) OR isnull((id#78 = id#80)))
          //     :- Relation[id#78,v#79] parquet
          //     +- Relation[id#80] parquet
          val nullAwareJoinConds = inConditions.map(c => Or(c, IsNull(c)))
          val finalJoinCond = (nullAwareJoinConds ++ conditions).reduceLeft(And)
          newPlan = Join(newPlan, newSub, ExistenceJoin(exists), Some(finalJoinCond), JoinHint.NONE)
          Not(exists)
        case InSubquery(values, ListQuery(sub, _, _, _, conditions)) =>
          val exists = AttributeReference("exists", BooleanType, nullable = false)()
          // Deduplicate conflicting attributes if any.
          val newSub = dedupSubqueryOnSelfJoin(newPlan, sub, Some(values))
          val inConditions = values.zip(newSub.output).map(EqualTo.tupled)
          val newConditions = (inConditions ++ conditions).reduceLeftOption(And)
          newPlan = Join(newPlan, newSub, ExistenceJoin(exists), newConditions, JoinHint.NONE)
          exists
      }
    }
    (newExprs.reduceOption(And), newPlan)
  }
}

 /**
  * Pull out all (outer) correlated predicates from a given subquery. This method removes the
  * correlated predicates from subquery [[Filter]]s and adds the references of these predicates
  * to all intermediate [[Project]] and [[Aggregate]] clauses (if they are missing) in order to
  * be able to evaluate the predicates at the top level.
  *
  * TODO: Look to merge this rule with RewritePredicateSubquery.
  */
object PullupCorrelatedPredicates extends Rule[LogicalPlan] with PredicateHelper {
   /**
    * Returns the correlated predicates and a updated plan that removes the outer references.
    */
  private def pullOutCorrelatedPredicates(
      sub: LogicalPlan,
      outer: LogicalPlan): (LogicalPlan, Seq[Expression]) = {
    val predicateMap = scala.collection.mutable.Map.empty[LogicalPlan, Seq[Expression]]

    /** Determine which correlated predicate references are missing from this plan. */
    def missingReferences(p: LogicalPlan): AttributeSet = {
      val localPredicateReferences = p.collect(predicateMap)
        .flatten
        .map(_.references)
        .reduceOption(_ ++ _)
        .getOrElse(AttributeSet.empty)
      localPredicateReferences -- p.outputSet
    }

    // Simplify the predicates before pulling them out.
    val transformed = BooleanSimplification(sub) transformUp {
      case f @ Filter(cond, child) =>
        val (correlated, local) =
          splitConjunctivePredicates(cond).partition(containsOuter)

        // Rewrite the filter without the correlated predicates if any.
        correlated match {
          case Nil => f
          case xs if local.nonEmpty =>
            val newFilter = Filter(local.reduce(And), child)
            predicateMap += newFilter -> xs
            newFilter
          case xs =>
            predicateMap += child -> xs
            child
        }
      case p @ Project(expressions, child) =>
        val referencesToAdd = missingReferences(p)
        if (referencesToAdd.nonEmpty) {
          Project(expressions ++ referencesToAdd, child)
        } else {
          p
        }
      case a @ Aggregate(grouping, expressions, child) =>
        val referencesToAdd = missingReferences(a)
        if (referencesToAdd.nonEmpty) {
          Aggregate(grouping ++ referencesToAdd, expressions ++ referencesToAdd, child)
        } else {
          a
        }
      case p =>
        p
    }

    // Make sure the inner and the outer query attributes do not collide.
    // In case of a collision, change the subquery plan's output to use
    // different attribute by creating alias(s).
    val baseConditions = predicateMap.values.flatten.toSeq
    val outerPlanInputAttrs = outer.inputSet
    val (newPlan, newCond) = if (outerPlanInputAttrs.nonEmpty) {
      val (plan, deDuplicatedConditions) =
        DecorrelateInnerQuery.deduplicate(transformed, baseConditions, outerPlanInputAttrs)
      (plan, stripOuterReferences(deDuplicatedConditions))
    } else {
      (transformed, stripOuterReferences(baseConditions))
    }
    (newPlan, newCond)
  }

  private def rewriteSubQueries(plan: LogicalPlan): LogicalPlan = {
    /**
     * This function is used as a aid to enforce idempotency of pullUpCorrelatedPredicate rule.
     * In the first call to rewriteSubqueries, all the outer references from the subplan are
     * pulled up and join predicates are recorded as children of the enclosing subquery expression.
     * The subsequent call to rewriteSubqueries would simply re-records the `children` which would
     * contains the pulled up correlated predicates (from the previous call) in the enclosing
     * subquery expression.
     */
    def getJoinCondition(newCond: Seq[Expression], oldCond: Seq[Expression]): Seq[Expression] = {
      if (newCond.isEmpty) oldCond else newCond
    }

    def decorrelate(
        sub: LogicalPlan,
        outer: LogicalPlan,
        handleCountBug: Boolean = false): (LogicalPlan, Seq[Expression]) = {
      if (SQLConf.get.decorrelateInnerQueryEnabled) {
        DecorrelateInnerQuery(sub, outer, handleCountBug)
      } else {
        pullOutCorrelatedPredicates(sub, outer)
      }
    }

    plan.transformExpressionsWithPruning(_.containsPattern(PLAN_EXPRESSION)) {
      case ScalarSubquery(sub, children, exprId, conditions) if children.nonEmpty =>
        val (newPlan, newCond) = decorrelate(sub, plan)
        ScalarSubquery(newPlan, children, exprId, getJoinCondition(newCond, conditions))
      case Exists(sub, children, exprId, conditions) if children.nonEmpty =>
        val (newPlan, newCond) = pullOutCorrelatedPredicates(sub, plan)
        Exists(newPlan, children, exprId, getJoinCondition(newCond, conditions))
      case ListQuery(sub, children, exprId, childOutputs, conditions) if children.nonEmpty =>
        val (newPlan, newCond) = pullOutCorrelatedPredicates(sub, plan)
        ListQuery(newPlan, children, exprId, childOutputs, getJoinCondition(newCond, conditions))
      case LateralSubquery(sub, children, exprId, conditions) if children.nonEmpty =>
        val (newPlan, newCond) = decorrelate(sub, plan, handleCountBug = true)
        LateralSubquery(newPlan, children, exprId, getJoinCondition(newCond, conditions))
    }
  }

  /**
   * Pull up the correlated predicates and rewrite all subqueries in an operator tree..
   */
  def apply(plan: LogicalPlan): LogicalPlan = plan.transformUpWithPruning(
    _.containsPattern(PLAN_EXPRESSION)) {
    case j: LateralJoin =>
      val newPlan = rewriteSubQueries(j)
      // Since a lateral join's output depends on its left child output and its lateral subquery's
      // plan output, we need to trim the domain attributes added to the subquery's plan output
      // to preserve the original output of the join.
      if (!j.sameOutput(newPlan)) {
        Project(j.output, newPlan)
      } else {
        newPlan
      }
    // Only a few unary nodes (Project/Filter/Aggregate) can contain subqueries.
    case q: UnaryNode =>
      rewriteSubQueries(q)
    case s: SupportsSubquery =>
      rewriteSubQueries(s)
  }
}

/**
 * This rule rewrites correlated [[ScalarSubquery]] expressions into LEFT OUTER joins.
 */
object RewriteCorrelatedScalarSubquery extends Rule[LogicalPlan] with AliasHelper {
  /**
   * Extract all correlated scalar subqueries from an expression. The subqueries are collected using
   * the given collector. The expression is rewritten and returned.
   */
  private def extractCorrelatedScalarSubqueries[E <: Expression](
      expression: E,
      subqueries: ArrayBuffer[ScalarSubquery]): E = {
    val newExpression = expression.transformWithPruning(_.containsPattern(SCALAR_SUBQUERY)) {
      case s: ScalarSubquery if s.children.nonEmpty =>
        subqueries += s
        s.plan.output.head
    }
    newExpression.asInstanceOf[E]
  }

  /**
   * Checks if given expression is foldable. Evaluates it and returns it as literal, if yes.
   * If not, returns the original expression without evaluation.
   */
  private def tryEvalExpr(expr: Expression): Expression = {
    // Removes Alias over given expression, because Alias is not foldable.
    if (!trimAliases(expr).foldable) {
      // SPARK-28441: Some expressions, like PythonUDF, can't be statically evaluated.
      // Needs to evaluate them on query runtime.
      expr
    } else {
      Literal.create(expr.eval(), expr.dataType)
    }
  }

  /**
   * Statically evaluate an expression containing zero or more placeholders, given a set
   * of bindings for placeholder values, if the expression is evaluable. If it is not,
   * bind statically evaluated expression results to an expression.
   */
  private def bindingExpr(
      expr: Expression,
      bindings: Map[ExprId, Expression]): Expression = {
    val rewrittenExpr = expr transform {
      case r: AttributeReference =>
        bindings.getOrElse(r.exprId, Literal.create(null, r.dataType))
    }

    tryEvalExpr(rewrittenExpr)
  }

  /**
   * Statically evaluate an expression containing one or more aggregates on an empty input.
   */
  private def evalAggExprOnZeroTups(expr: Expression) : Expression = {
    // AggregateExpressions are Unevaluable, so we need to replace all aggregates
    // in the expression with the value they would return for zero input tuples.
    // Also replace attribute refs (for example, for grouping columns) with NULL.
    val rewrittenExpr = expr transform {
      case a @ AggregateExpression(aggFunc, _, _, resultId, _) =>
        aggFunc.defaultResult.getOrElse(Literal.create(null, aggFunc.dataType))

      case a: AttributeReference => Literal.create(null, a.dataType)
    }

    tryEvalExpr(rewrittenExpr)
  }

  /**
   * Statically evaluate an [[Aggregate]] on an empty input and return a mapping
   * between its output attribute expression ID and evaluated result.
   */
  def evalAggregateOnZeroTups(a: Aggregate): Map[ExprId, Expression] = {
    // Some of the expressions under the Aggregate node are the join columns
    // for joining with the outer query block. Fill those expressions in with
    // nulls and statically evaluate the remainder.
    a.aggregateExpressions.map {
      case ref: AttributeReference => (ref.exprId, Literal.create(null, ref.dataType))
      case alias @ Alias(_: AttributeReference, _) =>
        (alias.exprId, Literal.create(null, alias.dataType))
      case alias @ Alias(l: Literal, _) =>
        (alias.exprId, l.copy(value = null))
      case ne => (ne.exprId, evalAggExprOnZeroTups(ne))
    }.toMap
  }

  /**
   * Statically evaluate a scalar subquery on an empty input.
   *
   * <b>WARNING:</b> This method only covers subqueries that pass the checks under
   * [[org.apache.spark.sql.catalyst.analysis.CheckAnalysis]]. If the checks in
   * CheckAnalysis become less restrictive, this method will need to change.
   */
  private def evalSubqueryOnZeroTups(plan: LogicalPlan) : Option[Expression] = {
    // Inputs to this method will start with a chain of zero or more SubqueryAlias
    // and Project operators, followed by an optional Filter, followed by an
    // Aggregate. Traverse the operators recursively.
    def evalPlan(lp : LogicalPlan) : Map[ExprId, Expression] = lp match {
      case SubqueryAlias(_, child) => evalPlan(child)
      case Filter(condition, child) =>
        val bindings = evalPlan(child)
        if (bindings.isEmpty) {
          bindings
        } else {
          val bindCondition = bindingExpr(condition, bindings)

          if (!bindCondition.foldable) {
            // We can't evaluate the condition. Evaluate it in query runtime.
            bindings.map { case (id, expr) =>
              val newExpr = If(bindCondition, expr, Literal.create(null, expr.dataType))
              (id, newExpr)
            }
          } else {
            // The bound condition can be evaluated.
            bindCondition.eval() match {
              // For filter condition, null is the same as false.
              case null | false => Map.empty
              case true => bindings
            }
          }
        }

      case Project(projectList, child) =>
        val bindings = evalPlan(child)
        if (bindings.isEmpty) {
          bindings
        } else {
          projectList.map(ne => (ne.exprId, bindingExpr(ne, bindings))).toMap
        }

      case a: Aggregate =>
        evalAggregateOnZeroTups(a)

      case l: LeafNode =>
        l.output.map(a => (a.exprId, Literal.create(null, a.dataType))).toMap

      case p: LogicalPlan =>
        val bindings = p.children.map(evalPlan).reduce(_ ++ _)
        p.output.map(e => (e.exprId, bindingExpr(e, bindings))).toMap
    }

    val resultMap = evalPlan(plan)

    // By convention, the scalar subquery result is the leftmost field.
    resultMap.get(plan.output.head.exprId) match {
      case Some(Literal(null, _)) | None => None
      case o => o
    }
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
          return (topPart.toSeq, Option(havingPart), aggPart)

        case aggPart: Aggregate =>
          // No HAVING clause
          return (topPart.toSeq, None, aggPart)

        case p @ Project(_, child) =>
          topPart += p
          bottomPart = child

        case s @ SubqueryAlias(_, child) =>
          topPart += s
          bottomPart = child

        case Filter(_, op) =>
          throw QueryExecutionErrors.unexpectedOperatorInCorrelatedSubquery(op, " below filter")

        case op @ _ => throw QueryExecutionErrors.unexpectedOperatorInCorrelatedSubquery(op)
      }
    }

    throw QueryExecutionErrors.unreachableError()

  }

  // Name of generated column used in rewrite below
  val ALWAYS_TRUE_COLNAME = "alwaysTrue"

  /**
   * Construct a new child plan by left joining the given subqueries to a base plan.
   * This method returns the child plan and an attribute mapping
   * for the updated `ExprId`s of subqueries. If the non-empty mapping returned,
   * this rule will rewrite subquery references in a parent plan based on it.
   */
  private def constructLeftJoins(
      child: LogicalPlan,
      subqueries: ArrayBuffer[ScalarSubquery]): (LogicalPlan, AttributeMap[Attribute]) = {
    val subqueryAttrMapping = ArrayBuffer[(Attribute, Attribute)]()
    val newChild = subqueries.foldLeft(child) {
      case (currentChild, ScalarSubquery(sub, _, _, conditions)) =>
        val query = DecorrelateInnerQuery.rewriteDomainJoins(currentChild, sub, conditions)
        val origOutput = query.output.head

        val resultWithZeroTups = evalSubqueryOnZeroTups(query)
        if (resultWithZeroTups.isEmpty) {
          // CASE 1: Subquery guaranteed not to have the COUNT bug
          Project(
            currentChild.output :+ origOutput,
            Join(currentChild, query, LeftOuter, conditions.reduceOption(And), JoinHint.NONE))
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
            val subqueryResultExpr =
              Alias(If(IsNull(alwaysTrueRef),
                resultWithZeroTups.get,
                aggValRef), origOutput.name)()
            subqueryAttrMapping += ((origOutput, subqueryResultExpr.toAttribute))
            Project(
              currentChild.output :+ subqueryResultExpr,
              Join(currentChild,
                Project(query.output :+ alwaysTrueExpr, query),
                LeftOuter, conditions.reduceOption(And), JoinHint.NONE))

          } else {
            // CASE 3: Subquery with HAVING clause. Pull the HAVING clause above the join.
            // Need to modify any operators below the join to pass through all columns
            // referenced in the HAVING clause.
            var subqueryRoot: UnaryNode = aggNode
            val havingInputs: Seq[NamedExpression] = aggNode.output

            topPart.reverse.foreach {
              case Project(projList, _) =>
                subqueryRoot = Project(projList ++ havingInputs, subqueryRoot)
              case s @ SubqueryAlias(alias, _) =>
                subqueryRoot = SubqueryAlias(alias, subqueryRoot)
              case op => throw QueryExecutionErrors.unexpectedOperatorInCorrelatedSubquery(op)
            }

            // CASE WHEN alwaysTrue IS NULL THEN resultOnZeroTups
            //      WHEN NOT (original HAVING clause expr) THEN CAST(null AS <type of aggVal>)
            //      ELSE (aggregate value) END AS (original column name)
            val caseExpr = Alias(CaseWhen(Seq(
              (IsNull(alwaysTrueRef), resultWithZeroTups.get),
              (Not(havingNode.get.condition), Literal.create(null, aggValRef.dataType))),
              aggValRef),
              origOutput.name)()

            subqueryAttrMapping += ((origOutput, caseExpr.toAttribute))

            Project(
              currentChild.output :+ caseExpr,
              Join(currentChild,
                Project(subqueryRoot.output :+ alwaysTrueExpr, subqueryRoot),
                LeftOuter, conditions.reduceOption(And), JoinHint.NONE))

          }
        }
    }
    (newChild, AttributeMap(subqueryAttrMapping.toSeq))
  }

  private def updateAttrs[E <: Expression](
      exprs: Seq[E],
      attrMap: AttributeMap[Attribute]): Seq[E] = {
    if (attrMap.nonEmpty) {
      val newExprs = exprs.map { _.transform {
        case a: AttributeReference => attrMap.getOrElse(a, a)
      }}
      newExprs.asInstanceOf[Seq[E]]
    } else {
      exprs
    }
  }

  /**
   * Check if an [[Aggregate]] has no correlated subquery in aggregate expressions but
   * still has correlated scalar subqueries in its grouping expressions, which will not
   * be rewritten.
   */
  private def checkScalarSubqueryInAgg(a: Aggregate): Unit = {
    if (a.groupingExpressions.exists(hasCorrelatedScalarSubquery) &&
        !a.aggregateExpressions.exists(hasCorrelatedScalarSubquery)) {
      throw new IllegalStateException(
        s"Fail to rewrite correlated scalar subqueries in Aggregate:\n$a")
    }
  }

  /**
   * Rewrite [[Filter]], [[Project]] and [[Aggregate]] plans containing correlated scalar
   * subqueries.
   */
  def apply(plan: LogicalPlan): LogicalPlan = plan transformUpWithNewOutput {
    case a @ Aggregate(grouping, expressions, child) =>
      val subqueries = ArrayBuffer.empty[ScalarSubquery]
      val rewriteExprs = expressions.map(extractCorrelatedScalarSubqueries(_, subqueries))
      if (subqueries.nonEmpty) {
        // We currently only allow correlated subqueries in an aggregate if they are part of the
        // grouping expressions. As a result we need to replace all the scalar subqueries in the
        // grouping expressions by their result.
        val newGrouping = grouping.map { e =>
          subqueries.find(_.semanticEquals(e)).map(_.plan.output.head).getOrElse(e)
        }
        val (newChild, subqueryAttrMapping) = constructLeftJoins(child, subqueries)
        val newExprs = updateAttrs(rewriteExprs, subqueryAttrMapping)
        val newAgg = Aggregate(newGrouping, newExprs, newChild)
        val attrMapping = a.output.zip(newAgg.output)
        checkScalarSubqueryInAgg(newAgg)
        newAgg -> attrMapping
      } else {
        a -> Nil
      }
    case p @ Project(expressions, child) =>
      val subqueries = ArrayBuffer.empty[ScalarSubquery]
      val rewriteExprs = expressions.map(extractCorrelatedScalarSubqueries(_, subqueries))
      if (subqueries.nonEmpty) {
        val (newChild, subqueryAttrMapping) = constructLeftJoins(child, subqueries)
        val newExprs = updateAttrs(rewriteExprs, subqueryAttrMapping)
        val newProj = Project(newExprs, newChild)
        val attrMapping = p.output.zip(newProj.output)
        newProj -> attrMapping
      } else {
        p -> Nil
      }
    case f @ Filter(condition, child) =>
      val subqueries = ArrayBuffer.empty[ScalarSubquery]
      val rewriteCondition = extractCorrelatedScalarSubqueries(condition, subqueries)
      if (subqueries.nonEmpty) {
        val (newChild, subqueryAttrMapping) = constructLeftJoins(child, subqueries)
        val newCondition = updateAttrs(Seq(rewriteCondition), subqueryAttrMapping).head
        val newProj = Project(f.output, Filter(newCondition, newChild))
        val attrMapping = f.output.zip(newProj.output)
        newProj -> attrMapping
      } else {
        f -> Nil
      }
  }
}

/**
 * This rule rewrites [[LateralSubquery]] expressions into joins.
 */
object RewriteLateralSubquery extends Rule[LogicalPlan] {
  def apply(plan: LogicalPlan): LogicalPlan = plan.transformUpWithPruning(
    _.containsPattern(LATERAL_JOIN)) {
    case LateralJoin(left, LateralSubquery(sub, _, _, joinCond), joinType, condition) =>
      val newRight = DecorrelateInnerQuery.rewriteDomainJoins(left, sub, joinCond)
      val newCond = (condition ++ joinCond).reduceOption(And)
      Join(left, newRight, joinType, newCond, JoinHint.NONE)
  }
}

/**
 * This rule optimizes subqueries with OneRowRelation as leaf nodes.
 */
object OptimizeOneRowRelationSubquery extends Rule[LogicalPlan] {

  object OneRowSubquery {
    def unapply(plan: LogicalPlan): Option[Seq[NamedExpression]] = {
      CollapseProject(EliminateSubqueryAliases(plan)) match {
        case Project(projectList, _: OneRowRelation) => Some(stripOuterReferences(projectList))
        case _ => None
      }
    }
  }

  private def hasCorrelatedSubquery(plan: LogicalPlan): Boolean = {
    plan.find(_.expressions.exists(SubqueryExpression.hasCorrelatedSubquery)).isDefined
  }

  /**
   * Rewrite a subquery expression into one or more expressions. The rewrite can only be done
   * if there is no nested subqueries in the subquery plan.
   */
  private def rewrite(plan: LogicalPlan): LogicalPlan = plan.transformUpWithSubqueries {
    case LateralJoin(left, right @ LateralSubquery(OneRowSubquery(projectList), _, _, _), _, None)
        if !hasCorrelatedSubquery(right.plan) && right.joinCond.isEmpty =>
      Project(left.output ++ projectList, left)
    case p: LogicalPlan => p.transformExpressionsUpWithPruning(
      _.containsPattern(SCALAR_SUBQUERY)) {
      case s @ ScalarSubquery(OneRowSubquery(projectList), _, _, _)
          if !hasCorrelatedSubquery(s.plan) && s.joinCond.isEmpty =>
        assert(projectList.size == 1)
        projectList.head
    }
  }

  def apply(plan: LogicalPlan): LogicalPlan = {
    if (!conf.getConf(SQLConf.OPTIMIZE_ONE_ROW_RELATION_SUBQUERY)) {
      plan
    } else {
      rewrite(plan)
    }
  }
}

/**
 * An optimizer rule which combines compatible scalar sub-queries with underlying aggregations.
 * The propose of this rule is to reuse the base plan of compatible scalar sub-queries so as to
 * prune unnecessary sub-queries jobs.
 *
 * This rule doesn't actually clean up sub-queries. Instead, it replaces underlying plans of
 * compatible sub-queries with an unified aggregation plan to make these sub-queries reusable.
 *
 * In overall, the rule can be divided into three steps:
 * <ol>
 *  <li>Extract aggregation, projections and base plans from aggregate-based scalar sub-queries.
 *     Then, put these sub-queries into groups according to their base plans. The base plan
 *     refers to top non-project descendant of Aggregate. Ideally, sub-queries with same kind
 *     of base plan can share a unified combined subquery plan. </li>
 *  <li>Combine aggregate and project expressions with corresponding base plans to build fused
 *      aggregates.</li>
 *   <li>Replace ScalarSubqueries with SharedScalarSubqueries based on fused aggregates.</li>
 * </ol>
 *
 * First example: sub-queries based on multiple fields of the same relation
 * {{{
 * SELECT SUM(i) FROM t
 * WHERE l > (SELECT MIN(l2) FROM t) AND l2 < (SELECT MAX(l) FROM t)
 * AND AND i2 <> (SELECT MAX(i2) FROM t) AND i2 <> (SELECT MIN(i2) FROM t)
 * }}}
 *
 * The optimized (pseudo) logical plan of above query:
 * {{{
 *  Aggregate [sum(i)]
 *  +- Project [i]
 *    +- Filter (((l > scalar-subquery#1) AND (l2 < scalar-subquery#2)) AND
 * (NOT (i2 = scalar-subquery#3) AND NOT (i2 = scalar-subquery#4)))
 *       :  :- Aggregate [min(l2)]
 *       :  :  +- Project [l2]
 *       :  :     +- Relation [l,l2,i,i2]
 *       :  +- Aggregate [max(l)]
 *       :     +- Project [l]
 *       :        +- Relation [l,l2,i,i2]
 *       :  +- Aggregate [max(i2)]
 *       :     +- Project [l]
 *       :        +- Relation [l,l2,i,i2]
 *       :  +- Aggregate [min(i2)]
 *       :     +- Project [l]
 *       :        +- Relation [l,l2,i,i2]
 *       +- Relation [l,l2,i,i2]
 * }}}
 *
 * With current rule, underlying plans of sub-queries are replaced with a fused one:
 * {{{
 *  Aggregate [sum(i)]
 *  +- Project [i]
 *    +- Filter (((l > shared-scalar-subquery#1) AND (l2 < shared-scalar-subquery#2)) AND
 * (NOT (i2 = shared-scalar-subquery#3) AND NOT (i2 = shared-scalar-subquery#4)))
 *       :  :- Aggregate [min(l2),max(l),max(i2),min(i2)]
 *       :  :  +- Project [l2,l,i2]
 *       :  :     +- Relation [l,l2,i,i2]
 *       :  :- Aggregate [min(l2),max(l),max(i2),min(i2)]
 *       :  :  +- Project [l2,l,i2]
 *       :        +- Relation [l,l2,i,i2]
 *       :  :- Aggregate [min(l2),max(l),max(i2),min(i2)]
 *       :  :  +- Project [l2,l,i2]
 *       :        +- Relation [l,l2,i,i2]
 *       :  :- Aggregate [min(l2),max(l),max(i2),min(i2)]
 *       :  :  +- Project [l2,l,i2]
 *       :        +- Relation [l,l2,i,i2]
 *       +- Relation [l,l2,i,i2]
 * }}}
 *
 * Second example: sub-queries of different filter conditions
 * {{{
 * SELECT SUM(i) FROM {0}
 * WHERE l > (SELECT MIN(l + l2 + i) FROM {0} WHERE l > 0)
 * AND l2 < (SELECT MAX(i) + MAX(i2) FROM {0} WHERE l > 0)
 * AND i2 > (SELECT COUNT(IF(i % 2 == 0, 1, NULL)) FROM {0} WHERE l < 0)
 * AND i > (SELECT COUNT(IF(i2 % 2 == 0, 1, NULL)) FROM {0} WHERE l < 0)
 * }}}
 *
 * With current rule, sub-queries sharing same base plan (Filter + Scan) will be combined:
 * {{{
 * Aggregate [sum(i)]
 * +- Project [i]
 *    +- Filter (((l > shared-scalar-subquery#1) AND (l2 < shared-scalar-subquery#2)) AND
 * ((i2 > shared-scalar-subquery#3) AND (i > shared-scalar-subquery#4)))
 *       :  :- Aggregate [min(l + l2 + i), (max(i) + max(i2))]
 *       :  :  +- Project [l, l2, i, i2]
 *       :  :     +- Filter (l#46L > 0)
 *       :  :        +- Relation [l,l2,i,i2]
 *       :  :- Aggregate [min(l + l2 + i), (max(i) + max(i2))]
 *       :  :  +- Project [l, l2, i, i2]
 *       :  :     +- Filter (l#46L > 0)
 *       :  :        +- Relation [l,l2,i,i2]
 *       :  :- Aggregate [count(if (((i % 2) = 0)) 1 else null),
 *                        count(if (((i2 % 2) = 0)) 1 else null)]
 *       :  :  +- Project [i, i2]
 *       :  :     +- Filter (l < 0)
 *       :  :        +- Relation [l,l2,i,i2]
 *       :  :- Aggregate [count(if (((i % 2) = 0)) 1 else null),
 *                        count(if (((i2 % 2) = 0)) 1 else null)]
 *       :  :  +- Project [i, i2]
 *       :  :     +- Filter (l < 0)
 *       :  :        +- Relation [l,l2,i,i2]
 *       +- Relation [l,l2,i,i2]
 * }}}
 */
object CombineScalarSubquery extends Rule[LogicalPlan] {

  // Data structure contains all information extracted from a ScalarSubquery, which is friendly
  // to the subsequent plan combination.
  // Among all fields, basePlan, scalar and aggregate are pretty straightforward.
  // The field `inputRefOrder` aligns with `projects`, which records the binding ordinals in the
  // child plan's output for corresponding project expression. As a Map, the key represents the
  // exprId of project's AttributeReference; the value represents the ordinal to fetch the input
  // data of the leaf expression from the output of the child plan.
  private case class CombineBlock(
      basePlan: LogicalPlan,
      scalar: ExprId,
      aggregate: NamedExpression,
      inputRefOrder: List[Map[ExprId, Int]],
      projects: List[NamedExpression])

  // Data structure holds the result of plan combination. The ordinal of scalar in the scalar list
  // represents the ordinal to fetch its value from the output of the fused plan.
  private case class FusedAggregate(aggregate: Aggregate, scalars: List[ExprId])

  private def createCombineBlock(scalar: ExprId, agg: Aggregate): CombineBlock = {
    agg.child match {
      case prj: Project =>
        val baseOutput = prj.child.output.map(_.exprId)
        val prjExpressions = mutable.ListBuffer.empty[NamedExpression]
        val inputRefs = mutable.ListBuffer.empty[Map[ExprId, Int]]
        prj.projectList.foreach { e =>
          prjExpressions += e
          // Collect all attributeReferences.
          // Then, find and bind their ordinals in the base plan's output.
          inputRefs += e.collectLeaves().collect {
            case ar: AttributeReference =>
              ar.exprId -> baseOutput.indexOf(ar.exprId)
          }.toMap
        }
        CombineBlock(prj.child,
                     scalar,
                     agg.aggregateExpressions.head,
                     inputRefs.toList,
                     prjExpressions.toList)

      case plan =>
        val refOrderMaps = plan.output.zipWithIndex
            .map { case (attr, i) => Map(attr.exprId -> i) }
        CombineBlock(plan,
                     scalar,
                     agg.aggregateExpressions.head,
                     refOrderMaps.toList,
                     plan.output.toList)
    }
  }

  private def mergeCombineBlocks(blocks: Seq[CombineBlock]): FusedAggregate = {
    // Simply choose the first block's base as the global base
    val basePlan = blocks.head.basePlan
    val baseOutput = basePlan.output

    val scalars = ArrayBuffer.empty[ExprId]
    val aggBuffer = ArrayBuffer.empty[NamedExpression]
    val prjBuffer = ArrayBuffer.empty[NamedExpression]
    val prjMap = mutable.Map.empty[LogicalPlan, ExprId]

    blocks.foreach { blk =>
      scalars += blk.scalar

      // Merge project expressions which are semantically equal. And rebase distinct
      // project expressions to the global base.
      val boundProjects: List[ExprId] = blk.projects.zipWithIndex.map { case (p, i) =>
        // reuse project expression among different aggregations if possible
        val singlePrj = Project(p :: Nil, blk.basePlan).canonicalized
        prjMap.getOrElseUpdate(singlePrj, {
          val refMap = blk.inputRefOrder(i)
          val bound = p.transform {
            case ar: AttributeReference =>
              val newExprId = baseOutput(refMap(ar.exprId)).exprId
              ar.withExprId(newExprId)
          }.asInstanceOf[NamedExpression]
          prjBuffer += bound
          bound.exprId
        })
      }

      // Rebase aggregate expressions
      val oldPrjExprIds = blk.projects.map(_.exprId)
      aggBuffer += blk.aggregate.transform {
        case ar: AttributeReference =>
          val newExprId = boundProjects(oldPrjExprIds.indexOf(ar.exprId))
          ar.withExprId(newExprId)
      }.asInstanceOf[NamedExpression]
    }

    val fusedProject = Project(prjBuffer.toList, basePlan)
    val fusedAggregate = Aggregate(Nil, aggBuffer.toList, fusedProject)

    FusedAggregate(fusedAggregate, scalars.toList)
  }

  override def apply(plan: LogicalPlan): LogicalPlan = {

    val builder = mutable.Map.empty[LogicalPlan, ArrayBuffer[CombineBlock]]
    val scalarSet = mutable.HashSet.empty[ExprId]

    // Collect combination candidates from scalar sub-queries. Assign these candidates into
    // groups according to the canonicalized base plan.
    plan.transformAllExpressionsWithPruning(_.containsPattern(SCALAR_SUBQUERY)) {
      case s @ ScalarSubquery(agg: Aggregate, _, _, _)
        if !s.isCorrelated && !scalarSet.contains(s.exprId) =>

        val combBlock = createCombineBlock(s.exprId, agg)
        val blockSeq = builder.getOrElseUpdate(
          combBlock.basePlan.canonicalized, ArrayBuffer.empty[CombineBlock])
        blockSeq += combBlock
        scalarSet += s.exprId
        s
    }

    // Combine plans which share the same base to form fused aggregates. And create links between
    // scalar sub-queries and fused aggregates.
    val fusedAggregates = builder.filter(_._2.length > 1)
        .flatMap { case (_, blocks) =>
          val fusedAgg = mergeCombineBlocks(blocks.toList)
          fusedAgg.scalars.indices.map { i =>
            fusedAgg.scalars(i) -> Tuple2(i, fusedAgg.aggregate)
          }
        }.toMap

    // Replace scalar sub-queries, which share base with other queries, with sharedScalarSubquery
    // of fused aggregate.
    if (fusedAggregates.isEmpty) {
      plan
    } else {
      plan.transformAllExpressionsWithPruning(_.containsPattern(SCALAR_SUBQUERY)) {
        case s: ScalarSubquery if fusedAggregates.contains(s.exprId) =>
          val (ordinal, agg) = fusedAggregates(s.exprId)
          SharedScalarSubquery(agg, ordinal, s.outerAttrs, s.exprId, s.joinCond)
      }
    }
  }
}
