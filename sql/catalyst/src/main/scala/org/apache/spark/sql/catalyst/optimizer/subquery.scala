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

import org.apache.spark.sql.AnalysisException
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.expressions.ScalarSubquery._
import org.apache.spark.sql.catalyst.expressions.SubExprUtils._
import org.apache.spark.sql.catalyst.expressions.aggregate._
import org.apache.spark.sql.catalyst.plans._
import org.apache.spark.sql.catalyst.plans.logical._
import org.apache.spark.sql.catalyst.rules._
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
            throw new AnalysisException("Found conflicting attributes " +
              s"${conflictingAttrs.mkString(",")} in the condition joining outer plan:\n  " +
              s"$outerPlan\nand subplan:\n  $subplan")
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

  def apply(plan: LogicalPlan): LogicalPlan = plan transform {
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
        case (p, Exists(sub, conditions, _)) =>
          val (joinCond, outerPlan) = rewriteExistentialExpr(conditions, p)
          buildJoin(outerPlan, sub, LeftSemi, joinCond)
        case (p, Not(Exists(sub, conditions, _))) =>
          val (joinCond, outerPlan) = rewriteExistentialExpr(conditions, p)
          buildJoin(outerPlan, sub, LeftAnti, joinCond)
        case (p, InSubquery(values, ListQuery(sub, conditions, _, _))) =>
          // Deduplicate conflicting attributes if any.
          val newSub = dedupSubqueryOnSelfJoin(p, sub, Some(values))
          val inConditions = values.zip(newSub.output).map(EqualTo.tupled)
          val (joinCond, outerPlan) = rewriteExistentialExpr(inConditions ++ conditions, p)
          Join(outerPlan, newSub, LeftSemi, joinCond, JoinHint.NONE)
        case (p, Not(InSubquery(values, ListQuery(sub, conditions, _, _)))) =>
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
      e transformDown {
        case Exists(sub, conditions, _) =>
          val exists = AttributeReference("exists", BooleanType, nullable = false)()
          newPlan =
            buildJoin(newPlan, sub, ExistenceJoin(exists), conditions.reduceLeftOption(And))
          exists
        case Not(InSubquery(values, ListQuery(sub, conditions, _, _))) =>
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
        case InSubquery(values, ListQuery(sub, conditions, _, _)) =>
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
      outer: Seq[LogicalPlan]): (LogicalPlan, Seq[Expression]) = {
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
    val (newPlan, newCond) = if (outer.nonEmpty) {
      val outputSet = outer.map(_.outputSet).reduce(_ ++ _)
      val (plan, deDuplicatedConditions) =
        DecorrelateInnerQuery.deduplicate(transformed, baseConditions, outputSet)
      (plan, stripOuterReferences(deDuplicatedConditions))
    } else {
      (transformed, stripOuterReferences(baseConditions))
    }
    (newPlan, newCond)
  }

  private def rewriteSubQueries(plan: LogicalPlan, outerPlans: Seq[LogicalPlan]): LogicalPlan = {
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

    def rewrite(sub: LogicalPlan, outer: Seq[LogicalPlan]): (LogicalPlan, Seq[Expression]) = {
      if (SQLConf.get.decorrelateInnerQueryEnabled) {
        DecorrelateInnerQuery(sub, outer)
      } else {
        pullOutCorrelatedPredicates(sub, outer)
      }
    }

    plan transformExpressions {
      case ScalarSubquery(sub, children, exprId) if children.nonEmpty =>
        val (newPlan, newCond) = rewrite(sub, outerPlans)
        ScalarSubquery(newPlan, getJoinCondition(newCond, children), exprId)
      case Exists(sub, children, exprId) if children.nonEmpty =>
        val (newPlan, newCond) = pullOutCorrelatedPredicates(sub, outerPlans)
        Exists(newPlan, getJoinCondition(newCond, children), exprId)
      case ListQuery(sub, children, exprId, childOutputs) if children.nonEmpty =>
        val (newPlan, newCond) = pullOutCorrelatedPredicates(sub, outerPlans)
        ListQuery(newPlan, getJoinCondition(newCond, children), exprId, childOutputs)
    }
  }

  /**
   * Pull up the correlated predicates and rewrite all subqueries in an operator tree..
   */
  def apply(plan: LogicalPlan): LogicalPlan = plan transformUp {
    case f @ Filter(_, a: Aggregate) =>
      rewriteSubQueries(f, Seq(a, a.child))
    // Only a few unary nodes (Project/Filter/Aggregate) can contain subqueries.
    case q: UnaryNode =>
      rewriteSubQueries(q, q.children)
    case s: SupportsSubquery =>
      rewriteSubQueries(s, s.children)
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
    val newExpression = expression transform {
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
  private def evalAggOnZeroTups(expr: Expression) : Expression = {
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

      case Aggregate(_, aggExprs, _) =>
        // Some of the expressions under the Aggregate node are the join columns
        // for joining with the outer query block. Fill those expressions in with
        // nulls and statically evaluate the remainder.
        aggExprs.map {
          case ref: AttributeReference => (ref.exprId, Literal.create(null, ref.dataType))
          case alias @ Alias(_: AttributeReference, _) =>
            (alias.exprId, Literal.create(null, alias.dataType))
          case alias @ Alias(l: Literal, _) =>
            (alias.exprId, l.copy(value = null))
          case ne => (ne.exprId, evalAggOnZeroTups(ne))
        }.toMap

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
          sys.error(s"Correlated subquery has unexpected operator $op below filter")

        case op @ _ => sys.error(s"Unexpected operator $op in correlated subquery")
      }
    }

    sys.error("This line should be unreachable")
  }

  // Name of generated column used in rewrite below
  val ALWAYS_TRUE_COLNAME = "alwaysTrue"

  /**
   * Build a mapping between domain attributes and corresponding outer query expressions
   * using the join conditions.
   */
  private def buildDomainAttrMap(
      conditions: Seq[Expression],
      domainAttrs: Seq[Attribute]): Map[Attribute, Expression] = {
    val outputSet = AttributeSet(domainAttrs)
    conditions.collect {
      // When we build the equality conditions, the left side is always the
      // domain attributes used in the inner plan, and the right side is the
      // attribute from outer plan. Note the right hand side is not necessarily
      // an attribute, for example it can be a literal (if foldable) or a cast expression.
      case EqualNullSafe(left: Attribute, right: Expression) if outputSet.contains(left) =>
        left -> right
    }.toMap
  }

  /**
   * Rewrite domain join placeholder to actual inner joins.
   */
  private def rewriteDomainJoins(
      outerPlan: LogicalPlan,
      innerPlan: LogicalPlan,
      conditions: Seq[Expression]): LogicalPlan = {
    innerPlan transform {
      case d @ DomainJoin(domainAttrs, child) =>
        val domainAttrMap = buildDomainAttrMap(conditions, domainAttrs)
        // We should only rewrite a domain join when all corresponding outer plan attributes
        // can be found from the join condition.
        if (domainAttrMap.size == domainAttrs.size) {
          val groupingExprs = domainAttrs.map(domainAttrMap)
          val aggregateExprs = groupingExprs.zip(domainAttrs).map {
            // Rebuild the aliases.
            case (inputAttr, outputAttr) => Alias(inputAttr, outputAttr.name)(outputAttr.exprId)
          }
          val domain = Aggregate(groupingExprs, aggregateExprs, outerPlan)
          child match {
            // A special optimization for OneRowRelation.
            // TODO: add a more general rule to optimize join with OneRowRelation.
            case _: OneRowRelation => domain
            case _ => Join(child, domain, Inner, None, JoinHint.NONE)
          }
        } else {
          throw new UnsupportedOperationException(
            s"Unable to rewrite domain join with conditions: $conditions\n$d")
        }
    }
  }

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
      case (currentChild, ScalarSubquery(sub, conditions, _)) =>
        val query = rewriteDomainJoins(currentChild, sub, conditions)
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
              case op => sys.error(s"Unexpected operator $op in correlated subquery")
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
    case a @ Aggregate(grouping, _, child) =>
      val subqueries = ArrayBuffer.empty[ScalarSubquery]
      val rewriteExprs = a.aggregateExpressionsWithoutGroupingRefs
        .map(extractCorrelatedScalarSubqueries(_, subqueries))
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
 * Decorrelate the inner query by eliminating outer references and create domain joins.
 * The implementation is based on the paper: Unnesting Arbitrary Queries by Thomas Neumann
 * and Alfons Kemper. https://dl.gi.de/handle/20.500.12116/2418.
 * (1) Recursively collects outer references from the inner query until it reaches a node
 *     that does not contain correlated value.
 * (2) Inserts an optional [[DomainJoin]] node to indicate whether a domain (inner) join is
 *     needed between the outer query and the specific subtree of the inner query.
 * (3) Returns a list of join conditions with the outer query and a mapping between outer
 *     references with references inside the inner query. The parent nodes need to preserve
 *     the references inside the join conditions and substitute all outer references using
 *     the mapping.
 *
 * E.g. decorrelate an inner query with equality predicates:
 *
 * Aggregate [] [min(c2)]            Aggregate [c1] [min(c2), c1]
 * +- Filter [outer(c3) = c1]   =>   +- Relation [t]
 *    +- Relation [t]
 *
 * Join conditions: [c3 = c1]
 *
 * E.g. decorrelate an inner query with non-equality predicates:
 *
 * Aggregate [] [min(c2)]            Aggregate [c3'] [min(c2), c3']
 * +- Filter [outer(c3) > c1]   =>   +- Filter [c3' > c1]
 *    +- Relation [t]                   +- DomainJoin [c3']
 *                                         +- Relation [t]
 *
 * Join conditions: [c3 <=> c3']
 */
object DecorrelateInnerQuery extends PredicateHelper {

  /**
   * Check if the given expression is an equality condition.
   */
  private def isEquality(expression: Expression): Boolean = expression match {
    case Equality(_, _) => true
    case _ => false
  }

  /**
   * Collect outer references in an expressions that are in the output attributes of the outer plan.
   */
  private def collectOuterReferences(expression: Expression): AttributeSet = {
    AttributeSet(expression.collect { case o: OuterReference => o.toAttribute })
  }

  /**
   * Collect outer references in a sequence of expressions that are in the output attributes
   * of the outer plan.
   */
  private def collectOuterReferences(expressions: Seq[Expression]): AttributeSet = {
    AttributeSet.fromAttributeSets(expressions.map(collectOuterReferences))
  }

  /**
   * Build a mapping between outer references with equivalent inner query attributes.
   * E.g. [outer(a) = x, y = outer(b), outer(c) = z + 1] => {a -> x, b -> y}
   */
  private def collectEquivalentOuterReferences(
      expressions: Seq[Expression]): Map[Attribute, Attribute] = {
    expressions.collect {
      case Equality(o: OuterReference, a: Attribute) => (o.toAttribute, a.toAttribute)
      case Equality(a: Attribute, o: OuterReference) => (o.toAttribute, a.toAttribute)
    }.toMap
  }

  /**
   * Replace all outer references using the expressions in the given outer reference map.
   */
  private def replaceOuterReference[E <: Expression](
      expression: E,
      outerReferenceMap: Map[Attribute, Attribute]): E = {
    expression.transform {
      case o: OuterReference => outerReferenceMap.getOrElse(o.toAttribute, o)
    }.asInstanceOf[E]
  }

  /**
   * Replace all outer references in the given expressions using the expressions in the
   * outer reference map.
   */
  private def replaceOuterReferences[E <: Expression](
      expressions: Seq[E],
      outerReferenceMap: Map[Attribute, Attribute]): Seq[E] = {
    expressions.map(replaceOuterReference(_, outerReferenceMap))
  }

  /**
   * Return all missing references of the attribute set from the required attributes
   * in the join condition.
   */
  private def missingReferences(
      expressions: Seq[Expression],
      joinCond: Seq[Expression]): AttributeSet = {
    val outputSet = AttributeSet(expressions)
    AttributeSet(joinCond.flatMap(_.references)) -- outputSet
  }

  /**
   * Deduplicate the inner and the outer query attributes and return an aliased
   * subquery plan and join conditions if duplicates are found. Duplicated attributes
   * can break the structural integrity when joining the inner and outer plan together.
   */
  def deduplicate(
      innerPlan: LogicalPlan,
      conditions: Seq[Expression],
      outerOutputSet: AttributeSet): (LogicalPlan, Seq[Expression]) = {
    val duplicates = innerPlan.outputSet.intersect(outerOutputSet)
    if (duplicates.nonEmpty) {
      val aliasMap = AttributeMap(duplicates.map { dup =>
        dup -> Alias(dup, dup.toString)()
      }.toSeq)
      val aliasedExpressions = innerPlan.output.map { ref =>
        aliasMap.getOrElse(ref, ref)
      }
      val aliasedProjection = Project(aliasedExpressions, innerPlan)
      val aliasedConditions = conditions.map(_.transform {
        case ref: Attribute => aliasMap.getOrElse(ref, ref).toAttribute
      })
      (aliasedProjection, aliasedConditions)
    } else {
      (innerPlan, conditions)
    }
  }

  def apply(
      innerPlan: LogicalPlan,
      outerPlan: LogicalPlan): (LogicalPlan, Seq[Expression]) = {
    apply(innerPlan, Seq(outerPlan))
  }

  def apply(
      innerPlan: LogicalPlan,
      outerPlans: Seq[LogicalPlan]): (LogicalPlan, Seq[Expression]) = {
    val outputSet = AttributeSet(outerPlans.flatMap(_.outputSet))

    // The return type of the recursion.
    // The first parameter is a new logical plan with correlation eliminated.
    // The second parameter is a list of join conditions with the outer query.
    // The third parameter is a mapping between the outer references and equivalent
    // expressions from the inner query that is used to replace outer references.
    type ReturnType = (LogicalPlan, Seq[Expression], Map[Attribute, Attribute])

    // Recursively decorrelate the input plan with a set of parent outer references and
    // a boolean flag indicating whether the result of the plan will be aggregated.
    def decorrelate(
        plan: LogicalPlan,
        parentOuterReferences: AttributeSet,
        aggregated: Boolean = false): ReturnType = {
      val isCorrelated = hasOuterReferences(plan)
      if (!isCorrelated) {
        // We have reached a plan without correlation to the outer plan.
        if (parentOuterReferences.isEmpty) {
          // If there is no outer references from the parent nodes, it means all outer
          // attributes can be substituted by attributes from the inner plan. So no
          // domain join is needed.
          (plan, Nil, Map.empty[Attribute, Attribute])
        } else {
          // Build the domain join with the parent outer references.
          val attributes = parentOuterReferences.toSeq
          val domains = attributes.map(_.newInstance())
          // A placeholder to be rewritten into domain join.
          val domainJoin = DomainJoin(domains, plan)
          val outerReferenceMap = attributes.zip(domains).toMap
          // Build join conditions between domain attributes and outer references.
          // EqualNullSafe is used to make sure null key can be joined together. Note
          // outer referenced attributes can be changed during the outer query optimization.
          // The equality conditions will also serve as an attribute mapping between new
          // outer references and domain attributes when rewriting the domain joins.
          // E.g. if the attribute a is changed to a1, the join condition a' <=> outer(a)
          // will become a' <=> a1, and we can construct the aliases based on the condition:
          // DomainJoin [a']        Join Inner
          // +- InnerQuery     =>   :- InnerQuery
          //                        +- Aggregate [a1] [a1 AS a']
          //                           +- OuterQuery
          val conditions = outerReferenceMap.map {
            case (o, a) => EqualNullSafe(a, OuterReference(o))
          }
          (domainJoin, conditions.toSeq, outerReferenceMap)
        }
      } else {
        // Collect outer references from the current node.
        val outerReferences = collectOuterReferences(plan.expressions)
        plan match {
          case Filter(condition, child) =>
            val (correlated, uncorrelated) =
              splitConjunctivePredicates(condition)
              .partition(containsOuter)
            val (equality, nonEquality) = correlated.partition(isEquality)
            // Find equivalent outer reference relations and remove equivalent attributes from
            // parentOuterReferences since they can be replaced directly by expressions
            // inside the inner plan.
            val equivalences = collectEquivalentOuterReferences(equality)
            // When the results are aggregated, outer references inside the non-equality
            // predicates cannot be used directly as join conditions with the outer query.
            val outerReferences = if (aggregated) {
              collectOuterReferences(nonEquality)
            } else {
              AttributeSet.empty
            }
            val newOuterReferences = parentOuterReferences ++ outerReferences -- equivalences.keySet
            val (newChild, joinCond, outerReferenceMap) =
              decorrelate(child, newOuterReferences, aggregated)
            // Add the mapping from the current node.
            val newOuterReferenceMap = outerReferenceMap ++ equivalences
            // Replace all outer references in non-equality filter conditions using the domain
            // attributes produced for inner query with aggregates. This step is necessary
            // for pushing down the non-equality filters into the domain join as join conditions.
            val (newFilterCond, newJoinCond) = if (aggregated) {
              val nonEqualityCond = replaceOuterReferences(nonEquality, newOuterReferenceMap)
              (nonEqualityCond ++ uncorrelated, equality)
            } else {
              (uncorrelated, correlated)
            }
            val newFilter = newFilterCond match {
              case Nil => newChild
              case xs => Filter(xs.reduce(And), newChild)
            }
            (newFilter, joinCond ++ newJoinCond, newOuterReferenceMap)

          case Project(projectList, child) =>
            val newOuterReferences = parentOuterReferences ++ outerReferences
            val (newChild, joinCond, outerReferenceMap) =
              decorrelate(child, newOuterReferences, aggregated)
            // Replace all outer references in the original project list.
            val newProjectList = replaceOuterReferences(projectList, outerReferenceMap)
            // Preserve required domain attributes in the join condition by adding the missing
            // references to the new project list.
            val referencesToAdd = missingReferences(newProjectList.map(_.toAttribute), joinCond)
            val newProject = Project(newProjectList ++ referencesToAdd, newChild)
            (newProject, joinCond, outerReferenceMap)

          case a @ Aggregate(groupingExpressions, aggregateExpressions, child) =>
            val newOuterReferences = parentOuterReferences ++ outerReferences
            val (newChild, joinCond, outerReferenceMap) =
              decorrelate(child, newOuterReferences, aggregated = true)
            // Replace all outer references in grouping and aggregate expressions.
            val newGroupingExpr = replaceOuterReferences(groupingExpressions, outerReferenceMap)
            val newAggExpr = replaceOuterReferences(aggregateExpressions, outerReferenceMap)
            // Add all required domain attributes to both grouping and aggregate expressions.
            val groupingExprToAdd = missingReferences(newGroupingExpr, joinCond)
            val aggExprToAdd = missingReferences(newAggExpr.map(_.toAttribute), joinCond)
            val newAggregate = a.copy(
              groupingExpressions = newGroupingExpr ++ groupingExprToAdd,
              aggregateExpressions = newAggExpr ++ aggExprToAdd,
              child = newChild)
            (newAggregate, joinCond, outerReferenceMap)

          case j @ Join(left, right, joinType, condition, _) =>
            // Join condition containing outer references is not supported.
            assert(outerReferences.isEmpty, s"Correlated column is not allowed in join: $j")
            val newOuterReferences = parentOuterReferences ++ outerReferences
            val shouldPushToLeft = joinType match {
              case LeftOuter | LeftSemiOrAnti(_) | FullOuter => true
              case _ => hasOuterReferences(left)
            }
            val shouldPushToRight = joinType match {
              case RightOuter | FullOuter => true
              case _ => hasOuterReferences(right)
            }
            val (newLeft, leftJoinCond, leftOuterReferenceMap) = if (shouldPushToLeft) {
              decorrelate(left, newOuterReferences, aggregated)
            } else {
              (left, Nil, Map.empty[Attribute, Attribute])
            }
            val (newRight, rightJoinCond, rightOuterReferenceMap) = if (shouldPushToRight) {
              decorrelate(right, newOuterReferences, aggregated)
            } else {
              (right, Nil, Map.empty[Attribute, Attribute])
            }
            val newOuterReferenceMap = leftOuterReferenceMap ++ rightOuterReferenceMap
            val newJoinCond = leftJoinCond ++ rightJoinCond
            // If we push the dependent join to both sides, we will need to augment the join
            // condition such that both sides are matched on the domain attributes.
            val augmentedConditions = leftOuterReferenceMap.flatMap {
              case (outer, inner) => rightOuterReferenceMap.get(outer).map(EqualNullSafe(inner, _))
            }
            val newCondition = (condition ++ augmentedConditions).reduceOption(And)
            val newJoin = j.copy(left = newLeft, right = newRight, condition = newCondition)
            (newJoin, newJoinCond, newOuterReferenceMap)

          case s: UnaryNode =>
            assert(outerReferences.isEmpty, s"Correlated column is not allowed in $s")
            decorrelate(s.child, parentOuterReferences, aggregated)

          case o =>
            throw new UnsupportedOperationException(
              s"Push down dependent joins through $o is not supported.")
        }
      }
    }
    val (newChild, joinCond, _) = decorrelate(BooleanSimplification(innerPlan), AttributeSet.empty)
    val (plan, conditions) = deduplicate(newChild, joinCond, outputSet)
    (plan, stripOuterReferences(conditions))
  }
}
