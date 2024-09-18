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

import org.apache.spark.SparkException
import org.apache.spark.sql.AnalysisException
import org.apache.spark.sql.catalyst.analysis.EliminateSubqueryAliases
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.expressions.ScalarSubquery._
import org.apache.spark.sql.catalyst.expressions.SubExprUtils._
import org.apache.spark.sql.catalyst.expressions.aggregate._
import org.apache.spark.sql.catalyst.optimizer.RewriteCorrelatedScalarSubquery.splitSubquery
import org.apache.spark.sql.catalyst.plans._
import org.apache.spark.sql.catalyst.plans.logical._
import org.apache.spark.sql.catalyst.rules._
import org.apache.spark.sql.catalyst.trees.TreePattern.{EXISTS_SUBQUERY, IN_SUBQUERY, LATERAL_JOIN, LIST_SUBQUERY, PLAN_EXPRESSION, SCALAR_SUBQUERY}
import org.apache.spark.sql.errors.{QueryCompilationErrors, QueryExecutionErrors}
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.internal.SQLConf.{DECORRELATE_PREDICATE_SUBQUERIES_IN_JOIN_CONDITION, OPTIMIZE_UNCORRELATED_IN_SUBQUERIES_IN_JOIN_CONDITION,
  WRAP_EXISTS_IN_AGGREGATE_FUNCTION}
import org.apache.spark.sql.types._
import org.apache.spark.util.Utils

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
      condition: Option[Expression],
      subHint: Option[HintInfo]): Join = {
    // Deduplicate conflicting attributes if any.
    val dedupSubplan = dedupSubqueryOnSelfJoin(outerPlan, subplan, None, condition)
    // Add subquery hint as right hint as the subquery plan is on the right side of the join
    Join(outerPlan, dedupSubplan, joinType, condition, JoinHint(None, subHint))
  }

  private def rewriteDomainJoinsIfPresent(
      outerPlan: LogicalPlan,
      sub: LogicalPlan,
      joinCond: Option[Expression]): LogicalPlan = {
    if (SQLConf.get.decorrelateInnerQueryEnabledForExistsIn) {
      DecorrelateInnerQuery.rewriteDomainJoins(outerPlan,
        sub, splitConjunctivePredicates(joinCond.get))
    } else {
      sub
    }
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
      })
      val aliasedExpressions = subplan.output.map { ref =>
        rewrites.getOrElse(ref, ref)
      }
      Project(aliasedExpressions, subplan)
    } else {
      subplan
    }
  }

  def apply(plan: LogicalPlan): LogicalPlan = plan.transformWithPruning(
    _.containsAnyPattern(EXISTS_SUBQUERY, LIST_SUBQUERY)) {
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
        case (p, Exists(sub, _, _, conditions, subHint)) =>
          val (joinCond, outerPlan) = rewriteExistentialExpr(conditions, p)
          val join = buildJoin(outerPlan, rewriteDomainJoinsIfPresent(outerPlan, sub, joinCond),
            LeftSemi, joinCond, subHint)
          Project(p.output, join)
        case (p, Not(Exists(sub, _, _, conditions, subHint))) =>
          val (joinCond, outerPlan) = rewriteExistentialExpr(conditions, p)
          val join = buildJoin(outerPlan, rewriteDomainJoinsIfPresent(outerPlan, sub, joinCond),
            LeftAnti, joinCond, subHint)
          Project(p.output, join)
        case (p, InSubquery(values, ListQuery(sub, _, _, _, conditions, subHint))) =>
          // Deduplicate conflicting attributes if any.
          val newSub = dedupSubqueryOnSelfJoin(p, sub, Some(values))
          val inConditions = values.zip(newSub.output).map(EqualTo.tupled)
          val (joinCond, outerPlan) = rewriteExistentialExpr(inConditions ++ conditions, p)
          val join = Join(outerPlan, rewriteDomainJoinsIfPresent(outerPlan, newSub, joinCond),
            LeftSemi, joinCond, JoinHint(None, subHint))
          Project(p.output, join)
        case (p, Not(InSubquery(values, ListQuery(sub, _, _, _, conditions, subHint)))) =>
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
          Join(outerPlan, rewriteDomainJoinsIfPresent(outerPlan, newSub, Some(finalJoinCond)),
            LeftAnti, Option(finalJoinCond), JoinHint(None, subHint))
        case (p, predicate) =>
          val (newCond, inputPlan) = rewriteExistentialExpr(Seq(predicate), p)
          Project(p.output, Filter(newCond.get, inputPlan))
      }

    // This case takes care of predicate subqueries in join conditions that are not pushed down
    // to the children nodes by [[PushDownPredicates]].
    case j: Join if j.condition.exists(cond =>
      SubqueryExpression.hasInOrCorrelatedExistsSubquery(cond)) &&
      conf.getConf(DECORRELATE_PREDICATE_SUBQUERIES_IN_JOIN_CONDITION) =>

      val optimizeUncorrelatedInSubqueries =
        conf.getConf(OPTIMIZE_UNCORRELATED_IN_SUBQUERIES_IN_JOIN_CONDITION)
      val relevantSubqueries = j.condition.get.collect {
        case i: InSubquery if i.query.isCorrelated => i
        case i: InSubquery if !i.query.isCorrelated && optimizeUncorrelatedInSubqueries => i
        case e: Exists if e.isCorrelated => e
      }
      if (relevantSubqueries.isEmpty) {
        j
      } else {
        // `subqueriesWithJoinInputReferenceInfo`is of type Seq[(Expression, Boolean, Boolean)]
        // (1): Expression, the join predicate containing some predicate subquery we are interested
        // in re-writing
        // (2): Boolean, whether (1) references the left join input
        // (3): Boolean, whether (1) references the right join input
        val subqueriesWithJoinInputReferenceInfo = relevantSubqueries.map { e =>
          val referenceLeft = e.references.intersect(j.left.outputSet).nonEmpty
          val referenceRight = e.references.intersect(j.right.outputSet).nonEmpty
          (e, referenceLeft, referenceRight)
        }
        val subqueriesReferencingBothJoinInputs = subqueriesWithJoinInputReferenceInfo
          .filter(i => i._2 && i._3)

        // Currently do not support correlated subqueries in the join predicate that reference both
        // join inputs
        if (subqueriesReferencingBothJoinInputs.nonEmpty) {
          throw QueryCompilationErrors.unsupportedCorrelatedSubqueryInJoinConditionError(
            subqueriesReferencingBothJoinInputs.map(_._1))
        }
        val subqueriesReferencingLeft = subqueriesWithJoinInputReferenceInfo.filter(_._2).map(_._1)
        val subqueriesReferencingRight = subqueriesWithJoinInputReferenceInfo.filter(_._3).map(_._1)
        if (subqueriesReferencingLeft.isEmpty && subqueriesReferencingRight.isEmpty) {
          j
        } else {
          var newCondition = j.condition.get
          val newLeft = subqueriesReferencingLeft.foldLeft(j.left) {
            case (p, e) =>
              val (newCond, newInputPlan) = rewriteExistentialExpr(Seq(e), p)
              // Update the join condition to rewrite the subquery expression
              newCondition = newCondition.transform {
                case expr if expr.fastEquals(e) => newCond.get
              }
              newInputPlan
          }
          val newRight = subqueriesReferencingRight.foldLeft(j.right) {
            case (p, e) =>
              val (newCond, newInputPlan) = rewriteExistentialExpr(Seq(e), p)
              // Update the join condition to rewrite the subquery expression
              newCondition = newCondition.transform {
                case expr if expr.fastEquals(e) => newCond.get
              }
              newInputPlan
          }
          // Remove unwanted exists columns from new existence joins with new Project
          Project(j.output, j.copy(left = newLeft, right = newRight,
            condition = Some(newCondition)))
        }
      }

    case u: UnaryNode if u.expressions.exists(
        SubqueryExpression.hasInOrCorrelatedExistsSubquery) =>
      var newChild = u.child
      var introducedAttrs = Seq.empty[Attribute]
      val updatedNode = u.mapExpressions(expr => {
        val (newExpr, p, newAttrs) = rewriteExistentialExprWithAttrs(Seq(expr), newChild)
        newChild = p
        introducedAttrs ++= newAttrs
        // The newExpr can not be None
        newExpr.get
      }).withNewChildren(Seq(newChild))
      updatedNode match {
        case a: Aggregate if conf.getConf(WRAP_EXISTS_IN_AGGREGATE_FUNCTION) =>
          // If we have introduced new `exists`-attributes that are referenced by
          // aggregateExpressions within a non-aggregateFunction expression, we wrap them in
          // first() aggregate function. first() is Spark's executable version of any_value()
          // aggregate function.
          // We do this to keep the aggregation valid, i.e avoid references outside of aggregate
          // functions that are not in grouping expressions.
          // Note that the same `exists` attr will never appear in groupingExpressions due to
          // PullOutGroupingExpressions rule.
          // Also note: the value of `exists` is functionally determined by grouping expressions,
          // so applying any aggregate function is semantically safe.
          val aggFunctionReferences = a.aggregateExpressions.
            flatMap(extractAggregateExpressions).
            flatMap(_.references).toSet
          val nonAggFuncReferences =
            a.aggregateExpressions.flatMap(_.references).filterNot(aggFunctionReferences.contains)
          val toBeWrappedExistsAttrs = introducedAttrs.filter(nonAggFuncReferences.contains)

          // Replace all eligible `exists` by `First(exists)` among aggregateExpressions.
          val newAggregateExpressions = a.aggregateExpressions.map { aggExpr =>
            aggExpr.transformUp {
              case attr: Attribute if toBeWrappedExistsAttrs.contains(attr) =>
                new First(attr).toAggregateExpression()
            }.asInstanceOf[NamedExpression]
          }
          a.copy(aggregateExpressions = newAggregateExpressions)
        case _ => updatedNode
      }
  }

  /**
   * Extract all aggregate expressions from the expression tree routed at `expr`.
   */
  private def extractAggregateExpressions(expr: Expression): Seq[AggregateExpression] = {
    expr match {
      case a: AggregateExpression => Seq(a)
      case e: Expression => e.children.flatMap(extractAggregateExpressions)
    }
  }

  /**
   * Given a predicate expression and an input plan, it rewrites any embedded existential sub-query
   * into an existential join. It returns the rewritten expression together with the updated plan,
   * as well as the newly introduced attributes.
   * Currently, it does not support NOT IN nested inside a NOT expression. This case is blocked in
   * the Analyzer.
   */
  private def rewriteExistentialExpr(
    exprs: Seq[Expression],
    plan: LogicalPlan): (Option[Expression], LogicalPlan) = {
    val (newExpr, newPlan, _) = rewriteExistentialExprWithAttrs(exprs, plan)
    (newExpr, newPlan)
  }

  private def rewriteExistentialExprWithAttrs(
    exprs: Seq[Expression],
    plan: LogicalPlan): (Option[Expression], LogicalPlan, Seq[Attribute]) = {
    var newPlan = plan
    val introducedAttrs = ArrayBuffer.empty[Attribute]
    val newExprs = exprs.map { e =>
      e.transformDownWithPruning(_.containsAnyPattern(EXISTS_SUBQUERY, IN_SUBQUERY)) {
        case Exists(sub, _, _, conditions, subHint) =>
          val exists = AttributeReference("exists", BooleanType, nullable = false)()
          val existenceJoin = ExistenceJoin(exists)
          val newCondition = conditions.reduceLeftOption(And)
          newPlan =
            buildJoin(newPlan, rewriteDomainJoinsIfPresent(newPlan, sub, newCondition),
              existenceJoin, newCondition, subHint)
          introducedAttrs += exists
          exists
        case Not(InSubquery(values, ListQuery(sub, _, _, _, conditions, subHint))) =>
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
          val joinHint = JoinHint(None, subHint)
          newPlan = Join(newPlan,
            rewriteDomainJoinsIfPresent(newPlan, newSub, Some(finalJoinCond)),
            ExistenceJoin(exists), Some(finalJoinCond), joinHint)
          introducedAttrs += exists
          Not(exists)
        case InSubquery(values, ListQuery(sub, _, _, _, conditions, subHint)) =>
          val exists = AttributeReference("exists", BooleanType, nullable = false)()
          // Deduplicate conflicting attributes if any.
          val newSub = dedupSubqueryOnSelfJoin(newPlan, sub, Some(values))
          val inConditions = values.zip(newSub.output).map(EqualTo.tupled)
          val newConditions = (inConditions ++ conditions).reduceLeftOption(And)
          val joinHint = JoinHint(None, subHint)
          newPlan = Join(newPlan, rewriteDomainJoinsIfPresent(newPlan, newSub, newConditions),
            ExistenceJoin(exists), newConditions, joinHint)
          introducedAttrs += exists
          exists
      }
    }
    (newExprs.reduceOption(And), newPlan, introducedAttrs.toSeq)
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

      case l @ Limit(_, _) if predicateMap.nonEmpty =>
        // LIMIT is not supported in PullupCorrelatedPredicates, EXISTS/IN subqueries should be
        // handled by DecorrelateInnerQuery.
        throw new AnalysisException(
          errorClass =
            "UNSUPPORTED_SUBQUERY_EXPRESSION_CATEGORY.ACCESSING_OUTER_QUERY_COLUMN_IS_NOT_ALLOWED",
          origin = l.origin,
          messageParameters = Map("treeNode" -> l.toString))
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

   private def guaranteedToReturnOneRow(query: LogicalPlan): Boolean = {
     if (query.maxRows.exists(_ <= 1)) {
       return true
     }
     val aggNode = query match {
       case havingPart@Filter(_, aggPart: Aggregate) => Some(aggPart)
       case aggPart: Aggregate => Some(aggPart)
       // LIMIT 1 is handled above, this is for all other types of LIMITs
       case Limit(_, aggPart: Aggregate) => Some(aggPart)
       case Project(_, aggPart: Aggregate) => Some(aggPart)
       case _: LogicalPlan => None
     }
     (aggNode.isDefined && aggNode.get.groupingExpressions.isEmpty)
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
      case ScalarSubquery(sub, children, exprId, conditions, hint,
      mayHaveCountBugOld, needSingleJoinOld)
        if children.nonEmpty =>

        def mayHaveCountBugAgg(a: Aggregate): Boolean = {
          a.groupingExpressions.isEmpty && a.aggregateExpressions.exists(_.exists {
            case a: AggregateExpression => a.aggregateFunction.defaultResult.isDefined
            case _ => false
          })
        }

        // The below logic controls handling count bug for scalar subqueries in
        // [[DecorrelateInnerQuery]], and if we don't handle it here, we handle it in
        // [[RewriteCorrelatedScalarSubquery#constructLeftJoins]]. Note that handling it in
        // [[DecorrelateInnerQuery]] is always correct, and turning it off to handle it in
        // constructLeftJoins is an optimization, so that additional, redundant left outer joins are
        // not introduced.
        val handleCountBugInDecorrelate = SQLConf.get.decorrelateInnerQueryEnabled &&
          !conf.getConf(SQLConf.LEGACY_SCALAR_SUBQUERY_COUNT_BUG_HANDLING) && !(sub match {
          // Handle count bug only if there exists lower level Aggs with count bugs. It does not
          // matter if the top level agg is count bug vulnerable or not, because:
          // 1. If the top level agg is count bug vulnerable, it can be handled in
          // constructLeftJoins, unless there are lower aggs that are count bug vulnerable.
          // E.g. COUNT(COUNT + COUNT)
          // 2. If the top level agg is not count bug vulnerable, it can be count bug vulnerable if
          // there are lower aggs that are count bug vulnerable. E.g. SUM(COUNT)
          case agg: Aggregate => !agg.child.exists {
            case lowerAgg: Aggregate => mayHaveCountBugAgg(lowerAgg)
            case _ => false
          }
          case _ => false
        })
        val (newPlan, newCond) = decorrelate(sub, plan, handleCountBugInDecorrelate)
        val mayHaveCountBug = if (mayHaveCountBugOld.isDefined) {
          // For idempotency, we must save this variable the first time this rule is run, because
          // decorrelation introduces a GROUP BY is if one wasn't already present.
          mayHaveCountBugOld.get
        } else if (handleCountBugInDecorrelate) {
          // Count bug was already handled in the above decorrelate function call.
          false
        } else {
          // Check whether the pre-rewrite subquery had empty groupingExpressions. If yes, it may
          // be subject to the COUNT bug. If it has non-empty groupingExpressions, there is
          // no COUNT bug.
          val (topPart, havingNode, aggNode) = splitSubquery(sub)
          (aggNode.isDefined && aggNode.get.groupingExpressions.isEmpty)
        }
        val needSingleJoin = if (needSingleJoinOld.isDefined) {
          needSingleJoinOld.get
        } else {
          conf.getConf(SQLConf.SCALAR_SUBQUERY_USE_SINGLE_JOIN) && !guaranteedToReturnOneRow(sub)
        }
        ScalarSubquery(newPlan, children, exprId, getJoinCondition(newCond, conditions),
          hint, Some(mayHaveCountBug), Some(needSingleJoin))
      case Exists(sub, children, exprId, conditions, hint) if children.nonEmpty =>
        val (newPlan, newCond) = if (SQLConf.get.decorrelateInnerQueryEnabledForExistsIn) {
          decorrelate(sub, plan, handleCountBug = true)
        } else {
          pullOutCorrelatedPredicates(sub, plan)
        }
        Exists(newPlan, children, exprId, getJoinCondition(newCond, conditions), hint)
      case ListQuery(sub, children, exprId, numCols, conditions, hint) if children.nonEmpty =>
        val (newPlan, newCond) = if (SQLConf.get.decorrelateInnerQueryEnabledForExistsIn) {
          decorrelate(sub, plan, handleCountBug = true)
        } else {
          pullOutCorrelatedPredicates(sub, plan)
        }
        val joinCond = getJoinCondition(newCond, conditions)
        ListQuery(newPlan, children, exprId, numCols, joinCond, hint)
      case LateralSubquery(sub, children, exprId, conditions, hint) if children.nonEmpty =>
        val (newPlan, newCond) = decorrelate(sub, plan, handleCountBug = true)
        LateralSubquery(newPlan, children, exprId, getJoinCondition(newCond, conditions), hint)
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
    case j: Join if conf.getConf(DECORRELATE_PREDICATE_SUBQUERIES_IN_JOIN_CONDITION) =>
      rewriteSubQueries(j)
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
        // Results of scalar subqueries are nullable (as they get connected to the rest of the
        // query via left outer join)
        s.plan.output.head.withNullability(true)
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
        // SPARK-43156: return literal real value, count bug does not apply to the count
        // function only, but all expressions in Aggregate that return non-null value for
        // empty input. So we return the real value of the literal here, then the literal
        // can be used for aggregate query result.
        (alias.exprId, l)
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
   * (optional second part) and the Aggregate below the HAVING CLAUSE (optional third part).
   * When the third part is empty, it means the subquery is a non-aggregated single-row subquery.
   */
  def splitSubquery(
      plan: LogicalPlan): (Seq[LogicalPlan], Option[Filter], Option[Aggregate]) = {
    val topPart = ArrayBuffer.empty[LogicalPlan]
    var bottomPart: LogicalPlan = plan
    while (true) {
      bottomPart match {
        case havingPart @ Filter(_, aggPart: Aggregate) =>
          return (topPart.toSeq, Option(havingPart), Some(aggPart))

        case aggPart: Aggregate =>
          // No HAVING clause
          return (topPart.toSeq, None, Some(aggPart))

        case p @ Project(_, child) =>
          topPart += p
          bottomPart = child

        case s @ SubqueryAlias(_, child) =>
          topPart += s
          bottomPart = child

        case p: LogicalPlan if p.maxRows.exists(_ <= 1) =>
          // Non-aggregated one row subquery.
          return (topPart.toSeq, None, None)

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
      case (currentChild, ScalarSubquery(sub, _, _, conditions, subHint, mayHaveCountBug,
      needSingleJoin)) =>
        val query = DecorrelateInnerQuery.rewriteDomainJoins(currentChild, sub, conditions)
        val origOutput = query.output.head
        // The subquery appears on the right side of the join, hence add its hint to the right
        // of a join hint
        val joinHint = JoinHint(None, subHint)

        val resultWithZeroTups = evalSubqueryOnZeroTups(query)
        val joinType = needSingleJoin match {
          case Some(true) => LeftSingle
          case _ => LeftOuter
        }
        lazy val planWithoutCountBug = Project(
          currentChild.output :+ origOutput,
          Join(currentChild, query, joinType, conditions.reduceOption(And), joinHint))

        if (Utils.isTesting) {
          assert(mayHaveCountBug.isDefined)
        }
        if (!SQLConf.get.legacyDuplicateBetweenInput && currentChild.output.contains(origOutput)) {
          // If we had multiple of the same scalar subqueries they will resolve to the same aliases.
          currentChild
        } else if (resultWithZeroTups.isEmpty) {
          // CASE 1: Subquery guaranteed not to have the COUNT bug because it evaluates to NULL
          // with zero tuples.
          planWithoutCountBug
        } else if (!mayHaveCountBug.getOrElse(true) &&
          !conf.getConf(SQLConf.DECORRELATE_SUBQUERY_LEGACY_INCORRECT_COUNT_HANDLING_ENABLED)) {
          // Subquery guaranteed not to have the COUNT bug because it had non-empty GROUP BY clause
          planWithoutCountBug
        } else {
          val (topPart, havingNode, aggNode) = splitSubquery(query)
          if (aggNode.isEmpty) {
            // SPARK-40862: When the aggregate node is empty, it means the subquery produces
            // at most one row and it is not subject to the COUNT bug.
            planWithoutCountBug
          } else {
            // Subquery might have the COUNT bug. Add appropriate corrections.
            val aggregate = aggNode.get

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
                  joinType, conditions.reduceOption(And), joinHint))

            } else {
              // CASE 3: Subquery with HAVING clause. Pull the HAVING clause above the join.
              // Need to modify any operators below the join to pass through all columns
              // referenced in the HAVING clause.
              var subqueryRoot: UnaryNode = aggregate
              val havingInputs: Seq[NamedExpression] = aggregate.output

              topPart.reverse.foreach {
                case Project(projList, _) =>
                  subqueryRoot = Project(projList ++ havingInputs, subqueryRoot)
                case s@SubqueryAlias(alias, _) =>
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
                  joinType, conditions.reduceOption(And), joinHint))
            }
          }
        }
    }
    (newChild, AttributeMap(subqueryAttrMapping))
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
      throw SparkException.internalError(
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
    case LateralJoin(left, LateralSubquery(sub, _, _, joinCond, subHint), joinType, condition) =>
      val newRight = DecorrelateInnerQuery.rewriteDomainJoins(left, sub, joinCond)
      val newCond = (condition ++ joinCond).reduceOption(And)
      // The subquery appears on the right side of the join, hence add the hint to the right side
      Join(left, newRight, joinType, newCond, JoinHint(None, subHint))
  }
}

/**
 * This rule optimizes subqueries with OneRowRelation as leaf nodes.
 */
object OptimizeOneRowRelationSubquery extends Rule[LogicalPlan] {

  object OneRowSubquery {
    def unapply(plan: LogicalPlan): Option[UnaryNode] = {
      // SPARK-40800: always inline expressions to support a broader range of correlated
      // subqueries and avoid expensive domain joins.
      val alwaysInline = conf.getConf(SQLConf.ALWAYS_INLINE_ONE_ROW_RELATION_SUBQUERY)
      CollapseProject(EliminateSubqueryAliases(plan), alwaysInline = alwaysInline) match {
        case p @ Project(_, _: OneRowRelation) => Some(p)
        case g @ Generate(_, _, _, _, _, _: OneRowRelation) => Some(g)
        case p @ Project(_, Generate(_, _, _, _, _, _: OneRowRelation)) => Some(p)
        case _ => None
      }
    }
  }

  private def hasCorrelatedSubquery(plan: LogicalPlan): Boolean = {
    plan.exists(_.expressions.exists(SubqueryExpression.hasCorrelatedSubquery))
  }

  /**
   * Rewrite a subquery expression into one or more expressions. The rewrite can only be done
   * if there is no nested subqueries in the subquery plan.
   */
  private def rewrite(plan: LogicalPlan): LogicalPlan = plan.transformUpWithSubqueries {
    case LateralJoin(
      left, right @ LateralSubquery(OneRowSubquery(plan), _, _, _, _), _, None)
        if !hasCorrelatedSubquery(right.plan) && right.joinCond.isEmpty =>
      plan match {
        case Project(projectList, _: OneRowRelation) =>
          val newPList = stripOuterReferences(projectList)
          Project(left.output ++ newPList, left)

        case g @ Generate(generator, _, _, _, _, _: OneRowRelation) =>
          val newGenerator = stripOuterReference(generator)
          g.copy(generator = newGenerator, child = left)

        case Project(projectList, g @ Generate(generator, _, _, _, _, _: OneRowRelation)) =>
          val newPList = stripOuterReferences(projectList)
          val newGenerator = stripOuterReference(generator)
          Project(left.output ++ newPList, g.copy(generator = newGenerator, child = left))

        case o =>
          throw SparkException.internalError(
            s"Unexpected plan when optimizing one row relation subquery: $o")
      }

    case p: LogicalPlan => p.transformExpressionsUpWithPruning(
      _.containsPattern(SCALAR_SUBQUERY)) {
      case s @ ScalarSubquery(OneRowSubquery(p @ Project(_, _: OneRowRelation)), _, _, _, _, _, _)
          if !hasCorrelatedSubquery(s.plan) && s.joinCond.isEmpty =>
        assert(p.projectList.size == 1)
        stripOuterReferences(p.projectList).head
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
