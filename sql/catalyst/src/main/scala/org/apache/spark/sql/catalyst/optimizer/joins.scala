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

import scala.annotation.tailrec

import org.apache.spark.sql.AnalysisException
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.planning.ExtractFiltersAndInnerJoins
import org.apache.spark.sql.catalyst.plans._
import org.apache.spark.sql.catalyst.plans.logical._
import org.apache.spark.sql.catalyst.rules._
import org.apache.spark.sql.internal.SQLConf

/**
 * Reorder the joins and push all the conditions into join, so that the bottom ones have at least
 * one condition.
 *
 * The order of joins will not be changed if all of them already have at least one condition.
 *
 * If star schema detection is enabled, reorder the star join plans based on heuristics.
 */
object ReorderJoin extends Rule[LogicalPlan] with PredicateHelper {
  /**
   * Join a list of plans together and push down the conditions into them.
   *
   * The joined plan are picked from left to right, prefer those has at least one join condition.
   *
   * @param input a list of LogicalPlans to inner join and the type of inner join.
   * @param conditions a list of condition for join.
   */
  @tailrec
  final def createOrderedJoin(input: Seq[(LogicalPlan, InnerLike)], conditions: Seq[Expression])
    : LogicalPlan = {
    assert(input.size >= 2)
    if (input.size == 2) {
      val (joinConditions, others) = conditions.partition(canEvaluateWithinJoin)
      val ((left, leftJoinType), (right, rightJoinType)) = (input(0), input(1))
      val innerJoinType = (leftJoinType, rightJoinType) match {
        case (Inner, Inner) => Inner
        case (_, _) => Cross
      }
      val join = Join(left, right, innerJoinType, joinConditions.reduceLeftOption(And))
      if (others.nonEmpty) {
        Filter(others.reduceLeft(And), join)
      } else {
        join
      }
    } else {
      val (left, _) :: rest = input.toList
      // find out the first join that have at least one join condition
      val conditionalJoin = rest.find { planJoinPair =>
        val plan = planJoinPair._1
        val refs = left.outputSet ++ plan.outputSet
        conditions
          .filterNot(l => l.references.nonEmpty && canEvaluate(l, left))
          .filterNot(r => r.references.nonEmpty && canEvaluate(r, plan))
          .exists(_.references.subsetOf(refs))
      }
      // pick the next one if no condition left
      val (right, innerJoinType) = conditionalJoin.getOrElse(rest.head)

      val joinedRefs = left.outputSet ++ right.outputSet
      val (joinConditions, others) = conditions.partition(
        e => e.references.subsetOf(joinedRefs) && canEvaluateWithinJoin(e))
      val joined = Join(left, right, innerJoinType, joinConditions.reduceLeftOption(And))

      // should not have reference to same logical plan
      createOrderedJoin(Seq((joined, Inner)) ++ rest.filterNot(_._1 eq right), others)
    }
  }

  def apply(plan: LogicalPlan): LogicalPlan = plan transform {
    case ExtractFiltersAndInnerJoins(input, conditions)
        if input.size > 2 && conditions.nonEmpty =>
      if (SQLConf.get.starSchemaDetection && !SQLConf.get.cboEnabled) {
        val starJoinPlan = StarSchemaDetection.reorderStarJoins(input, conditions)
        if (starJoinPlan.nonEmpty) {
          val rest = input.filterNot(starJoinPlan.contains(_))
          createOrderedJoin(starJoinPlan ++ rest, conditions)
        } else {
          createOrderedJoin(input, conditions)
        }
      } else {
        createOrderedJoin(input, conditions)
      }
  }
}

/**
 * Elimination of outer joins, if the predicates can restrict the result sets so that
 * all null-supplying rows are eliminated
 *
 * - full outer -> inner if both sides have such predicates
 * - left outer -> inner if the right side has such predicates
 * - right outer -> inner if the left side has such predicates
 * - full outer -> left outer if only the left side has such predicates
 * - full outer -> right outer if only the right side has such predicates
 *
 * This rule should be executed before pushing down the Filter
 */
object EliminateOuterJoin extends Rule[LogicalPlan] with PredicateHelper {

  /**
   * Returns whether the expression returns null or false when all inputs are nulls.
   */
  private def canFilterOutNull(e: Expression): Boolean = {
    if (!e.deterministic || SubqueryExpression.hasCorrelatedSubquery(e)) return false
    val attributes = e.references.toSeq
    val emptyRow = new GenericInternalRow(attributes.length)
    val boundE = BindReferences.bindReference(e, attributes)
    if (boundE.find(_.isInstanceOf[Unevaluable]).isDefined) return false
    val v = boundE.eval(emptyRow)
    v == null || v == false
  }

  private def buildNewJoinType(filter: Filter, join: Join): JoinType = {
    val conditions = splitConjunctivePredicates(filter.condition) ++ filter.constraints
    val leftConditions = conditions.filter(_.references.subsetOf(join.left.outputSet))
    val rightConditions = conditions.filter(_.references.subsetOf(join.right.outputSet))

    lazy val leftHasNonNullPredicate = leftConditions.exists(canFilterOutNull)
    lazy val rightHasNonNullPredicate = rightConditions.exists(canFilterOutNull)

    join.joinType match {
      case RightOuter if leftHasNonNullPredicate => Inner
      case LeftOuter if rightHasNonNullPredicate => Inner
      case FullOuter if leftHasNonNullPredicate && rightHasNonNullPredicate => Inner
      case FullOuter if leftHasNonNullPredicate => LeftOuter
      case FullOuter if rightHasNonNullPredicate => RightOuter
      case o => o
    }
  }

  def apply(plan: LogicalPlan): LogicalPlan = plan transform {
    case f @ Filter(condition, j @ Join(_, _, RightOuter | LeftOuter | FullOuter, _)) =>
      val newJoinType = buildNewJoinType(f, j)
      if (j.joinType == newJoinType) f else Filter(condition, j.copy(joinType = newJoinType))
  }
}

/**
 * PythonUDF in join condition can not be evaluated, this rule will detect the PythonUDF
 * and pull them out from join condition. For python udf accessing attributes from only one side,
 * they are pushed down by operation push down rules. If not (e.g. user disables filter push
 * down rules), we need to pull them out in this rule too.
 */
object PullOutPythonUDFInJoinCondition extends Rule[LogicalPlan] with PredicateHelper {
  def hasPythonUDF(expression: Expression): Boolean = {
    expression.collectFirst { case udf: PythonUDF => udf }.isDefined
  }

  override def apply(plan: LogicalPlan): LogicalPlan = plan transformUp {
    case j @ Join(_, _, joinType, condition)
        if condition.isDefined && hasPythonUDF(condition.get) =>
      if (!joinType.isInstanceOf[InnerLike] && joinType != LeftSemi) {
        // The current strategy only support InnerLike and LeftSemi join because for other type,
        // it breaks SQL semantic if we run the join condition as a filter after join. If we pass
        // the plan here, it'll still get a an invalid PythonUDF RuntimeException with message
        // `requires attributes from more than one child`, we throw firstly here for better
        // readable information.
        throw new AnalysisException("Using PythonUDF in join condition of join type" +
          s" $joinType is not supported.")
      }
      // If condition expression contains python udf, it will be moved out from
      // the new join conditions.
      val (udf, rest) =
        splitConjunctivePredicates(condition.get).partition(hasPythonUDF)
      val newCondition = if (rest.isEmpty) {
        logWarning(s"The join condition:$condition of the join plan contains PythonUDF only," +
          s" it will be moved out and the join plan will be turned to cross join.")
        None
      } else {
        Some(rest.reduceLeft(And))
      }
      val newJoin = j.copy(condition = newCondition)
      joinType match {
        case _: InnerLike => Filter(udf.reduceLeft(And), newJoin)
        case LeftSemi =>
          Project(
            j.left.output.map(_.toAttribute),
            Filter(udf.reduceLeft(And), newJoin.copy(joinType = Inner)))
        case _ =>
          throw new AnalysisException("Using PythonUDF in join condition of join type" +
            s" $joinType is not supported.")
      }
  }
}
