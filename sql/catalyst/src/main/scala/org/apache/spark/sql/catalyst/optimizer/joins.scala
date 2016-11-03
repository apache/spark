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

import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.planning.{BaseTableAccess, ExtractFiltersAndInnerJoins}
import org.apache.spark.sql.catalyst.plans._
import org.apache.spark.sql.catalyst.plans.logical._
import org.apache.spark.sql.catalyst.rules._
import org.apache.spark.sql.catalyst.CatalystConf

/**
 * Reorder the joins and push all the conditions into join, so that the bottom ones have at least
 * one condition.
 *
 * The order of joins will not be changed if all of them already have at least one condition.
 *
 * If starJoinOptimization configuration option is set, reorder joins based
 * on star schema detection.
 */
case class ReorderJoin(conf: CatalystConf) extends Rule[LogicalPlan] with PredicateHelper {

  /**
   * Join a list of plans together and push down the conditions into them.
   *
   * The joined plan are picked from left to right, prefer those has at least one join condition.
   *
   * @param input a list of LogicalPlans to inner join and the type of inner join.
   * @param conditions a list of condition for join.
   */
  @tailrec
  private def createOrderedJoin(
      input: Seq[(LogicalPlan, InnerLike)],
      conditions: Seq[Expression]): LogicalPlan = {
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

  /**
   * Helper function that computes the size of a table access after applying
   * the predicates. Currently, the function returns the table size.
   * When plan cardinality and predicate selectivity are implemented in Catalyst,
   * the function will be refined based on these estimates.
   */
  private def computeBaseTableSize(
      input: LogicalPlan,
      conditions: Seq[Expression]): Option[BigInt] = {
    input match {
      case BaseTableAccess(t, cond) if t.statistics.sizeInBytes >= 0 =>
        // To be replaced by the table cardinality, when available.
        val tableSize = t.statistics.sizeInBytes

        // Collect predicate selectivity, when available.
        // val predicates = conditions.filter(canEvaluate(_, p)) ++ cond
        // Compute the output cardinality = tableSize * predicates' selectivity.
        Option(tableSize)

      case _ => None
    }
  }

  /**
   * Helper case class to hold (plan, size) pairs.
   */
  private case class TableSize(plan: (LogicalPlan, InnerLike), size: Option[BigInt])

  /**
   * Checks if a star join is a selective join. A star join is assumed
   * to be selective if (1) there are local predicates on the dimension
   * tables and (2) the join predicates are equi joins.
   */
  private def isSelectiveStarJoin(
      factTable: LogicalPlan,
      dimTables: Seq[LogicalPlan],
      conditions: Seq[Expression]): Boolean = {

    val starJoinPlan = factTable +: dimTables

    // Checks if all star plans are base table access.
    val allBaseTables = starJoinPlan.forall {
      case BaseTableAccess(_, _) => true
      case _ => false
    }

    // Checks if any condition applies to the dimension tables.
    val localPredicates = dimTables.exists { plan =>
      // Exclude the IsNotNull predicates until predicate selectivity is available.
      // In most cases, this predicate is artificially introduced by the Optimizer
      // to enforce nullability constraints.
      conditions.filterNot(_.isInstanceOf[IsNotNull]).exists(canEvaluate(_, plan))
    }

    // Checks if there are any predicates pushed down to the base table access.
    // Similarly, exclude IsNotNull predicates.
    val pushedDownPredicates = dimTables.exists {
      case BaseTableAccess(_, p) if p.nonEmpty => !p.forall(_.isInstanceOf[IsNotNull])
      case _ => false
    }

    val isSelectiveDimensions = allBaseTables && (localPredicates || pushedDownPredicates)

    // Checks if the join predicates are equi joins of the form fact.col = dim.col.
    val isEquiJoin = dimTables.forall { plan =>
      val refs = factTable.outputSet ++ plan.outputSet
      conditions.filterNot(canEvaluate(_, factTable))
        .filterNot(canEvaluate(_, plan))
        .filter(_.references.subsetOf(refs))
        .forall {
          case EqualTo(_: Attribute, _: Attribute) => true
          case EqualNullSafe(_: Attribute, _: Attribute) => true
          case _ => false
        }
    }

    isSelectiveDimensions && isEquiJoin
  }

  /**
   * Finds an optimal join order for star schema queries i.e.
   * + The largest fact table on the driving arm to avoid large tables on the inner of a join,
   *   and thus favor hash joins.
   * + Apply the most selective dimensions early in the plan to reduce the data flow.
   *
   * In general, star-schema joins are detected using the following conditions:
   *  1. Informational RI constraints (reliable detection)
   *    + Dimension contains a primary key that is being joined to the fact table.
   *    + Fact table contains foreign keys referencing multiple dimension tables.
   *  2. Cardinality based heuristics
   *    + Usually, the table with the highest cardinality is the fact table.
   *    + Table being joined with the most number of tables is the fact table.
   *
   * Given the current Spark optimizer limitations (e.g. limited statistics, no RI
   * constraints information, etc.), the algorithm uses table size statistics and the
   * type of predicates as a naive approach to infer table cardinality and join selectivity.
   * When plan cardinality and predicate selectivity features are implemented in Catalyst,
   * the star detection logic will be refined based on these estimates.
   *
   * The highlights of the algorithm are the following:
   *
   * Given a set of joined tables/plans, the algorithm finds the set of eligible fact tables.
   * An eligible fact table is a base table access with valid statistics. A base table access
   * represents Project or Filter operators above a LeafNode. Conservatively, the algorithm
   * only considers base table access as part of a star join since they provide reliable
   * statistics.
   *
   * If there are no eligible fact tables (e.g. no base table access i.e. complex sub-plans),
   * the algorithm falls back to the positional join reordering, which is the default join
   * ordering in Catalyst. Otherwise, the algorithm finds the largest fact table by sorting
   * the tables based on their size
   *
   * The algorithm next computes the set of dimension tables for the current fact table.
   * Conservatively, only equality joins are considered between a fact and a dimension table.
   *
   * Given a star join, i.e. fact and dimension tables, the algorithm considers three cases:
   *
   * 1) The star join is an expanding join i.e. the fact table is joined using inequality
   * predicates, or Cartesian product. In this case, the algorithm is looking for the next
   * eligible star join. The rational is that a selective star join, if found, may reduce
   * the data stream early in the plan. An alternative, more conservative design, is to simply
   * fall back to the default join reordering if the star join with the largest fact table
   * is not selective.
   *
   * 2) The star join is a selective join. This case is detected by observing local predicates
   * on the dimension tables. In a star schema relationship, the join between the fact and the
   * dimension table is in general a FK-PK join. Heuristically, a selective dimension may reduce
   * the result of a join.
   *
   * 3) The star join is not a selective join (i.e. doesn't reduce the number of rows on the
   * driving arm). In this case, the algorithm conservatively falls back to the default join
   * reordering.
   */
  @tailrec
  private def findEligibleStarJoinPlan(
      joinedTables: Seq[(LogicalPlan, InnerLike)],
      eligibleFactTables: Seq[(LogicalPlan, InnerLike)],
      conditions: Seq[Expression]): Seq[(LogicalPlan, InnerLike)] = {
    if (eligibleFactTables.isEmpty || joinedTables.size < 2) {
      // There is no eligible star join.
      Seq.empty[(LogicalPlan, InnerLike)]
    } else {
      // Compute the size of the eligible fact tables and sort them in descending order.
      // An eligible fact table is a base table access with valid statistics.
      val sortedFactTables = eligibleFactTables.map { plan =>
        TableSize(plan, computeBaseTableSize(plan._1, conditions))
      }.collect {
        case t @ TableSize(_, Some(_)) => t
      }.sortBy(_.size)(implicitly[Ordering[Option[BigInt]]].reverse)

      sortedFactTables match {
        case Nil =>
          // There are no stats available for these plans.
          // Return an empty plan list and fall back to the default join reordering.
          Seq.empty[(LogicalPlan, InnerLike)]

        case table1 :: table2 :: _ if table1.size == table2.size =>
          // There are more tables with the same size. Conservatively, fall back to the
          // default join reordering since we cannot make good plan choices.
          Seq.empty[(LogicalPlan, InnerLike)]

        case TableSize(factPlan @ (factTable, _), _) :: _ =>
          // A fact table was found. Get the largest fact table and compute the corresponding
          // dimension tables. Conservatively, a dimension table is assumed to be joined with
          // the fact table using equality predicates.
          val eligibleDimPlans = joinedTables.filterNot(_._1 eq factTable).filter { dimPlan =>
            val dimTable = dimPlan._1
            val refs = factTable.outputSet ++ dimTable.outputSet
            conditions.filter(p => p.isInstanceOf[EqualTo] || p.isInstanceOf[EqualNullSafe])
              .filterNot(canEvaluate(_, factTable))
              .filterNot(canEvaluate(_, dimTable))
              .exists(_.references.subsetOf(refs))
          }

          val dimTables = eligibleDimPlans.map { _._1 }

          if (dimTables.isEmpty) {
            // This is an expanding join since there are no equality joins
            // between the current fact table and the joined tables.
            // Look for the next eligible star join plan.
            findEligibleStarJoinPlan(
              joinedTables,
              eligibleFactTables.filterNot(_._1 eq factTable),
              conditions)

          } else if (dimTables.size < 2) {
            // Conservatively assume at least two dimensions in a star join.
            Seq.empty[(LogicalPlan, InnerLike)]

          } else if (isSelectiveStarJoin(factTable, dimTables, conditions)) {
            // This is a selective star join and all dimensions are base table access.
            // Compute the size of the dimensions and return the star join
            // with the most selective dimensions joined lower in the plan.
            val sortedDims = eligibleDimPlans.map { plan =>
              TableSize(plan, computeBaseTableSize(plan._1, conditions))
            }.sortBy(_.size).map {
              case TableSize(plan, _) => plan
            }

            factPlan +: sortedDims
          } else {
            // This is a non selective star join or some of the dimensions are not
            // over the base tables. Conservatively fall back to the default join order.
            Seq.empty[(LogicalPlan, InnerLike)]
          }
      }
    }
  }

  def apply(plan: LogicalPlan): LogicalPlan = plan transform {
    case ExtractFiltersAndInnerJoins(input, conditions)
      if input.size >= 2 && conditions.nonEmpty =>
        if (conf.starJoinOptimization) {
          val starJoinPlan = findEligibleStarJoinPlan(input, input, conditions)
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

    val leftHasNonNullPredicate = leftConditions.exists(canFilterOutNull)
    val rightHasNonNullPredicate = rightConditions.exists(canFilterOutNull)

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
