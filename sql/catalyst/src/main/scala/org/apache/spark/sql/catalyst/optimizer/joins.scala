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

import org.apache.spark.sql.catalyst.CatalystConf
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.planning.{ExtractFiltersAndInnerJoins, PhysicalOperation}
import org.apache.spark.sql.catalyst.plans._
import org.apache.spark.sql.catalyst.plans.logical._
import org.apache.spark.sql.catalyst.rules._
import org.apache.spark.sql.internal.SQLConf

/**
 * Encapsulates star-schema join detection.
 */
case class StarSchemaDetection(conf: SQLConf) extends PredicateHelper {

  /**
   * Star schema consists of one or more fact tables referencing a number of dimension
   * tables. In general, star-schema joins are detected using the following conditions:
   *  1. Informational RI constraints (reliable detection)
   *    + Dimension contains a primary key that is being joined to the fact table.
   *    + Fact table contains foreign keys referencing multiple dimension tables.
   *  2. Cardinality based heuristics
   *    + Usually, the table with the highest cardinality is the fact table.
   *    + Table being joined with the most number of tables is the fact table.
   *
   * To detect star joins, the algorithm uses a combination of the above two conditions.
   * The fact table is chosen based on the cardinality heuristics, and the dimension
   * tables are chosen based on the RI constraints. A star join will consist of the largest
   * fact table joined with the dimension tables on their primary keys. To detect that a
   * column is a primary key, the algorithm uses table and column statistics.
   *
   * Since Catalyst only supports left-deep tree plans, the algorithm currently returns only
   * the star join with the largest fact table. Choosing the largest fact table on the
   * driving arm to avoid large inners is in general a good heuristic. This restriction can
   * be lifted with support for bushy tree plans.
   *
   * The highlights of the algorithm are the following:
   *
   * Given a set of joined tables/plans, the algorithm first verifies if they are eligible
   * for star join detection. An eligible plan is a base table access with valid statistics.
   * A base table access represents Project or Filter operators above a LeafNode. Conservatively,
   * the algorithm only considers base table access as part of a star join since they provide
   * reliable statistics.
   *
   * If some of the plans are not base table access, or statistics are not available, the algorithm
   * returns an empty star join plan since, in the absence of statistics, it cannot make
   * good planning decisions. Otherwise, the algorithm finds the table with the largest cardinality
   * (number of rows), which is assumed to be a fact table.
   *
   * Next, it computes the set of dimension tables for the current fact table. A dimension table
   * is assumed to be in a RI relationship with a fact table. To infer column uniqueness,
   * the algorithm compares the number of distinct values with the total number of rows in the
   * table. If their relative difference is within certain limits (i.e. ndvMaxError * 2, adjusted
   * based on 1TB TPC-DS data), the column is assumed to be unique.
   */
  def findStarJoins(
      input: Seq[LogicalPlan],
      conditions: Seq[Expression]): Seq[Seq[LogicalPlan]] = {

    val emptyStarJoinPlan = Seq.empty[Seq[LogicalPlan]]

    if (!conf.starSchemaDetection || input.size < 2) {
      emptyStarJoinPlan
    } else {
      // Find if the input plans are eligible for star join detection.
      // An eligible plan is a base table access with valid statistics.
      val foundEligibleJoin = input.forall {
        case PhysicalOperation(_, _, t: LeafNode) if t.stats(conf).rowCount.isDefined => true
        case _ => false
      }

      if (!foundEligibleJoin) {
        // Some plans don't have stats or are complex plans. Conservatively,
        // return an empty star join. This restriction can be lifted
        // once statistics are propagated in the plan.
        emptyStarJoinPlan
      } else {
        // Find the fact table using cardinality based heuristics i.e.
        // the table with the largest number of rows.
        val sortedFactTables = input.map { plan =>
          TableAccessCardinality(plan, getTableAccessCardinality(plan))
        }.collect { case t @ TableAccessCardinality(_, Some(_)) =>
          t
        }.sortBy(_.size)(implicitly[Ordering[Option[BigInt]]].reverse)

        sortedFactTables match {
          case Nil =>
            emptyStarJoinPlan
          case table1 :: table2 :: _
            if table2.size.get.toDouble > conf.starSchemaFTRatio * table1.size.get.toDouble =>
            // If the top largest tables have comparable number of rows, return an empty star plan.
            // This restriction will be lifted when the algorithm is generalized
            // to return multiple star plans.
            emptyStarJoinPlan
          case TableAccessCardinality(factTable, _) :: rest =>
            // Find the fact table joins.
            val allFactJoins = rest.collect { case TableAccessCardinality(plan, _)
                if findJoinConditions(factTable, plan, conditions).nonEmpty =>
              plan
            }

            // Find the corresponding join conditions.
            val allFactJoinCond = allFactJoins.flatMap { plan =>
              val joinCond = findJoinConditions(factTable, plan, conditions)
              joinCond
            }

            // Verify if the join columns have valid statistics.
            // Allow any relational comparison between the tables. Later
            // we will heuristically choose a subset of equi-join
            // tables.
            val areStatsAvailable = allFactJoins.forall { dimTable =>
              allFactJoinCond.exists {
                case BinaryComparison(lhs: AttributeReference, rhs: AttributeReference) =>
                  val dimCol = if (dimTable.outputSet.contains(lhs)) lhs else rhs
                  val factCol = if (factTable.outputSet.contains(lhs)) lhs else rhs
                  hasStatistics(dimCol, dimTable) && hasStatistics(factCol, factTable)
                case _ => false
              }
            }

            if (!areStatsAvailable) {
              emptyStarJoinPlan
            } else {
              // Find the subset of dimension tables. A dimension table is assumed to be in a
              // RI relationship with the fact table. Only consider equi-joins
              // between a fact and a dimension table to avoid expanding joins.
              val eligibleDimPlans = allFactJoins.filter { dimTable =>
                allFactJoinCond.exists {
                  case cond @ Equality(lhs: AttributeReference, rhs: AttributeReference) =>
                    val dimCol = if (dimTable.outputSet.contains(lhs)) lhs else rhs
                    isUnique(dimCol, dimTable)
                  case _ => false
                }
              }

              if (eligibleDimPlans.isEmpty) {
                // An eligible star join was not found because the join is not
                // an RI join, or the star join is an expanding join.
                emptyStarJoinPlan
              } else {
                Seq(factTable +: eligibleDimPlans)
              }
            }
        }
      }
    }
  }

  /**
   * Reorders a star join based on heuristics:
   *   1) Finds the star join with the largest fact table and places it on the driving
   *      arm of the left-deep tree. This plan avoids large table access on the inner, and
   *      thus favor hash joins.
   *   2) Applies the most selective dimensions early in the plan to reduce the amount of
   *      data flow.
   */
  def reorderStarJoins(
      input: Seq[(LogicalPlan, InnerLike)],
      conditions: Seq[Expression]): Seq[(LogicalPlan, InnerLike)] = {
    assert(input.size >= 2)

    val emptyStarJoinPlan = Seq.empty[(LogicalPlan, InnerLike)]

    // Find the eligible star plans. Currently, it only returns
    // the star join with the largest fact table.
    val eligibleJoins = input.collect{ case (plan, Inner) => plan }
    val starPlans = findStarJoins(eligibleJoins, conditions)

    if (starPlans.isEmpty) {
      emptyStarJoinPlan
    } else {
      val starPlan = starPlans.head
      val (factTable, dimTables) = (starPlan.head, starPlan.tail)

      // Only consider selective joins. This case is detected by observing local predicates
      // on the dimension tables. In a star schema relationship, the join between the fact and the
      // dimension table is a FK-PK join. Heuristically, a selective dimension may reduce
      // the result of a join.
      // Also, conservatively assume that a fact table is joined with more than one dimension.
      if (dimTables.size >= 2 && isSelectiveStarJoin(dimTables, conditions)) {
        val reorderDimTables = dimTables.map { plan =>
          TableAccessCardinality(plan, getTableAccessCardinality(plan))
        }.sortBy(_.size).map {
          case TableAccessCardinality(p1, _) => p1
        }

        val reorderStarPlan = factTable +: reorderDimTables
        reorderStarPlan.map(plan => (plan, Inner))
      } else {
        emptyStarJoinPlan
      }
    }
  }

  /**
   * Determines if a column referenced by a base table access is a primary key.
   * A column is a PK if it is not nullable and has unique values.
   * To determine if a column has unique values in the absence of informational
   * RI constraints, the number of distinct values is compared to the total
   * number of rows in the table. If their relative difference
   * is within the expected limits (i.e. 2 * spark.sql.statistics.ndv.maxError based
   * on TPCDS data results), the column is assumed to have unique values.
   */
  private def isUnique(
      column: Attribute,
      plan: LogicalPlan): Boolean = plan match {
    case PhysicalOperation(_, _, t: LeafNode) =>
      val leafCol = findLeafNodeCol(column, plan)
      leafCol match {
        case Some(col) if t.outputSet.contains(col) =>
          val stats = t.stats(conf)
          stats.rowCount match {
            case Some(rowCount) if rowCount >= 0 =>
              if (stats.attributeStats.nonEmpty && stats.attributeStats.contains(col)) {
                val colStats = stats.attributeStats.get(col)
                if (colStats.get.nullCount > 0) {
                  false
                } else {
                  val distinctCount = colStats.get.distinctCount
                  val relDiff = math.abs((distinctCount.toDouble / rowCount.toDouble) - 1.0d)
                  // ndvMaxErr adjusted based on TPCDS 1TB data results
                  relDiff <= conf.ndvMaxError * 2
                }
              } else {
                false
              }
            case None => false
          }
        case None => false
      }
    case _ => false
  }

  /**
   * Given a column over a base table access, it returns
   * the leaf node column from which the input column is derived.
   */
  @tailrec
  private def findLeafNodeCol(
      column: Attribute,
      plan: LogicalPlan): Option[Attribute] = plan match {
    case pl @ PhysicalOperation(_, _, _: LeafNode) =>
      pl match {
        case t: LeafNode if t.outputSet.contains(column) =>
          Option(column)
        case p: Project if p.outputSet.exists(_.semanticEquals(column)) =>
          val col = p.outputSet.find(_.semanticEquals(column)).get
          findLeafNodeCol(col, p.child)
        case f: Filter =>
          findLeafNodeCol(column, f.child)
        case _ => None
      }
    case _ => None
  }

  /**
   * Checks if a column has statistics.
   * The column is assumed to be over a base table access.
   */
  private def hasStatistics(
      column: Attribute,
      plan: LogicalPlan): Boolean = plan match {
    case PhysicalOperation(_, _, t: LeafNode) =>
      val leafCol = findLeafNodeCol(column, plan)
      leafCol match {
        case Some(col) if t.outputSet.contains(col) =>
          val stats = t.stats(conf)
          stats.attributeStats.nonEmpty && stats.attributeStats.contains(col)
        case None => false
      }
    case _ => false
  }

  /**
   * Returns the join predicates between two input plans. It only
   * considers basic comparison operators.
   */
  @inline
  private def findJoinConditions(
      plan1: LogicalPlan,
      plan2: LogicalPlan,
      conditions: Seq[Expression]): Seq[Expression] = {
    val refs = plan1.outputSet ++ plan2.outputSet
    conditions.filter {
      case BinaryComparison(_, _) => true
      case _ => false
    }.filterNot(canEvaluate(_, plan1))
     .filterNot(canEvaluate(_, plan2))
     .filter(_.references.subsetOf(refs))
  }

  /**
   * Checks if a star join is a selective join. A star join is assumed
   * to be selective if there are local predicates on the dimension
   * tables.
   */
  private def isSelectiveStarJoin(
      dimTables: Seq[LogicalPlan],
      conditions: Seq[Expression]): Boolean = dimTables.exists {
    case plan @ PhysicalOperation(_, p, _: LeafNode) =>
      // Checks if any condition applies to the dimension tables.
      // Exclude the IsNotNull predicates until predicate selectivity is available.
      // In most cases, this predicate is artificially introduced by the Optimizer
      // to enforce nullability constraints.
      val localPredicates = conditions.filterNot(_.isInstanceOf[IsNotNull])
        .exists(canEvaluate(_, plan))

      // Checks if there are any predicates pushed down to the base table access.
      val pushedDownPredicates = p.nonEmpty && !p.forall(_.isInstanceOf[IsNotNull])

      localPredicates || pushedDownPredicates
    case _ => false
  }

  /**
   * Helper case class to hold (plan, rowCount) pairs.
   */
  private case class TableAccessCardinality(plan: LogicalPlan, size: Option[BigInt])

  /**
   * Returns the cardinality of a base table access. A base table access represents
   * a LeafNode, or Project or Filter operators above a LeafNode.
   */
  private def getTableAccessCardinality(
      input: LogicalPlan): Option[BigInt] = input match {
    case PhysicalOperation(_, cond, t: LeafNode) if t.stats(conf).rowCount.isDefined =>
      if (conf.cboEnabled && input.stats(conf).rowCount.isDefined) {
        Option(input.stats(conf).rowCount.get)
      } else {
        Option(t.stats(conf).rowCount.get)
      }
    case _ => None
  }
}

/**
 * Reorder the joins and push all the conditions into join, so that the bottom ones have at least
 * one condition.
 *
 * The order of joins will not be changed if all of them already have at least one condition.
 *
 * If star schema detection is enabled, reorder the star join plans based on heuristics.
 */
case class ReorderJoin(conf: SQLConf) extends Rule[LogicalPlan] with PredicateHelper {
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
      if (conf.starSchemaDetection && !conf.cboEnabled) {
        val starJoinPlan = StarSchemaDetection(conf).reorderStarJoins(input, conditions)
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
case class EliminateOuterJoin(conf: CatalystConf) extends Rule[LogicalPlan] with PredicateHelper {

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
    val conditions = splitConjunctivePredicates(filter.condition) ++
      filter.getConstraints(conf.constraintPropagationEnabled)
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
