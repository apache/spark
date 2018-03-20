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

import org.apache.spark.sql.catalyst.TableIdentifier
import org.apache.spark.sql.catalyst.catalog._
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.planning.{ExtractFiltersAndInnerJoins, PhysicalOperation}
import org.apache.spark.sql.catalyst.plans._
import org.apache.spark.sql.catalyst.plans.logical._
import org.apache.spark.sql.catalyst.rules._
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.types.{HIVE_TYPE_STRING, StringType}

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
 * The inner join elimination rewrite optimization eliminates tables
 * from the query when the join is a primary key to foreign key
 * join and only the primary key columns from the primary table
 * are referenced in the query.
 */
object EliminateRIInnerJoins extends Rule[LogicalPlan] with PredicateHelper {

  private def conf = SQLConf.get

  /**
   * Finds the primary key information from a base table access plan.
   */
  private def findPKInfo(plan: LogicalPlan): Option[(TableIdentifier, Seq[Attribute])] = {
    plan match {
      case p @ PhysicalOperation(_, _, t @ HiveTableRelation(catalogTable, _, _)) =>
        findPKInfo(catalogTable, p.outputSet)
      case p @ PhysicalOperation(_, _, t: CatalogMetadata) if t.metadata.isDefined =>
        findPKInfo(t.metadata.get, p.outputSet)
      case _ =>
        None
    }
  }

  /**
   * Finds the primary key info from a catalog table.
   * It maps the primary key to the input set of columns.
   */
  private def findPKInfo (
      catalogTable: CatalogTable,
      columns: AttributeSet): Option[(TableIdentifier, Seq[Attribute])] = {
    if (catalogTable.tableConstraints.nonEmpty &&
      catalogTable.tableConstraints.get.primaryKey.nonEmpty) {
      // TODO: Uncomment when SPARK-22064 is implemented.
      // && catalogTable.tableConstraints.get.primaryKey.get.isValidated
      val keys = catalogTable.tableConstraints.get.primaryKey.get.keyColumnNames
      val mappedKeys = mapRIKeys(keys, columns)
      if (mappedKeys.nonEmpty) {
        Some((catalogTable.identifier, mappedKeys))
      } else {
        None
      }
    } else {
      None
    }
  }

  /**
   * Finds the foreign keys from a base table access plan given a parent table.
   */
  private def findFKInfo(
      plan: LogicalPlan,
      referencedTable: TableIdentifier): Seq[Seq[Attribute]] = plan match {
    case p @ PhysicalOperation(_, _, t @ HiveTableRelation(catalogTable, _, _)) =>
      findFKInfo(catalogTable, referencedTable, p.outputSet)
    case p @ PhysicalOperation(_, _, t: CatalogMetadata) if t.metadata.isDefined =>
      findFKInfo(t.metadata.get, referencedTable, p.outputSet)
    case _ =>
      Seq.empty
  }

  /**
   * Finds the foreign keys from a catalog table given a parent table.
   * It maps the keys to the input set of columns.
   */
  private def findFKInfo (
      catalogTable: CatalogTable,
      referencedTable: TableIdentifier,
      columns: AttributeSet): Seq[Seq[Attribute]] = {
    if (catalogTable.tableConstraints.nonEmpty &&
      catalogTable.tableConstraints.get.foreignKeys.nonEmpty) {
      catalogTable.tableConstraints.get.findForeignKeys(referencedTable, conf.resolver)
        .map { fk => mapRIKeys(fk, columns) }
        .filter { f => f.nonEmpty }
    } else {
      Seq.empty
    }
  }

  /**
   * Maps the RI keys to a set of columns.
   */
  private def mapRIKeys(keys: Seq[String], columns: AttributeSet): Seq[Attribute] = {
    if (keys.nonEmpty && keys.forall(key => columns.exists(col => conf.resolver(col.name, key)))) {
      keys.map(key => columns.find(col => conf.resolver(col.name, key)).get)
    } else {
      Seq.empty
    }
  }

  /**
   * Determines if there is any non-eligible predicate for the RI join
   * elimination rewrite. A predicate referencing a column from the PK table
   * is non eligible under the following conditions:
   * 1. Non deterministic
   * 2. References non-PK columns.
   * 3. References columns from other tables in a non equi-join predicate.
   * 4. Contains length sensitive functions
   *
   * @param conditions Set of predicates
   * @param pkPlanCols PK table columns
   * @param pkCols     PK keys
   * @param fkPlanCols FK table columns
   * @return true/false
   */
  private def hasNonEligiblePredicates(
      conditions: Seq[Expression],
      pkPlanCols: Seq[Attribute],
      pkCols: Seq[Attribute],
      fkPlanCols: Seq[Attribute]): Boolean = conditions.exists { exp =>
    val referencedCols = exp.references

    if (referencedCols.exists(col => pkPlanCols.exists(pk => pk.semanticEquals(col)))) {

      // Has non-PK columns
      val hasNonPKCols = referencedCols.exists { expCol =>
        pkPlanCols.exists(planCol => planCol.semanticEquals(expCol)) &&
          pkCols.forall(pk => !pk.semanticEquals(expCol))
      }

      // Has columns from other tables than PK/FK tables
      val hasOtherCols = referencedCols.exists { expCol =>
        pkPlanCols.forall(pk => !pk.semanticEquals(expCol)) &&
          fkPlanCols.forall(pk => !pk.semanticEquals(expCol))
      }

      if (!exp.deterministic || hasNonPKCols) {
        true
      } else if (hasOtherCols) {
        // Conservatively, only allow equi joins between PK cols and other
        // tables.
        exp match {
          case Equality(l: AttributeReference, r: AttributeReference) => false
          case _ => true
        }

      } else {
        val hasLengthSensFcts = hasLengthSensitiveExpr(exp, pkPlanCols)
        val hasSubq = SubqueryExpression.hasSubquery(exp)

        hasLengthSensFcts || hasSubq
      }
    } else {
      false
    }
  }

  /**
   * Checks if two string data types have the same comparison
   * semantics. For example, CHAR type uses a blank padded
   * comparison, while VARCHAR type uses non-blank padded comparison.
   */
  private def sameComparisonSemantics(l: Attribute, r: Attribute): Boolean = {
    (l.dataType, r.dataType) match {
      case (lType: StringType, rType: StringType) =>
        val left = l.metadata.contains(HIVE_TYPE_STRING)
        val right = r.metadata.contains(HIVE_TYPE_STRING)
        (left, right) match {
          case (true, true) =>
            l.metadata.getString(HIVE_TYPE_STRING) == r.metadata.getString(HIVE_TYPE_STRING)
          case (false, false) => true
          case (_, _) => false
        }
      case (_, _) => true
    }
  }

  /**
   * Checks if any columns in the input set are used in length sensitive functions.
   */
  private def hasLengthSensitiveExpr(
      expr: Expression,
      cols: Seq[Attribute]): Boolean = expr match {
    case BinaryComparison(e1: AttributeReference, e2: Literal) => false
    case BinaryComparison(e1: Literal, e2: AttributeReference) => false
    case BinaryComparison(e1, e2) =>
      hasLengthSensitiveExpr(e1, cols) || hasLengthSensitiveExpr(e2, cols)
    case Like(e1: AttributeReference, e2: Literal) => false
    case Like(e1: Literal, e2: AttributeReference) => false
    case Like(e1, e2) =>
      hasLengthSensitiveExpr(e1, cols) || hasLengthSensitiveExpr(e2, cols)
    case IsNotNull(e: AttributeReference) => false
    case IsNotNull(e) =>
      hasLengthSensitiveExpr(e, cols)
    // TODO: Refine the restriction with character specific functions.
    // e.g. LENGTH()
    case e =>
      e.references.exists { col =>
        col.dataType == StringType && cols.exists(_.semanticEquals(col))
      }
  }

  // Keeps PK info for a given query plan
  private case class RIPKInfo(
    plan: LogicalPlan,
    table: TableIdentifier,
    key: Seq[Attribute])

  // Keeps RI related information for a given query plan
  private case class RIInfo(
    pkPlan: LogicalPlan,
    pkCols: Seq[Attribute],
    fkPlan: LogicalPlan,
    fkCols: Seq[Attribute],
    joinCond: Seq[Expression],
    joinKeys: Seq[(AttributeReference, AttributeReference)])

  /**
   * Returns a new plan after the join elimination transformation.
   * The method takes as input an inner join and the required output.
   * It first finds the PK-FK relationship among the input tables. The input
   * tables are assumed to be simple base table accesses (i.e. simple Project/Filter
   * over leaf nodes). Next, it decides if any PK tables can be removed. For that, it
   * analyzes the predicates and the output columns for the presence of the PK table columns.
   * If any PK tables can be removed, it rebuilds the plan after the transformation.
   *
   * @param plan Inner join
   * @param outputCols Required output
   * @return Transformed plan
   */
  private def eliminateInnerJoins(
      plan: LogicalPlan,
      outputCols: Seq[Expression]): Option[LogicalPlan] = plan match {
    case join @ Join(_, _, t: InnerLike, joinCond) =>

      // Find the input plans of the inner joins.
      // Use CostBasedJoinReorder.extractInnerJoins() method to traverse both
      // left and right deep trees.
      val (innerPlans, jConditions) = CostBasedJoinReorder.extractInnerJoins(join)
      val conditions = jConditions.toSeq

      // Find the PK tables
      val pkTables = innerPlans.collect {
        case p @ PhysicalOperation(projectList, _, t: LeafNode)
          if projectList.forall(_.isInstanceOf[Attribute]) =>
          val pkInfo = findPKInfo(p)
          (p, pkInfo)
      }.collect {
        case (p, Some(pkInfo)) => RIPKInfo(p, pkInfo._1, pkInfo._2)
      }

      // Find the PK-FK pairs
      val pkFkPairs = pkTables.collect {
        case RIPKInfo(pkPl, pkTbl, pkKey) =>
          innerPlans.collect {
            case f @ PhysicalOperation(projectList, _, t: LeafNode)
              if projectList.forall(_.isInstanceOf[Attribute]) =>
              val fkCols = findFKInfo(f, pkTbl)
              (f, fkCols)
          }.collect {
            case (fkPl, fkCols) if fkCols.nonEmpty =>
              fkCols.map { col => (RIPKInfo(pkPl, pkTbl, pkKey), fkPl, col)}
          }.flatten
      }.flatten

      // Construct the RI info including the RI join predicates
      val riInfo = pkFkPairs.collect {
        case (RIPKInfo(pkPlan, _, pkCols), fkPlan, fkCols) =>

          val pkFkPreds = conditions.flatMap {
            case pred @ Equality(l: AttributeReference, r: AttributeReference)
              if canEvaluate(l, pkPlan) && pkCols.exists(_.semanticEquals(l)) &&
                canEvaluate(r, fkPlan) && fkCols.exists(_.semanticEquals(r)) &&
                sameComparisonSemantics(l, r) =>
              Some(pred)

            case pred @ Equality(l: AttributeReference, r: AttributeReference)
              if canEvaluate(l, fkPlan) && fkCols.exists(_.semanticEquals(l)) &&
                canEvaluate(r, pkPlan) && pkCols.exists(_.semanticEquals(r)) &&
                sameComparisonSemantics(l, r) =>
              Some(pred)

            case _ =>
              None
          }

          // Deconstruct the RI predicates into the join keys
          val pkFkKeys = pkFkPreds.flatMap {
            case Equality(l: AttributeReference, r: AttributeReference)
              if canEvaluate(l, pkPlan) && canEvaluate(r, fkPlan) =>
              Some((l, r))

            case Equality(l: AttributeReference, r: AttributeReference)
              if canEvaluate(l, fkPlan) && canEvaluate(r, pkPlan) =>
              Some((r, l))

            case _ =>
              None
          }

          // Check if the complete pk/fk keys are present in the RI join predicates
          if (pkFkKeys.nonEmpty) {
            val (pkKeys, fkKeys) = pkFkKeys.unzip
            val foundAllPKs = pkCols.forall(col => pkKeys.exists(_.semanticEquals(col)))
            val foundAllFKs = fkCols.forall(col => fkKeys.exists(_.semanticEquals(col)))

            if (foundAllPKs && foundAllFKs) {
              Some(RIInfo(pkPlan, pkCols, fkPlan, fkCols, pkFkPreds, pkFkKeys))
            } else {
              None
            }
          } else {
            None
          }
      }.flatten

      // Collect all the RI predicates.
      val riJoins = riInfo.collect {
        case RIInfo(_, _, _, _, riJoin, _) => riJoin
      }.flatten

      // Collect other predicates than the RI predicates.
      val otherPreds = conditions.filterNot(riJoins.contains(_))

      // Find the candidate PK tables to be removed
      val (candidateTables, requiredTables) = riInfo.collect {
        case ri @ RIInfo(pkPlan, pkCols, fkPlan, fkCols, preds, keys) =>

          // Collect all PK table columns below the project.
          // For example, if there is a pushed down Filter, some of the
          // columns might not be projected out, but they need to be considered
          // for the analysis.
          val (allPkPlanCols, pkPlanPDPreds) = pkPlan match {
            case PhysicalOperation(_, conds, t: LeafNode) =>
              (t.output, conds)
            case _ => (Seq.empty, Seq.empty)
          }

          // Construct non-RI predicates
          val nonRIPreds = otherPreds ++ pkPlanPDPreds ++
            riJoins.filterNot(ri.joinCond.contains(_))

          // Check for non eligible predicates.
          val nonEligiblePreds = hasNonEligiblePredicates(nonRIPreds,
            allPkPlanCols, pkCols, fkPlan.output)

          // Check if any PK table columns are used in the projected/output columns
          val inOutputCols = outputCols.exists { exp =>
            exp.references.exists { expCol =>
              pkPlan.outputSet.exists { planCol =>
                planCol.semanticEquals(expCol)
              }
            }
          }

          if (nonEligiblePreds || inOutputCols) {
            (None, Some(pkPlan))
          } else {
            // Find those eligible predicates that have only PK columns and
            // therefore can be replaced by their corresponding FK columns.
            val replaceableConds = nonRIPreds.filter { exp =>
              exp.references.exists { expCol =>
                allPkPlanCols.exists(planCol => planCol.semanticEquals(expCol)) &&
                  pkCols.exists(pk => pk.semanticEquals(expCol))
              }
            }

            (Some((ri, replaceableConds)), None)
          }
      }.unzip


      // For each candidate pk table, check if it can be removed
      val removedPKTables = candidateTables.collect {
        case Some(e) => e
      }.collect {
        case e @ (RIInfo(pkPlan, _, _, _, _, _), _) if !requiredTables.contains(Some(pkPlan)) => e
      }

      // Next, apply the transformations:
      // For each PK plan to be removed, do the following:
      // 1) replace the conditions i.e. cond -> newCond
      // 2) remove the join predicates i.e cond -> newCond
      // 3) add new ISNOTNULL predicates for the FK columns to enforce PK column semantics.
      // 4) remove the PK tables
      // 5) rebuild the join
      removedPKTables match {
        case Nil => None
        case pkRIRemove =>
          val newConds = pkRIRemove.collect {
            case (RIInfo(_, _, _, _, _, jKeys), replaceableConds) =>
              val (pkKeys, fkKeys) = jKeys.unzip
              replaceableConds.map { expr =>
                expr transform {
                  case a: AttributeReference if pkKeys.contains(a) =>
                    jKeys.find {
                      case (e1, e2) if e1.semanticEquals(a) => true
                      case _ => false
                    }.get._2
                }
              }
          }.flatten

          // Add IsNotNull predicates for the FK columns
          val isNotNullPreds = pkRIRemove.collect {
            case (RIInfo(_, _, _, _, _, jKeys), _) =>
              jKeys.collect {
                case (e1, e2) => IsNotNull(e2)
              }
          }.flatten

          // Build the new set of predicates.
          val removedRIJoins = pkRIRemove.collect {
            case (RIInfo(_, _, _, _, riJns, _), _) => riJns

          }.flatten

          val replaceablePreds = pkRIRemove.collect {
            case (RIInfo(_, _, _, _, _, _), oldCond) => oldCond

          }.flatten


          val newConditions = conditions
            .filterNot(removedRIJoins.contains(_))
            .filterNot(replaceablePreds.contains(_)) ++ newConds ++ isNotNullPreds

          // Find the new set of joins
          val removedPlans = pkRIRemove.collect {
            case (RIInfo(p, _, _, _, _, _), _) => p

          }

          val joinPlans = innerPlans.map { p =>
            (p, Inner)
          }

          val newJoinPlans = joinPlans.filterNot {
            case (p, _) if removedPlans.contains(p) => true
            case _ => false
          }

          newJoinPlans match {
            case Nil => None
            case p1 :: Nil =>
              newConditions match {
                case Nil =>
                  Some(p1._1)
                case _ =>
                  val newFilter = Filter(newConditions.reduce(And), p1._1)
                  Some(newFilter)
              }
            case p1 :: p2 :: Nil =>
              val newJoinPlan = ReorderJoin.createOrderedJoin(newJoinPlans, newConditions)
              Some(newJoinPlan)
          }
      }
    case _ => None
  }

  def apply(plan: LogicalPlan): LogicalPlan = plan transform {
    case p @ Project(output, j @ Join(_, _, t: InnerLike, _)) if conf.riJoinElimination =>
      val newJoinPlan = eliminateInnerJoins(j, output)
      if (newJoinPlan.nonEmpty) {
        Project(output, newJoinPlan.get)
      } else {
        p
      }
    case j @ Join(_, _, t: InnerLike, _) if conf.riJoinElimination =>
      val newJoinPlan = eliminateInnerJoins(j, j.output)
      if (newJoinPlan.nonEmpty) {
        newJoinPlan.get
      } else {
        j
      }
  }
}
