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

import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.planning.ExtractEquiJoinKeys
import org.apache.spark.sql.catalyst.plans._
import org.apache.spark.sql.catalyst.plans.logical._

/**
 * Finds left or right outer joins where only the outer table's columns are kept, and a key from the
 * inner table is involved in the join so no duplicates would be generated.
 */
object CanEliminateUniqueKeyOuterJoin {
  /** (outer, projectList) */
  type ReturnType = (LogicalPlan, Seq[NamedExpression])

  def unapply(plan: LogicalPlan): Option[ReturnType] = plan match {
    case p @ Project(projectList,
        ExtractEquiJoinKeys(
          joinType @ (LeftOuter | RightOuter), leftJoinExprs, rightJoinExprs, _, left, right)) =>
      val (outer, inner, innerJoinExprs) = (joinType: @unchecked) match {
        case LeftOuter => (left, right, rightJoinExprs)
        case RightOuter => (right, left, leftJoinExprs)
      }

      val onlyOuterColsKept = AttributeSet(projectList).subsetOf(outer.outputSet)

      val innerUniqueKeys = AttributeSet(inner.keys.collect { case UniqueKey(attr) => attr })
      val innerKeyIsInvolved = innerUniqueKeys.intersect(AttributeSet(innerJoinExprs)).nonEmpty

      if (onlyOuterColsKept && innerKeyIsInvolved) {
        Some((outer, projectList))
      } else {
        None
      }

    case _ => None
  }
}

/**
 * Finds joins based on foreign-key referential integrity, followed by [[Project]]s that reference
 * no columns from the parent table other than the referenced unique keys. Such joins can be
 * eliminated and replaced by the child table.
 *
 * The table containing the foreign key is referred to as the child table, while the table
 * containing the referenced unique key is referred to as the parent table.
 *
 * For inner joins, all involved foreign keys must be non-nullable.
 */
object CanEliminateReferentialIntegrityJoin {
  /** (parent, child, primaryForeignMap, projectList) */
  type ReturnType =
    (LogicalPlan, LogicalPlan, AttributeMap[Attribute], Seq[NamedExpression])

  def unapply(plan: LogicalPlan): Option[ReturnType] = plan match {
    case p @ Project(projectList, ExtractEquiJoinKeys(
        joinType @ (Inner | LeftOuter | RightOuter),
        leftJoinExprs, rightJoinExprs, _, left, right)) =>
      val innerJoin = joinType == Inner

      val leftParentPFM = getPrimaryForeignMap(left, right, leftJoinExprs, rightJoinExprs)
      val rightForeignKeysAreNonNullable = leftParentPFM.values.forall(!_.nullable)
      val leftIsParent =
        (leftParentPFM.nonEmpty && onlyPrimaryKeysKept(projectList, leftParentPFM, left)
          && (!innerJoin || rightForeignKeysAreNonNullable))

      val rightParentPFM = getPrimaryForeignMap(right, left, rightJoinExprs, leftJoinExprs)
      val leftForeignKeysAreNonNullable = rightParentPFM.values.forall(!_.nullable)
      val rightIsParent =
        (rightParentPFM.nonEmpty && onlyPrimaryKeysKept(projectList, rightParentPFM, right)
          && (!innerJoin || leftForeignKeysAreNonNullable))

      if (leftIsParent) {
        Some((left, right, leftParentPFM, projectList))
      } else if (rightIsParent) {
        Some((right, left, rightParentPFM, projectList))
      } else {
        None
      }

    case _ => None
  }

  /**
   * Return a map where, for each PK=FK join expression based on referential integrity between
   * `parent` and `child`, the unique key from `parent` is mapped to its corresponding foreign
   * key from `child`.
   */
  private def getPrimaryForeignMap(
      parent: LogicalPlan,
      child: LogicalPlan,
      parentJoinExprs: Seq[Expression],
      childJoinExprs: Seq[Expression])
    : AttributeMap[Attribute] = {
    val primaryKeys = AttributeSet(parent.keys.collect { case UniqueKey(attr) => attr })
    val foreignKeys = new ForeignKeyFinder(child, parent)
    AttributeMap(parentJoinExprs.zip(childJoinExprs).collect {
      case (parentExpr: NamedExpression, childExpr: NamedExpression)
          if primaryKeys.contains(parentExpr.toAttribute)
          && foreignKeys.foreignKeyExists(childExpr.toAttribute, parentExpr.toAttribute) =>
        (parentExpr.toAttribute, childExpr.toAttribute)
    })
  }

  /**
   * Return true if `kept` references no columns from `parent` except those involved in a PK=FK
   * join expression. Such join expressions are stored in `primaryForeignMap`.
   */
  private def onlyPrimaryKeysKept(
      kept: Seq[NamedExpression],
      primaryForeignMap: AttributeMap[Attribute],
      parent: LogicalPlan)
    : Boolean = {
    AttributeSet(kept).forall { keptAttr =>
      if (parent.outputSet.contains(keptAttr)) {
        primaryForeignMap.contains(keptAttr)
      } else {
        true
      }
    }
  }
}

private class ForeignKeyFinder(plan: LogicalPlan, referencedPlan: LogicalPlan) {
  val equivalent = equivalences(referencedPlan)

  def foreignKeyExists(attr: Attribute, referencedAttr: Attribute): Boolean = {
    plan.keys.exists {
      case ForeignKey(attr2, _, referencedAttr2)
          if (attr semanticEquals attr2)
            && equivalent.query(referencedAttr, referencedAttr2) => true
      case _ => false
    }
  }

  private def equivalences(plan: LogicalPlan): MutableDisjointAttributeSets = {
    val s = new MutableDisjointAttributeSets
    plan.collect {
      case Project(projectList, _) => projectList.collect {
        case a @ Alias(old: Attribute, _) => s.union(old, a.toAttribute)
      }
    }
    s
  }
}

private class MutableDisjointAttributeSets() {
  private var sets = Set[AttributeSet]()
  def add(x: Attribute): Unit = {
    if (!sets.exists(_.contains(x))) {
      sets += AttributeSet(x)
    }
  }
  def union(x: Attribute, y: Attribute): Unit = {
    add(x)
    add(y)
    val xSet = sets.find(_.contains(x)).get
    val ySet = sets.find(_.contains(y)).get
    sets -= xSet
    sets -= ySet
    sets += (xSet ++ ySet)
  }
  def query(x: Attribute, y: Attribute): Boolean = {
    (x semanticEquals y) || sets.exists(s => s.contains(x) && s.contains(y))
  }
}
