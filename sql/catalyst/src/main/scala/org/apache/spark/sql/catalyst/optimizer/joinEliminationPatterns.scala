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
import org.apache.spark.sql.catalyst.plans.JoinType
import org.apache.spark.sql.catalyst.plans.LeftOuter
import org.apache.spark.sql.catalyst.plans.RightOuter
import org.apache.spark.sql.catalyst.plans.logical._

/**
 * Finds outer joins where only the outer table's columns are kept, and a key from the inner table
 * is involved in the join so no duplicates would be generated.
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
 * Finds equijoins based on foreign-key referential integrity, followed by [[Project]]s that
 * reference no columns from the parent table other than the referenced unique keys.
 *
 * The table containing the foreign key is referred to as the child table, while the table
 * containing the referenced unique key is referred to as the parent table. Such equijoins can be
 * eliminated and replaced by the child table.
 *
 * See [[http://www.info.teradata.com/HTMLPubs/DB_TTU_14_00/index.html#page/SQL_Reference/B035_1142_111A/ch02.124.045.html]].
 */
object CanEliminateReferentialIntegrityEquiJoin {
  /** (joinType, parent, child, primaryForeignMap, projectList) */
  type ReturnType =
    (JoinType, LogicalPlan, LogicalPlan, AttributeMap[Attribute], Seq[NamedExpression])

  def unapply(plan: LogicalPlan): Option[ReturnType] = plan match {
    case Project(projectList,
      ExtractEquiJoinKeys(joinType, leftJoinExprs, rightJoinExprs, _, left, right)) =>
      val leftParentPFM = getPrimaryForeignMap(left, right, leftJoinExprs, rightJoinExprs)
      val leftIsParent =
        leftParentPFM.nonEmpty && onlyPrimaryKeysKept(projectList, leftParentPFM, left)
      val rightParentPFM = getPrimaryForeignMap(right, left, rightJoinExprs, leftJoinExprs)
      val rightIsParent =
        rightParentPFM.nonEmpty && onlyPrimaryKeysKept(projectList, rightParentPFM, right)

      if (leftIsParent) {
        Some((joinType, left, right, leftParentPFM, projectList))
      } else if (rightIsParent) {
        Some((joinType, right, left, rightParentPFM, projectList))
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
      case ForeignKey(attr2, referencedAttr2) if attr == attr2 && equivalent.query(referencedAttr, referencedAttr2) => true
      case _ => false
    }
  }

  private def equivalences(plan: LogicalPlan): MutableDisjointSet[Attribute] = {
    val s = new MutableDisjointSet[Attribute]
    plan.collect {
      case Project(projectList, _) => projectList.collect {
        case a @ Alias(old: Attribute, _) => s.union(old, a.toAttribute)
      }
    }
    s
  }

}

private class MutableDisjointSet[A]() {
  import scala.collection.mutable.Set
  private var sets = Set[Set[A]]()
  def add(x: A): Unit = {
    if (!sets.exists(_.contains(x))) {
      sets += Set(x)
    }
  }
  def union(x: A, y: A): Unit = {
    add(x)
    add(y)
    val xSet = sets.find(_.contains(x)).get
    val ySet = sets.find(_.contains(y)).get
    sets -= xSet
    sets -= ySet
    sets += (xSet ++ ySet)
  }
  def query(x: A, y: A): Boolean = {
    x == y || sets.exists(s => s.contains(x) && s.contains(y))
  }
}
