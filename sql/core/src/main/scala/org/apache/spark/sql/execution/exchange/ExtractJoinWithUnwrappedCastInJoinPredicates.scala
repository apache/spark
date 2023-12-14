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

package org.apache.spark.sql.execution.exchange

import org.apache.spark.sql.catalyst.analysis.AnsiTypeCoercion.findWiderTypeForTwo
import org.apache.spark.sql.catalyst.expressions.{Cast, EvalMode, Expression}
import org.apache.spark.sql.catalyst.plans.physical.{HashPartitioning, PartitioningCollection}
import org.apache.spark.sql.execution.SparkPlan
import org.apache.spark.sql.execution.joins.ShuffledJoin
import org.apache.spark.sql.types.{DataType, DecimalType, IntegralType}

/**
 * An extractor that extracts `SortMergeJoinExec` and `ShuffledHashJoin`,
 * where one sides can do bucketed read after unwrap cast in join keys.
 */
object ExtractJoinWithUnwrappedCastInJoinPredicates {
  private def isIntegralType(dt: DataType): Boolean = dt match {
    case _: IntegralType => true
    case DecimalType.Fixed(_, 0) => true
    case _ => false
  }

  private def unwrapCastInJoinKeys(joinKeys: Seq[Expression]): Seq[Expression] = {
    joinKeys.map {
      case c: Cast if isIntegralType(c.child.dataType) => c.child
      case e => e
    }
  }

  // Casts the left or right side of join keys to the same data type.
  private def coerceJoinKeyType(
      unwrapLeftKeys: Seq[Expression],
      unwrapRightKeys: Seq[Expression],
      isAddCastToLeftSide: Boolean): Seq[(Expression, Expression)] = {
    unwrapLeftKeys.zip(unwrapRightKeys).map {
      case (l, r) if l.dataType != r.dataType =>
        // Use TRY mode to avoid runtime exception in ANSI mode or data issue in non-ANSI mode.
        if (isAddCastToLeftSide) {
          Cast(l, r.dataType, evalMode = EvalMode.TRY) -> r
        } else {
          l -> Cast(r, l.dataType, evalMode = EvalMode.TRY)
        }
      case (l, r) => l -> r
    }
  }

  private def unwrapCastInJoinPredicates(j: ShuffledJoin): Option[Seq[(Expression, Expression)]] = {
    val leftKeys = unwrapCastInJoinKeys(j.leftKeys)
    val rightKeys = unwrapCastInJoinKeys(j.rightKeys)
    // Make sure cast to wider type.
    // For example, we do not support: cast(longCol as int) = cast(decimalCol as int).
    val isCastToWiderType = leftKeys.zip(rightKeys).zipWithIndex.forall {
      case ((e1, e2), i) =>
        findWiderTypeForTwo(e1.dataType, e2.dataType).contains(j.leftKeys(i).dataType)
    }
    if (isCastToWiderType) {
      val leftSatisfies = j.satisfiesOutputPartitioning(leftKeys, j.left.outputPartitioning)
      val rightSatisfies = j.satisfiesOutputPartitioning(rightKeys, j.right.outputPartitioning)
      if (leftSatisfies && rightSatisfies) {
        // If there is a bucketed read, their number of partitions may be inconsistent.
        // If the number of partitions on the left side is less than the number of partitions
        // on the right side, cast the left side keys to the data type of the right side keys.
        // Otherwise, cast the right side keys to the data type of the left side keys.
        Some(coerceJoinKeyType(leftKeys, rightKeys,
          j.left.outputPartitioning.numPartitions < j.right.outputPartitioning.numPartitions))
      } else if (leftSatisfies) {
        Some(coerceJoinKeyType(leftKeys, rightKeys, false))
      } else if (rightSatisfies) {
        Some(coerceJoinKeyType(leftKeys, rightKeys, true))
      } else {
        None
      }
    } else {
      None
    }
  }

  private def isTryToUnwrapCastInJoinPredicates(j: ShuffledJoin): Boolean = {
    (j.leftKeys.exists(_.isInstanceOf[Cast]) || j.rightKeys.exists(_.isInstanceOf[Cast])) &&
      !j.satisfiesOutputPartitioning(j.leftKeys, j.left.outputPartitioning) &&
      !j.satisfiesOutputPartitioning(j.rightKeys, j.right.outputPartitioning) &&
      j.children.map(_.outputPartitioning).exists { _ match {
        case _: PartitioningCollection => true
        case _: HashPartitioning => true
        case _ => false
      }}
  }

  def unapply(plan: SparkPlan): Option[(ShuffledJoin, Seq[(Expression, Expression)])] = {
    plan match {
      case j: ShuffledJoin if isTryToUnwrapCastInJoinPredicates(j) =>
        unwrapCastInJoinPredicates(j) match {
          case Some(joinKeys) => Some(j, joinKeys)
          case _ => None
        }
      case _ => None
    }
  }
}
