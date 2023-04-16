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

package org.apache.spark.sql.execution.bucketing

import org.apache.spark.sql.catalyst.analysis.AnsiTypeCoercion.findWiderTypeForTwo
import org.apache.spark.sql.catalyst.expressions.{Attribute, Cast, EvalMode, Expression}
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.execution.SparkPlan
import org.apache.spark.sql.execution.joins.{ShuffledHashJoinExec, ShuffledJoin, SortMergeJoinExec}
import org.apache.spark.sql.types.{DataType, DecimalType, IntegralType}

/**
 * This rule unwrap the cast in one side of the `SortMergeJoin` and `ShuffledHashJoin` join keys.
 */
object UnwrapCastInJoinCondition extends Rule[SparkPlan] {
  def apply(plan: SparkPlan): SparkPlan = {
    if (!conf.unwrapCastInJoinConditionEnabled) {
      return plan
    }

    plan transform {
      case ExtractJoinWithUnwrapCastInJoinCondition(join, joinKeys) =>
        val (leftKeys, rightKeys) = joinKeys.unzip
        join match {
          case j: SortMergeJoinExec =>
            j.copy(leftKeys = leftKeys, rightKeys = rightKeys)
          case j: ShuffledHashJoinExec =>
            j.copy(leftKeys = leftKeys, rightKeys = rightKeys)
          case other => other
        }
      case other => other
    }
  }
}

/**
 * An extractor that extracts `SortMergeJoinExec` and `ShuffledHashJoin`,
 * where one sides can do bucketed read after unwrap cast in join keys.
 */
object ExtractJoinWithUnwrapCastInJoinCondition extends BucketJoinHelper {
  private def isIntegralType(dt: DataType): Boolean = dt match {
    case _: IntegralType => true
    case DecimalType.Fixed(_, 0) => true
    case _ => false
  }

  private def unwrapCastInJoinKeys(joinKeys: Seq[Expression]): Seq[Expression] = {
    joinKeys.map {
      case Cast(a: Attribute, _, _, _) if isIntegralType(a.dataType) => a
      case e => e
    }
  }

  private def followLeftJoinKeyType(
      unwrapLeftKeys: Seq[Expression],
      unwrapRightKeys: Seq[Expression]): Seq[(Expression, Expression)] = {
    unwrapLeftKeys.zip(unwrapRightKeys).map {
      case (l, r) if l.dataType != r.dataType =>
        // Use TRY mode to avoid runtime exception in ANSI mode or data issue in non-ANSI mode.
        l -> Cast(r, l.dataType, evalMode = EvalMode.TRY)
      case (l, r) => l -> r
    }
  }

  private def followRightJoinKeyType(
      unwrapLeftKeys: Seq[Expression],
      unwrapRightKeys: Seq[Expression]): Seq[(Expression, Expression)] = {
    unwrapLeftKeys.zip(unwrapRightKeys).map {
      case (l, r) if l.dataType != r.dataType =>
        Cast(l, r.dataType, evalMode = EvalMode.TRY) -> r
      case (l, r) => l -> r
    }
  }

  private def unwrapCastInJoinCondition(
      j: ShuffledJoin): Option[Seq[(Expression, Expression)]] = {
    val unwrapLeftKeys = unwrapCastInJoinKeys(j.leftKeys)
    val unwrapRightKeys = unwrapCastInJoinKeys(j.rightKeys)
    // Make sure cast to wider type.
    // For example, we do not support: cast(longCol as int) = cast(decimalCol as int).
    val isCastToWiderType = unwrapLeftKeys.zip(unwrapRightKeys).zipWithIndex.forall {
      case ((e1, e2), i) =>
        findWiderTypeForTwo(e1.dataType, e2.dataType).contains(j.leftKeys(i).dataType)
    }
    if (isCastToWiderType) {
      val leftOutputPart = j.left.outputPartitioning
      val rightOutputPart = j.right.outputPartitioning
      val leftSatisfies = satisfiesOutputPartitioning(j.leftKeys, leftOutputPart)
      val leftUnwrapSatisfies = satisfiesOutputPartitioning(unwrapLeftKeys, leftOutputPart)
      val rightSatisfies = satisfiesOutputPartitioning(j.rightKeys, rightOutputPart)
      val rightUnwrapSatisfies = satisfiesOutputPartitioning(unwrapRightKeys, rightOutputPart)

      (leftSatisfies, leftUnwrapSatisfies, rightSatisfies, rightUnwrapSatisfies) match {
        case (_, true, _, true) =>
          // Follows join side key types with larger bucket numbers.
          if (j.left.outputPartitioning.numPartitions > j.right.outputPartitioning.numPartitions) {
            Some(followLeftJoinKeyType(unwrapLeftKeys, unwrapRightKeys))
          } else {
            Some(followRightJoinKeyType(unwrapLeftKeys, unwrapRightKeys))
          }
        case (false, true, _, _) =>
          Some(followLeftJoinKeyType(unwrapLeftKeys, unwrapRightKeys))
        case (_, _, false, true) =>
          Some(followRightJoinKeyType(unwrapLeftKeys, unwrapRightKeys))
        case _ =>
          None
      }
    } else {
      None
    }
  }

  def unapply(plan: SparkPlan): Option[(ShuffledJoin, Seq[(Expression, Expression)])] = {
    plan match {
      case j: ShuffledJoin if (hasScanOperation(j.left) &&
        !satisfiesOutputPartitioning(j.leftKeys, j.left.outputPartitioning)) ||
        (hasScanOperation(j.right) &&
          !satisfiesOutputPartitioning(j.rightKeys, j.right.outputPartitioning)) =>
        unwrapCastInJoinCondition(j) match {
          case Some(joinKeys) => Some(j, joinKeys)
          case _ => None
        }
      case _ => None
    }
  }
}
