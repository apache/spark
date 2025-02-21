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

package org.apache.spark.sql.catalyst.analysis

import org.apache.spark.sql.catalyst.SQLConfHelper
import org.apache.spark.sql.catalyst.expressions.{
  Alias,
  And,
  Attribute,
  AttributeSet,
  Coalesce,
  EqualTo,
  Expression,
  NamedExpression
}
import org.apache.spark.sql.catalyst.plans.{
  FullOuter,
  InnerLike,
  JoinType,
  LeftExistence,
  LeftOuter,
  RightOuter
}
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.errors.{
  DataTypeErrorsBase,
  QueryCompilationErrors,
  QueryExecutionErrors
}

object NaturalAndUsingJoinResolution extends DataTypeErrorsBase with SQLConfHelper {

  /**
   * For a given [[Join]], computes output, hidden output and new condition, if such exists.
   */
  def computeJoinOutputsAndNewCondition(
      left: LogicalPlan,
      leftOutput: Seq[Attribute],
      right: LogicalPlan,
      rightOutput: Seq[Attribute],
      joinType: JoinType,
      joinNames: Seq[String],
      condition: Option[Expression],
      resolveName: (String, String) => Boolean)
      : (Seq[NamedExpression], Seq[Attribute], Option[Expression]) = {
    val (leftKeys, rightKeys) = resolveKeysForNaturalAndUsingJoin(
      left,
      leftOutput,
      right,
      rightOutput,
      joinNames,
      resolveName
    )
    val joinPairs = leftKeys.zip(rightKeys)

    val newCondition = (condition ++ joinPairs.map(EqualTo.tupled)).reduceOption(And)

    // the output list looks like: join keys, columns from left, columns from right
    val (output, hiddenOutput) = computeOutputAndHiddenOutput(
      leftOutput,
      leftKeys,
      rightOutput,
      rightKeys,
      joinPairs,
      joinType
    )
    (output, hiddenOutput, newCondition)
  }

  /**
   * Returns resolved keys for joining based on the output of [[Join]]'s children or throws and
   * error if a key name doesn't exist.
   */
  private def resolveKeysForNaturalAndUsingJoin(
      left: LogicalPlan,
      leftOutput: Seq[Attribute],
      right: LogicalPlan,
      rightOutput: Seq[Attribute],
      joinNames: Seq[String],
      resolveName: (String, String) => Boolean): (Seq[Attribute], Seq[Attribute]) = {
    val leftKeys = joinNames.map { keyName =>
      leftOutput.find(attribute => resolveName(attribute.name, keyName)).getOrElse {
        throw QueryCompilationErrors.unresolvedUsingColForJoinError(
          keyName,
          left.schema.fieldNames.sorted.map(toSQLId).mkString(", "),
          "left"
        )
      }
    }
    val rightKeys = joinNames.map { keyName =>
      rightOutput.find(attribute => resolveName(attribute.name, keyName)).getOrElse {
        throw QueryCompilationErrors.unresolvedUsingColForJoinError(
          keyName,
          right.schema.fieldNames.sorted.map(toSQLId).mkString(", "),
          "right"
        )
      }
    }
    (leftKeys, rightKeys)
  }

  /**
   * Computes the output and hidden output for a given [[Join]], based on the output of its
   * children.
   */
  private def computeOutputAndHiddenOutput(
      leftOutput: Seq[Attribute],
      leftKeys: Seq[Attribute],
      rightOutput: Seq[Attribute],
      rightKeys: Seq[Attribute],
      joinPairs: Seq[(Attribute, Attribute)],
      joinType: JoinType): (Seq[NamedExpression], Seq[Attribute]) = {
    // columns not in joinPairs
    val leftKeysLookup = AttributeSet(leftKeys)
    val rightKeysLookup = AttributeSet(rightKeys)
    val lUniqueOutput = leftOutput.filterNot(att => leftKeysLookup.contains(att))
    val rUniqueOutput = rightOutput.filterNot(att => rightKeysLookup.contains(att))
    joinType match {
      case LeftOuter =>
        (
          leftKeys ++ lUniqueOutput ++ rUniqueOutput.map(_.withNullability(true)),
          rightKeys.map(_.withNullability(true))
        )
      case LeftExistence(_) =>
        (leftKeys ++ lUniqueOutput, Seq.empty)
      case RightOuter =>
        (
          rightKeys ++ lUniqueOutput.map(_.withNullability(true)) ++ rUniqueOutput,
          leftKeys.map(_.withNullability(true))
        )
      case FullOuter =>
        // In full outer join, we should return non-null values for the join columns
        // if either side has non-null values for those columns. Therefore, for each
        // join column pair, add a coalesce to return the non-null value, if it exists.
        val joinedCols = joinPairs.map {
          case (l, r) =>
            // Since this is a full outer join, either side could be null, so we explicitly
            // set the nullability to true for both sides.
            Alias(Coalesce(Seq(l.withNullability(true), r.withNullability(true))), l.name)()
        }
        (
          joinedCols ++
          lUniqueOutput.map(_.withNullability(true)) ++
          rUniqueOutput.map(_.withNullability(true)),
          leftKeys.map(_.withNullability(true)) ++
          rightKeys.map(_.withNullability(true))
        )
      case _: InnerLike =>
        (leftKeys ++ lUniqueOutput ++ rUniqueOutput, rightKeys)
      case _ =>
        throw QueryExecutionErrors.unsupportedNaturalJoinTypeError(joinType)
    }
  }
}
