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

package org.apache.spark.sql.catalyst.analysis.resolver

import org.apache.spark.sql.catalyst.expressions.{
  Cast,
  DefaultStringProducingExpression,
  Expression,
  Literal
}
import org.apache.spark.sql.types.{DataType, StringType}

/**
 * This type coercion object is only used in the single-pass analyzer and is not part of the
 * [[TypeCoercion]] rules used in the fixed-point analyzer.
 *
 * When the database object (e.g. [[View]]) has a custom default collation and a
 * resolving expression's dataType is the companion object [[StringType]], we need to treat the
 * dataType as a collated [[StringType]]. To do this, we wrap the expression with a cast to a
 * [[StringType]] with the default collation or change the dataType to [[StringType]] with the
 * default collation. Note that when the dataType is equal to the companion object [[StringType]],
 * but isn't the same by reference, we shouldn't change the dataType, since that means the user
 * explicitly specified UTF8_BINARY collation.
 */
object DefaultCollationTypeCoercion {

  /**
   * Apply [[View]]'s default collation to the expression.
   */
  def apply(expression: Expression, collation: String): Expression = {
    val collatedStringType = StringType(collation)
    expression match {
      // Skip an expression carrying an explicitly collated string type (e.g. `JSON_ARRAY(...
      // RETURNING STRING COLLATE UTF8_BINARY)`): a deliberate choice the object/view default must
      // not overwrite, even for the default UTF8_BINARY. Only the explicit `COLLATE` form is a
      // distinct instance; `DataTypeAstBuilder` returns the companion for a plain `STRING`, so a
      // plain `RETURNING STRING` is indistinguishable from omission and recolored like it.
      // `isExplicitlyCollatedStringType` tells the distinct instance from the companion by
      // reference identity.
      case e: DefaultStringProducingExpression
          if collatedStringType != StringType && !isExplicitlyCollatedStringType(e.dataType) =>
        Cast(child = expression, dataType = collatedStringType)
      case literal: Literal if hasDefaultStringType(literal.dataType) =>
        literal.copy(dataType = replaceDefaultStringType(literal.dataType, collatedStringType))
      case cast: Cast if shouldApplyCollationToCast(cast) =>
        cast.copy(dataType = replaceDefaultStringType(cast.dataType, collatedStringType))
      case _ => expression
    }
  }

  /**
   * Check if [[DataType]] contains companion object [[StringType]].
   *
   * Should not be called with a [[DataType]] that will traverse the [[Expression]] recursively.
   */
  private def hasDefaultStringType(dataType: DataType): Boolean =
    dataType.existsRecursively(isDefaultStringType)

  /**
   * Replace each instance of companion object [[StringType]] with a newType.
   *
   * Should not be called with a [[DataType]] that will traverse the [[Expression]] recursively.
   */
  private def replaceDefaultStringType(dataType: DataType, newType: StringType): DataType =
    dataType.transformRecursively {
      case currentType: StringType if isDefaultStringType(currentType) =>
        newType
    }

  /**
   * STRING (without explicit collation) is considered default string type.
   * STRING COLLATE <collation_name> (with explicit collation) is not considered
   * default string type even when explicit collation is UTF8_BINARY (default collation).
   * Should only return true for StringType object and not for StringType("UTF8_BINARY").
   */
  private def isDefaultStringType(dataType: DataType): Boolean =
    dataType match {
      case stringType: StringType => stringType.eq(StringType)
      case _ => false
    }

  /**
   * A [[StringType]] that was written explicitly -- a distinct instance, not the companion-object
   * default -- even when its collation is the default UTF8_BINARY (see [[isDefaultStringType]]).
   */
  private def isExplicitlyCollatedStringType(dataType: DataType): Boolean =
    dataType.isInstanceOf[StringType] && !isDefaultStringType(dataType)

  /**
   * When a default collation is specified for a View,
   * and the cast's dataType contains companion object [[StringType]],
   * we should change all its occurrences to [[StringType]] with default collation.
   */
  private def shouldApplyCollationToCast(cast: Cast): Boolean = {
    cast.containsTag(Cast.USER_SPECIFIED_CAST) &&
    hasDefaultStringType(cast.dataType)
  }
}
