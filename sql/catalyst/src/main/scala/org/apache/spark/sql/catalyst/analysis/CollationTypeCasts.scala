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

import javax.annotation.Nullable
import scala.annotation.tailrec

import org.apache.spark.sql.catalyst.expressions.{Alias, BinaryExpression, Cast, Collate, ComplexTypeMergingExpression, CreateArray, ExpectsInputTypes, Expression, Predicate, SortOrder}
import org.apache.spark.sql.catalyst.expressions.objects.Invoke
import org.apache.spark.sql.catalyst.util.CollationFactory
import org.apache.spark.sql.errors.QueryCompilationErrors
import org.apache.spark.sql.types.{AbstractDataType, ArrayType, DataType, StringType}

object CollationTypeCasts extends TypeCoercionRule {
  override val transform: PartialFunction[Expression, Expression] = {
    case e if !e.childrenResolved => e
    // Case when we do not fail if resulting collation is indeterminate
    case checkCastWithIndeterminate @ (_: ComplexTypeMergingExpression
                                       | _: CreateArray) =>
      val newChildren =
        collateToSingleType(checkCastWithIndeterminate.children, failOnIndeterminate = false)
      checkCastWithIndeterminate.withNewChildren(newChildren)
    // Case when we do fail if resulting collation is indeterminate
    case checkCastWithoutIndeterminate @ (_: BinaryExpression
                                          | _: Predicate
                                          | _: SortOrder
                                          | _: ExpectsInputTypes)
      if shouldCast(checkCastWithoutIndeterminate.children) =>
      val newChildren = collateToSingleType(checkCastWithoutIndeterminate.children)
      checkCastWithoutIndeterminate.withNewChildren(newChildren)
    // Case if casting is not needed, but we only have indeterminate
    // collations and we do not want to fail
    case checkIndeterminateWithoutFail@(_: Invoke)
      if hasIndeterminate(checkIndeterminateWithoutFail.children
      .filter(e => hasStringType(e.dataType))
      .map(e => extractStringType(e.dataType))) =>
      checkIndeterminateWithoutFail
    // Case if casting is not needed, but we only have indeterminate collations
    case checkIndeterminate@(_: BinaryExpression
                             | _: Predicate
                             | _: SortOrder
                             | _: ExpectsInputTypes
                             | _: Alias)
      if hasIndeterminate(checkIndeterminate.children
        .filter(e => hasStringType(e.dataType))
        .map(e => extractStringType(e.dataType))) =>
      throw QueryCompilationErrors.indeterminateCollationError()
  }

  def shouldCast(types: Seq[Expression]): Boolean = {
    types.filter(e => hasStringType(e.dataType))
      .map(e => extractStringType(e.dataType).collationId).distinct.size > 1
  }

  /**
   * Checks whether given data type contains StringType.
   */
  @tailrec
  def hasStringType(dt: DataType): Boolean = dt match {
    case _: StringType => true
    case ArrayType(et, _) => hasStringType(et)
    case _ => false
  }

  /**
   * Extracts StringTypes from filtered hasStringType
   */
  @tailrec
  private def extractStringType(dt: DataType): StringType = dt match {
    case st: StringType => st
    case ArrayType(et, _) => extractStringType(et)
  }

  /**
   * Casts given expression to collated StringType with id equal to collationId only
   * if expression has StringType in the first place.
   * @param expr
   * @param collationId
   * @return
   */
  def castStringType(expr: Expression, collationId: Int): Option[Expression] =
    castStringType(expr.dataType, collationId).map { dt =>
      if (dt == expr.dataType) expr else Cast(expr, dt)
    }

  private def castStringType(inType: AbstractDataType, collationId: Int): Option[DataType] = {
    @Nullable val ret: DataType = inType match {
      case st: StringType if st.collationId == collationId => st
      case _: StringType => StringType(collationId)
      case ArrayType(arrType, nullable) =>
        castStringType(arrType, collationId).map(ArrayType(_, nullable)).orNull
      case _ => null
    }
    Option(ret)
  }

  /**
   * Collates input expressions to a single collation.
   */
  def collateToSingleType(exprs: Seq[Expression],
                          failOnIndeterminate: Boolean = true): Seq[Expression] = {
    val collationId = getOutputCollation(exprs, failOnIndeterminate)

    exprs.map(e => castStringType(e, collationId).getOrElse(e))
  }

  /**
   * Based on the data types of the input expressions this method determines
   * a collation type which the output will have. This function accepts Seq of
   * any expressions, but will only be affected by collated StringTypes or
   * complex DataTypes with collated StringTypes (e.g. ArrayType)
   */
  def getOutputCollation(exprs: Seq[Expression], failOnIndeterminate: Boolean = true): Int = {
    val explicitTypes = exprs.filter(hasExplicitCollation)
      .map(e => extractStringType(e.dataType).collationId).distinct

    explicitTypes.size match {
      // We have 1 explicit collation
      case 1 => explicitTypes.head
      // Multiple explicit collations occurred
      case size if size > 1 =>
        throw QueryCompilationErrors
          .explicitCollationMismatchError(
            explicitTypes.map(t => StringType(t).typeName)
          )
      // Only implicit or default collations present
      case 0 =>
        val dataTypes = exprs.filter(e => hasStringType(e.dataType))
          .map(e => extractStringType(e.dataType))

        if (hasMultipleImplicits(dataTypes)) {
          if (failOnIndeterminate) {
            throw QueryCompilationErrors.implicitCollationMismatchError()
          } else {
            CollationFactory.INDETERMINATE_COLLATION_ID
          }
        }
        else {
          dataTypes.find(!_.isDefaultCollation)
            .getOrElse(StringType)
            .collationId
        }
    }
  }

  /**
   * Checks if there exists an input with input type StringType(-1)
   * @param dataTypes
   * @return
   */
  private def hasIndeterminate(dataTypes: Seq[DataType]): Boolean =
    dataTypes.exists(dt => dt.isInstanceOf[StringType]
      && dt.asInstanceOf[StringType].isIndeterminateCollation)

  /**
   * This check is always preformed when we have no explicit collation. It returns true
   * if there are more than one implicit collations. Collations are distinguished by their
   * collationId.
   * @param dataTypes
   * @return
   */
  private def hasMultipleImplicits(dataTypes: Seq[StringType]): Boolean =
    dataTypes.filter(!_.isDefaultCollation).map(_.collationId).distinct.size > 1

  /**
   * Checks if a given expression has explicitly set collation. For complex DataTypes
   * we need to check nested children.
   * @param expression
   * @return
   */
  private def hasExplicitCollation(expression: Expression): Boolean = {
    expression match {
      case _: Collate => true
      case e if e.dataType.isInstanceOf[ArrayType]
      => expression.children.exists(hasExplicitCollation)
      case _ => false
    }
  }
}
