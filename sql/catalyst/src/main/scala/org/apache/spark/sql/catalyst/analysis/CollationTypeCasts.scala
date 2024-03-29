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

import org.apache.spark.sql.catalyst.analysis.TypeCoercion.hasStringType
import org.apache.spark.sql.catalyst.expressions.{BinaryExpression, Cast, ComplexTypeMergingExpression, CreateArray, ExpectsInputTypes, Expression, Predicate, SortOrder}
import org.apache.spark.sql.errors.QueryCompilationErrors
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.types.{AbstractDataType, ArrayType, DataType, StringType}

object CollationTypeCasts extends TypeCoercionRule {
  override val transform: PartialFunction[Expression, Expression] = {
    case e if !e.childrenResolved => e
    case sc @ (_: BinaryExpression
              | _: Predicate
              | _: SortOrder
              | _: ExpectsInputTypes
              | _: ComplexTypeMergingExpression
              | _: CreateArray) =>
      val newChildren = collateToSingleType(sc.children)
      sc.withNewChildren(newChildren)
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
    castStringType(expr.dataType, collationId).map { dt => Cast(expr, dt)}

  private def castStringType(inType: AbstractDataType, collationId: Int): Option[DataType] = {
    @Nullable val ret: DataType = inType match {
      case st: StringType if st.collationId == collationId && !st.isExplicit => null
      case _: StringType => StringType(collationId, isExplicit = false)
      case ArrayType(arrType, nullable) =>
        castStringType(arrType, collationId).map(ArrayType(_, nullable)).orNull
      case _ => null
    }
    Option(ret)
  }

  /**
   * Collates input expressions to a single collation.
   */
  def collateToSingleType(exprs: Seq[Expression]): Seq[Expression] = {
    val collationId = getOutputCollation(exprs.map(_.dataType))

    exprs.map(e => castStringType(e, collationId).getOrElse(e))
  }

  /**
   * Based on the data types of the input expressions this method determines
   * a collation type which the output will have. This function accepts Seq of
   * any expressions, but will only be affected by collated StringTypes or
   * complex DataTypes with collated StringTypes (e.g. ArrayType)
   */
  def getOutputCollation(dataTypes: Seq[DataType]): Int = {
    val explicitTypes =
      dataTypes.filter(hasStringType)
        .map(extractStringType)
        .filter(_.isExplicit)
        .map(_.collationId)
        .distinct

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
        val implicitTypes = dataTypes.filter(hasStringType).map(extractStringType)

        if (hasMultipleImplicits(implicitTypes)) {
          throw QueryCompilationErrors.implicitCollationMismatchError()
        }
        else {
          implicitTypes.find(dt => !(dt == SQLConf.get.defaultStringType))
            .getOrElse(SQLConf.get.defaultStringType)
            .collationId
        }
    }
  }

  /**
   * This check is always preformed when we have no explicit collation. It returns true
   * if there are more than one implicit collations. Collations are distinguished by their
   * collationId.
   * @param dataTypes
   * @return
   */
  private def hasMultipleImplicits(dataTypes: Seq[StringType]): Boolean =
    dataTypes.map(_.collationId)
      .filter(dt => !(dt == SQLConf.get.defaultStringType.collationId)).distinct.size > 1

}
