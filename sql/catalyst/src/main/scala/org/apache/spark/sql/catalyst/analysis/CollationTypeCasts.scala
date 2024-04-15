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

import org.apache.spark.sql.catalyst.analysis.TypeCoercion.{hasStringType, haveSameType}
import org.apache.spark.sql.catalyst.expressions.{ArrayJoin, BinaryExpression, CaseWhen, Cast, Coalesce, Collate, Concat, ConcatWs, CreateArray, Expression, Greatest, If, In, InSubquery, Least}
import org.apache.spark.sql.errors.QueryCompilationErrors
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.types.{ArrayType, DataType, StringType}

object CollationTypeCasts extends TypeCoercionRule {
  override val transform: PartialFunction[Expression, Expression] = {
    case e if !e.childrenResolved => e

    case ifExpr: If =>
      ifExpr.withNewChildren(
        ifExpr.predicate +: collateToSingleType(Seq(ifExpr.trueValue, ifExpr.falseValue)))

    case caseWhenExpr: CaseWhen if !haveSameType(caseWhenExpr.inputTypesForMerging) =>
      val outputStringType =
        getOutputCollation(caseWhenExpr.branches.map(_._2) ++ caseWhenExpr.elseValue)
        val newBranches = caseWhenExpr.branches.map { case (condition, value) =>
          (condition, castStringType(value, outputStringType).getOrElse(value))
        }
        val newElseValue =
          caseWhenExpr.elseValue.map(e => castStringType(e, outputStringType).getOrElse(e))
        CaseWhen(newBranches, newElseValue)

    case otherExpr @ (
      _: In | _: InSubquery | _: CreateArray | _: ArrayJoin | _: Concat | _: Greatest | _: Least |
      _: Coalesce | _: BinaryExpression | _: ConcatWs) =>
      val newChildren = collateToSingleType(otherExpr.children)
      otherExpr.withNewChildren(newChildren)
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
  def castStringType(expr: Expression, st: StringType): Option[Expression] =
    castStringType(expr.dataType, st).map { dt => Cast(expr, dt)}

  private def castStringType(inType: DataType, castType: StringType): Option[DataType] = {
    @Nullable val ret: DataType = inType match {
      case st: StringType if st.collationId != castType.collationId => castType
      case ArrayType(arrType, nullable) =>
        castStringType(arrType, castType).map(ArrayType(_, nullable)).orNull
      case _ => null
    }
    Option(ret)
  }

  /**
   * Collates input expressions to a single collation.
   */
  def collateToSingleType(exprs: Seq[Expression]): Seq[Expression] = {
    val st = getOutputCollation(exprs)

    exprs.map(e => castStringType(e, st).getOrElse(e))
  }

  /**
   * Based on the data types of the input expressions this method determines
   * a collation type which the output will have. This function accepts Seq of
   * any expressions, but will only be affected by collated StringTypes or
   * complex DataTypes with collated StringTypes (e.g. ArrayType)
   */
  def getOutputCollation(expr: Seq[Expression]): StringType = {
    val explicitTypes = expr.filter(_.isInstanceOf[Collate])
      .map(_.dataType.asInstanceOf[StringType].collationId)
      .distinct

    explicitTypes.size match {
      // We have 1 explicit collation
      case 1 => StringType(explicitTypes.head)
      // Multiple explicit collations occurred
      case size if size > 1 =>
        throw QueryCompilationErrors
          .explicitCollationMismatchError(
            explicitTypes.map(t => StringType(t).typeName)
          )
      // Only implicit or default collations present
      case 0 =>
        val implicitTypes = expr.map(_.dataType)
          .filter(hasStringType)
          .map(extractStringType)
          .filter(dt => dt.collationId != SQLConf.get.defaultStringType.collationId)
          .distinctBy(_.collationId)

        if (implicitTypes.length > 1) {
          throw QueryCompilationErrors.implicitCollationMismatchError()
        }
        else {
          implicitTypes.headOption.getOrElse(SQLConf.get.defaultStringType)
        }
    }
  }
}
