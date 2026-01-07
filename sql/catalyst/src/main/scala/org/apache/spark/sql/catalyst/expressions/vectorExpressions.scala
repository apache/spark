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

package org.apache.spark.sql.catalyst.expressions

import org.apache.spark.sql.catalyst.analysis.TypeCheckResult
import org.apache.spark.sql.catalyst.analysis.TypeCheckResult.DataTypeMismatch
import org.apache.spark.sql.catalyst.expressions.objects.StaticInvoke
import org.apache.spark.sql.errors.QueryErrorsBase
import org.apache.spark.sql.types.{ArrayType, FloatType, StringType}

// scalastyle:off line.size.limit
@ExpressionDescription(
  usage = """
    _FUNC_(array1, array2) - Returns the cosine similarity between two float vectors.
    The vectors must have the same dimension.
  """,
  examples = """
    Examples:
      > SELECT _FUNC_(array(1.0F, 2.0F, 3.0F), array(4.0F, 5.0F, 6.0F));
       0.97463185
  """,
  since = "4.2.0",
  group = "vector_funcs"
)
// scalastyle:on line.size.limit
case class VectorCosineSimilarity(left: Expression, right: Expression)
    extends RuntimeReplaceable with QueryErrorsBase {

  override def checkInputDataTypes(): TypeCheckResult = {
    (left.dataType, right.dataType) match {
      case (ArrayType(FloatType, _), ArrayType(FloatType, _)) =>
        TypeCheckResult.TypeCheckSuccess
      case (ArrayType(FloatType, _), _) =>
        DataTypeMismatch(
          errorSubClass = "UNEXPECTED_INPUT_TYPE",
          messageParameters = Map(
            "paramIndex" -> ordinalNumber(1),
            "requiredType" -> toSQLType(ArrayType(FloatType)),
            "inputSql" -> toSQLExpr(right),
            "inputType" -> toSQLType(right.dataType)))
      case _ =>
        DataTypeMismatch(
          errorSubClass = "UNEXPECTED_INPUT_TYPE",
          messageParameters = Map(
            "paramIndex" -> ordinalNumber(0),
            "requiredType" -> toSQLType(ArrayType(FloatType)),
            "inputSql" -> toSQLExpr(left),
            "inputType" -> toSQLType(left.dataType)))
    }
  }

  override lazy val replacement: Expression = StaticInvoke(
    classOf[VectorFunctionImplUtils],
    FloatType,
    "vectorCosineSimilarity",
    Seq(left, right, Literal(prettyName)),
    Seq(ArrayType(FloatType), ArrayType(FloatType), StringType))

  override def prettyName: String = "vector_cosine_similarity"

  override def children: Seq[Expression] = Seq(left, right)

  override protected def withNewChildrenInternal(
      newChildren: IndexedSeq[Expression]): VectorCosineSimilarity = {
    copy(left = newChildren(0), right = newChildren(1))
  }
}

// scalastyle:off line.size.limit
@ExpressionDescription(
  usage = """
    _FUNC_(array1, array2) - Returns the inner product (dot product) between two float vectors.
    The vectors must have the same dimension.
  """,
  examples = """
    Examples:
      > SELECT _FUNC_(array(1.0F, 2.0F, 3.0F), array(4.0F, 5.0F, 6.0F));
       32.0
  """,
  since = "4.2.0",
  group = "vector_funcs"
)
// scalastyle:on line.size.limit
case class VectorInnerProduct(left: Expression, right: Expression)
    extends RuntimeReplaceable with QueryErrorsBase {

  override def checkInputDataTypes(): TypeCheckResult = {
    (left.dataType, right.dataType) match {
      case (ArrayType(FloatType, _), ArrayType(FloatType, _)) =>
        TypeCheckResult.TypeCheckSuccess
      case (ArrayType(FloatType, _), _) =>
        DataTypeMismatch(
          errorSubClass = "UNEXPECTED_INPUT_TYPE",
          messageParameters = Map(
            "paramIndex" -> ordinalNumber(1),
            "requiredType" -> toSQLType(ArrayType(FloatType)),
            "inputSql" -> toSQLExpr(right),
            "inputType" -> toSQLType(right.dataType)))
      case _ =>
        DataTypeMismatch(
          errorSubClass = "UNEXPECTED_INPUT_TYPE",
          messageParameters = Map(
            "paramIndex" -> ordinalNumber(0),
            "requiredType" -> toSQLType(ArrayType(FloatType)),
            "inputSql" -> toSQLExpr(left),
            "inputType" -> toSQLType(left.dataType)))
    }
  }

  override lazy val replacement: Expression = StaticInvoke(
    classOf[VectorFunctionImplUtils],
    FloatType,
    "vectorInnerProduct",
    Seq(left, right, Literal(prettyName)),
    Seq(ArrayType(FloatType), ArrayType(FloatType), StringType))

  override def prettyName: String = "vector_inner_product"

  override def children: Seq[Expression] = Seq(left, right)

  override protected def withNewChildrenInternal(
      newChildren: IndexedSeq[Expression]): VectorInnerProduct = {
    copy(left = newChildren(0), right = newChildren(1))
  }
}

// scalastyle:off line.size.limit
@ExpressionDescription(
  usage = """
    _FUNC_(array1, array2) - Returns the Euclidean (L2) distance between two float vectors.
    The vectors must have the same dimension.
  """,
  examples = """
    Examples:
      > SELECT _FUNC_(array(1.0F, 2.0F, 3.0F), array(4.0F, 5.0F, 6.0F));
       5.196152
  """,
  since = "4.2.0",
  group = "vector_funcs"
)
// scalastyle:on line.size.limit
case class VectorL2Distance(left: Expression, right: Expression)
    extends RuntimeReplaceable with QueryErrorsBase {

  override def checkInputDataTypes(): TypeCheckResult = {
    (left.dataType, right.dataType) match {
      case (ArrayType(FloatType, _), ArrayType(FloatType, _)) =>
        TypeCheckResult.TypeCheckSuccess
      case (ArrayType(FloatType, _), _) =>
        DataTypeMismatch(
          errorSubClass = "UNEXPECTED_INPUT_TYPE",
          messageParameters = Map(
            "paramIndex" -> ordinalNumber(1),
            "requiredType" -> toSQLType(ArrayType(FloatType)),
            "inputSql" -> toSQLExpr(right),
            "inputType" -> toSQLType(right.dataType)))
      case _ =>
        DataTypeMismatch(
          errorSubClass = "UNEXPECTED_INPUT_TYPE",
          messageParameters = Map(
            "paramIndex" -> ordinalNumber(0),
            "requiredType" -> toSQLType(ArrayType(FloatType)),
            "inputSql" -> toSQLExpr(left),
            "inputType" -> toSQLType(left.dataType)))
    }
  }

  override lazy val replacement: Expression = StaticInvoke(
    classOf[VectorFunctionImplUtils],
    FloatType,
    "vectorL2Distance",
    Seq(left, right, Literal(prettyName)),
    Seq(ArrayType(FloatType), ArrayType(FloatType), StringType))

  override def prettyName: String = "vector_l2_distance"

  override def children: Seq[Expression] = Seq(left, right)

  override protected def withNewChildrenInternal(
      newChildren: IndexedSeq[Expression]): VectorL2Distance = {
    copy(left = newChildren(0), right = newChildren(1))
  }
}
