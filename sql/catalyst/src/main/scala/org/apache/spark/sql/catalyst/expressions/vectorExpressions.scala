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
       0.9746319
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

// scalastyle:off line.size.limit
@ExpressionDescription(
  usage = """
    _FUNC_(vector, degree) - Returns the Lp norm of a float vector using the specified degree.
    Degree defaults to 2.0 (Euclidean norm) if unspecified. Supported values: 1.0 (L1 norm),
    2.0 (L2 norm), float('inf') (infinity norm).
  """,
  examples = """
    Examples:
      > SELECT _FUNC_(array(3.0F, 4.0F), 2.0F);
       5.0
      > SELECT _FUNC_(array(3.0F, 4.0F), 1.0F);
       7.0
      > SELECT _FUNC_(array(3.0F, 4.0F), float('inf'));
       4.0
  """,
  since = "4.2.0",
  group = "vector_funcs"
)
// scalastyle:on line.size.limit
case class VectorNorm(vector: Expression, degree: Expression)
    extends RuntimeReplaceable with QueryErrorsBase {

  def this(vector: Expression) = this(vector, Literal(2.0f))

  override def checkInputDataTypes(): TypeCheckResult = {
    (vector.dataType, degree.dataType) match {
      case (ArrayType(FloatType, _), FloatType) =>
        TypeCheckResult.TypeCheckSuccess
      case (ArrayType(FloatType, _), _) =>
        DataTypeMismatch(
          errorSubClass = "UNEXPECTED_INPUT_TYPE",
          messageParameters = Map(
            "paramIndex" -> ordinalNumber(1),
            "requiredType" -> toSQLType(FloatType),
            "inputSql" -> toSQLExpr(degree),
            "inputType" -> toSQLType(degree.dataType)))
      case _ =>
        DataTypeMismatch(
          errorSubClass = "UNEXPECTED_INPUT_TYPE",
          messageParameters = Map(
            "paramIndex" -> ordinalNumber(0),
            "requiredType" -> toSQLType(ArrayType(FloatType)),
            "inputSql" -> toSQLExpr(vector),
            "inputType" -> toSQLType(vector.dataType)))
    }
  }

  override lazy val replacement: Expression = StaticInvoke(
    classOf[VectorFunctionImplUtils],
    FloatType,
    "vectorNorm",
    Seq(vector, degree, Literal(prettyName)),
    Seq(ArrayType(FloatType), FloatType, StringType)
  )

  override def prettyName: String = "vector_norm"

  override def children: Seq[Expression] = Seq(vector, degree)

  override protected def withNewChildrenInternal(
      newChildren: IndexedSeq[Expression]): VectorNorm = {
    copy(vector = newChildren(0), degree = newChildren(1))
  }
}

// scalastyle:off line.size.limit
@ExpressionDescription(
  usage = """
    _FUNC_(vector, degree) - Normalizes a float vector to unit length using the specified norm degree.
    Degree defaults to 2.0 (Euclidean norm) if unspecified. Supported values: 1.0 (L1 norm),
    2.0 (L2 norm), float('inf') (infinity norm).
  """,
  examples = """
    Examples:
      > SELECT _FUNC_(array(3.0F, 4.0F), 2.0F);
       [0.6,0.8]
      > SELECT _FUNC_(array(3.0F, 4.0F), 1.0F);
       [0.42857143,0.5714286]
      > SELECT _FUNC_(array(3.0F, 4.0F), float('inf'));
       [0.75,1.0]
  """,
  since = "4.2.0",
  group = "vector_funcs"
)
// scalastyle:on line.size.limit
case class VectorNormalize(vector: Expression, degree: Expression)
    extends RuntimeReplaceable with QueryErrorsBase {

  def this(vector: Expression) = this(vector, Literal(2.0f))

  override def checkInputDataTypes(): TypeCheckResult = {
    (vector.dataType, degree.dataType) match {
      case (ArrayType(FloatType, _), FloatType) =>
        TypeCheckResult.TypeCheckSuccess
      case (ArrayType(FloatType, _), _) =>
        DataTypeMismatch(
          errorSubClass = "UNEXPECTED_INPUT_TYPE",
          messageParameters = Map(
            "paramIndex" -> ordinalNumber(1),
            "requiredType" -> toSQLType(FloatType),
            "inputSql" -> toSQLExpr(degree),
            "inputType" -> toSQLType(degree.dataType)))
      case _ =>
        DataTypeMismatch(
          errorSubClass = "UNEXPECTED_INPUT_TYPE",
          messageParameters = Map(
            "paramIndex" -> ordinalNumber(0),
            "requiredType" -> toSQLType(ArrayType(FloatType)),
            "inputSql" -> toSQLExpr(vector),
            "inputType" -> toSQLType(vector.dataType)))
    }
  }

  override lazy val replacement: Expression = StaticInvoke(
    classOf[VectorFunctionImplUtils],
    ArrayType(FloatType),
    "vectorNormalize",
    Seq(vector, degree, Literal(prettyName)),
    Seq(ArrayType(FloatType), FloatType, StringType)
  )

  override def prettyName: String = "vector_normalize"

  override def children: Seq[Expression] = Seq(vector, degree)

  override protected def withNewChildrenInternal(
      newChildren: IndexedSeq[Expression]): VectorNormalize = {
    copy(vector = newChildren(0), degree = newChildren(1))
  }
}
