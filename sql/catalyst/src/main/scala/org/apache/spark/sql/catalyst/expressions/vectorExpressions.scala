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
import org.apache.spark.sql.catalyst.expressions.codegen.{CodegenContext, ExprCode}
import org.apache.spark.sql.catalyst.util.ArrayData
import org.apache.spark.sql.errors.{QueryErrorsBase, QueryExecutionErrors}
import org.apache.spark.sql.types.{ArrayType, DataType, FloatType}

// scalastyle:off line.size.limit
@ExpressionDescription(
  usage = """
    _FUNC_(vector1, vector2) - Returns the cosine similarity between two float vectors.
    The vectors must have the same dimension.
  """,
  examples = """
    Examples:
      > SELECT _FUNC_(array(1.0, 2.0, 3.0), array(4.0, 5.0, 6.0));
       0.9746318461970762
  """,
  since = "4.1.0",
  group = "misc_funcs"
)
// scalastyle:on line.size.limit
case class VectorCosineSimilarity(left: Expression, right: Expression)
    extends BinaryExpression with QueryErrorsBase {

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

  override def dataType: DataType = FloatType

  override def nullable: Boolean = true

  override def nullIntolerant: Boolean = true

  override def prettyName: String = "vector_cosine_similarity"

  override def nullSafeEval(leftValue: Any, rightValue: Any): Any = {
    val leftArray = leftValue.asInstanceOf[ArrayData]
    val rightArray = rightValue.asInstanceOf[ArrayData]

    val leftLen = leftArray.numElements()
    val rightLen = rightArray.numElements()

    // Validate dimensions match
    if (leftLen != rightLen) {
      throw QueryExecutionErrors.vectorDimensionMismatchError(prettyName, leftLen, rightLen)
    }

    // Handle empty vectors - return NULL as it's undefined for cosine similarity
    if (leftLen == 0) {
      return null
    }

    var dotProduct = 0.0
    var norm1Sq = 0.0
    var norm2Sq = 0.0
    var i = 0

    // Check for nulls in arrays and compute values
    while (i < leftLen) {
      if (leftArray.isNullAt(i) || rightArray.isNullAt(i)) {
        return null
      }
      val a = leftArray.getFloat(i)
      val b = rightArray.getFloat(i)
      dotProduct += a * b
      norm1Sq += a * a
      norm2Sq += b * b
      i += 1
    }

    val normProduct = Math.sqrt(norm1Sq * norm2Sq)
    // Return NULL if either vector has zero magnitude
    if (normProduct == 0.0) null else (dotProduct / normProduct).toFloat
  }

  override protected def doGenCode(ctx: CodegenContext, ev: ExprCode): ExprCode = {
    // manual unroll loop to process 8 floats at a time - speculative SIMD optimization
    nullSafeCodeGen(ctx, ev, (leftArray, rightArray) => {
      val leftLen = ctx.freshName("leftLen")
      val rightLen = ctx.freshName("rightLen")
      val i = ctx.freshName("i")
      val simdLimit = ctx.freshName("simdLimit")
      val dotProduct = ctx.freshName("dotProduct")
      val norm1Sq = ctx.freshName("norm1Sq")
      val norm2Sq = ctx.freshName("norm2Sq")
      val normProduct = ctx.freshName("normProduct")
      val hasNull = ctx.freshName("hasNull")

      // Generate variable names for unrolled loop
      val a = (0 to 7).map(j => ctx.freshName(s"a$j"))
      val b = (0 to 7).map(j => ctx.freshName(s"b$j"))

      s"""
        int $leftLen = $leftArray.numElements();
        int $rightLen = $rightArray.numElements();

        if ($leftLen != $rightLen) {
          throw org.apache.spark.sql.errors.QueryExecutionErrors.vectorDimensionMismatchError(
            "${prettyName}", $leftLen, $rightLen);
        } else if ($leftLen == 0) {
          ${ev.isNull} = true;
        } else {
          double $dotProduct = 0.0;
          double $norm1Sq = 0.0;
          double $norm2Sq = 0.0;
          boolean $hasNull = false;

          int $i = 0;
          int $simdLimit = ($leftLen / 8) * 8;

          // Manual unroll loop - process 8 floats at a time
          while ($i < $simdLimit && !$hasNull) {
            // Check for nulls
            if ($leftArray.isNullAt($i) || $leftArray.isNullAt($i + 1) ||
                $leftArray.isNullAt($i + 2) || $leftArray.isNullAt($i + 3) ||
                $leftArray.isNullAt($i + 4) || $leftArray.isNullAt($i + 5) ||
                $leftArray.isNullAt($i + 6) || $leftArray.isNullAt($i + 7) ||
                $rightArray.isNullAt($i) || $rightArray.isNullAt($i + 1) ||
                $rightArray.isNullAt($i + 2) || $rightArray.isNullAt($i + 3) ||
                $rightArray.isNullAt($i + 4) || $rightArray.isNullAt($i + 5) ||
                $rightArray.isNullAt($i + 6) || $rightArray.isNullAt($i + 7)) {
              $hasNull = true;
            } else {
              float ${a(0)} = $leftArray.getFloat($i);
              float ${a(1)} = $leftArray.getFloat($i + 1);
              float ${a(2)} = $leftArray.getFloat($i + 2);
              float ${a(3)} = $leftArray.getFloat($i + 3);
              float ${a(4)} = $leftArray.getFloat($i + 4);
              float ${a(5)} = $leftArray.getFloat($i + 5);
              float ${a(6)} = $leftArray.getFloat($i + 6);
              float ${a(7)} = $leftArray.getFloat($i + 7);

              float ${b(0)} = $rightArray.getFloat($i);
              float ${b(1)} = $rightArray.getFloat($i + 1);
              float ${b(2)} = $rightArray.getFloat($i + 2);
              float ${b(3)} = $rightArray.getFloat($i + 3);
              float ${b(4)} = $rightArray.getFloat($i + 4);
              float ${b(5)} = $rightArray.getFloat($i + 5);
              float ${b(6)} = $rightArray.getFloat($i + 6);
              float ${b(7)} = $rightArray.getFloat($i + 7);

              $dotProduct += (double)(
                ${a(0)} * ${b(0)} + ${a(1)} * ${b(1)} + ${a(2)} * ${b(2)} + ${a(3)} * ${b(3)} +
                ${a(4)} * ${b(4)} + ${a(5)} * ${b(5)} + ${a(6)} * ${b(6)} + ${a(7)} * ${b(7)}
              );

              $norm1Sq += (double)(
                ${a(0)} * ${a(0)} + ${a(1)} * ${a(1)} + ${a(2)} * ${a(2)} + ${a(3)} * ${a(3)} +
                ${a(4)} * ${a(4)} + ${a(5)} * ${a(5)} + ${a(6)} * ${a(6)} + ${a(7)} * ${a(7)}
              );

              $norm2Sq += (double)(
                ${b(0)} * ${b(0)} + ${b(1)} * ${b(1)} + ${b(2)} * ${b(2)} + ${b(3)} * ${b(3)} +
                ${b(4)} * ${b(4)} + ${b(5)} * ${b(5)} + ${b(6)} * ${b(6)} + ${b(7)} * ${b(7)}
              );

              $i += 8;
            }
          }

          // Handle remaining elements
          while ($i < $leftLen && !$hasNull) {
            if ($leftArray.isNullAt($i) || $rightArray.isNullAt($i)) {
              $hasNull = true;
            } else {
              float a_val = $leftArray.getFloat($i);
              float b_val = $rightArray.getFloat($i);
              $dotProduct += (double)(a_val * b_val);
              $norm1Sq += (double)(a_val * a_val);
              $norm2Sq += (double)(b_val * b_val);
              $i++;
            }
          }

          if ($hasNull) {
            ${ev.isNull} = true;
          } else {
            double $normProduct = Math.sqrt($norm1Sq * $norm2Sq);
            if ($normProduct == 0.0) {
              ${ev.isNull} = true;
            } else {
              ${ev.value} = (float)($dotProduct / $normProduct);
            }
          }
        }
      """
    })
  }

  override protected def withNewChildrenInternal(
      newLeft: Expression,
      newRight: Expression): VectorCosineSimilarity = {
    copy(left = newLeft, right = newRight)
  }
}

// scalastyle:off line.size.limit
@ExpressionDescription(
  usage = """
    _FUNC_(vector1, vector2) - Returns the inner product (dot product) between two float vectors.
    The vectors must have the same dimension.
  """,
  examples = """
    Examples:
      > SELECT _FUNC_(array(1.0, 2.0, 3.0), array(4.0, 5.0, 6.0));
       32.0
  """,
  since = "4.1.0",
  group = "misc_funcs"
)
// scalastyle:on line.size.limit
case class VectorInnerProduct(left: Expression, right: Expression)
    extends BinaryExpression with QueryErrorsBase {

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

  override def dataType: DataType = FloatType

  override def nullable: Boolean = true

  override def nullIntolerant: Boolean = true

  override def prettyName: String = "vector_inner_product"

  override def nullSafeEval(leftValue: Any, rightValue: Any): Any = {
    val leftArray = leftValue.asInstanceOf[ArrayData]
    val rightArray = rightValue.asInstanceOf[ArrayData]

    val leftLen = leftArray.numElements()
    val rightLen = rightArray.numElements()

    // Validate dimensions match
    if (leftLen != rightLen) {
      throw QueryExecutionErrors.vectorDimensionMismatchError(prettyName, leftLen, rightLen)
    }

    // Handle empty vectors
    if (leftLen == 0) {
      return 0.0f
    }

    var dotProduct = 0.0
    var i = 0

    // Check for nulls in arrays and compute values
    while (i < leftLen) {
      if (leftArray.isNullAt(i) || rightArray.isNullAt(i)) {
        return null
      }
      val a = leftArray.getFloat(i)
      val b = rightArray.getFloat(i)
      dotProduct += a * b
      i += 1
    }

    dotProduct.toFloat
  }

  override protected def doGenCode(ctx: CodegenContext, ev: ExprCode): ExprCode = {
    // manual unroll loop to process 8 floats at a time - speculative SIMD optimization
    nullSafeCodeGen(ctx, ev, (leftArray, rightArray) => {
      val leftLen = ctx.freshName("leftLen")
      val rightLen = ctx.freshName("rightLen")
      val i = ctx.freshName("i")
      val simdLimit = ctx.freshName("simdLimit")
      val dotProduct = ctx.freshName("dotProduct")
      val hasNull = ctx.freshName("hasNull")

      // Generate variable names for unrolled loop
      val a = (0 to 7).map(j => ctx.freshName(s"a$j"))
      val b = (0 to 7).map(j => ctx.freshName(s"b$j"))

      s"""
        int $leftLen = $leftArray.numElements();
        int $rightLen = $rightArray.numElements();

        if ($leftLen != $rightLen) {
          throw org.apache.spark.sql.errors.QueryExecutionErrors.vectorDimensionMismatchError(
            "${prettyName}", $leftLen, $rightLen);
        } else if ($leftLen == 0) {
          ${ev.value} = 0.0f;
        } else {
          double $dotProduct = 0.0;
          boolean $hasNull = false;

          int $i = 0;
          int $simdLimit = ($leftLen / 8) * 8;

          // Manual unroll loop - process 8 floats at a time
          while ($i < $simdLimit && !$hasNull) {
            // Check for nulls
            if ($leftArray.isNullAt($i) || $leftArray.isNullAt($i + 1) ||
                $leftArray.isNullAt($i + 2) || $leftArray.isNullAt($i + 3) ||
                $leftArray.isNullAt($i + 4) || $leftArray.isNullAt($i + 5) ||
                $leftArray.isNullAt($i + 6) || $leftArray.isNullAt($i + 7) ||
                $rightArray.isNullAt($i) || $rightArray.isNullAt($i + 1) ||
                $rightArray.isNullAt($i + 2) || $rightArray.isNullAt($i + 3) ||
                $rightArray.isNullAt($i + 4) || $rightArray.isNullAt($i + 5) ||
                $rightArray.isNullAt($i + 6) || $rightArray.isNullAt($i + 7)) {
              $hasNull = true;
            } else {
              float ${a(0)} = $leftArray.getFloat($i);
              float ${a(1)} = $leftArray.getFloat($i + 1);
              float ${a(2)} = $leftArray.getFloat($i + 2);
              float ${a(3)} = $leftArray.getFloat($i + 3);
              float ${a(4)} = $leftArray.getFloat($i + 4);
              float ${a(5)} = $leftArray.getFloat($i + 5);
              float ${a(6)} = $leftArray.getFloat($i + 6);
              float ${a(7)} = $leftArray.getFloat($i + 7);

              float ${b(0)} = $rightArray.getFloat($i);
              float ${b(1)} = $rightArray.getFloat($i + 1);
              float ${b(2)} = $rightArray.getFloat($i + 2);
              float ${b(3)} = $rightArray.getFloat($i + 3);
              float ${b(4)} = $rightArray.getFloat($i + 4);
              float ${b(5)} = $rightArray.getFloat($i + 5);
              float ${b(6)} = $rightArray.getFloat($i + 6);
              float ${b(7)} = $rightArray.getFloat($i + 7);

              $dotProduct += (double)(
                ${a(0)} * ${b(0)} + ${a(1)} * ${b(1)} + ${a(2)} * ${b(2)} + ${a(3)} * ${b(3)} +
                ${a(4)} * ${b(4)} + ${a(5)} * ${b(5)} + ${a(6)} * ${b(6)} + ${a(7)} * ${b(7)}
              );

              $i += 8;
            }
          }

          // Handle remaining elements
          while ($i < $leftLen && !$hasNull) {
            if ($leftArray.isNullAt($i) || $rightArray.isNullAt($i)) {
              $hasNull = true;
            } else {
              float a_val = $leftArray.getFloat($i);
              float b_val = $rightArray.getFloat($i);
              $dotProduct += (double)(a_val * b_val);
              $i++;
            }
          }

          if ($hasNull) {
            ${ev.isNull} = true;
          } else {
            ${ev.value} = (float)$dotProduct;
          }
        }
      """
    })
  }

  override protected def withNewChildrenInternal(
      newLeft: Expression,
      newRight: Expression): VectorInnerProduct = {
    copy(left = newLeft, right = newRight)
  }
}

// scalastyle:off line.size.limit
@ExpressionDescription(
  usage = """
    _FUNC_(vector1, vector2) - Returns the Euclidean (L2) distance between two float vectors.
    The vectors must have the same dimension.
  """,
  examples = """
    Examples:
      > SELECT _FUNC_(array(1.0, 2.0, 3.0), array(4.0, 5.0, 6.0));
       5.196152422706632
  """,
  since = "4.1.0",
  group = "misc_funcs"
)
// scalastyle:on line.size.limit
case class VectorL2Distance(left: Expression, right: Expression)
    extends BinaryExpression with QueryErrorsBase {

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

  override def dataType: DataType = FloatType

  override def nullable: Boolean = true

  override def nullIntolerant: Boolean = true

  override def prettyName: String = "vector_l2_distance"

  override def nullSafeEval(leftValue: Any, rightValue: Any): Any = {
    val leftArray = leftValue.asInstanceOf[ArrayData]
    val rightArray = rightValue.asInstanceOf[ArrayData]

    val leftLen = leftArray.numElements()
    val rightLen = rightArray.numElements()

    // Validate dimensions match
    if (leftLen != rightLen) {
      throw QueryExecutionErrors.vectorDimensionMismatchError(prettyName, leftLen, rightLen)
    }

    // Handle empty vectors - return 0.0 per spec
    if (leftLen == 0) {
      return 0.0f
    }

    var sumSq = 0.0
    var i = 0

    // Check for nulls in arrays and compute values
    while (i < leftLen) {
      if (leftArray.isNullAt(i) || rightArray.isNullAt(i)) {
        return null
      }
      val a = leftArray.getFloat(i)
      val b = rightArray.getFloat(i)
      val diff = a - b
      sumSq += diff * diff
      i += 1
    }

    Math.sqrt(sumSq).toFloat
  }

  override protected def doGenCode(ctx: CodegenContext, ev: ExprCode): ExprCode = {
    // manual unroll loop to process 8 floats at a time - speculative SIMD optimization
    nullSafeCodeGen(ctx, ev, (leftArray, rightArray) => {
      val leftLen = ctx.freshName("leftLen")
      val rightLen = ctx.freshName("rightLen")
      val i = ctx.freshName("i")
      val simdLimit = ctx.freshName("simdLimit")
      val sumSq = ctx.freshName("sumSq")
      val hasNull = ctx.freshName("hasNull")

      // Generate variable names for unrolled loop
      val a = (0 to 7).map(j => ctx.freshName(s"a$j"))
      val b = (0 to 7).map(j => ctx.freshName(s"b$j"))
      val d = (0 to 7).map(j => ctx.freshName(s"d$j"))

      s"""
        int $leftLen = $leftArray.numElements();
        int $rightLen = $rightArray.numElements();

        if ($leftLen != $rightLen) {
          throw org.apache.spark.sql.errors.QueryExecutionErrors.vectorDimensionMismatchError(
            "${prettyName}", $leftLen, $rightLen);
        } else if ($leftLen == 0) {
          ${ev.value} = 0.0f;
        } else {
          double $sumSq = 0.0;
          boolean $hasNull = false;

          int $i = 0;
          int $simdLimit = ($leftLen / 8) * 8;

          // Manual unroll loop - process 8 floats at a time
          while ($i < $simdLimit && !$hasNull) {
            // Check for nulls
            if ($leftArray.isNullAt($i) || $leftArray.isNullAt($i + 1) ||
                $leftArray.isNullAt($i + 2) || $leftArray.isNullAt($i + 3) ||
                $leftArray.isNullAt($i + 4) || $leftArray.isNullAt($i + 5) ||
                $leftArray.isNullAt($i + 6) || $leftArray.isNullAt($i + 7) ||
                $rightArray.isNullAt($i) || $rightArray.isNullAt($i + 1) ||
                $rightArray.isNullAt($i + 2) || $rightArray.isNullAt($i + 3) ||
                $rightArray.isNullAt($i + 4) || $rightArray.isNullAt($i + 5) ||
                $rightArray.isNullAt($i + 6) || $rightArray.isNullAt($i + 7)) {
              $hasNull = true;
            } else {
              float ${a(0)} = $leftArray.getFloat($i);
              float ${a(1)} = $leftArray.getFloat($i + 1);
              float ${a(2)} = $leftArray.getFloat($i + 2);
              float ${a(3)} = $leftArray.getFloat($i + 3);
              float ${a(4)} = $leftArray.getFloat($i + 4);
              float ${a(5)} = $leftArray.getFloat($i + 5);
              float ${a(6)} = $leftArray.getFloat($i + 6);
              float ${a(7)} = $leftArray.getFloat($i + 7);

              float ${b(0)} = $rightArray.getFloat($i);
              float ${b(1)} = $rightArray.getFloat($i + 1);
              float ${b(2)} = $rightArray.getFloat($i + 2);
              float ${b(3)} = $rightArray.getFloat($i + 3);
              float ${b(4)} = $rightArray.getFloat($i + 4);
              float ${b(5)} = $rightArray.getFloat($i + 5);
              float ${b(6)} = $rightArray.getFloat($i + 6);
              float ${b(7)} = $rightArray.getFloat($i + 7);

              float ${d(0)} = ${a(0)} - ${b(0)};
              float ${d(1)} = ${a(1)} - ${b(1)};
              float ${d(2)} = ${a(2)} - ${b(2)};
              float ${d(3)} = ${a(3)} - ${b(3)};
              float ${d(4)} = ${a(4)} - ${b(4)};
              float ${d(5)} = ${a(5)} - ${b(5)};
              float ${d(6)} = ${a(6)} - ${b(6)};
              float ${d(7)} = ${a(7)} - ${b(7)};

              $sumSq += (double)(
                ${d(0)} * ${d(0)} + ${d(1)} * ${d(1)} + ${d(2)} * ${d(2)} + ${d(3)} * ${d(3)} +
                ${d(4)} * ${d(4)} + ${d(5)} * ${d(5)} + ${d(6)} * ${d(6)} + ${d(7)} * ${d(7)}
              );

              $i += 8;
            }
          }

          // Handle remaining elements
          while ($i < $leftLen && !$hasNull) {
            if ($leftArray.isNullAt($i) || $rightArray.isNullAt($i)) {
              $hasNull = true;
            } else {
              float a_val = $leftArray.getFloat($i);
              float b_val = $rightArray.getFloat($i);
              float diff = a_val - b_val;
              $sumSq += (double)(diff * diff);
              $i++;
            }
          }

          if ($hasNull) {
            ${ev.isNull} = true;
          } else {
            ${ev.value} = (float)Math.sqrt($sumSq);
          }
        }
      """
    })
  }

  override protected def withNewChildrenInternal(
      newLeft: Expression,
      newRight: Expression): VectorL2Distance = {
    copy(left = newLeft, right = newRight)
  }
}
