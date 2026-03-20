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

import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.analysis.TypeCheckResult
import org.apache.spark.sql.catalyst.analysis.TypeCheckResult.DataTypeMismatch
import org.apache.spark.sql.catalyst.expressions.aggregate.ImperativeAggregate
import org.apache.spark.sql.catalyst.expressions.objects.StaticInvoke
import org.apache.spark.sql.catalyst.trees.UnaryLike
import org.apache.spark.sql.catalyst.types.DataTypeUtils
import org.apache.spark.sql.catalyst.util.ArrayData
import org.apache.spark.sql.errors.{QueryErrorsBase, QueryExecutionErrors}
import org.apache.spark.sql.types.{
  ArrayType,
  BinaryType,
  DataType,
  FloatType,
  LongType,
  StringType,
  StructType
}
import org.apache.spark.unsafe.Platform

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

// Base trait for vector aggregate functions (vector_avg, vector_sum).
// Provides a unified aggregate buffer schema: (acc: BINARY, count: LONG)
// - acc: BINARY representation of the running vector (sum or average)
// - count: number of valid vectors seen so far
// - dimension is inferred from acc.length / 4 (4 bytes per float)
// Subclasses only need to implement the element-wise update and merge logic.
trait VectorAggregateBase extends ImperativeAggregate
    with UnaryLike[Expression]
    with QueryErrorsBase {

  override def nullable: Boolean = true

  override def dataType: DataType = ArrayType(FloatType, containsNull = false)

  override def checkInputDataTypes(): TypeCheckResult = {
    child.dataType match {
      case ArrayType(FloatType, _) =>
        TypeCheckResult.TypeCheckSuccess
      case _ =>
        DataTypeMismatch(
          errorSubClass = "UNEXPECTED_INPUT_TYPE",
          messageParameters = Map(
            "paramIndex" -> ordinalNumber(0),
            "requiredType" -> toSQLType(ArrayType(FloatType)),
            "inputSql" -> toSQLExpr(child),
            "inputType" -> toSQLType(child.dataType)
          )
        )
    }
  }

  // Aggregate buffer schema: (acc: BINARY, count: LONG)
  private lazy val accAttr = AttributeReference(
    "acc",
    BinaryType,
    nullable = true
  )()
  private lazy val countAttr =
    AttributeReference("count", LongType, nullable = false)()

  override def aggBufferAttributes: Seq[AttributeReference] =
    Seq(accAttr, countAttr)

  override def aggBufferSchema: StructType =
    DataTypeUtils.fromAttributes(aggBufferAttributes)

  override lazy val inputAggBufferAttributes: Seq[AttributeReference] =
    aggBufferAttributes.map(_.newInstance())

  // Buffer indices
  protected val accIndex = 0
  protected val countIndex = 1

  protected lazy val inputContainsNull =
    child.dataType.asInstanceOf[ArrayType].containsNull

  override def initialize(buffer: InternalRow): Unit = {
    buffer.update(mutableAggBufferOffset + accIndex, null)
    buffer.setLong(mutableAggBufferOffset + countIndex, 0L)
  }

  // Infer vector dimension from byte array length (4 bytes per float)
  protected def getDim(bytes: Array[Byte]): Int = bytes.length / 4

  // Element-wise update for non-first vectors.
  // accBytes contains the running vector; update it in-place with inputArray.
  protected def updateElements(
      accBytes: Array[Byte],
      inputArray: ArrayData,
      dim: Int,
      newCount: Long): Unit

  // Element-wise merge of two non-empty buffers.
  // accBytes contains the left running vector; update it in-place.
  protected def mergeElements(
      accBytes: Array[Byte],
      inputBytes: Array[Byte],
      dim: Int,
      currentCount: Long,
      inputCount: Long,
      newCount: Long): Unit

  override def update(buffer: InternalRow, input: InternalRow): Unit = {
    val inputValue = child.eval(input)
    if (inputValue == null) {
      return
    }

    val inputArray = inputValue.asInstanceOf[ArrayData]
    val inputLen = inputArray.numElements()

    // Check for NULL elements in input vector - skip if any NULL element found
    // Only check if the array type can contain nulls
    if (inputContainsNull) {
      var i = 0
      while (i < inputLen) {
        if (inputArray.isNullAt(i)) {
          return
        }
        i += 1
      }
    }

    val accOffset = mutableAggBufferOffset + accIndex
    val countOffset = mutableAggBufferOffset + countIndex

    val currentCount = buffer.getLong(countOffset)

    if (currentCount == 0L) {
      // First valid vector - just copy it
      val bytes = new Array[Byte](inputLen * 4)
      var i = 0
      while (i < inputLen) {
        Platform.putFloat(bytes, Platform.BYTE_ARRAY_OFFSET + i.toLong * 4, inputArray.getFloat(i))
        i += 1
      }
      buffer.update(accOffset, bytes)
      buffer.setLong(countOffset, 1L)
    } else {
      val accBytes = buffer.getBinary(accOffset)
      val accDim = getDim(accBytes)

      // Empty array case - if current is empty and input is empty, keep empty
      if (accDim == 0 && inputLen == 0) {
        buffer.setLong(countOffset, currentCount + 1L)
        return
      }

      // Dimension mismatch check
      if (accDim != inputLen) {
        throw QueryExecutionErrors.vectorDimensionMismatchError(
          prettyName,
          accDim,
          inputLen
        )
      }

      val newCount = currentCount + 1L
      updateElements(accBytes, inputArray, accDim, newCount)
      buffer.setLong(countOffset, newCount)
    }
  }

  override def merge(buffer: InternalRow, inputBuffer: InternalRow): Unit = {
    val accOffset = mutableAggBufferOffset + accIndex
    val countOffset = mutableAggBufferOffset + countIndex
    val inputAccOffset = inputAggBufferOffset + accIndex
    val inputCountOffset = inputAggBufferOffset + countIndex

    val inputCount = inputBuffer.getLong(inputCountOffset)
    if (inputCount == 0L) {
      return
    }

    val inputAccBytes = inputBuffer.getBinary(inputAccOffset)
    val currentCount = buffer.getLong(countOffset)

    if (currentCount == 0L) {
      // Copy input buffer to current buffer
      buffer.update(accOffset, inputAccBytes.clone())
      buffer.setLong(countOffset, inputCount)
    } else {
      val accBytes = buffer.getBinary(accOffset)
      val accDim = getDim(accBytes)
      val inputDim = getDim(inputAccBytes)

      // Empty array case
      if (accDim == 0 && inputDim == 0) {
        buffer.setLong(countOffset, currentCount + inputCount)
        return
      }

      // Dimension mismatch check
      if (accDim != inputDim) {
        throw QueryExecutionErrors.vectorDimensionMismatchError(
          prettyName,
          accDim,
          inputDim
        )
      }

      val newCount = currentCount + inputCount
      mergeElements(accBytes, inputAccBytes, accDim,
        currentCount, inputCount, newCount)
      buffer.setLong(countOffset, newCount)
    }
  }

  override def eval(buffer: InternalRow): Any = {
    val count = buffer.getLong(mutableAggBufferOffset + countIndex)
    if (count == 0L) {
      null
    } else {
      val accBytes = buffer.getBinary(mutableAggBufferOffset + accIndex)
      val dim = getDim(accBytes)
      val result = new Array[Float](dim)
      var i = 0
      while (i < dim) {
        result(i) = Platform.getFloat(accBytes, Platform.BYTE_ARRAY_OFFSET + i.toLong * 4)
        i += 1
      }
      ArrayData.toArrayData(result)
    }
  }
}

// scalastyle:off line.size.limit
@ExpressionDescription(
  usage = """
    _FUNC_(array) - Returns the element-wise mean of float vectors in a group.
    All vectors must have the same dimension.
  """,
  examples = """
    Examples:
      > SELECT _FUNC_(col) FROM VALUES (array(1.0F, 2.0F)), (array(3.0F, 4.0F)) AS tab(col);
       [2.0,3.0]
  """,
  since = "4.2.0",
  group = "vector_funcs"
)
// scalastyle:on line.size.limit
// Note: This implementation uses single-precision floating-point arithmetic (Float).
// Precision loss is expected for very large aggregates due to:
// 1. Accumulated rounding errors in incremental average updates
// 2. Loss of significance when dividing by large counts
case class VectorAvg(
    child: Expression,
    mutableAggBufferOffset: Int = 0,
    inputAggBufferOffset: Int = 0
) extends VectorAggregateBase {

  def this(child: Expression) = this(child, 0, 0)

  override def prettyName: String = "vector_avg"

  override def withNewMutableAggBufferOffset(
      newMutableAggBufferOffset: Int
  ): ImperativeAggregate =
    copy(mutableAggBufferOffset = newMutableAggBufferOffset)

  override def withNewInputAggBufferOffset(
      newInputAggBufferOffset: Int
  ): ImperativeAggregate =
    copy(inputAggBufferOffset = newInputAggBufferOffset)

  override protected def updateElements(
      accBytes: Array[Byte],
      inputArray: ArrayData,
      dim: Int,
      newCount: Long): Unit = {
    // Update running average: new_avg = old_avg + (new_value - old_avg) / new_count
    val invCount = 1.0f / newCount
    var i = 0
    while (i < dim) {
      val off = Platform.BYTE_ARRAY_OFFSET + i.toLong * 4
      val oldAvg = Platform.getFloat(accBytes, off)
      Platform.putFloat(accBytes, off, oldAvg + (inputArray.getFloat(i) - oldAvg) * invCount)
      i += 1
    }
  }

  override protected def mergeElements(
      accBytes: Array[Byte],
      inputBytes: Array[Byte],
      dim: Int,
      currentCount: Long,
      inputCount: Long,
      newCount: Long): Unit = {
    // Merge running averages:
    // combined_avg = left_avg * (left_count / total) + right_avg * (right_count / total)
    val leftWeight = currentCount.toFloat / newCount
    val rightWeight = inputCount.toFloat / newCount
    var i = 0
    while (i < dim) {
      val off = Platform.BYTE_ARRAY_OFFSET + i.toLong * 4
      val leftAvg = Platform.getFloat(accBytes, off)
      val rightAvg = Platform.getFloat(inputBytes, off)
      Platform.putFloat(accBytes, off, leftAvg * leftWeight + rightAvg * rightWeight)
      i += 1
    }
  }

  override protected def withNewChildInternal(newChild: Expression): VectorAvg =
    copy(child = newChild)
}

// scalastyle:off line.size.limit
@ExpressionDescription(
  usage = """
    _FUNC_(array) - Returns the element-wise sum of float vectors in a group.
    All vectors must have the same dimension.
  """,
  examples = """
    Examples:
      > SELECT _FUNC_(col) FROM VALUES (array(1.0F, 2.0F)), (array(3.0F, 4.0F)) AS tab(col);
       [4.0,6.0]
  """,
  since = "4.2.0",
  group = "vector_funcs"
)
// scalastyle:on line.size.limit
// Note: This implementation uses single-precision floating-point arithmetic (Float).
// Precision loss is expected for very large aggregates due to:
// 1. Accumulated rounding errors when summing many values
// 2. Loss of significance when adding small values to large accumulated sums
case class VectorSum(
    child: Expression,
    mutableAggBufferOffset: Int = 0,
    inputAggBufferOffset: Int = 0
) extends VectorAggregateBase {

  def this(child: Expression) = this(child, 0, 0)

  override def prettyName: String = "vector_sum"

  override def withNewMutableAggBufferOffset(
      newMutableAggBufferOffset: Int
  ): ImperativeAggregate =
    copy(mutableAggBufferOffset = newMutableAggBufferOffset)

  override def withNewInputAggBufferOffset(
      newInputAggBufferOffset: Int
  ): ImperativeAggregate =
    copy(inputAggBufferOffset = newInputAggBufferOffset)

  override protected def updateElements(
      accBytes: Array[Byte],
      inputArray: ArrayData,
      dim: Int,
      newCount: Long): Unit = {
    // Update sum: new_sum = old_sum + new_value
    var i = 0
    while (i < dim) {
      val off = Platform.BYTE_ARRAY_OFFSET + i.toLong * 4
      Platform.putFloat(accBytes, off, Platform.getFloat(accBytes, off) + inputArray.getFloat(i))
      i += 1
    }
  }

  override protected def mergeElements(
      accBytes: Array[Byte],
      inputBytes: Array[Byte],
      dim: Int,
      currentCount: Long,
      inputCount: Long,
      newCount: Long): Unit = {
    // Merge sums: combined_sum = left_sum + right_sum
    var i = 0
    while (i < dim) {
      val off = Platform.BYTE_ARRAY_OFFSET + i.toLong * 4
      Platform.putFloat(accBytes, off,
        Platform.getFloat(accBytes, off) + Platform.getFloat(inputBytes, off))
      i += 1
    }
  }

  override protected def withNewChildInternal(newChild: Expression): VectorSum =
    copy(child = newChild)
}
