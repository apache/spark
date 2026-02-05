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

import java.nio.{ByteBuffer, ByteOrder}

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
  IntegerType,
  LongType,
  StringType,
  StructType
}

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
) extends ImperativeAggregate
    with UnaryLike[Expression]
    with QueryErrorsBase {

  def this(child: Expression) = this(child, 0, 0)

  override def prettyName: String = "vector_avg"

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

  // Aggregate buffer schema: (avg: BINARY, dim: INTEGER, count: LONG)
  // avg is a BINARY representation of the average vector of floats in the group
  // dim is the dimension of the vector
  // count is the number of vectors in the group
  // null avg means no valid input has been seen yet
  private lazy val avgAttr = AttributeReference(
    "avg",
    BinaryType,
    nullable = true
  )()
  private lazy val dimAttr = AttributeReference(
    "dim",
    IntegerType,
    nullable = true
  )()
  private lazy val countAttr =
    AttributeReference("count", LongType, nullable = false)()

  override def aggBufferAttributes: Seq[AttributeReference] =
    Seq(avgAttr, dimAttr, countAttr)

  override def aggBufferSchema: StructType =
    DataTypeUtils.fromAttributes(aggBufferAttributes)

  override lazy val inputAggBufferAttributes: Seq[AttributeReference] =
    aggBufferAttributes.map(_.newInstance())

  // Buffer indices
  private val avgIndex = 0
  private val dimIndex = 1
  private val countIndex = 2

  override def withNewMutableAggBufferOffset(
      newMutableAggBufferOffset: Int
  ): ImperativeAggregate =
    copy(mutableAggBufferOffset = newMutableAggBufferOffset)

  override def withNewInputAggBufferOffset(
      newInputAggBufferOffset: Int
  ): ImperativeAggregate =
    copy(inputAggBufferOffset = newInputAggBufferOffset)

  override def initialize(buffer: InternalRow): Unit = {
    buffer.update(mutableAggBufferOffset + avgIndex, null)
    buffer.update(mutableAggBufferOffset + dimIndex, null)
    buffer.setLong(mutableAggBufferOffset + countIndex, 0L)
  }

  private lazy val inputContainsNull = child.dataType.asInstanceOf[ArrayType].containsNull

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

    val avgOffset = mutableAggBufferOffset + avgIndex
    val dimOffset = mutableAggBufferOffset + dimIndex
    val countOffset = mutableAggBufferOffset + countIndex

    val currentCount = buffer.getLong(countOffset)

    if (currentCount == 0L) {
      // First valid vector - just copy it as the initial average
      val byteBuffer =
        ByteBuffer.allocate(inputLen * 4).order(ByteOrder.LITTLE_ENDIAN)
      var i = 0
      while (i < inputLen) {
        byteBuffer.putFloat(inputArray.getFloat(i))
        i += 1
      }
      buffer.update(avgOffset, byteBuffer.array())
      buffer.setInt(dimOffset, inputLen)
      buffer.setLong(countOffset, 1L)
    } else {
      val currentDim = buffer.getInt(dimOffset)

      // Empty array case - if current is empty and input is empty, keep empty
      if (currentDim == 0 && inputLen == 0) {
        buffer.setLong(countOffset, currentCount + 1L)
        return
      }

      // Dimension mismatch check
      if (currentDim != inputLen) {
        throw QueryExecutionErrors.vectorDimensionMismatchError(
          prettyName,
          currentDim,
          inputLen
        )
      }

      // Update running average: new_avg = old_avg + (new_value - old_avg) / (count + 1)
      val newCount = currentCount + 1L
      val invCount = 1.0f / newCount
      val currentAvgBytes = buffer.getBinary(avgOffset)
      // reuse the buffer without reallocation
      val avgBuffer =
        ByteBuffer.wrap(currentAvgBytes).order(ByteOrder.LITTLE_ENDIAN)
      var i = 0
      var idx = 0
      while (i < currentDim) {
        val oldAvg = avgBuffer.getFloat(idx)
        val newVal = inputArray.getFloat(i)
        avgBuffer.putFloat(idx, oldAvg + (newVal - oldAvg) * invCount)
        i += 1
        idx += 4 // 4 bytes per float
      }
      buffer.setLong(countOffset, newCount)
    }
  }

  override def merge(buffer: InternalRow, inputBuffer: InternalRow): Unit = {
    val avgOffset = mutableAggBufferOffset + avgIndex
    val dimOffset = mutableAggBufferOffset + dimIndex
    val countOffset = mutableAggBufferOffset + countIndex
    val inputAvgOffset = inputAggBufferOffset + avgIndex
    val inputDimOffset = inputAggBufferOffset + dimIndex
    val inputCountOffset = inputAggBufferOffset + countIndex

    val inputCount = inputBuffer.getLong(inputCountOffset)
    if (inputCount == 0L) {
      return
    }

    val inputAvgBytes = inputBuffer.getBinary(inputAvgOffset)
    val inputDim = inputBuffer.getInt(inputDimOffset)
    val currentCount = buffer.getLong(countOffset)

    if (currentCount == 0L) {
      // Copy input buffer to current buffer
      buffer.update(avgOffset, inputAvgBytes.clone())
      buffer.setInt(dimOffset, inputDim)
      buffer.setLong(countOffset, inputCount)
    } else {
      val currentDim = buffer.getInt(dimOffset)

      // Empty array case
      if (currentDim == 0 && inputDim == 0) {
        buffer.setLong(countOffset, currentCount + inputCount)
        return
      }

      // Dimension mismatch check
      if (currentDim != inputDim) {
        throw QueryExecutionErrors.vectorDimensionMismatchError(
          prettyName,
          currentDim,
          inputDim
        )
      }

      // Merge running averages:
      // combined_avg = (left_avg * left_count) / (left_count + right_count) +
      //   (right_avg * right_count) / (left_count + right_count)
      val newCount = currentCount + inputCount
      val leftWeight = currentCount.toFloat / newCount
      val rightWeight = inputCount.toFloat / newCount
      val currentAvgBytes = buffer.getBinary(avgOffset)
      // reuse the buffer without reallocation
      val avgBuffer =
        ByteBuffer.wrap(currentAvgBytes).order(ByteOrder.LITTLE_ENDIAN)
      val inputAvgBuffer =
        ByteBuffer.wrap(inputAvgBytes).order(ByteOrder.LITTLE_ENDIAN)
      var i = 0
      var idx = 0
      while (i < currentDim) {
        val leftAvg = avgBuffer.getFloat(idx)
        val rightAvg = inputAvgBuffer.getFloat(idx)
        avgBuffer.putFloat(idx, leftAvg * leftWeight + rightAvg * rightWeight)
        i += 1
        idx += 4 // 4 bytes per float
      }
      buffer.setLong(countOffset, newCount)
    }
  }

  override def eval(buffer: InternalRow): Any = {
    val countOffset = mutableAggBufferOffset + countIndex
    val count = buffer.getLong(countOffset)
    if (count == 0L) {
      null
    } else {
      val dim = buffer.getInt(mutableAggBufferOffset + dimIndex)
      val avgBytes = buffer.getBinary(mutableAggBufferOffset + avgIndex)
      val avgBuffer = ByteBuffer.wrap(avgBytes).order(ByteOrder.LITTLE_ENDIAN)
      val result = new Array[Float](dim)
      var i = 0
      while (i < dim) {
        result(i) = avgBuffer.getFloat()
        i += 1
      }
      ArrayData.toArrayData(result)
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
) extends ImperativeAggregate
    with UnaryLike[Expression]
    with QueryErrorsBase {

  def this(child: Expression) = this(child, 0, 0)

  override def prettyName: String = "vector_sum"

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

  // Aggregate buffer schema: (sum: BINARY, dim: INTEGER)
  // sum is a BINARY representation of the sum vector of floats in the group
  // dim is the dimension of the vector
  // null sum means no valid input has been seen yet
  private lazy val sumAttr = AttributeReference(
    "sum",
    BinaryType,
    nullable = true
  )()
  private lazy val dimAttr = AttributeReference(
    "dim",
    IntegerType,
    nullable = true
  )()

  override def aggBufferAttributes: Seq[AttributeReference] =
    Seq(sumAttr, dimAttr)

  override def aggBufferSchema: StructType =
    DataTypeUtils.fromAttributes(aggBufferAttributes)

  override lazy val inputAggBufferAttributes: Seq[AttributeReference] =
    aggBufferAttributes.map(_.newInstance())

  // Buffer indices
  private val sumIndex = 0
  private val dimIndex = 1

  override def withNewMutableAggBufferOffset(
      newMutableAggBufferOffset: Int
  ): ImperativeAggregate =
    copy(mutableAggBufferOffset = newMutableAggBufferOffset)

  override def withNewInputAggBufferOffset(
      newInputAggBufferOffset: Int
  ): ImperativeAggregate =
    copy(inputAggBufferOffset = newInputAggBufferOffset)

  private lazy val inputContainsNull = child.dataType.asInstanceOf[ArrayType].containsNull

  override def initialize(buffer: InternalRow): Unit = {
    buffer.update(mutableAggBufferOffset + sumIndex, null)
    buffer.update(mutableAggBufferOffset + dimIndex, null)
  }

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

    val sumOffset = mutableAggBufferOffset + sumIndex
    val dimOffset = mutableAggBufferOffset + dimIndex

    if (buffer.isNullAt(sumOffset)) {
      // First valid vector - just copy it as the initial sum
      val byteBuffer =
        ByteBuffer.allocate(inputLen * 4).order(ByteOrder.LITTLE_ENDIAN)
      var i = 0
      while (i < inputLen) {
        byteBuffer.putFloat(inputArray.getFloat(i))
        i += 1
      }
      buffer.update(sumOffset, byteBuffer.array())
      buffer.setInt(dimOffset, inputLen)
    } else {
      val currentDim = buffer.getInt(dimOffset)

      // Empty array case - if current is empty and input is empty, keep empty
      if (currentDim == 0 && inputLen == 0) {
        return
      }

      // Dimension mismatch check
      if (currentDim != inputLen) {
        throw QueryExecutionErrors.vectorDimensionMismatchError(
          prettyName,
          currentDim,
          inputLen
        )
      }

      // Update sum: new_sum = old_sum + new_value
      val currentSumBytes = buffer.getBinary(sumOffset)
      // reuse the buffer without reallocation
      val sumBuffer =
        ByteBuffer.wrap(currentSumBytes).order(ByteOrder.LITTLE_ENDIAN)
      var i = 0
      var idx = 0
      while (i < currentDim) {
        sumBuffer.putFloat(idx, sumBuffer.getFloat(idx) + inputArray.getFloat(i))
        i += 1
        idx += 4 // 4 bytes per float
      }
    }
  }

  override def merge(buffer: InternalRow, inputBuffer: InternalRow): Unit = {
    val sumOffset = mutableAggBufferOffset + sumIndex
    val dimOffset = mutableAggBufferOffset + dimIndex
    val inputSumOffset = inputAggBufferOffset + sumIndex
    val inputDimOffset = inputAggBufferOffset + dimIndex

    if (inputBuffer.isNullAt(inputSumOffset)) {
      return
    }

    val inputSumBytes = inputBuffer.getBinary(inputSumOffset)
    val inputDim = inputBuffer.getInt(inputDimOffset)

    if (buffer.isNullAt(sumOffset)) {
      // Copy input buffer to current buffer
      buffer.update(sumOffset, inputSumBytes.clone())
      buffer.setInt(dimOffset, inputDim)
    } else {
      val currentDim = buffer.getInt(dimOffset)

      // Empty array case
      if (currentDim == 0 && inputDim == 0) {
        return
      }

      // Dimension mismatch check
      if (currentDim != inputDim) {
        throw QueryExecutionErrors.vectorDimensionMismatchError(
          prettyName,
          currentDim,
          inputDim
        )
      }

      // Merge sums: combined_sum = left_sum + right_sum
      val currentSumBytes = buffer.getBinary(sumOffset)
      // reuse the buffer without reallocation
      val sumBuffer =
        ByteBuffer.wrap(currentSumBytes).order(ByteOrder.LITTLE_ENDIAN)
      val inputSumBuffer =
        ByteBuffer.wrap(inputSumBytes).order(ByteOrder.LITTLE_ENDIAN)
      var i = 0
      var idx = 0
      while (i < currentDim) {
        sumBuffer.putFloat(idx, sumBuffer.getFloat(idx) + inputSumBuffer.getFloat(idx))
        i += 1
        idx += 4 // 4 bytes per float
      }
    }
  }

  override def eval(buffer: InternalRow): Any = {
    val sumOffset = mutableAggBufferOffset + sumIndex
    if (buffer.isNullAt(sumOffset)) {
      null
    } else {
      val dim = buffer.getInt(mutableAggBufferOffset + dimIndex)
      val sumBytes = buffer.getBinary(sumOffset)
      val sumBuffer = ByteBuffer.wrap(sumBytes).order(ByteOrder.LITTLE_ENDIAN)
      val result = new Array[Float](dim)
      var i = 0
      while (i < dim) {
        result(i) = sumBuffer.getFloat()
        i += 1
      }
      ArrayData.toArrayData(result)
    }
  }

  override protected def withNewChildInternal(newChild: Expression): VectorSum =
    copy(child = newChild)
}
