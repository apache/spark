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
  DoubleType,
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
  arguments = """
    Arguments:
      * array1 - An ARRAY of FLOAT values representing the first vector.
      * array2 - An ARRAY of FLOAT values representing the second vector. It must have the
          same dimension as array1.
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
  arguments = """
    Arguments:
      * array1 - An ARRAY of FLOAT values representing the first vector.
      * array2 - An ARRAY of FLOAT values representing the second vector. It must have the
          same dimension as array1.
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
  arguments = """
    Arguments:
      * array1 - An ARRAY of FLOAT values representing the first vector.
      * array2 - An ARRAY of FLOAT values representing the second vector. It must have the
          same dimension as array1.
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
  arguments = """
    Arguments:
      * vector - An ARRAY of FLOAT values representing the vector.
      * degree - A FLOAT specifying the norm degree. Defaults to 2.0 when omitted. Supported
          values are 1.0 (L1 norm), 2.0 (L2 norm), and float('inf') (infinity norm).
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
  arguments = """
    Arguments:
      * vector - An ARRAY of FLOAT values representing the vector to normalize.
      * degree - A FLOAT specifying the norm degree used for normalization. Defaults to 2.0
          when omitted. Supported values are 1.0 (L1 norm), 2.0 (L2 norm), and float('inf')
          (infinity norm).
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

private[expressions] sealed abstract class VectorAggregateKernel(
    val elementType: DataType,
    val elementSize: Int) {

  def copyElements(inputArray: ArrayData, dim: Int): Array[Byte]

  // Element-wise update for non-first vectors.
  // accBytes contains the running vector; update it in-place with inputArray.
  def updateElements(
      accBytes: Array[Byte],
      inputArray: ArrayData,
      dim: Int,
      newCount: Long): Unit

  // Element-wise merge of two non-empty buffers.
  // accBytes contains the left running vector; update it in-place.
  def mergeElements(
      accBytes: Array[Byte],
      inputBytes: Array[Byte],
      dim: Int,
      currentCount: Long,
      inputCount: Long,
      newCount: Long): Unit

  final def toArrayData(accBytes: Array[Byte], dim: Int): ArrayData =
    UnsafeArrayData.fromPrimitiveArray(accBytes, Platform.BYTE_ARRAY_OFFSET, dim, elementSize)
}

private[expressions] object VectorAggregateKernel {

  abstract class FloatKernel extends VectorAggregateKernel(FloatType, 4) {
    override def copyElements(inputArray: ArrayData, dim: Int): Array[Byte] = {
      val bytes = new Array[Byte](dim * 4)
      var i = 0
      while (i < dim) {
        Platform.putFloat(bytes, Platform.BYTE_ARRAY_OFFSET + i.toLong * 4, inputArray.getFloat(i))
        i += 1
      }
      bytes
    }
  }

  abstract class DoubleKernel extends VectorAggregateKernel(DoubleType, 8) {
    override def copyElements(inputArray: ArrayData, dim: Int): Array[Byte] = {
      val bytes = new Array[Byte](dim * 8)
      var i = 0
      while (i < dim) {
        Platform.putDouble(bytes, Platform.BYTE_ARRAY_OFFSET + i.toLong * 8,
          inputArray.getDouble(i))
        i += 1
      }
      bytes
    }
  }
}

// Base trait for vector aggregate functions (vector_avg, vector_sum).
// Provides a unified aggregate buffer schema: (acc: BINARY, count: LONG)
// - acc: BINARY representation of the running vector (sum or average)
// - count: number of valid vectors seen so far
// - dimension is inferred from acc.length / element size (4 bytes per float, 8 per double)
// Subclasses only need to provide the element-wise update and merge kernels.
trait VectorAggregateBase extends ImperativeAggregate
    with UnaryLike[Expression]
    with QueryErrorsBase {

  override def nullable: Boolean = true

  override def dataType: DataType = ArrayType(kernel.elementType, containsNull = false)

  protected def kernels: Seq[VectorAggregateKernel]

  @transient protected lazy val kernel: VectorAggregateKernel = child.dataType match {
    case ArrayType(et, _) => kernels.find(_.elementType == et).getOrElse(kernels.head)
    case _ => kernels.head
  }

  override def checkInputDataTypes(): TypeCheckResult = {
    child.dataType match {
      case ArrayType(et, _) if kernels.exists(_.elementType == et) =>
        TypeCheckResult.TypeCheckSuccess
      case _ =>
        DataTypeMismatch(
          errorSubClass = "UNEXPECTED_INPUT_TYPE",
          messageParameters = Map(
            "paramIndex" -> ordinalNumber(0),
            "requiredType" ->
              kernels.map(k => toSQLType(ArrayType(k.elementType))).mkString(" or "),
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

  // Infer vector dimension from byte array length (element size bytes per element)
  protected def getDim(bytes: Array[Byte]): Int = bytes.length / kernel.elementSize

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
      buffer.update(accOffset, kernel.copyElements(inputArray, inputLen))
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
      kernel.updateElements(accBytes, inputArray, accDim, newCount)
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
      kernel.mergeElements(accBytes, inputAccBytes, accDim,
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
      kernel.toArrayData(accBytes, getDim(accBytes))
    }
  }
}

// scalastyle:off line.size.limit
@ExpressionDescription(
  usage = """
    _FUNC_(array) - Returns the element-wise mean of float vectors in a group.
    All vectors must have the same dimension.
  """,
  arguments = """
    Arguments:
      * array - An ARRAY of FLOAT values representing a vector. All vectors aggregated in the
          group must have the same dimension.
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

  override protected def kernels: Seq[VectorAggregateKernel] = VectorAvg.kernels

  override def withNewMutableAggBufferOffset(
      newMutableAggBufferOffset: Int
  ): ImperativeAggregate =
    copy(mutableAggBufferOffset = newMutableAggBufferOffset)

  override def withNewInputAggBufferOffset(
      newInputAggBufferOffset: Int
  ): ImperativeAggregate =
    copy(inputAggBufferOffset = newInputAggBufferOffset)

  override protected def withNewChildInternal(newChild: Expression): VectorAvg =
    copy(child = newChild)
}

object VectorAvg {

  private object FloatAvg extends VectorAggregateKernel.FloatKernel {
    override def updateElements(
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
        val value = inputArray.getFloat(i)
        val delta = value - oldAvg
        val newAvg = if (java.lang.Float.isFinite(delta)) {
          oldAvg + delta * invCount
        } else if (java.lang.Float.isFinite(oldAvg) && java.lang.Float.isFinite(value)) {
          oldAvg + (value * invCount - oldAvg * invCount)
        } else {
          oldAvg + value
        }
        Platform.putFloat(accBytes, off, newAvg)
        i += 1
      }
    }

    override def mergeElements(
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
  }

  private val kernels: Seq[VectorAggregateKernel] = Seq(FloatAvg)
}

// scalastyle:off line.size.limit
@ExpressionDescription(
  usage = """
    _FUNC_(array) - Returns the element-wise sum of float or double vectors in a group.
    All vectors must have the same dimension.
  """,
  arguments = """
    Arguments:
      * array - An ARRAY of FLOAT or DOUBLE values representing a vector. All vectors aggregated
          in the group must have the same dimension. The result has the same element type as
          the input.
  """,
  examples = """
    Examples:
      > SELECT _FUNC_(col) FROM VALUES (array(1.0F, 2.0F)), (array(3.0F, 4.0F)) AS tab(col);
       [4.0,6.0]
      > SELECT _FUNC_(col) FROM VALUES (array(1.0D, 2.0D)), (array(3.0D, 4.0D)) AS tab(col);
       [4.0,6.0]
  """,
  since = "4.2.0",
  group = "vector_funcs"
)
// scalastyle:on line.size.limit
// Note: Each update or merge uses the input element type's precision:
// single precision for ARRAY<FLOAT> and double precision for ARRAY<DOUBLE>.
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

  override protected def kernels: Seq[VectorAggregateKernel] = VectorSum.kernels

  override def withNewMutableAggBufferOffset(
      newMutableAggBufferOffset: Int
  ): ImperativeAggregate =
    copy(mutableAggBufferOffset = newMutableAggBufferOffset)

  override def withNewInputAggBufferOffset(
      newInputAggBufferOffset: Int
  ): ImperativeAggregate =
    copy(inputAggBufferOffset = newInputAggBufferOffset)

  override protected def withNewChildInternal(newChild: Expression): VectorSum =
    copy(child = newChild)
}

object VectorSum {

  private object FloatSum extends VectorAggregateKernel.FloatKernel {
    override def updateElements(
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

    override def mergeElements(
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
  }

  private object DoubleSum extends VectorAggregateKernel.DoubleKernel {
    override def updateElements(
        accBytes: Array[Byte],
        inputArray: ArrayData,
        dim: Int,
        newCount: Long): Unit = {
      // Update sum: new_sum = old_sum + new_value
      var i = 0
      while (i < dim) {
        val off = Platform.BYTE_ARRAY_OFFSET + i.toLong * 8
        Platform.putDouble(accBytes, off,
          Platform.getDouble(accBytes, off) + inputArray.getDouble(i))
        i += 1
      }
    }

    override def mergeElements(
        accBytes: Array[Byte],
        inputBytes: Array[Byte],
        dim: Int,
        currentCount: Long,
        inputCount: Long,
        newCount: Long): Unit = {
      // Merge sums: combined_sum = left_sum + right_sum
      var i = 0
      while (i < dim) {
        val off = Platform.BYTE_ARRAY_OFFSET + i.toLong * 8
        Platform.putDouble(accBytes, off,
          Platform.getDouble(accBytes, off) + Platform.getDouble(inputBytes, off))
        i += 1
      }
    }
  }

  private val kernels: Seq[VectorAggregateKernel] = Seq(FloatSum, DoubleSum)
}
