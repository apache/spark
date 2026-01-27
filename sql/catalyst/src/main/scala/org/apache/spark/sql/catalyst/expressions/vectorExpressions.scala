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
import org.apache.spark.sql.catalyst.util.ArrayData
import org.apache.spark.sql.errors.{QueryErrorsBase, QueryExecutionErrors}
import org.apache.spark.sql.types.{
  ArrayType,
  DataType,
  FloatType,
  LongType,
  StringType,
  StructField,
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

// scalastyle:off line.size.limit
@ExpressionDescription(
  usage = """
    _FUNC_(array) - Returns the element-wise mean of float vectors in a group.
    All vectors must have the same dimension.
  """,
  examples = """
    Examples:
      > SELECT _FUNC_(col) FROM VALUES (array(1.0F, 2.0F)), (array(3.0F, 4.0F)) AS tab(col);
       [2.0, 3.0]
  """,
  since = "4.2.0",
  group = "vector_funcs"
)
// scalastyle:on line.size.limit
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

  // Aggregate buffer schema: (avg: ARRAY<FLOAT>, count: LONG)
  // Using running average instead of sum to avoid overflow issues
  private lazy val avgAttr = AttributeReference(
    "avg",
    ArrayType(FloatType, containsNull = false),
    nullable = true
  )()
  private lazy val countAttr =
    AttributeReference("count", LongType, nullable = false)()

  override def aggBufferSchema: StructType = StructType(
    Seq(
      StructField(
        "avg",
        ArrayType(FloatType, containsNull = false),
        nullable = true
      ),
      StructField("count", LongType, nullable = false)
    )
  )

  override def aggBufferAttributes: Seq[AttributeReference] =
    Seq(avgAttr, countAttr)

  override lazy val inputAggBufferAttributes: Seq[AttributeReference] =
    aggBufferAttributes.map(_.newInstance())

  // Buffer indices
  private val avgIndex = 0
  private val countIndex = 1

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
    buffer.setLong(mutableAggBufferOffset + countIndex, 0L)
  }

  override def update(buffer: InternalRow, input: InternalRow): Unit = {
    val inputValue = child.eval(input)
    if (inputValue == null) {
      return
    }

    val inputArray = inputValue.asInstanceOf[ArrayData]
    val inputLen = inputArray.numElements()

    // Check for NULL elements in input vector - skip if any NULL element found
    for (i <- 0 until inputLen) {
      if (inputArray.isNullAt(i)) {
        return
      }
    }

    val currentCount = buffer.getLong(mutableAggBufferOffset + countIndex)

    if (currentCount == 0L) {
      // First valid vector - just copy it as the initial average
      val avgArray = new Array[Float](inputLen)
      for (i <- 0 until inputLen) {
        avgArray(i) = inputArray.getFloat(i)
      }
      buffer.update(
        mutableAggBufferOffset + avgIndex,
        ArrayData.toArrayData(avgArray)
      )
      buffer.setLong(mutableAggBufferOffset + countIndex, 1L)
    } else {
      val currentAvg = buffer.getArray(mutableAggBufferOffset + avgIndex)
      val currentLen = currentAvg.numElements()

      // Empty array case - if current is empty and input is empty, keep empty
      if (currentLen == 0 && inputLen == 0) {
        buffer.setLong(mutableAggBufferOffset + countIndex, currentCount + 1L)
        return
      }

      // Dimension mismatch check
      if (currentLen != inputLen) {
        throw QueryExecutionErrors.vectorDimensionMismatchError(
          prettyName,
          currentLen,
          inputLen
        )
      }

      // Update running average: new_avg = old_avg + (new_value - old_avg) / (count + 1)
      val newCount = currentCount + 1L
      val avgArray = new Array[Float](currentLen)
      for (i <- 0 until currentLen) {
        val oldAvg = currentAvg.getFloat(i)
        val newVal = inputArray.getFloat(i)
        avgArray(i) = oldAvg + ((newVal - oldAvg) / newCount.toFloat)
      }
      buffer.update(
        mutableAggBufferOffset + avgIndex,
        ArrayData.toArrayData(avgArray)
      )
      buffer.setLong(mutableAggBufferOffset + countIndex, newCount)
    }
  }

  override def merge(buffer: InternalRow, inputBuffer: InternalRow): Unit = {
    val inputCount = inputBuffer.getLong(inputAggBufferOffset + countIndex)
    if (inputCount == 0L) {
      return
    }

    val inputAvg = inputBuffer.getArray(inputAggBufferOffset + avgIndex)
    val currentCount = buffer.getLong(mutableAggBufferOffset + countIndex)

    if (currentCount == 0L) {
      // Copy input buffer to current buffer
      val inputLen = inputAvg.numElements()
      val avgArray = new Array[Float](inputLen)
      for (i <- 0 until inputLen) {
        avgArray(i) = inputAvg.getFloat(i)
      }
      buffer.update(
        mutableAggBufferOffset + avgIndex,
        ArrayData.toArrayData(avgArray)
      )
      buffer.setLong(mutableAggBufferOffset + countIndex, inputCount)
    } else {
      val currentAvg = buffer.getArray(mutableAggBufferOffset + avgIndex)
      val currentLen = currentAvg.numElements()
      val inputLen = inputAvg.numElements()

      // Empty array case
      if (currentLen == 0 && inputLen == 0) {
        buffer.setLong(
          mutableAggBufferOffset + countIndex,
          currentCount + inputCount
        )
        return
      }

      // Dimension mismatch check
      if (currentLen != inputLen) {
        throw QueryExecutionErrors.vectorDimensionMismatchError(
          prettyName,
          currentLen,
          inputLen
        )
      }

      // Merge running averages:
      // combined_avg = (left_avg * left_count) / (left_count + right_count) +
      //   (right_avg * right_count) / (left_count + right_count)
      val newCount = currentCount + inputCount
      val avgArray = new Array[Float](currentLen)
      for (i <- 0 until currentLen) {
        val leftAvg = currentAvg.getFloat(i)
        val rightAvg = inputAvg.getFloat(i)
        avgArray(i) = ((leftAvg * currentCount) / newCount.toFloat +
          (rightAvg * inputCount) / newCount.toFloat)
      }
      buffer.update(
        mutableAggBufferOffset + avgIndex,
        ArrayData.toArrayData(avgArray)
      )
      buffer.setLong(mutableAggBufferOffset + countIndex, newCount)
    }
  }

  override def eval(buffer: InternalRow): Any = {
    val count = buffer.getLong(mutableAggBufferOffset + countIndex)
    if (count == 0L) {
      null
    } else {
      buffer.getArray(mutableAggBufferOffset + avgIndex)
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
       [4.0, 6.0]
  """,
  since = "4.2.0",
  group = "vector_funcs"
)
// scalastyle:on line.size.limit
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

  // Aggregate buffer schema: just the sum vector
  // null means no valid input has been seen yet
  private lazy val sumAttr = AttributeReference(
    "sum",
    ArrayType(FloatType, containsNull = false),
    nullable = true
  )()

  override def aggBufferSchema: StructType = StructType(
    Seq(
      StructField(
        "sum",
        ArrayType(FloatType, containsNull = false),
        nullable = true
      )
    )
  )

  override def aggBufferAttributes: Seq[AttributeReference] = Seq(sumAttr)

  override lazy val inputAggBufferAttributes: Seq[AttributeReference] =
    aggBufferAttributes.map(_.newInstance())

  override def withNewMutableAggBufferOffset(
      newMutableAggBufferOffset: Int
  ): ImperativeAggregate =
    copy(mutableAggBufferOffset = newMutableAggBufferOffset)

  override def withNewInputAggBufferOffset(
      newInputAggBufferOffset: Int
  ): ImperativeAggregate =
    copy(inputAggBufferOffset = newInputAggBufferOffset)

  override def initialize(buffer: InternalRow): Unit = {
    buffer.update(mutableAggBufferOffset, null)
  }

  override def update(buffer: InternalRow, input: InternalRow): Unit = {
    val inputValue = child.eval(input)
    if (inputValue == null) {
      return
    }

    val inputArray = inputValue.asInstanceOf[ArrayData]
    val inputLen = inputArray.numElements()

    // Check for NULL elements in input vector - skip if any NULL element found
    for (i <- 0 until inputLen) {
      if (inputArray.isNullAt(i)) {
        return
      }
    }

    if (buffer.isNullAt(mutableAggBufferOffset)) {
      // First valid vector - just copy it as the initial sum
      val sumArray = new Array[Float](inputLen)
      for (i <- 0 until inputLen) {
        sumArray(i) = inputArray.getFloat(i)
      }
      buffer.update(mutableAggBufferOffset, ArrayData.toArrayData(sumArray))
    } else {
      val currentSum = buffer.getArray(mutableAggBufferOffset)
      val currentLen = currentSum.numElements()

      // Empty array case - if current is empty and input is empty, keep empty
      if (currentLen == 0 && inputLen == 0) {
        return
      }

      // Dimension mismatch check
      if (currentLen != inputLen) {
        throw QueryExecutionErrors.vectorDimensionMismatchError(
          prettyName,
          currentLen,
          inputLen
        )
      }

      // Update sum: new_sum = old_sum + new_value
      val sumArray = new Array[Float](currentLen)
      for (i <- 0 until currentLen) {
        sumArray(i) = currentSum.getFloat(i) + inputArray.getFloat(i)
      }
      buffer.update(mutableAggBufferOffset, ArrayData.toArrayData(sumArray))
    }
  }

  override def merge(buffer: InternalRow, inputBuffer: InternalRow): Unit = {
    if (inputBuffer.isNullAt(inputAggBufferOffset)) {
      return
    }

    val inputSum = inputBuffer.getArray(inputAggBufferOffset)

    if (buffer.isNullAt(mutableAggBufferOffset)) {
      // Copy input buffer to current buffer
      val inputLen = inputSum.numElements()
      val sumArray = new Array[Float](inputLen)
      for (i <- 0 until inputLen) {
        sumArray(i) = inputSum.getFloat(i)
      }
      buffer.update(mutableAggBufferOffset, ArrayData.toArrayData(sumArray))
    } else {
      val currentSum = buffer.getArray(mutableAggBufferOffset)
      val currentLen = currentSum.numElements()
      val inputLen = inputSum.numElements()

      // Empty array case
      if (currentLen == 0 && inputLen == 0) {
        return
      }

      // Dimension mismatch check
      if (currentLen != inputLen) {
        throw QueryExecutionErrors.vectorDimensionMismatchError(
          prettyName,
          currentLen,
          inputLen
        )
      }

      // Merge sums: combined_sum = left_sum + right_sum
      val sumArray = new Array[Float](currentLen)
      for (i <- 0 until currentLen) {
        sumArray(i) = currentSum.getFloat(i) + inputSum.getFloat(i)
      }
      buffer.update(mutableAggBufferOffset, ArrayData.toArrayData(sumArray))
    }
  }

  override def eval(buffer: InternalRow): Any = {
    buffer.getArray(mutableAggBufferOffset)
  }

  override protected def withNewChildInternal(newChild: Expression): VectorSum =
    copy(child = newChild)
}
