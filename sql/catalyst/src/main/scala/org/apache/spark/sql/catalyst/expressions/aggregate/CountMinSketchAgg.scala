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

package org.apache.spark.sql.catalyst.expressions.aggregate

import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.analysis.{ExpressionBuilder, TypeCheckResult}
import org.apache.spark.sql.catalyst.analysis.TypeCheckResult.{DataTypeMismatch, TypeCheckSuccess}
import org.apache.spark.sql.catalyst.expressions.{ExpectsInputTypes, Expression, ExpressionDescription, Literal}
import org.apache.spark.sql.catalyst.plans.logical.{FunctionSignature, InputParameter}
import org.apache.spark.sql.catalyst.trees.QuaternaryLike
import org.apache.spark.sql.errors.QueryErrorsBase
import org.apache.spark.sql.types._
import org.apache.spark.unsafe.types.UTF8String
import org.apache.spark.util.sketch.CountMinSketch

/**
 * This function returns a count-min sketch of a column with the given esp, confidence and seed.
 * A count-min sketch is a probabilistic data structure used for summarizing streams of data in
 * sub-linear space, which is useful for equality predicates and join size estimation.
 * The result returned by the function is an array of bytes, which should be deserialized to a
 * `CountMinSketch` before usage.
 *
 * @param child child expression that can produce column value with `child.eval(inputRow)`
 * @param epsExpression relative error, must be positive
 * @param confidenceExpression confidence, must be positive and less than 1.0
 * @param seedExpression random seed
 */
case class CountMinSketchAgg(
    child: Expression,
    epsExpression: Expression,
    confidenceExpression: Expression,
    seedExpression: Expression,
    override val mutableAggBufferOffset: Int,
    override val inputAggBufferOffset: Int)
  extends TypedImperativeAggregate[CountMinSketch]
  with ExpectsInputTypes
  with QuaternaryLike[Expression]
  with QueryErrorsBase {

  def this(
      child: Expression,
      epsExpression: Expression,
      confidenceExpression: Expression,
      seedExpression: Expression) = {
    this(child, epsExpression, confidenceExpression, seedExpression, 0, 0)
  }

  // Mark as lazy so that they are not evaluated during tree transformation.
  private lazy val eps: Double = epsExpression.eval().asInstanceOf[Double]
  private lazy val confidence: Double = confidenceExpression.eval().asInstanceOf[Double]
  private lazy val seed: Int = seedExpression.eval() match {
    case i: Int => i
    case l: Long => l.toInt
  }

  override def checkInputDataTypes(): TypeCheckResult = {
    val defaultCheck = super.checkInputDataTypes()
    if (defaultCheck.isFailure) {
      defaultCheck
    } else if (!epsExpression.foldable) {
      DataTypeMismatch(
        errorSubClass = "NON_FOLDABLE_INPUT",
        messageParameters = Map(
          "inputName" -> toSQLId("eps"),
          "inputType" -> toSQLType(epsExpression.dataType),
          "inputExpr" -> toSQLExpr(epsExpression))
      )
    } else if (!confidenceExpression.foldable) {
      DataTypeMismatch(
        errorSubClass = "NON_FOLDABLE_INPUT",
        messageParameters = Map(
          "inputName" -> toSQLId("confidence"),
          "inputType" -> toSQLType(confidenceExpression.dataType),
          "inputExpr" -> toSQLExpr(confidenceExpression))
      )
    } else if (!seedExpression.foldable) {
      DataTypeMismatch(
        errorSubClass = "NON_FOLDABLE_INPUT",
        messageParameters = Map(
          "inputName" -> toSQLId("seed"),
          "inputType" -> toSQLType(seedExpression.dataType),
          "inputExpr" -> toSQLExpr(seedExpression))
      )
    } else if (epsExpression.eval() == null) {
      DataTypeMismatch(
        errorSubClass = "UNEXPECTED_NULL",
        messageParameters = Map("exprName" -> "eps"))
    } else if (confidenceExpression.eval() == null) {
      DataTypeMismatch(
        errorSubClass = "UNEXPECTED_NULL",
        messageParameters = Map("exprName" -> "confidence"))
    } else if (seedExpression.eval() == null) {
      DataTypeMismatch(
        errorSubClass = "UNEXPECTED_NULL",
        messageParameters = Map("exprName" -> "seed"))
    } else if (eps <= 0.0) {
      DataTypeMismatch(
        errorSubClass = "VALUE_OUT_OF_RANGE",
        messageParameters = Map(
          "exprName" -> "eps",
          "valueRange" -> s"(${0.toDouble}, ${Double.MaxValue}]",
          "currentValue" -> toSQLValue(eps, DoubleType)
        )
      )
    } else if (confidence <= 0.0 || confidence >= 1.0) {
      DataTypeMismatch(
        errorSubClass = "VALUE_OUT_OF_RANGE",
        messageParameters = Map(
          "exprName" -> "confidence",
          "valueRange" -> s"(${0.toDouble}, ${1.toDouble}]",
          "currentValue" -> toSQLValue(confidence, DoubleType)
        )
      )
    } else {
      TypeCheckSuccess
    }
  }

  override def createAggregationBuffer(): CountMinSketch = {
    CountMinSketch.create(eps, confidence, seed)
  }

  override def update(buffer: CountMinSketch, input: InternalRow): CountMinSketch = {
    val value = child.eval(input)
    // Ignore empty rows
    if (value != null) {
      child.dataType match {
        // For string type, we can get bytes of our `UTF8String` directly, and call the `addBinary`
        // instead of `addString` to avoid unnecessary conversion.
        case StringType => buffer.addBinary(value.asInstanceOf[UTF8String].getBytes)
        case _ => buffer.add(value)
      }
    }
    buffer
  }

  override def merge(buffer: CountMinSketch, input: CountMinSketch): CountMinSketch = {
    buffer.mergeInPlace(input)
    buffer
  }

  override def eval(buffer: CountMinSketch): Any = serialize(buffer)

  override def serialize(buffer: CountMinSketch): Array[Byte] = {
    buffer.toByteArray
  }

  override def deserialize(storageFormat: Array[Byte]): CountMinSketch = {
    CountMinSketch.readFrom(storageFormat)
  }

  override def withNewMutableAggBufferOffset(newMutableAggBufferOffset: Int): CountMinSketchAgg =
    copy(mutableAggBufferOffset = newMutableAggBufferOffset)

  override def withNewInputAggBufferOffset(newInputAggBufferOffset: Int): CountMinSketchAgg =
    copy(inputAggBufferOffset = newInputAggBufferOffset)

  override def inputTypes: Seq[AbstractDataType] = {
    Seq(TypeCollection(IntegralType, StringType, BinaryType), DoubleType, DoubleType,
      TypeCollection(IntegerType, LongType))
  }

  override def nullable: Boolean = false

  override def dataType: DataType = BinaryType

  override def defaultResult: Option[Literal] =
    Option(Literal.create(eval(createAggregationBuffer()), dataType))

  override def prettyName: String = "count_min_sketch"

  override def first: Expression = child
  override def second: Expression = epsExpression
  override def third: Expression = confidenceExpression
  override def fourth: Expression = seedExpression

  override protected def withNewChildrenInternal(first: Expression, second: Expression,
      third: Expression, fourth: Expression): CountMinSketchAgg =
    copy(
      child = first,
      epsExpression = second,
      confidenceExpression = third,
      seedExpression = fourth)
}

// scalastyle:off line.size.limit
@ExpressionDescription(
  usage = """
    _FUNC_(col, eps, confidence, seed) - Returns a count-min sketch of a column with the given esp,
      confidence and seed. The result is an array of bytes, which can be deserialized to a
      `CountMinSketch` before usage. Count-min sketch is a probabilistic data structure used for
      cardinality estimation using sub-linear space.
  """,
  examples = """
    Examples:
      > SELECT hex(_FUNC_(col, 0.5d, 0.5d, 1)) FROM VALUES (1), (2), (1) AS tab(col);
       0000000100000000000000030000000100000004000000005D8D6AB90000000000000000000000000000000200000000000000010000000000000000
  """,
  group = "agg_funcs",
  since = "2.2.0")
// scalastyle:on line.size.limit
object CountMinSketchAggExpressionBuilder extends ExpressionBuilder {
  final val defaultFunctionSignature = FunctionSignature(Seq(
    InputParameter("column"),
    InputParameter("epsilon"),
    InputParameter("confidence"),
    InputParameter("seed")
  ))
  override def functionSignature: Option[FunctionSignature] = Some(defaultFunctionSignature)
  override def build(funcName: String, expressions: Seq[Expression]): Expression = {
    assert(expressions.size == 4)
    new CountMinSketchAgg(expressions(0), expressions(1), expressions(2), expressions(3))
  }
}
