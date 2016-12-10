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
import org.apache.spark.sql.catalyst.analysis.TypeCheckResult
import org.apache.spark.sql.catalyst.analysis.TypeCheckResult.{TypeCheckFailure, TypeCheckSuccess}
import org.apache.spark.sql.catalyst.expressions.{ExpectsInputTypes, Expression, ExpressionDescription}
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
@ExpressionDescription(
  usage = """
    _FUNC_(col, eps, confidence, seed) - Returns a count-min sketch of a column with the given esp,
      confidence and seed. The result is an array of bytes, which can be deserialized to a
      `CountMinSketch` before usage. Count-min sketch is a probabilistic data structure used for
      cardinality estimation using sub-linear space.
  """)
case class CountMinSketchAgg(
    child: Expression,
    epsExpression: Expression,
    confidenceExpression: Expression,
    seedExpression: Expression,
    override val mutableAggBufferOffset: Int,
    override val inputAggBufferOffset: Int)
  extends TypedImperativeAggregate[CountMinSketch] with ExpectsInputTypes {

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
  private lazy val seed: Int = seedExpression.eval().asInstanceOf[Int]

  override def checkInputDataTypes(): TypeCheckResult = {
    val defaultCheck = super.checkInputDataTypes()
    if (defaultCheck.isFailure) {
      defaultCheck
    } else if (!epsExpression.foldable || !confidenceExpression.foldable ||
      !seedExpression.foldable) {
      TypeCheckFailure(
        "The eps, confidence or seed provided must be a literal or foldable")
    } else if (epsExpression.eval() == null || confidenceExpression.eval() == null ||
      seedExpression.eval() == null) {
      TypeCheckFailure("The eps, confidence or seed provided should not be null")
    } else if (eps <= 0.0) {
      TypeCheckFailure(s"Relative error must be positive (current value = $eps)")
    } else if (confidence <= 0.0 || confidence >= 1.0) {
      TypeCheckFailure(s"Confidence must be within range (0.0, 1.0) (current value = $confidence)")
    } else {
      TypeCheckSuccess
    }
  }

  override def createAggregationBuffer(): CountMinSketch = {
    CountMinSketch.create(eps, confidence, seed)
  }

  override def update(buffer: CountMinSketch, input: InternalRow): Unit = {
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
  }

  override def merge(buffer: CountMinSketch, input: CountMinSketch): Unit = {
    buffer.mergeInPlace(input)
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
    Seq(TypeCollection(IntegralType, StringType, BinaryType), DoubleType, DoubleType, IntegerType)
  }

  override def nullable: Boolean = false

  override def dataType: DataType = BinaryType

  override def children: Seq[Expression] =
    Seq(child, epsExpression, confidenceExpression, seedExpression)

  override def prettyName: String = "count_min_sketch"
}
