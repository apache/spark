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

import org.apache.spark.sql.AnalysisException
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.analysis.TypeCheckResult
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.util._
import org.apache.spark.sql.types._
import org.apache.spark.util.collection.OpenHashMap

/**
 * The Percentile aggregate function returns the exact percentile(s) of numeric column `expr` at
 * the given percentage(s) with value range in [0.0, 1.0].
 *
 * The operator is bound to the slower sort based aggregation path because the number of elements
 * and their partial order cannot be determined in advance. Therefore we have to store all the
 * elements in memory, and that too many elements can cause GC paused and eventually OutOfMemory
 * Errors.
 *
 * @param child child expression that produce numeric column value with `child.eval(inputRow)`
 * @param percentageExpression Expression that represents a single percentage value or an array of
 *                             percentage values. Each percentage value must be in the range
 *                             [0.0, 1.0].
 */
@ExpressionDescription(
  usage =
    """
      _FUNC_(col, percentage) - Returns the exact percentile value of numeric column `col` at the
      given percentage. The value of percentage must be between 0.0 and 1.0.

      _FUNC_(col, array(percentage1 [, percentage2]...)) - Returns the exact percentile value array
      of numeric column `col` at the given percentage(s). Each value of the percentage array must
      be between 0.0 and 1.0.
    """)
case class Percentile(
  child: Expression,
  percentageExpression: Expression,
  mutableAggBufferOffset: Int = 0,
  inputAggBufferOffset: Int = 0) extends ImperativeAggregate {

  def this(child: Expression, percentageExpression: Expression) = {
    this(child, percentageExpression, 0, 0)
  }

  override def prettyName: String = "percentile"

  override def withNewMutableAggBufferOffset(newMutableAggBufferOffset: Int): ImperativeAggregate =
    copy(mutableAggBufferOffset = newMutableAggBufferOffset)

  override def withNewInputAggBufferOffset(newInputAggBufferOffset: Int): ImperativeAggregate =
    copy(inputAggBufferOffset = newInputAggBufferOffset)

  private var counts = new OpenHashMap[Number, Long]

  // Mark as lazy so that percentageExpression is not evaluated during tree transformation.
  private lazy val (returnPercentileArray: Boolean, percentages: Seq[Number]) =
    evalPercentages(percentageExpression)

  override def children: Seq[Expression] = child :: percentageExpression :: Nil

  // Returns null for empty inputs
  override def nullable: Boolean = true

  override def dataType: DataType =
    if (returnPercentileArray) ArrayType(DoubleType) else DoubleType

  override def inputTypes: Seq[AbstractDataType] =
    Seq(NumericType, TypeCollection(NumericType, ArrayType))

  override def checkInputDataTypes(): TypeCheckResult =
    TypeUtils.checkForNumericExpr(child.dataType, "function percentile")

  override def supportsPartial: Boolean = false

  override def aggBufferSchema: StructType = StructType.fromAttributes(aggBufferAttributes)

  override val aggBufferAttributes: Seq[AttributeReference] = Nil

  override val inputAggBufferAttributes: Seq[AttributeReference] = Nil

  override def initialize(buffer: InternalRow): Unit = {
    // The counts OpenHashMap will contain values of other groups if we don't initialize it here.
    // Since OpenHashMap doesn't support deletions, we have to create a new instance.
    counts = new OpenHashMap[Number, Long]
  }

  private def evalPercentages(expr: Expression): (Boolean, Seq[Number]) = {
    val (isArrayType, values) = (expr.dataType, expr.eval()) match {
      case (_, n: Number) => (false, Array(n))
      case (_, d: Decimal) => (false, Array(d.toDouble.asInstanceOf[Number]))
      case (ArrayType(baseType: NumericType, _), arrayData: ArrayData) =>
        val numericArray = arrayData.toObjectArray(baseType)
        (true, numericArray.map { x =>
          baseType.numeric.toDouble(x.asInstanceOf[baseType.InternalType]).asInstanceOf[Number]
        })
      case other =>
        throw new AnalysisException(s"Invalid data type ${other._1} for parameter percentage")
    }

    require(values.forall(value => value.doubleValue() >= 0.0 && value.doubleValue() <= 1.0),
      s"Percentage values must be between 0.0 and 1.0, current values = ${values.mkString(", ")}")

    (isArrayType, values)
  }

  override def update(buffer: InternalRow, input: InternalRow): Unit = {
    val key = child.eval(input).asInstanceOf[Number]

    // Null values are ignored when computing percentiles.
    if (key != null) {
      counts.changeValue(key, 1L, _ + 1L)
    }
  }

  override def merge(buffer: InternalRow, inputBuffer: InternalRow): Unit = {
    sys.error("Percentile cannot be used in partial aggregations.")
  }

  override def eval(buffer: InternalRow): Any = {
    if (counts.isEmpty) {
      return generateOutput(Seq.empty)
    }

    val sortedCounts = counts.toSeq.sortBy(_._1)(new Ordering[Number]() {
      override def compare(a: Number, b: Number): Int =
        scala.math.signum(a.doubleValue() - b.doubleValue()).toInt
    })
    val aggreCounts = sortedCounts.scanLeft(sortedCounts.head._1, 0L) {
      (k1: (Number, Long), k2: (Number, Long)) => (k2._1, k1._2 + k2._2)
    }.tail
    val maxPosition = aggreCounts.last._2 - 1

    generateOutput(percentages.map { percentile =>
      getPercentile(aggreCounts, maxPosition * percentile.doubleValue()).doubleValue()
    })
  }

  private def generateOutput(results: Seq[Double]): Any = {
    if (results.isEmpty) {
      null
    } else if (returnPercentileArray) {
      new GenericArrayData(results)
    } else {
      results.head
    }
  }

  /**
   * Get the percentile value.
   */
  private def getPercentile(aggreCounts: Seq[(Number, Long)], position: Double): Number = {
    // We may need to do linear interpolation to get the exact percentile
    val lower = position.floor
    val higher = position.ceil

    // Linear search since this won't take much time from the total execution anyway
    // lower has the range of [0 .. total-1]
    // The first entry with accumulated count (lower+1) corresponds to the lower position.
    var i = 0
    while (aggreCounts(i)._2 < lower + 1) {
      i += 1
    }

    val lowerKey = aggreCounts(i)._1
    if (higher == lower) {
      // no interpolation needed because position does not have a fraction
      return lowerKey
    }

    if (aggreCounts(i)._2 < higher + 1) {
      i += 1
    }
    val higherKey = aggreCounts(i)._1

    if (higherKey == lowerKey) {
      // no interpolation needed because lower position and higher position has the same key
      return lowerKey
    }

    // Linear interpolation to get the exact percentile
    return (higher - position) * lowerKey.doubleValue() +
      (position - lower) * higherKey.doubleValue()
  }
}
