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
import org.apache.spark.sql.catalyst.util.{TypeUtils, GenericArrayData}
import org.apache.spark.sql.types._
import org.apache.spark.util.collection.OpenHashMap

/**
 * The Percentile aggregate function computes the exact percentile(s) of expr at pc with range in
 * [0, 1].
 * The parameter pc can be a DoubleType or DoubleType array.
 *
 * The operator is bound to the slower sort based aggregation path because the number of elements
 * and their partial order cannot be determined in advance. Therefore we have to store all the
 * elements in memory, and that too many elements can cause GC paused and eventually OutOfMemory
 * Errors.
 */
@ExpressionDescription(
  usage = """_FUNC_(epxr, pc) - Returns the percentile(s) of expr at pc (range: [0,1]). pc can be
  a double or double array.""")
case class Percentile(
  child: Expression,
  pc: Expression,
  mutableAggBufferOffset: Int = 0,
  inputAggBufferOffset: Int = 0) extends ImperativeAggregate {

  def this(child: Expression, pc: Expression) = {
    this(child = child, pc = pc, mutableAggBufferOffset = 0, inputAggBufferOffset = 0)
  }

  private val percentiles: Seq[Double] = pc match {
    case Literal(ar: GenericArrayData, _: ArrayType) =>
      ar.asInstanceOf[GenericArrayData].array.map{ d => d.asInstanceOf[Double]}
    case _ => Seq.empty
  }

  override def prettyName: String = "percentile"

  override def withNewMutableAggBufferOffset(newMutableAggBufferOffset: Int): ImperativeAggregate =
    copy(mutableAggBufferOffset = newMutableAggBufferOffset)

  override def withNewInputAggBufferOffset(newInputAggBufferOffset: Int): ImperativeAggregate =
    copy(inputAggBufferOffset = newInputAggBufferOffset)

  private var counts = new OpenHashMap[Double, Long]()

  override def children: Seq[Expression] = child :: pc :: Nil

  override def nullable: Boolean = false

  override def dataType: DataType = ArrayType(DoubleType)

  override def inputTypes: Seq[AbstractDataType] = Seq(NumericType, NumericType)

  override def checkInputDataTypes(): TypeCheckResult =
    TypeUtils.checkForOrderingExpr(child.dataType, "function percentile")

  override def supportsPartial: Boolean = false

  override def aggBufferSchema: StructType = StructType.fromAttributes(aggBufferAttributes)

  override val aggBufferAttributes: Seq[AttributeReference] = percentiles.map(percentile =>
    AttributeReference(percentile.toString, DoubleType)())

  override val inputAggBufferAttributes: Seq[AttributeReference] =
    aggBufferAttributes.map(_.newInstance())

  override def initialize(buffer: MutableRow): Unit = {
    var i = 0
    while (i < percentiles.size) {
      buffer.setNullAt(mutableAggBufferOffset + i)
      i += 1
    }
  }

  override def update(buffer: MutableRow, input: InternalRow): Unit = {
    val v = child.eval(input)

    val key = v match {
      case o: Byte => o.toDouble
      case o: Short => o.toDouble
      case o: Int => o.toDouble
      case o: Long => o.toDouble
      case o: Float => o.toDouble
      case o: Decimal => o.toDouble
      case o: Double => o
      case _ => sys.error("Percentile is restricted to Numeric types only.")
    }

    counts.changeValue(key, 1L, _ + 1L)
  }

  override def merge(buffer: MutableRow, inputBuffer: InternalRow): Unit = {
    sys.error("Percentile cannot be used in partial aggregations.")
  }

  override def eval(buffer: InternalRow): Any = {
    if (percentiles.forall(percentile => percentile >= 0.0 && percentile <= 1.0)) {
      if (counts.size == 0) {
        return new GenericArrayData(Seq.empty)
      }

      // Sort all items and generate a sequence, then accumulate the counts
      val sortedCounts = counts.toSeq.sortBy(_._1)
      val aggreCounts = sortedCounts.scanLeft(sortedCounts.head._1, 0L) {
        (k1: (Double, Long), k2: (Double, Long)) => (k2._1, k1._2 + k2._2)
      }.drop(1)
      val maxPosition = aggreCounts.last._2 - 1

      new GenericArrayData(percentiles.map { percentile =>
        getPercentile(aggreCounts, maxPosition * percentile)
      })
    } else {
      sys.error("Percentile value must be within the range of 0 to 1.")
    }
  }

  /**
   * Get the percentile value.
   */
  private def getPercentile(aggreCounts: Seq[(Double, Long)], position: Double): Double = {
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
    return (higher - position) * lowerKey + (position - lower) * higherKey
  }
}
