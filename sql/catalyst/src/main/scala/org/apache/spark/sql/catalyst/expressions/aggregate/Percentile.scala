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
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.util._
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

  private val percentiles: Seq[Number] = pc match {
    case e: Literal =>
      analyzePercentile(e)
    case CreateArray(e: Seq[Expression]) =>
      analyzePercentile(e)
    case e: Expression =>
      analyzePercentile(e.children)
    case _ => sys.error("Percentiles expression cannot be analyzed.")
  }

  private def analyzePercentile(e: Expression): Seq[Number] = e match {
    case Literal(ar: GenericArrayData, _: ArrayType) =>
      ar.asInstanceOf[GenericArrayData].array.map{ d => d.asInstanceOf[Number]}
    case Literal(d: Any, _: NumericType) =>
      Seq(d.asInstanceOf[Number])
    case PrettyAttribute(n: String, _: NumericType) =>
      Seq(n.toDouble.asInstanceOf[Number])
    case _ => sys.error("Percentiles expression cannot be analyzed.")
  }

  private def analyzePercentile(e: Seq[Expression]): Seq[Number] = {
    e.map { expr =>
      expr match {
        case Literal(d: Any, _: NumericType) =>
          d.asInstanceOf[Number]
        case PrettyAttribute(n: String, _: NumericType) =>
          n.toDouble.asInstanceOf[Number]
        case _ => sys.error("Percentiles expression cannot be analyzed.")
      }
    }
  }

  require(percentiles.size > 0, "Percentiles should not be empty.")

  require(percentiles.forall(percentile =>
    percentile.doubleValue() >= 0.0 && percentile.doubleValue() <= 1.0),
    "Percentile value must be within the range of 0 to 1.")

  override def prettyName: String = "percentile"

  override def withNewMutableAggBufferOffset(newMutableAggBufferOffset: Int): ImperativeAggregate =
    copy(mutableAggBufferOffset = newMutableAggBufferOffset)

  override def withNewInputAggBufferOffset(newInputAggBufferOffset: Int): ImperativeAggregate =
    copy(inputAggBufferOffset = newInputAggBufferOffset)

  private var counts = new OpenHashMap[Number, Long]

  override def children: Seq[Expression] = child :: pc :: Nil

  override def nullable: Boolean = false

  override def dataType: DataType = ArrayType(DoubleType)

  override def inputTypes: Seq[AbstractDataType] = Seq(NumericType, NumericType)

  override def checkInputDataTypes(): TypeCheckResult =
    TypeUtils.checkForOrderingExpr(child.dataType, "function percentile")

  override def supportsPartial: Boolean = false

  override def aggBufferSchema: StructType = StructType.fromAttributes(aggBufferAttributes)

  override val aggBufferAttributes: Seq[AttributeReference] = Nil

  override val inputAggBufferAttributes: Seq[AttributeReference] = Nil

  override def initialize(buffer: MutableRow): Unit = {
    var i = 0
    while (i < percentiles.size) {
      buffer.setNullAt(mutableAggBufferOffset + i)
      i += 1
    }

    //The counts openhashmap will contain values of other groups if we don't initialize it here.
    counts = new OpenHashMap[Number, Long]
  }

  override def update(buffer: MutableRow, input: InternalRow): Unit = {
    val key = child.eval(input).asInstanceOf[Number]

    counts.changeValue(key, 1L, _ + 1L)
  }

  override def merge(buffer: MutableRow, inputBuffer: InternalRow): Unit = {
    sys.error("Percentile cannot be used in partial aggregations.")
  }

  override def eval(buffer: InternalRow): Any = {
    if (counts.isEmpty) {
      return new GenericArrayData(Seq.empty)
    }
    // Sort all items and generate a sequence, then accumulate the counts
    var ascOrder = new Ordering[Int](){
      override def compare(a:Int,b:Int):Int = a - b
    }
    val sortedCounts = counts.toSeq.sortBy(_._1)(new Ordering[Number]() {
      override def compare(a: Number, b: Number): Int =
        scala.math.signum(a.doubleValue() - b.doubleValue()).toInt
    })
    val aggreCounts = sortedCounts.scanLeft(sortedCounts.head._1, 0L) {
      (k1: (Number, Long), k2: (Number, Long)) => (k2._1, k1._2 + k2._2)
    }.drop(1)
    val maxPosition = aggreCounts.last._2 - 1

    new GenericArrayData(percentiles.map { percentile =>
      getPercentile(aggreCounts, maxPosition * percentile.doubleValue()).doubleValue()
    })
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
