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

import scala.collection.JavaConverters._

import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.analysis.TypeCheckResult
import org.apache.spark.sql.catalyst.analysis.TypeCheckResult.{TypeCheckFailure, TypeCheckSuccess}
import org.apache.spark.sql.catalyst.expressions.{Expression, ExpressionDescription, ImplicitCastInputTypes}
import org.apache.spark.sql.catalyst.trees.BinaryLike
import org.apache.spark.sql.catalyst.util.{DistributedHistogramSerializer, DistributeHistogram, GenericArrayData}
import org.apache.spark.sql.errors.QueryExecutionErrors
import org.apache.spark.sql.types.{AbstractDataType, ArrayType, DataType, DateType, DoubleType, IntegerType, NumericType, StructField, StructType, TimestampNTZType, TimestampType, TypeCollection}

/**
 * Computes an approximate histogram of a numerical column using a user-specified number of bins.
 *
 * The output is an array of (x,y) pairs as Hive struct objects that represents the histogram's
 * bin centers and heights.
 */
@ExpressionDescription(
  usage = """
    _FUNC_(expr, nb) - Computes a histogram on numeric 'expr' using nb bins.
      The return value is an array of (x,y) pairs representing the centers of the
      histogram's bins. As the value of 'nb' is increased, the histogram approximation
      gets finer-grained, but may yield artifacts around outliers. In practice, 20-40
      histogram bins appear to work well, with more bins being required for skewed or
      smaller datasets. Note that this function creates a histogram with non-uniform
      bin widths. It offers no guarantees in terms of the mean-squared-error of the
      histogram, but in practice is comparable to the histograms produced by the R/S-Plus
      statistical computing packages.
    """,
  examples = """
    Examples:
      > SELECT _FUNC_(col, 5) FROM VALUES (0), (1), (2), (10) AS tab(col);
      [{"x":0.0,"y":1.0},{"x":1.0,"y":1.0},{"x":2.0,"y":1.0},{"x":10.0,"y":1.0}]
    """,
  group = "agg_funcs",
  since = "3.3.0")
case class HistogramNumeric(
    child: Expression,
    nBins: Expression,
    override val mutableAggBufferOffset: Int,
    override val inputAggBufferOffset: Int)
  extends TypedImperativeAggregate[DistributeHistogram] with ImplicitCastInputTypes
  with BinaryLike[Expression] {

  def this(child: Expression, nBins: Expression) = {
    this(child, nBins, 0, 0)
  }

  private lazy val nb = nBins.eval() match {
    case null => null
    case n: Int => n
  }

  override def inputTypes: Seq[AbstractDataType] = {
    // Support NumericType, DateType, TimestampType and TimestampNTZType since their internal types
    // are all numeric, and can be easily cast to double for processing.
    Seq(TypeCollection(NumericType, DateType, TimestampType, TimestampNTZType), IntegerType)
  }

  override def checkInputDataTypes(): TypeCheckResult = {
    val defaultCheck = super.checkInputDataTypes()
    if (defaultCheck.isFailure) {
      defaultCheck
    } else if (!nBins.foldable) {
      TypeCheckFailure(s"${this.prettyName} needs the nBins provided must be a constant literal.")
    } else if (nb == null) {
      TypeCheckFailure(s"${this.prettyName} needs nBins value must not be null.")
    } else if (nb.asInstanceOf[Int] < 2) {
      TypeCheckFailure(s"${this.prettyName} needs nBins to be at least 2, but you supplied $nb.")
    } else {
      TypeCheckSuccess
    }
  }

  override def createAggregationBuffer(): DistributeHistogram = {
    new DistributeHistogram(nb.asInstanceOf[Int])
  }

  override def update(buffer: DistributeHistogram, inputRow: InternalRow): DistributeHistogram = {
    val value = child.eval(inputRow)
    // Ignore empty rows, for example: percentile_approx(null)
    if (value != null) {
      // Convert the value to a double value
      val doubleValue = child.dataType match {
        case DateType => value.asInstanceOf[Int].toDouble
        case TimestampType | TimestampNTZType => value.asInstanceOf[Long].toDouble
        case n: NumericType => n.numeric.toDouble(value.asInstanceOf[n.InternalType])
        case other: DataType =>
          throw QueryExecutionErrors.dataTypeUnexpectedError(other)
      }
      buffer.add(doubleValue)
    }
    buffer
  }

  override def merge(
      buffer: DistributeHistogram,
      other: DistributeHistogram): DistributeHistogram = {
    buffer.merge(other)
    buffer
  }

  override def eval(buffer: DistributeHistogram): Any = {
    if (buffer.getUsedBins < 1) {
      null
    } else {
      val result = buffer.getBins.asScala.map { coord =>
        InternalRow.apply(coord.x, coord.y)
      }.toArray
      new GenericArrayData(result)
    }
  }

  override def serialize(obj: DistributeHistogram): Array[Byte] = {
    HistogramNumeric.serializer.serialize(obj)
  }

  override def deserialize(bytes: Array[Byte]): DistributeHistogram = {
    HistogramNumeric.serializer.deserialize(bytes)
  }

  override def left: Expression = child

  override def right: Expression = nBins

  override protected def withNewChildrenInternal(
      newLeft: Expression,
      newRight: Expression): HistogramNumeric = {
    copy(child = newLeft, nBins = newRight)
  }


  override def withNewMutableAggBufferOffset(newOffset: Int): HistogramNumeric =
    copy(mutableAggBufferOffset = newOffset)

  override def withNewInputAggBufferOffset(newOffset: Int): HistogramNumeric =
    copy(inputAggBufferOffset = newOffset)

  override def nullable: Boolean = true

  override def dataType: DataType =
    ArrayType(new StructType(Array(StructField("x", DoubleType), StructField("y", DoubleType))))

  override def prettyName: String = "histogram_numeric"
}

object HistogramNumeric {
  val serializer: DistributedHistogramSerializer = new DistributedHistogramSerializer
}
