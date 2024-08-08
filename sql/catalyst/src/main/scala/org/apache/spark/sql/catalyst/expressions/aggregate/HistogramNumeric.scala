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

import java.nio.ByteBuffer

import com.google.common.primitives.{Doubles, Ints}

import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.analysis.TypeCheckResult
import org.apache.spark.sql.catalyst.analysis.TypeCheckResult.{DataTypeMismatch, TypeCheckSuccess}
import org.apache.spark.sql.catalyst.expressions.{Expression, ExpressionDescription, ImplicitCastInputTypes}
import org.apache.spark.sql.catalyst.trees.BinaryLike
import org.apache.spark.sql.catalyst.util.GenericArrayData
import org.apache.spark.sql.errors.QueryErrorsBase
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.types._
import org.apache.spark.sql.util.NumericHistogram

/**
 * Computes an approximate histogram of a numerical column using a user-specified number of bins.
 *
 * The output is an array of (x,y) pairs as struct objects that represents the histogram's
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
      statistical computing packages. Note: the output type of the 'x' field in the return value is
      propagated from the input value consumed in the aggregate function.
    """,
  examples = """
    Examples:
      > SELECT _FUNC_(col, 5) FROM VALUES (0), (1), (2), (10) AS tab(col);
       [{"x":0,"y":1.0},{"x":1,"y":1.0},{"x":2,"y":1.0},{"x":10,"y":1.0}]
  """,
  group = "agg_funcs",
  since = "3.3.0")
case class HistogramNumeric(
    child: Expression,
    nBins: Expression,
    override val mutableAggBufferOffset: Int,
    override val inputAggBufferOffset: Int)
  extends TypedImperativeAggregate[NumericHistogram] with ImplicitCastInputTypes
  with BinaryLike[Expression] with QueryErrorsBase {

  def this(child: Expression, nBins: Expression) = {
    this(child, nBins, 0, 0)
  }

  private lazy val nb = nBins.eval() match {
    case null => null
    case n: Int => n
  }

  private lazy val propagateInputType: Boolean = SQLConf.get.histogramNumericPropagateInputType

  override def inputTypes: Seq[AbstractDataType] = {
    // Support NumericType, DateType, TimestampType and TimestampNTZType, YearMonthIntervalType,
    // DayTimeIntervalType since their internal types are all numeric,
    // and can be easily cast to double for processing.
    Seq(TypeCollection(NumericType, DateType, TimestampType, TimestampNTZType,
      YearMonthIntervalType, DayTimeIntervalType), IntegerType)
  }

  override def checkInputDataTypes(): TypeCheckResult = {
    val defaultCheck = super.checkInputDataTypes()
    if (defaultCheck.isFailure) {
      defaultCheck
    } else if (!nBins.foldable) {
      DataTypeMismatch(
        errorSubClass = "NON_FOLDABLE_INPUT",
        messageParameters = Map(
          "inputName" -> toSQLId("nb"),
          "inputType" -> toSQLType(nBins.dataType),
          "inputExpr" -> toSQLExpr(nBins))
      )
    } else if (nb == null) {
      DataTypeMismatch(
        errorSubClass = "UNEXPECTED_NULL",
        messageParameters = Map("exprName" -> "nb"))
    } else if (nb.asInstanceOf[Int] < 2) {
      DataTypeMismatch(
        errorSubClass = "VALUE_OUT_OF_RANGE",
        messageParameters = Map(
          "exprName" -> "nb",
          "valueRange" -> s"[2, ${Int.MaxValue}]",
          "currentValue" -> toSQLValue(nb, IntegerType)
        )
      )
    } else {
      TypeCheckSuccess
    }
  }

  override def createAggregationBuffer(): NumericHistogram = {
    val buffer = new NumericHistogram()
    buffer.allocate(nb.asInstanceOf[Int])
    buffer
  }

  override def update(buffer: NumericHistogram, inputRow: InternalRow): NumericHistogram = {
    val value = child.eval(inputRow)
    // Ignore empty rows, for example: histogram_numeric(null)
    if (value != null) {
      // Convert the value to a double value
      val doubleValue = value.asInstanceOf[Number].doubleValue
      buffer.add(doubleValue)
    }
    buffer
  }

  override def merge(
      buffer: NumericHistogram,
      other: NumericHistogram): NumericHistogram = {
    buffer.merge(other)
    buffer
  }

  override def eval(buffer: NumericHistogram): Any = {
    if (buffer.getUsedBins < 1) {
      null
    } else {
      val array = new Array[AnyRef](buffer.getUsedBins)
      (0 until buffer.getUsedBins).foreach { index =>
        // Note that the 'coord.x' and 'coord.y' have double-precision floating point type here.
        val coord = buffer.getBin(index)
        if (propagateInputType) {
          // If the SQLConf.spark.sql.legacy.histogramNumericPropagateInputType is set to true,
          // we need to internally convert the 'coord.x' value to the expected result type, for
          // cases like integer types, timestamps, and intervals which are valid inputs to the
          // numeric histogram aggregate function. For example, in this case:
          // 'SELECT histogram_numeric(val, 3) FROM VALUES (0L), (1L), (2L), (10L) AS tab(col)'
          // returns an array of structs where the first field has LongType.
          val result: Any = left.dataType match {
            case ByteType => coord.x.toByte
            case IntegerType | DateType | _: YearMonthIntervalType =>
              coord.x.toInt
            case FloatType => coord.x.toFloat
            case ShortType => coord.x.toShort
            case _: DayTimeIntervalType | LongType | TimestampType | TimestampNTZType =>
              coord.x.toLong
            case _ => coord.x
          }
          array(index) = InternalRow.apply(result, coord.y)
        } else {
          // Otherwise, just apply the double-precision values in 'coord.x' and 'coord.y' to the
          // output row directly. In this case: 'SELECT histogram_numeric(val, 3)
          // FROM VALUES (0L), (1L), (2L), (10L) AS tab(col)' returns an array of structs where the
          // first field has DoubleType.
          array(index) = InternalRow.apply(coord.x, coord.y)
        }
      }
      new GenericArrayData(array)
    }
  }

  override def serialize(obj: NumericHistogram): Array[Byte] = {
    NumericHistogramSerializer.serialize(obj)
  }

  override def deserialize(bytes: Array[Byte]): NumericHistogram = {
    NumericHistogramSerializer.deserialize(bytes)
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

  override def dataType: DataType = {
    // If the SQLConf.spark.sql.legacy.histogramNumericPropagateInputType is set to true,
    // the output data type of this aggregate function is an array of structs, where each struct
    // has two fields (x, y): one of the same data type as the left child and another of double
    // type. Otherwise, the 'x' field always has double type.
    ArrayType(new StructType(Array(
      StructField(name = "x",
        dataType = if (propagateInputType) left.dataType else DoubleType,
        nullable = true),
      StructField("y", DoubleType, true))), true)
  }

  override def prettyName: String = "histogram_numeric"
}

object NumericHistogramSerializer {
    private final def length(histogram: NumericHistogram): Int = {
      // histogram.nBins, histogram.nUsedBins
      Ints.BYTES + Ints.BYTES +
        //  histogram.bins, Array[Coord(x: Double, y: Double)]
        histogram.getUsedBins * (Doubles.BYTES + Doubles.BYTES)
    }

    def serialize(histogram: NumericHistogram): Array[Byte] = {
      val buffer = ByteBuffer.wrap(new Array(length(histogram)))
      buffer.putInt(histogram.getNumBins)
      buffer.putInt(histogram.getUsedBins)

      var i = 0
      while (i < histogram.getUsedBins) {
        val coord = histogram.getBin(i)
        buffer.putDouble(coord.x)
        buffer.putDouble(coord.y)
        i += 1
      }
      buffer.array()
    }

    def deserialize(bytes: Array[Byte]): NumericHistogram = {
      val buffer = ByteBuffer.wrap(bytes)
      val nBins = buffer.getInt()
      val nUsedBins = buffer.getInt()
      val histogram = new NumericHistogram()
      histogram.allocate(nBins)
      histogram.setUsedBins(nUsedBins)
      var i: Int = 0
      while (i < nUsedBins) {
        val x = buffer.getDouble()
        val y = buffer.getDouble()
        histogram.addBin(x, y, i)
        i += 1
      }
      histogram
    }
}
