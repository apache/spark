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

import com.google.common.primitives.{Doubles, Ints, Longs}

import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.analysis.TypeCheckResult
import org.apache.spark.sql.catalyst.analysis.TypeCheckResult.{TypeCheckFailure, TypeCheckSuccess}
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.expressions.aggregate.ApproximatePercentile.PercentileDigest
import org.apache.spark.sql.catalyst.util.{ArrayData, GenericArrayData}
import org.apache.spark.sql.catalyst.util.QuantileSummaries
import org.apache.spark.sql.catalyst.util.QuantileSummaries.{defaultCompressThreshold, Stats}
import org.apache.spark.sql.types._

/**
 * The ApproximatePercentile function returns the approximate percentile(s) of a column at the given
 * percentage(s). A percentile is a watermark value below which a given percentage of the column
 * values fall. For example, the percentile of column `col` at percentage 50% is the median of
 * column `col`.
 *
 * This function supports partial aggregation.
 *
 * @param child child expression that can produce column value with `child.eval(inputRow)`
 * @param percentageExpression Expression that represents a single percentage value or
 *                             an array of percentage values. Each percentage value must be between
 *                             0.0 and 1.0.
 * @param accuracyExpression Integer literal expression of approximation accuracy. Higher value
 *                           yields better accuracy, the default value is
 *                           DEFAULT_PERCENTILE_ACCURACY.
 */
@ExpressionDescription(
  usage = """
    _FUNC_(col, percentage [, accuracy]) - Returns the approximate percentile value of numeric
      column `col` at the given percentage. The value of percentage must be between 0.0
      and 1.0. The `accuracy` parameter (default: 10000) is a positive numeric literal which
      controls approximation accuracy at the cost of memory. Higher value of `accuracy` yields
      better accuracy, `1.0/accuracy` is the relative error of the approximation.
      When `percentage` is an array, each value of the percentage array must be between 0.0 and 1.0.
      In this case, returns the approximate percentile array of column `col` at the given
      percentage array.
  """,
  extended = """
    Examples:
      > SELECT _FUNC_(10.0, array(0.5, 0.4, 0.1), 100);
       [10.0,10.0,10.0]
      > SELECT _FUNC_(10.0, 0.5, 100);
       10.0
  """)
case class ApproximatePercentile(
    child: Expression,
    percentageExpression: Expression,
    accuracyExpression: Expression,
    override val mutableAggBufferOffset: Int,
    override val inputAggBufferOffset: Int)
  extends TypedImperativeAggregate[PercentileDigest] with ImplicitCastInputTypes {

  def this(child: Expression, percentageExpression: Expression, accuracyExpression: Expression) = {
    this(child, percentageExpression, accuracyExpression, 0, 0)
  }

  def this(child: Expression, percentageExpression: Expression) = {
    this(child, percentageExpression, Literal(ApproximatePercentile.DEFAULT_PERCENTILE_ACCURACY))
  }

  // Mark as lazy so that accuracyExpression is not evaluated during tree transformation.
  private lazy val accuracy: Int = accuracyExpression.eval().asInstanceOf[Int]

  override def inputTypes: Seq[AbstractDataType] = {
    Seq(DoubleType, TypeCollection(DoubleType, ArrayType(DoubleType)), IntegerType)
  }

  // Mark as lazy so that percentageExpression is not evaluated during tree transformation.
  private lazy val (returnPercentileArray: Boolean, percentages: Array[Double]) =
    percentageExpression.eval() match {
      // Rule ImplicitTypeCasts can cast other numeric types to double
      case num: Double => (false, Array(num))
      case arrayData: ArrayData => (true, arrayData.toDoubleArray())
    }

  override def checkInputDataTypes(): TypeCheckResult = {
    val defaultCheck = super.checkInputDataTypes()
    if (defaultCheck.isFailure) {
      defaultCheck
    } else if (!percentageExpression.foldable || !accuracyExpression.foldable) {
      TypeCheckFailure(s"The accuracy or percentage provided must be a constant literal")
    } else if (accuracy <= 0) {
      TypeCheckFailure(
        s"The accuracy provided must be a positive integer literal (current value = $accuracy)")
    } else if (percentages.exists(percentage => percentage < 0.0D || percentage > 1.0D)) {
      TypeCheckFailure(
        s"All percentage values must be between 0.0 and 1.0 " +
          s"(current = ${percentages.mkString(", ")})")
    } else {
      TypeCheckSuccess
    }
  }

  override def createAggregationBuffer(): PercentileDigest = {
    val relativeError = 1.0D / accuracy
    new PercentileDigest(relativeError)
  }

  override def update(buffer: PercentileDigest, inputRow: InternalRow): PercentileDigest = {
    val value = child.eval(inputRow)
    // Ignore empty rows, for example: percentile_approx(null)
    if (value != null) {
      buffer.add(value.asInstanceOf[Double])
    }
    buffer
  }

  override def merge(buffer: PercentileDigest, other: PercentileDigest): PercentileDigest = {
    buffer.merge(other)
    buffer
  }

  override def eval(buffer: PercentileDigest): Any = {
    val result = buffer.getPercentiles(percentages)
    if (result.length == 0) {
      null
    } else if (returnPercentileArray) {
      new GenericArrayData(result)
    } else {
      result(0)
    }
  }

  override def withNewMutableAggBufferOffset(newOffset: Int): ApproximatePercentile =
    copy(mutableAggBufferOffset = newOffset)

  override def withNewInputAggBufferOffset(newOffset: Int): ApproximatePercentile =
    copy(inputAggBufferOffset = newOffset)

  override def children: Seq[Expression] = Seq(child, percentageExpression, accuracyExpression)

  // Returns null for empty inputs
  override def nullable: Boolean = true

  override def dataType: DataType = {
    if (returnPercentileArray) ArrayType(DoubleType, false) else DoubleType
  }

  override def prettyName: String = "percentile_approx"

  override def serialize(obj: PercentileDigest): Array[Byte] = {
    ApproximatePercentile.serializer.serialize(obj)
  }

  override def deserialize(bytes: Array[Byte]): PercentileDigest = {
    ApproximatePercentile.serializer.deserialize(bytes)
  }
}

object ApproximatePercentile {

  // Default accuracy of Percentile approximation. Larger value means better accuracy.
  // The default relative error can be deduced by defaultError = 1.0 / DEFAULT_PERCENTILE_ACCURACY
  val DEFAULT_PERCENTILE_ACCURACY: Int = 10000

  /**
   * PercentileDigest is a probabilistic data structure used for approximating percentiles
   * with limited memory. PercentileDigest is backed by [[QuantileSummaries]].
   *
   * @param summaries underlying probabilistic data structure [[QuantileSummaries]].
   * @param isCompressed An internal flag from class [[QuantileSummaries]] to indicate whether the
   *                   underlying quantileSummaries is compressed.
   */
  class PercentileDigest(
      private var summaries: QuantileSummaries,
      private var isCompressed: Boolean) {

    // Trigger compression if the QuantileSummaries's buffer length exceeds
    // compressThresHoldBufferLength. The buffer length can be get by
    // quantileSummaries.sampled.length
    private[this] final val compressThresHoldBufferLength: Int = {
      // Max buffer length after compression.
      val maxBufferLengthAfterCompression: Int = (1 / summaries.relativeError).toInt * 2
      // A safe upper bound for buffer length before compression
      maxBufferLengthAfterCompression * 2
    }

    def this(relativeError: Double) = {
      this(new QuantileSummaries(defaultCompressThreshold, relativeError), isCompressed = true)
    }

    /** Returns compressed object of [[QuantileSummaries]] */
    def quantileSummaries: QuantileSummaries = {
      if (!isCompressed) compress()
      summaries
    }

    /** Insert an observation value into the PercentileDigest data structure. */
    def add(value: Double): Unit = {
      summaries = summaries.insert(value)
      // The result of QuantileSummaries.insert is un-compressed
      isCompressed = false

      // Currently, QuantileSummaries ignores the construction parameter compressThresHold,
      // which may cause QuantileSummaries to occupy unbounded memory. We have to hack around here
      // to make sure QuantileSummaries doesn't occupy infinite memory.
      // TODO: Figure out why QuantileSummaries ignores construction parameter compressThresHold
      if (summaries.sampled.length >= compressThresHoldBufferLength) compress()
    }

    /** In-place merges in another PercentileDigest. */
    def merge(other: PercentileDigest): Unit = {
      if (!isCompressed) compress()
      summaries = summaries.merge(other.quantileSummaries)
    }

    /**
     * Returns the approximate percentiles of all observation values at the given percentages.
     * A percentile is a watermark value below which a given percentage of observation values fall.
     * For example, the following code returns the 25th, median, and 75th percentiles of
     * all observation values:
     *
     * {{{
     *   val Array(p25, median, p75) = percentileDigest.getPercentiles(Array(0.25, 0.5, 0.75))
     * }}}
     */
    def getPercentiles(percentages: Array[Double]): Array[Double] = {
      if (!isCompressed) compress()
      if (summaries.count == 0 || percentages.length == 0) {
        Array.empty[Double]
      } else {
        val result = new Array[Double](percentages.length)
        var i = 0
        while (i < percentages.length) {
          // Since summaries.count != 0, the query here never return None.
          result(i) = summaries.query(percentages(i)).get
          i += 1
        }
        result
      }
    }

    private final def compress(): Unit = {
      summaries = summaries.compress()
      isCompressed = true
    }
  }

  /**
   * Serializer  for class [[PercentileDigest]]
   *
   * This class is thread safe.
   */
  class PercentileDigestSerializer {

    private final def length(summaries: QuantileSummaries): Int = {
      // summaries.compressThreshold, summary.relativeError, summary.count
      Ints.BYTES + Doubles.BYTES + Longs.BYTES +
      // length of summary.sampled
      Ints.BYTES +
      // summary.sampled, Array[Stat(value: Double, g: Int, delta: Int)]
      summaries.sampled.length * (Doubles.BYTES + Ints.BYTES + Ints.BYTES)
    }

    final def serialize(obj: PercentileDigest): Array[Byte] = {
      val summary = obj.quantileSummaries
      val buffer = ByteBuffer.wrap(new Array(length(summary)))
      buffer.putInt(summary.compressThreshold)
      buffer.putDouble(summary.relativeError)
      buffer.putLong(summary.count)
      buffer.putInt(summary.sampled.length)

      var i = 0
      while (i < summary.sampled.length) {
        val stat = summary.sampled(i)
        buffer.putDouble(stat.value)
        buffer.putInt(stat.g)
        buffer.putInt(stat.delta)
        i += 1
      }
      buffer.array()
    }

    final def deserialize(bytes: Array[Byte]): PercentileDigest = {
      val buffer = ByteBuffer.wrap(bytes)
      val compressThreshold = buffer.getInt()
      val relativeError = buffer.getDouble()
      val count = buffer.getLong()
      val sampledLength = buffer.getInt()
      val sampled = new Array[Stats](sampledLength)

      var i = 0
      while (i < sampledLength) {
        val value = buffer.getDouble()
        val g = buffer.getInt()
        val delta = buffer.getInt()
        sampled(i) = Stats(value, g, delta)
        i += 1
      }
      val summary = new QuantileSummaries(compressThreshold, relativeError, sampled, count)
      new PercentileDigest(summary, isCompressed = true)
    }
  }

  val serializer: PercentileDigestSerializer = new PercentileDigestSerializer
}
