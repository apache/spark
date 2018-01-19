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

package org.apache.spark.sql.catalyst.plans.logical

import java.io.{ByteArrayInputStream, ByteArrayOutputStream, DataInputStream, DataOutputStream}
import java.math.{MathContext, RoundingMode}

import scala.util.control.NonFatal

import net.jpountz.lz4.{LZ4BlockInputStream, LZ4BlockOutputStream}

import org.apache.spark.internal.Logging
import org.apache.spark.sql.AnalysisException
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.catalog.CatalogColumnStat
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.expressions.aggregate._
import org.apache.spark.sql.catalyst.util.{ArrayData, DateTimeUtils}
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.types._
import org.apache.spark.util.Utils


/**
 * Estimates of various statistics.  The default estimation logic simply lazily multiplies the
 * corresponding statistic produced by the children.  To override this behavior, override
 * `statistics` and assign it an overridden version of `Statistics`.
 *
 * '''NOTE''': concrete and/or overridden versions of statistics fields should pay attention to the
 * performance of the implementations.  The reason is that estimations might get triggered in
 * performance-critical processes, such as query plan planning.
 *
 * Note that we are using a BigInt here since it is easy to overflow a 64-bit integer in
 * cardinality estimation (e.g. cartesian joins).
 *
 * @param sizeInBytes Physical size in bytes. For leaf operators this defaults to 1, otherwise it
 *                    defaults to the product of children's `sizeInBytes`.
 * @param rowCount Estimated number of rows.
 * @param attributeStats Statistics for Attributes.
 * @param hints Query hints.
 */
case class Statistics(
    sizeInBytes: BigInt,
    rowCount: Option[BigInt] = None,
    attributeStats: AttributeMap[ColumnStat] = AttributeMap(Nil),
    hints: HintInfo = HintInfo()) {

  override def toString: String = "Statistics(" + simpleString + ")"

  /** Readable string representation for the Statistics. */
  def simpleString: String = {
    Seq(s"sizeInBytes=${Utils.bytesToString(sizeInBytes)}",
      if (rowCount.isDefined) {
        // Show row count in scientific notation.
        s"rowCount=${BigDecimal(rowCount.get, new MathContext(3, RoundingMode.HALF_UP)).toString()}"
      } else {
        ""
      },
      s"hints=$hints"
    ).filter(_.nonEmpty).mkString(", ")
  }
}


/**
 * Statistics collected for a column.
 *
 * 1. Supported data types are defined in `ColumnStat.supportsType`.
 * 2. The JVM data type stored in min/max is the internal data type for the corresponding
 *    Catalyst data type. For example, the internal type of DateType is Int, and that the internal
 *    type of TimestampType is Long.
 * 3. There is no guarantee that the statistics collected are accurate. Approximation algorithms
 *    (sketches) might have been used, and the data collected can also be stale.
 *
 * @param distinctCount number of distinct values
 * @param min minimum value
 * @param max maximum value
 * @param nullCount number of nulls
 * @param avgLen average length of the values. For fixed-length types, this should be a constant.
 * @param maxLen maximum length of the values. For fixed-length types, this should be a constant.
 * @param histogram histogram of the values
 */
case class ColumnStat(
    distinctCount: Option[BigInt] = None,
    min: Option[Any] = None,
    max: Option[Any] = None,
    nullCount: Option[BigInt] = None,
    avgLen: Option[Long] = None,
    maxLen: Option[Long] = None,
    histogram: Option[Histogram] = None) {

  // Are distinctCount and nullCount statistics defined?
  val hasCountStats = distinctCount.isDefined && nullCount.isDefined

  // Are min and max statistics defined?
  val hasMinMaxStats = min.isDefined && max.isDefined

  // Are avgLen and maxLen statistics defined?
  val hasLenStats = avgLen.isDefined && maxLen.isDefined

  def toCatalogColumnStat(colName: String, dataType: DataType): CatalogColumnStat =
    CatalogColumnStat(
      distinctCount = distinctCount,
      min = min.map(ColumnStat.toExternalString(_, colName, dataType)),
      max = max.map(ColumnStat.toExternalString(_, colName, dataType)),
      nullCount = nullCount,
      avgLen = avgLen,
      maxLen = maxLen,
      histogram = histogram)
}


object ColumnStat extends Logging {

  /** Returns true iff the we support gathering column statistics on column of the given type. */
  def supportsType(dataType: DataType): Boolean = dataType match {
    case _: IntegralType => true
    case _: DecimalType => true
    case DoubleType | FloatType => true
    case BooleanType => true
    case DateType => true
    case TimestampType => true
    case BinaryType | StringType => true
    case _ => false
  }

  /** Returns true iff the we support gathering histogram on column of the given type. */
  def supportsHistogram(dataType: DataType): Boolean = dataType match {
    case _: IntegralType => true
    case _: DecimalType => true
    case DoubleType | FloatType => true
    case DateType => true
    case TimestampType => true
    case _ => false
  }

  /**
   * Converts from string representation of data type to the corresponding Catalyst data type.
   */
  def fromExternalString(s: String, name: String, dataType: DataType): Any = {
    dataType match {
      case BooleanType => s.toBoolean
      case DateType => DateTimeUtils.fromJavaDate(java.sql.Date.valueOf(s))
      case TimestampType => DateTimeUtils.fromJavaTimestamp(java.sql.Timestamp.valueOf(s))
      case ByteType => s.toByte
      case ShortType => s.toShort
      case IntegerType => s.toInt
      case LongType => s.toLong
      case FloatType => s.toFloat
      case DoubleType => s.toDouble
      case _: DecimalType => Decimal(s)
      // This version of Spark does not use min/max for binary/string types so we ignore it.
      case BinaryType | StringType => null
      case _ =>
        throw new AnalysisException("Column statistics deserialization is not supported for " +
          s"column $name of data type: $dataType.")
    }
  }

  /**
   * Converts the given value from Catalyst data type to string representation of external
   * data type.
   */
  def toExternalString(v: Any, colName: String, dataType: DataType): String = {
    val externalValue = dataType match {
      case DateType => DateTimeUtils.toJavaDate(v.asInstanceOf[Int])
      case TimestampType => DateTimeUtils.toJavaTimestamp(v.asInstanceOf[Long])
      case BooleanType | _: IntegralType | FloatType | DoubleType => v
      case _: DecimalType => v.asInstanceOf[Decimal].toJavaBigDecimal
      // This version of Spark does not use min/max for binary/string types so we ignore it.
      case _ =>
        throw new AnalysisException("Column statistics deserialization is not supported for " +
          s"column $colName of data type: $dataType.")
    }
    externalValue.toString
  }

  /**
   * Constructs an expression to compute column statistics for a given column.
   *
   * The expression should create a single struct column with the following schema:
   * distinctCount: Long, min: T, max: T, nullCount: Long, avgLen: Long, maxLen: Long,
   * distinctCountsForIntervals: Array[Long]
   *
   * Together with [[rowToColumnStat]], this function is used to create [[ColumnStat]] and
   * as a result should stay in sync with it.
   */
  def statExprs(
      col: Attribute,
      conf: SQLConf,
      colPercentiles: AttributeMap[ArrayData]): CreateNamedStruct = {
    def struct(exprs: Expression*): CreateNamedStruct = CreateStruct(exprs.map { expr =>
      expr.transformUp { case af: AggregateFunction => af.toAggregateExpression() }
    })
    val one = Literal(1, LongType)

    // the approximate ndv (num distinct value) should never be larger than the number of rows
    val numNonNulls = if (col.nullable) Count(col) else Count(one)
    val ndv = Least(Seq(HyperLogLogPlusPlus(col, conf.ndvMaxError), numNonNulls))
    val numNulls = Subtract(Count(one), numNonNulls)
    val defaultSize = Literal(col.dataType.defaultSize, LongType)
    val nullArray = Literal(null, ArrayType(LongType))

    def fixedLenTypeStruct: CreateNamedStruct = {
      val genHistogram =
        ColumnStat.supportsHistogram(col.dataType) && colPercentiles.contains(col)
      val intervalNdvsExpr = if (genHistogram) {
        ApproxCountDistinctForIntervals(col,
          Literal(colPercentiles(col), ArrayType(col.dataType)), conf.ndvMaxError)
      } else {
        nullArray
      }
      // For fixed width types, avg size should be the same as max size.
      struct(ndv, Cast(Min(col), col.dataType), Cast(Max(col), col.dataType), numNulls,
        defaultSize, defaultSize, intervalNdvsExpr)
    }

    col.dataType match {
      case _: IntegralType => fixedLenTypeStruct
      case _: DecimalType => fixedLenTypeStruct
      case DoubleType | FloatType => fixedLenTypeStruct
      case BooleanType => fixedLenTypeStruct
      case DateType => fixedLenTypeStruct
      case TimestampType => fixedLenTypeStruct
      case BinaryType | StringType =>
        // For string and binary type, we don't compute min, max or histogram
        val nullLit = Literal(null, col.dataType)
        struct(
          ndv, nullLit, nullLit, numNulls,
          // Set avg/max size to default size if all the values are null or there is no value.
          Coalesce(Seq(Ceil(Average(Length(col))), defaultSize)),
          Coalesce(Seq(Cast(Max(Length(col)), LongType), defaultSize)),
          nullArray)
      case _ =>
        throw new AnalysisException("Analyzing column statistics is not supported for column " +
          s"${col.name} of data type: ${col.dataType}.")
    }
  }

  /** Convert a struct for column stats (defined in `statExprs`) into [[ColumnStat]]. */
  def rowToColumnStat(
      row: InternalRow,
      attr: Attribute,
      rowCount: Long,
      percentiles: Option[ArrayData]): ColumnStat = {
    // The first 6 fields are basic column stats, the 7th is ndvs for histogram bins.
    val cs = ColumnStat(
      distinctCount = Option(BigInt(row.getLong(0))),
      // for string/binary min/max, get should return null
      min = Option(row.get(1, attr.dataType)),
      max = Option(row.get(2, attr.dataType)),
      nullCount = Option(BigInt(row.getLong(3))),
      avgLen = Option(row.getLong(4)),
      maxLen = Option(row.getLong(5))
    )
    if (row.isNullAt(6) || !cs.nullCount.isDefined) {
      cs
    } else {
      val ndvs = row.getArray(6).toLongArray()
      assert(percentiles.get.numElements() == ndvs.length + 1)
      val endpoints = percentiles.get.toArray[Any](attr.dataType).map(_.toString.toDouble)
      // Construct equi-height histogram
      val bins = ndvs.zipWithIndex.map { case (ndv, i) =>
        HistogramBin(endpoints(i), endpoints(i + 1), ndv)
      }
      val nonNullRows = rowCount - cs.nullCount.get
      val histogram = Histogram(nonNullRows.toDouble / ndvs.length, bins)
      cs.copy(histogram = Some(histogram))
    }
  }

}

/**
 * This class is an implementation of equi-height histogram.
 * Equi-height histogram represents the distribution of a column's values by a sequence of bins.
 * Each bin has a value range and contains approximately the same number of rows.
 *
 * @param height number of rows in each bin
 * @param bins equi-height histogram bins
 */
case class Histogram(height: Double, bins: Array[HistogramBin]) {

  // Only for histogram equality test.
  override def equals(other: Any): Boolean = other match {
    case otherHgm: Histogram =>
      height == otherHgm.height && bins.sameElements(otherHgm.bins)
    case _ => false
  }

  override def hashCode(): Int = {
    val temp = java.lang.Double.doubleToLongBits(height)
    var result = (temp ^ (temp >>> 32)).toInt
    result = 31 * result + java.util.Arrays.hashCode(bins.asInstanceOf[Array[AnyRef]])
    result
  }
}

/**
 * A bin in an equi-height histogram. We use double type for lower/higher bound for simplicity.
 *
 * @param lo lower bound of the value range in this bin
 * @param hi higher bound of the value range in this bin
 * @param ndv approximate number of distinct values in this bin
 */
case class HistogramBin(lo: Double, hi: Double, ndv: Long)

object HistogramSerializer {
  /**
   * Serializes a given histogram to a string. For advanced statistics like histograms, sketches,
   * etc, we don't provide readability for their serialized formats in metastore
   * (string-to-string table properties). This is because it's hard or unnatural for these
   * statistics to be human readable. For example, a histogram usually cannot fit in a single,
   * self-described property. And for count-min-sketch, it's essentially unnatural to make it
   * a readable string.
   */
  final def serialize(histogram: Histogram): String = {
    val bos = new ByteArrayOutputStream()
    val out = new DataOutputStream(new LZ4BlockOutputStream(bos))
    out.writeDouble(histogram.height)
    out.writeInt(histogram.bins.length)
    // Write data with same type together for compression.
    var i = 0
    while (i < histogram.bins.length) {
      out.writeDouble(histogram.bins(i).lo)
      i += 1
    }
    i = 0
    while (i < histogram.bins.length) {
      out.writeDouble(histogram.bins(i).hi)
      i += 1
    }
    i = 0
    while (i < histogram.bins.length) {
      out.writeLong(histogram.bins(i).ndv)
      i += 1
    }
    out.writeInt(-1)
    out.flush()
    out.close()

    org.apache.commons.codec.binary.Base64.encodeBase64String(bos.toByteArray)
  }

  /** Deserializes a given string to a histogram. */
  final def deserialize(str: String): Histogram = {
    val bytes = org.apache.commons.codec.binary.Base64.decodeBase64(str)
    val bis = new ByteArrayInputStream(bytes)
    val ins = new DataInputStream(new LZ4BlockInputStream(bis))
    val height = ins.readDouble()
    val numBins = ins.readInt()

    val los = new Array[Double](numBins)
    var i = 0
    while (i < numBins) {
      los(i) = ins.readDouble()
      i += 1
    }
    val his = new Array[Double](numBins)
    i = 0
    while (i < numBins) {
      his(i) = ins.readDouble()
      i += 1
    }
    val ndvs = new Array[Long](numBins)
    i = 0
    while (i < numBins) {
      ndvs(i) = ins.readLong()
      i += 1
    }
    ins.close()

    val bins = new Array[HistogramBin](numBins)
    i = 0
    while (i < numBins) {
      bins(i) = HistogramBin(los(i), his(i), ndvs(i))
      i += 1
    }
    Histogram(height, bins)
  }
}
