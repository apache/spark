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
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.expressions.aggregate._
import org.apache.spark.sql.catalyst.util.DateTimeUtils
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
    distinctCount: BigInt,
    min: Option[Any],
    max: Option[Any],
    nullCount: BigInt,
    avgLen: Long,
    maxLen: Long,
    histogram: Option[EquiHeightHistogram] = None) {

  // We currently don't store min/max for binary/string type. This can change in the future and
  // then we need to remove this require.
  require(min.isEmpty || (!min.get.isInstanceOf[Array[Byte]] && !min.get.isInstanceOf[String]))
  require(max.isEmpty || (!max.get.isInstanceOf[Array[Byte]] && !max.get.isInstanceOf[String]))

  /**
   * Returns a map from string to string that can be used to serialize the column stats.
   * The key is the name of the field (e.g. "distinctCount" or "min"), and the value is the string
   * representation for the value. min/max values are converted to the external data type. For
   * example, for DateType we store java.sql.Date, and for TimestampType we store
   * java.sql.Timestamp. The deserialization side is defined in [[ColumnStat.fromMap]].
   *
   * As part of the protocol, the returned map always contains a key called "version".
   * In the case min/max values are null (None), they won't appear in the map.
   */
  def toMap(colName: String, dataType: DataType): Map[String, String] = {
    val map = new scala.collection.mutable.HashMap[String, String]
    map.put(ColumnStat.KEY_VERSION, "1")
    map.put(ColumnStat.KEY_DISTINCT_COUNT, distinctCount.toString)
    map.put(ColumnStat.KEY_NULL_COUNT, nullCount.toString)
    map.put(ColumnStat.KEY_AVG_LEN, avgLen.toString)
    map.put(ColumnStat.KEY_MAX_LEN, maxLen.toString)
    min.foreach { v => map.put(ColumnStat.KEY_MIN_VALUE, toExternalString(v, colName, dataType)) }
    max.foreach { v => map.put(ColumnStat.KEY_MAX_VALUE, toExternalString(v, colName, dataType)) }
    histogram.foreach { h => map.put(ColumnStat.KEY_HISTOGRAM, HistogramSerializer.serialize(h)) }
    map.toMap
  }

  /**
   * Converts the given value from Catalyst data type to string representation of external
   * data type.
   */
  private def toExternalString(v: Any, colName: String, dataType: DataType): String = {
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

}


object ColumnStat extends Logging {

  // List of string keys used to serialize ColumnStat
  val KEY_VERSION = "version"
  private val KEY_DISTINCT_COUNT = "distinctCount"
  private val KEY_MIN_VALUE = "min"
  private val KEY_MAX_VALUE = "max"
  private val KEY_NULL_COUNT = "nullCount"
  private val KEY_AVG_LEN = "avgLen"
  private val KEY_MAX_LEN = "maxLen"
  private val KEY_HISTOGRAM = "histogram"

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
   * Creates a [[ColumnStat]] object from the given map. This is used to deserialize column stats
   * from some external storage. The serialization side is defined in [[ColumnStat.toMap]].
   */
  def fromMap(table: String, field: StructField, map: Map[String, String]): Option[ColumnStat] = {
    try {
      Some(ColumnStat(
        distinctCount = BigInt(map(KEY_DISTINCT_COUNT).toLong),
        // Note that flatMap(Option.apply) turns Option(null) into None.
        min = map.get(KEY_MIN_VALUE)
          .map(fromExternalString(_, field.name, field.dataType)).flatMap(Option.apply),
        max = map.get(KEY_MAX_VALUE)
          .map(fromExternalString(_, field.name, field.dataType)).flatMap(Option.apply),
        nullCount = BigInt(map(KEY_NULL_COUNT).toLong),
        avgLen = map.getOrElse(KEY_AVG_LEN, field.dataType.defaultSize.toString).toLong,
        maxLen = map.getOrElse(KEY_MAX_LEN, field.dataType.defaultSize.toString).toLong,
        histogram = map.get(KEY_HISTOGRAM).map(HistogramSerializer.deserialize)
      ))
    } catch {
      case NonFatal(e) =>
        logWarning(s"Failed to parse column statistics for column ${field.name} in table $table", e)
        None
    }
  }

  /**
   * Converts from string representation of external data type to the corresponding Catalyst data
   * type.
   */
  private def fromExternalString(s: String, name: String, dataType: DataType): Any = {
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
      colPercentiles: AttributeMap[Array[Any]]): CreateNamedStruct = {
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

    def fixedLenTypeExprs(castType: DataType) = {
      // For fixed width types, avg size should be the same as max size.
      Seq(ndv, Cast(Min(col), castType), Cast(Max(col), castType), numNulls, defaultSize,
        defaultSize)
    }

    def fixedLenTypeStruct(dataType: DataType) = {
      val genHistogram =
        ColumnStat.supportsHistogram(dataType) && colPercentiles.contains(col)
      val intervalNdvsExpr = if (genHistogram) {
        ApproxCountDistinctForIntervals(col,
          CreateArray(colPercentiles(col).map(Literal(_))), conf.ndvMaxError)
      } else {
        nullArray
      }
      struct(fixedLenTypeExprs(dataType) :+ intervalNdvsExpr: _*)
    }

    col.dataType match {
      case dt: IntegralType => fixedLenTypeStruct(dt)
      case _: DecimalType => fixedLenTypeStruct(col.dataType)
      case dt @ (DoubleType | FloatType) => fixedLenTypeStruct(dt)
      case BooleanType => fixedLenTypeStruct(col.dataType)
      case DateType => fixedLenTypeStruct(col.dataType)
      case TimestampType => fixedLenTypeStruct(col.dataType)
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
      percentiles: Option[Array[Any]]): ColumnStat = {
    // The first 6 fields are basic column stats, the 7th is ndvs for histogram buckets.
    val cs = ColumnStat(
      distinctCount = BigInt(row.getLong(0)),
      // for string/binary min/max, get should return null
      min = Option(row.get(1, attr.dataType)),
      max = Option(row.get(2, attr.dataType)),
      nullCount = BigInt(row.getLong(3)),
      avgLen = row.getLong(4),
      maxLen = row.getLong(5)
    )
    if (row.isNullAt(6)) {
      cs
    } else {
      val ndvs = row.getArray(6).toLongArray()
      assert(percentiles.get.length == ndvs.length + 1)
      val endpoints = percentiles.get.map(_.toString.toDouble)
      // Construct equi-height histogram
      val buckets = ndvs.zipWithIndex.map { case (ndv, i) =>
        EquiHeightBucket(endpoints(i), endpoints(i + 1), ndv)
      }
      val nonNullRows = rowCount - cs.nullCount
      val ehHistogram = EquiHeightHistogram(nonNullRows.toDouble / ndvs.length, buckets)
      cs.copy(histogram = Some(ehHistogram))
    }
  }

}

/**
 * Equi-height histogram represents the distribution of a column's values by a sequence of buckets.
 * Each bucket has a value range and contains approximately the same number of rows.
 * In the context of Spark SQL statistics, we may use "histogram" to denote "equi-height histogram"
 * for simplicity.
 * @param height number of rows in each bucket
 * @param buckets equi-height histogram buckets
 */
case class EquiHeightHistogram(height: Double, buckets: Array[EquiHeightBucket]) {

  // Only for histogram equality test.
  override def equals(other: Any): Boolean = other match {
    case otherEHH: EquiHeightHistogram =>
      height == otherEHH.height && buckets.sameElements(otherEHH.buckets)
    case _ => false
  }

  override def hashCode(): Int = super.hashCode()
}

/**
 * A bucket in an equi-height histogram. We use double type for lower/higher bound for simplicity.
 * @param lo lower bound of the value range in this bucket
 * @param hi higher bound of the value range in this bucket
 * @param ndv approximate number of distinct values in this bucket
 */
case class EquiHeightBucket(lo: Double, hi: Double, ndv: Long)

object HistogramSerializer {
  /**
   * Serializes a given histogram to a string. For advanced statistics like histograms, sketches,
   * etc, we don't provide readability for their serialized formats in metastore
   * (string-to-string table properties). This is because it's hard or unnatural for these
   * statistics to be human readable. For example, a histogram is probably split into multiple
   * key-value properties, instead of a single, self-described property. And for
   * count-min-sketch, it's essentially unnatural to make it a readable string.
   */
  final def serialize(histogram: EquiHeightHistogram): String = {
    val bos = new ByteArrayOutputStream()
    val out = new DataOutputStream(new LZ4BlockOutputStream(bos))
    out.writeDouble(histogram.height)
    out.writeInt(histogram.buckets.length)
    var i = 0
    while (i < histogram.buckets.length) {
      val bucket = histogram.buckets(i)
      out.writeDouble(bucket.lo)
      out.writeDouble(bucket.hi)
      out.writeLong(bucket.ndv)
      i += 1
    }
    out.writeInt(-1)
    out.flush()
    out.close()

    org.apache.commons.codec.binary.Base64.encodeBase64String(bos.toByteArray)
  }

  /** Deserializes a given string to a histogram. */
  final def deserialize(str: String): EquiHeightHistogram = {
    val bytes = org.apache.commons.codec.binary.Base64.decodeBase64(str)
    val bis = new ByteArrayInputStream(bytes)
    val ins = new DataInputStream(new LZ4BlockInputStream(bis))
    val height = ins.readDouble()
    val numBuckets = ins.readInt()
    val buckets = new Array[EquiHeightBucket](numBuckets)
    var i = 0
    while (i < numBuckets) {
      val lo = ins.readDouble()
      val hi = ins.readDouble()
      val ndv = ins.readLong()
      buckets(i) = EquiHeightBucket(lo, hi, ndv)
      i += 1
    }
    ins.close()
    EquiHeightHistogram(height, buckets)
  }
}
