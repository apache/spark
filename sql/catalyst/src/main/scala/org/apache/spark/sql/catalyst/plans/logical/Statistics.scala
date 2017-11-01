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

import java.math.{MathContext, RoundingMode}

import scala.util.control.NonFatal

import org.apache.spark.internal.Logging
import org.apache.spark.sql.AnalysisException
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.util.DateTimeUtils
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
    histogram: Option[Histogram] = None) {

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
    histogram.foreach { h => map.put(ColumnStat.KEY_HISTOGRAM, h.toString)}
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

  /**
   * Creates a [[ColumnStat]] object from the given map. This is used to deserialize column stats
   * from some external storage. The serialization side is defined in [[ColumnStat.toMap]].
   */
  def fromMap(table: String, field: StructField, map: Map[String, String]): Option[ColumnStat] = {
    try {
      Some(ColumnStat(
        distinctCount = BigInt(map(KEY_DISTINCT_COUNT).toLong),
        // Note that flatMap(Option.apply) turns Option(null) into None.
        min = map.get(KEY_MIN_VALUE).map(fromString(_, field.name, field.dataType)),
        max = map.get(KEY_MAX_VALUE).map(fromString(_, field.name, field.dataType)),
        nullCount = BigInt(map(KEY_NULL_COUNT).toLong),
        avgLen = map.getOrElse(KEY_AVG_LEN, field.dataType.defaultSize.toString).toLong,
        maxLen = map.getOrElse(KEY_MAX_LEN, field.dataType.defaultSize.toString).toLong,
        histogram = map.get(KEY_HISTOGRAM).map(convertToHistogram)
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
  private def fromString(s: String, name: String, dataType: DataType): Any = {
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

  private def convertToHistogram(s: String): EquiHeightHistogram = {
    val idx = s.indexOf(",")
    if (idx <= 0) {
      throw new AnalysisException("Failed to parse histogram.")
    }
    val height = s.substring(0, idx).toDouble
    val pattern = "Bucket\\(([^,]+), ([^,]+), ([^\\)]+)\\)".r
    val buckets = pattern.findAllMatchIn(s).map { m =>
      EquiHeightBucket(m.group(1).toDouble, m.group(2).toDouble, m.group(3).toLong)
    }.toSeq
    EquiHeightHistogram(height, buckets)
  }

}

/**
 * There are a few types of histograms in state-of-the-art estimation methods. E.g. equi-width
 * histogram, equi-height histogram, frequency histogram (value-frequency pairs) and hybrid
 * histogram, etc.
 * Currently in Spark, we support equi-height histogram since it is good at handling skew
 * distribution, and also provides reasonable accuracy in other cases.
 * We can add other histograms in the future, which will make estimation logic more complicated.
 * This is because we will have to deal with computation between different types of histograms in
 * some cases, e.g. for join columns.
 */
trait Histogram

/**
 * Equi-height histogram represents column value distribution by a sequence of buckets. Each bucket
 * has a value range and contains approximately the same number of rows.
 * @param height number of rows in each bucket
 * @param ehBuckets equi-height histogram buckets
 */
case class EquiHeightHistogram(height: Double, ehBuckets: Seq[EquiHeightBucket]) extends Histogram {

  override def toString: String = {
    def bucketString(bucket: EquiHeightBucket): String = {
      val sb = new StringBuilder
      sb.append("Bucket(")
      sb.append(bucket.lo)
      sb.append(", ")
      sb.append(bucket.hi)
      sb.append(", ")
      sb.append(bucket.ndv)
      sb.append(")")
      sb.toString()
    }
    height + ", " + ehBuckets.map(bucketString).mkString(", ")
  }
}

/**
 * A bucket in an equi-height histogram. We use double type for lower/higher bound for simplicity.
 * @param lo lower bound of the value range in this bucket
 * @param hi higher bound of the value range in this bucket
 * @param ndv approximate number of distinct values in this bucket
 */
case class EquiHeightBucket(lo: Double, hi: Double, ndv: Long)
