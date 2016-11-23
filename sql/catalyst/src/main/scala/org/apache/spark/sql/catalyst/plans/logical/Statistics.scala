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

import scala.util.control.NonFatal

import org.apache.spark.internal.Logging
import org.apache.spark.sql.{AnalysisException, Row}
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.expressions.aggregate._
import org.apache.spark.sql.types._


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
 * @param colStats Column-level statistics.
 * @param isBroadcastable If true, output is small enough to be used in a broadcast join.
 */
case class Statistics(
    sizeInBytes: BigInt,
    rowCount: Option[BigInt] = None,
    colStats: Map[String, ColumnStat] = Map.empty,
    isBroadcastable: Boolean = false) {

  override def toString: String = "Statistics(" + simpleString + ")"

  /** Readable string representation for the Statistics. */
  def simpleString: String = {
    Seq(s"sizeInBytes=$sizeInBytes",
      if (rowCount.isDefined) s"rowCount=${rowCount.get}" else "",
      s"isBroadcastable=$isBroadcastable"
    ).filter(_.nonEmpty).mkString(", ")
  }
}


/**
 * Statistics collected for a column.
 *
 * 1. Supported data types are defined in `ColumnStat.supportsType`.
 * 2. The JVM data type stored in min/max is the external data type (used in Row) for the
 * corresponding Catalyst data type. For example, for DateType we store java.sql.Date, and for
 * TimestampType we store java.sql.Timestamp.
 * 3. For integral types, they are all upcasted to longs, i.e. shorts are stored as longs.
 * 4. There is no guarantee that the statistics collected are accurate. Approximation algorithms
 *    (sketches) might have been used, and the data collected can also be stale.
 *
 * @param distinctCount number of distinct values
 * @param min minimum value
 * @param max maximum value
 * @param nullCount number of nulls
 * @param avgLen average length of the values. For fixed-length types, this should be a constant.
 * @param maxLen maximum length of the values. For fixed-length types, this should be a constant.
 */
case class ColumnStat(
    distinctCount: BigInt,
    min: Option[Any],
    max: Option[Any],
    nullCount: BigInt,
    avgLen: Long,
    maxLen: Long) {

  // We currently don't store min/max for binary/string type. This can change in the future and
  // then we need to remove this require.
  require(min.isEmpty || (!min.get.isInstanceOf[Array[Byte]] && !min.get.isInstanceOf[String]))
  require(max.isEmpty || (!max.get.isInstanceOf[Array[Byte]] && !max.get.isInstanceOf[String]))

  /**
   * Returns a map from string to string that can be used to serialize the column stats.
   * The key is the name of the field (e.g. "distinctCount" or "min"), and the value is the string
   * representation for the value. The deserialization side is defined in [[ColumnStat.fromMap]].
   *
   * As part of the protocol, the returned map always contains a key called "version".
   * In the case min/max values are null (None), they won't appear in the map.
   */
  def toMap: Map[String, String] = {
    val map = new scala.collection.mutable.HashMap[String, String]
    map.put(ColumnStat.KEY_VERSION, "1")
    map.put(ColumnStat.KEY_DISTINCT_COUNT, distinctCount.toString)
    map.put(ColumnStat.KEY_NULL_COUNT, nullCount.toString)
    map.put(ColumnStat.KEY_AVG_LEN, avgLen.toString)
    map.put(ColumnStat.KEY_MAX_LEN, maxLen.toString)
    min.foreach { v => map.put(ColumnStat.KEY_MIN_VALUE, v.toString) }
    max.foreach { v => map.put(ColumnStat.KEY_MAX_VALUE, v.toString) }
    map.toMap
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
  def fromMap(table: String, field: StructField, map: Map[String, String])
    : Option[ColumnStat] = {
    val str2val: (String => Any) = field.dataType match {
      case _: IntegralType => _.toLong
      case _: DecimalType => new java.math.BigDecimal(_)
      case DoubleType | FloatType => _.toDouble
      case BooleanType => _.toBoolean
      case DateType => java.sql.Date.valueOf
      case TimestampType => java.sql.Timestamp.valueOf
      // This version of Spark does not use min/max for binary/string types so we ignore it.
      case BinaryType | StringType => _ => null
      case _ =>
        throw new AnalysisException("Column statistics deserialization is not supported for " +
          s"column ${field.name} of data type: ${field.dataType}.")
    }

    try {
      Some(ColumnStat(
        distinctCount = BigInt(map(KEY_DISTINCT_COUNT).toLong),
        // Note that flatMap(Option.apply) turns Option(null) into None.
        min = map.get(KEY_MIN_VALUE).map(str2val).flatMap(Option.apply),
        max = map.get(KEY_MAX_VALUE).map(str2val).flatMap(Option.apply),
        nullCount = BigInt(map(KEY_NULL_COUNT).toLong),
        avgLen = map.getOrElse(KEY_AVG_LEN, field.dataType.defaultSize.toString).toLong,
        maxLen = map.getOrElse(KEY_MAX_LEN, field.dataType.defaultSize.toString).toLong
      ))
    } catch {
      case NonFatal(e) =>
        logWarning(s"Failed to parse column statistics for column ${field.name} in table $table", e)
        None
    }
  }

  /**
   * Constructs an expression to compute column statistics for a given column.
   *
   * The expression should create a single struct column with the following schema:
   * distinctCount: Long, min: T, max: T, nullCount: Long, avgLen: Long, maxLen: Long
   *
   * Together with [[rowToColumnStat]], this function is used to create [[ColumnStat]] and
   * as a result should stay in sync with it.
   */
  def statExprs(col: Attribute, relativeSD: Double): CreateNamedStruct = {
    def struct(exprs: Expression*): CreateNamedStruct = CreateStruct(exprs.map { expr =>
      expr.transformUp { case af: AggregateFunction => af.toAggregateExpression() }
    })
    val one = Literal(1, LongType)

    // the approximate ndv (num distinct value) should never be larger than the number of rows
    val numNonNulls = if (col.nullable) Count(col) else Count(one)
    val ndv = Least(Seq(HyperLogLogPlusPlus(col, relativeSD), numNonNulls))
    val numNulls = Subtract(Count(one), numNonNulls)

    def fixedLenTypeStruct(castType: DataType) = {
      // For fixed width types, avg size should be the same as max size.
      val avgSize = Literal(col.dataType.defaultSize, LongType)
      struct(ndv, Cast(Min(col), castType), Cast(Max(col), castType), numNulls, avgSize, avgSize)
    }

    col.dataType match {
      case _: IntegralType => fixedLenTypeStruct(LongType)
      case _: DecimalType => fixedLenTypeStruct(col.dataType)
      case DoubleType | FloatType => fixedLenTypeStruct(DoubleType)
      case BooleanType => fixedLenTypeStruct(col.dataType)
      case DateType => fixedLenTypeStruct(col.dataType)
      case TimestampType => fixedLenTypeStruct(col.dataType)
      case BinaryType | StringType =>
        // For string and binary type, we don't store min/max.
        val nullLit = Literal(null, col.dataType)
        struct(
          ndv, nullLit, nullLit, numNulls,
          Ceil(Average(Length(col))), Cast(Max(Length(col)), LongType))
      case _ =>
        throw new AnalysisException("Analyzing column statistics is not supported for column " +
            s"${col.name} of data type: ${col.dataType}.")
    }
  }

  /** Convert a struct for column stats (defined in statExprs) into [[ColumnStat]]. */
  def rowToColumnStat(row: Row): ColumnStat = {
    ColumnStat(
      distinctCount = BigInt(row.getLong(0)),
      min = Option(row.get(1)),  // for string/binary min/max, get should return null
      max = Option(row.get(2)),
      nullCount = BigInt(row.getLong(3)),
      avgLen = row.getLong(4),
      maxLen = row.getLong(5)
    )
  }

}
