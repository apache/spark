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
 *
 * @param ndv number of distinct values
 * @param min minimum value
 * @param max maximum value
 * @param numNulls number of nulls
 * @param avgLen average length of the values. For fixed-length types, this should be a constant.
 * @param maxLen maximum length of the values. For fixed-length types, this should be a constant.
 */
// TODO: decide if we want to use bigint to represent ndv and numNulls.
case class ColumnStat(
    ndv: Long,
    min: Any,
    max: Any,
    numNulls: Long,
    avgLen: Long,
    maxLen: Long) {

  /**
   * Returns a map from string to string that can be used to serialize the column stats.
   * The key is the name of the field (e.g. "ndv" or "min"), and the value is the string
   * representation for the value. The deserialization side is defined in [[ColumnStat.fromMap]].
   *
   * As part of the protocol, the returned map always contains a key called "version".
   */
  def toMap: Map[String, String] = Map(
    "version" -> "1",
    "ndv" -> ndv.toString,
    "min" -> min.toString,
    "max" -> max.toString,
    "numNulls" -> numNulls.toString,
    "avgLen" -> avgLen.toString,
    "maxLen" -> maxLen.toString
  )
}


object ColumnStat extends Logging {

  /** Returns true iff the we support gathering column statistics on column of the given type. */
  def supportsType(dataType: DataType): Boolean = dataType match {
    case _: NumericType | TimestampType | DateType | BooleanType => true
    case StringType | BinaryType => true
    case _ => false
  }

  /**
   * Creates a [[ColumnStat]] object from the given map. This is used to deserialize column stats
   * from some external storage. The serialization side is defined in [[ColumnStat.toMap]].
   */
  def fromMap(dataType: DataType, map: Map[String, String]): Option[ColumnStat] = {
    val str2val: (String => Any) = dataType match {
      case _: IntegralType => _.toLong
      case _: DecimalType => Decimal(_)
      case DoubleType | FloatType => _.toDouble
      case BooleanType => _.toBoolean
      case _ => identity
    }

    try {
      Some(ColumnStat(
        ndv = map("ndv").toLong,
        min = str2val(map.get("min").orNull),
        max = str2val(map.get("max").orNull),
        numNulls = map("numNulls").toLong,
        avgLen = map.getOrElse("avgLen", "1").toLong,
        maxLen = map.getOrElse("maxLen", "1").toLong
      ))
    } catch {
      case NonFatal(e) =>
        logWarning("Failed to parse column statistics", e)
        None
    }
  }

  /**
   * ndv: Long, min: T, max: T, numNulls: Long, avgLen: Long, maxLen: Long
   */
  def statExprs(col: Attribute, relativeSD: Double): CreateNamedStruct = {
    def struct(exprs: Expression*): CreateNamedStruct = CreateStruct(exprs.map { expr =>
      expr.transformUp { case af: AggregateFunction => af.toAggregateExpression() }
    })
    val zero = Literal(0, LongType)
    val one = Literal(1, LongType)

    // the approximate ndv should never be larger than the number of rows
    val ndv = Least(Seq(HyperLogLogPlusPlus(col, relativeSD), Count(one)))
    val numNulls = if (col.nullable) Sum(If(IsNull(col), one, zero)) else zero

    col.dataType match {
      case _: NumericType | TimestampType | DateType | BooleanType =>
        struct(ndv, Min(col), Max(col), numNulls, one, one)

      case StringType | BinaryType =>
        val emptyStr = Literal("")
        struct(ndv, emptyStr, emptyStr, numNulls, Ceil(Average(Length(col))), Max(Length(col)))

      case _ =>
        throw new AnalysisException("Analyzing columns is not supported for column " +
            s"${col.name} of data type: ${col.dataType}.")
    }
  }

  /** Convert a struct for column stats (defined in statExprs) into [[ColumnStat]]. */
  def rowToColumnStat(row: Row): ColumnStat = {
    ColumnStat(
      ndv = row.getLong(0),
      min = row.get(1),
      max = row.get(2),
      numNulls = row.getLong(3),
      avgLen = row.getLong(4),
      maxLen = row.getLong(5)
    )
  }

}
