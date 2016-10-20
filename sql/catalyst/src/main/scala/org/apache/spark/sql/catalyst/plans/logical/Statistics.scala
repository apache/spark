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

import org.apache.commons.codec.binary.Base64

import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.UnsafeRow
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
 * Statistics for a column.
 */
case class ColumnStat(statRow: InternalRow) {

  def forNumeric[T <: AtomicType](dataType: T): NumericColumnStat[T] = {
    NumericColumnStat(statRow, dataType)
  }
  def forString: StringColumnStat = StringColumnStat(statRow)
  def forBinary: BinaryColumnStat = BinaryColumnStat(statRow)
  def forBoolean: BooleanColumnStat = BooleanColumnStat(statRow)

  override def toString: String = {
    // use Base64 for encoding
    Base64.encodeBase64String(statRow.asInstanceOf[UnsafeRow].getBytes)
  }
}

object ColumnStat {
  def apply(numFields: Int, str: String): ColumnStat = {
    // use Base64 for decoding
    val bytes = Base64.decodeBase64(str)
    val unsafeRow = new UnsafeRow(numFields)
    unsafeRow.pointTo(bytes, bytes.length)
    ColumnStat(unsafeRow)
  }
}

case class NumericColumnStat[T <: AtomicType](statRow: InternalRow, dataType: T) {
  // The indices here must be consistent with `ColumnStatStruct.numericColumnStat`.
  val numNulls: Long = statRow.getLong(0)
  val max: T#InternalType = statRow.get(1, dataType).asInstanceOf[T#InternalType]
  val min: T#InternalType = statRow.get(2, dataType).asInstanceOf[T#InternalType]
  val ndv: Long = statRow.getLong(3)
}

case class StringColumnStat(statRow: InternalRow) {
  // The indices here must be consistent with `ColumnStatStruct.stringColumnStat`.
  val numNulls: Long = statRow.getLong(0)
  val avgColLen: Double = statRow.getDouble(1)
  val maxColLen: Long = statRow.getInt(2)
  val ndv: Long = statRow.getLong(3)
}

case class BinaryColumnStat(statRow: InternalRow) {
  // The indices here must be consistent with `ColumnStatStruct.binaryColumnStat`.
  val numNulls: Long = statRow.getLong(0)
  val avgColLen: Double = statRow.getDouble(1)
  val maxColLen: Long = statRow.getInt(2)
}

case class BooleanColumnStat(statRow: InternalRow) {
  // The indices here must be consistent with `ColumnStatStruct.booleanColumnStat`.
  val numNulls: Long = statRow.getLong(0)
  val numTrues: Long = statRow.getLong(1)
  val numFalses: Long = statRow.getLong(2)
}
