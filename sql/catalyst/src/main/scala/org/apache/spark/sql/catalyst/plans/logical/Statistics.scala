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

import org.apache.spark.sql.types.DataType

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
    colStats: Map[String, ColumnStats] = Map.empty,
    isBroadcastable: Boolean = false) {

  override def toString: String = "Statistics(" + simpleString + ")"

  /** Readable string representation for the Statistics. */
  def simpleString: String = {
    Seq(s"sizeInBytes=$sizeInBytes",
      if (rowCount.isDefined) s"rowCount=${rowCount.get}" else "",
      if (colStats.nonEmpty) s"colStats=$colStats" else "",
      s"isBroadcastable=$isBroadcastable"
    ).filter(_.nonEmpty).mkString(", ")
  }
}

/**
 * Statistics for a column.
 * @param ndv Number of distinct values of the column.
 */
case class ColumnStats(
    dataType: DataType,
    numNulls: Long,
    max: Option[Any] = None,
    min: Option[Any] = None,
    ndv: Option[Long] = None,
    avgColLen: Option[Double] = None,
    maxColLen: Option[Long] = None,
    numTrues: Option[Long] = None,
    numFalses: Option[Long] = None) {

  override def toString: String = "ColumnStats(" + simpleString + ")"

  def simpleString: String = {
    Seq(s"numNulls=$numNulls",
      if (max.isDefined) s"max=${max.get}" else "",
      if (min.isDefined) s"min=${min.get}" else "",
      if (ndv.isDefined) s"ndv=${ndv.get}" else "",
      if (avgColLen.isDefined) s"avgColLen=${avgColLen.get}" else "",
      if (maxColLen.isDefined) s"maxColLen=${maxColLen.get}" else "",
      if (numTrues.isDefined) s"numTrues=${numTrues.get}" else "",
      if (numFalses.isDefined) s"numFalses=${numFalses.get}" else ""
    ).filter(_.nonEmpty).mkString(", ")
  }
}

object ColumnStats {
  def fromString(str: String, dataType: DataType): ColumnStats = {
    val suffix = ",\\s|\\)"
    ColumnStats(
      dataType = dataType,
      numNulls = findItem(source = str, prefix = "numNulls=", suffix = suffix).map(_.toLong).get,
      max = findItem(source = str, prefix = "max=", suffix = suffix),
      min = findItem(source = str, prefix = "min=", suffix = suffix),
      ndv = findItem(source = str, prefix = "ndv=", suffix = suffix).map(_.toLong),
      avgColLen = findItem(source = str, prefix = "avgColLen=", suffix = suffix).map(_.toDouble),
      maxColLen = findItem(source = str, prefix = "maxColLen=", suffix = suffix).map(_.toLong),
      numTrues = findItem(source = str, prefix = "numTrues=", suffix = suffix).map(_.toLong),
      numFalses = findItem(source = str, prefix = "numFalses=", suffix = suffix).map(_.toLong))
  }

  private def findItem(source: String, prefix: String, suffix: String): Option[String] = {
    val pattern = s"(?<=$prefix)(.+?)(?=$suffix)".r
    pattern.findFirstIn(source)
  }
}
