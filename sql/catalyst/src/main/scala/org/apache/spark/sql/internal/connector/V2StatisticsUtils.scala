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

package org.apache.spark.sql.internal.connector

import java.util.OptionalLong

import org.apache.spark.sql.connector.read.{Scan, Statistics, SupportsReportStatistics}

object V2StatisticsUtils {

  def isNotEmpty(stats: Statistics): Boolean = {
    stats != null && hasAnyValue(stats)
  }

  private def hasAnyValue(stats: Statistics): Boolean = {
    stats.sizeInBytes().isPresent ||
      stats.numRows().isPresent ||
      (stats.columnStats() != null && !stats.columnStats().isEmpty)
  }

  def computeStats(scan: Scan): Option[Statistics] = scan match {
    case s: SupportsReportStatistics => Some(s.estimateStatistics()).filter(isNotEmpty)
    case _ => None
  }

  def computeSizeInBytes(
      scan: Scan,
      avgRowSize: => BigInt): Option[BigInt] = {
    extractSizeInBytes(scan).orElse(extractRowCount(scan).map(_ * avgRowSize))
  }

  private def extractSizeInBytes(scan: Scan): Option[BigInt] = scan match {
    case s: SupportsReportStatistics => toBigInt(s.estimateSizeInBytes())
    case _ => None
  }

  private def extractRowCount(scan: Scan): Option[BigInt] = scan match {
    case s: SupportsReportStatistics =>
      Option(s.estimateStatistics()).flatMap(stats => toBigInt(stats.numRows()))
    case _ =>
      None
  }

  private def toBigInt(value: OptionalLong): Option[BigInt] = {
    if (value.isPresent) Some(BigInt(value.getAsLong)) else None
  }
}
