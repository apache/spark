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

package org.apache.spark.sql.execution.datasources.pathfilters

import java.util.{Locale, TimeZone}

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileStatus, Path}

import org.apache.spark.sql.{AnalysisException, SparkSession}
import org.apache.spark.sql.catalyst.util.{CaseInsensitiveMap, DateTimeUtils}
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.unsafe.types.UTF8String

/**
 * SPARK-31962 - Provide modifiedAfter and modifiedBefore options when
 * filtering from a batch-based file data source.
 *
 * Example Usages
 * Load all CSV files modified after date:
 * spark.read.format("csv").option("modifiedAfter","2020-06-15T05:00:00").load()
 *
 * Load all CSV files modified before date:
 * spark.read.format("csv").option("modifiedBefore","2020-06-15T05:00:00").load()
 *
 * Load all CSV files modified between two dates:
 * spark.read.format("csv").option("modifiedAfter","2019-01-15T05:00:00")
 * .option("modifiedBefore","2020-06-15T05:00:00").load()
 *
 * @param sparkSession SparkSession
 * @param hadoopConf Hadoop Configuration object
 * @param options Map containing options
 */
abstract class ModifiedDateFilter(sparkSession: SparkSession,
                                  hadoopConf: Configuration,
                                  options: CaseInsensitiveMap[String])
    extends PathFilterStrategy(sparkSession, hadoopConf, options) {
  lazy val timeZoneId: String = options.getOrElse(
      DateTimeUtils.TIMEZONE_OPTION.toLowerCase(Locale.ROOT),
      SQLConf.get.sessionLocalTimeZone)

  /* Implicitly defaults to UTC if unable to parse */
  lazy val timeZone: TimeZone = DateTimeUtils.getTimeZone(timeZoneId)
  lazy val timeString: UTF8String = UTF8String.fromString(options.apply(strategy()))

  def thresholdTime(): Long = {
      DateTimeUtils
      .stringToTimestamp (timeString, timeZone.toZoneId)
      .getOrElse (throw new AnalysisException (
      s"The timestamp provided for the '${strategy ()}'" +
      s" option is invalid.  The expected format is 'YYYY-MM-DDTHH:mm:ss'. " +
      s" Provided timestamp:  " +
      s"${options.apply (strategy () )}") )
  }

  def localTime(micros: Long): Long =
    DateTimeUtils.fromUTCTime(micros, timeZoneId)

  def accept(fileStatus: FileStatus): Boolean
  def accept(path: Path): Boolean
  def strategy(): String
}
