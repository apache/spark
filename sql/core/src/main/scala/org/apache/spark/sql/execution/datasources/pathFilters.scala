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

package org.apache.spark.sql.execution.datasources

import java.util.{Locale, TimeZone}

import org.apache.hadoop.fs.{FileStatus, GlobFilter}

import org.apache.spark.sql.catalyst.util.{CaseInsensitiveMap, DateTimeUtils}
import org.apache.spark.sql.errors.QueryCompilationErrors
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.unsafe.types.UTF8String

trait PathFilterStrategy extends Serializable {
  def accept(fileStatus: FileStatus): Boolean
}

trait StrategyBuilder {
  def create(parameters: CaseInsensitiveMap[String]): Option[PathFilterStrategy]
}

class PathGlobFilter(filePatten: String) extends PathFilterStrategy {

  private val globFilter = new GlobFilter(filePatten)

  override def accept(fileStatus: FileStatus): Boolean =
    globFilter.accept(fileStatus.getPath)
}

object PathGlobFilter extends StrategyBuilder {
  override def create(parameters: CaseInsensitiveMap[String]): Option[PathFilterStrategy] = {
    parameters.get(FileIndexOptions.PATH_GLOB_FILTER).map(new PathGlobFilter(_))
  }
}

/**
 * Provide modifiedAfter and modifiedBefore options when
 * filtering from a batch-based file data source.
 *
 * Example Usages
 * Load all CSV files modified after date:
 * {{{
 *   spark.read.format("csv").option("modifiedAfter","2020-06-15T05:00:00").load()
 * }}}
 *
 * Load all CSV files modified before date:
 * {{{
 *   spark.read.format("csv").option("modifiedBefore","2020-06-15T05:00:00").load()
 * }}}
 *
 * Load all CSV files modified between two dates:
 * {{{
 *   spark.read.format("csv").option("modifiedAfter","2019-01-15T05:00:00")
 *     .option("modifiedBefore","2020-06-15T05:00:00").load()
 * }}}
 */
abstract class ModifiedDateFilter extends PathFilterStrategy {

  def timeZoneId: String

  protected def localTime(micros: Long): Long =
    DateTimeUtils.fromUTCTime(micros, timeZoneId)
}

object ModifiedDateFilter {

  def getTimeZoneId(options: CaseInsensitiveMap[String]): String = {
    options.getOrElse(
      DateTimeUtils.TIMEZONE_OPTION.toLowerCase(Locale.ROOT),
      SQLConf.get.sessionLocalTimeZone)
  }

  def toThreshold(timeString: String, timeZoneId: String, strategy: String): Long = {
    val timeZone: TimeZone = DateTimeUtils.getTimeZone(timeZoneId)
    val ts = UTF8String.fromString(timeString)
    DateTimeUtils.stringToTimestamp(ts, timeZone.toZoneId).getOrElse {
      throw QueryCompilationErrors.invalidTimestampProvidedForStrategyError(strategy, timeString)
    }
  }
}

/**
 * Filter used to determine whether file was modified before the provided timestamp.
 */
class ModifiedBeforeFilter(thresholdTime: Long, val timeZoneId: String)
    extends ModifiedDateFilter {

  override def accept(fileStatus: FileStatus): Boolean =
    // We standardize on microseconds wherever possible
    // getModificationTime returns in milliseconds
    thresholdTime - localTime(DateTimeUtils.millisToMicros(fileStatus.getModificationTime)) > 0
}

object ModifiedBeforeFilter extends StrategyBuilder {
  import ModifiedDateFilter._

  override def create(parameters: CaseInsensitiveMap[String]): Option[PathFilterStrategy] = {
    parameters.get(FileIndexOptions.MODIFIED_BEFORE).map { value =>
      val timeZoneId = getTimeZoneId(parameters)
      val thresholdTime = toThreshold(value, timeZoneId, FileIndexOptions.MODIFIED_BEFORE)
      new ModifiedBeforeFilter(thresholdTime, timeZoneId)
    }
  }
}

/**
 * Filter used to determine whether file was modified after the provided timestamp.
 */
class ModifiedAfterFilter(thresholdTime: Long, val timeZoneId: String)
    extends ModifiedDateFilter {

  override def accept(fileStatus: FileStatus): Boolean =
    // getModificationTime returns in milliseconds
    // We standardize on microseconds wherever possible
    localTime(DateTimeUtils.millisToMicros(fileStatus.getModificationTime)) - thresholdTime > 0
}

object ModifiedAfterFilter extends StrategyBuilder {
  import ModifiedDateFilter._

  override def create(parameters: CaseInsensitiveMap[String]): Option[PathFilterStrategy] = {
    parameters.get(FileIndexOptions.MODIFIED_AFTER).map { value =>
      val timeZoneId = getTimeZoneId(parameters)
      val thresholdTime = toThreshold(value, timeZoneId, FileIndexOptions.MODIFIED_AFTER)
      new ModifiedAfterFilter(thresholdTime, timeZoneId)
    }
  }
}

object PathFilterFactory {

  private val strategies =
    Seq(PathGlobFilter, ModifiedBeforeFilter, ModifiedAfterFilter)

  def create(parameters: CaseInsensitiveMap[String]): Seq[PathFilterStrategy] = {
    strategies.flatMap { _.create(parameters) }
  }
}
