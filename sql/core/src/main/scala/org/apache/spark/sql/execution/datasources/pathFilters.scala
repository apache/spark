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

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileStatus, GlobFilter, Path, PathFilter}

import org.apache.spark.sql.{AnalysisException, SparkSession}
import org.apache.spark.sql.catalyst.util.{CaseInsensitiveMap, DateTimeUtils}
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.unsafe.types.UTF8String

/**
 * Provide modifiedAfter and modifiedBefore options when
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
@param sparkSession SparkSession
@param hadoopConf Hadoop Configuration object
@param options Map containing options
 */
abstract class ModifiedDateFilter(
        sparkSession: SparkSession,
        hadoopConf: Configuration,
        options: CaseInsensitiveMap[String])
    extends PathFilterStrategy {

  lazy val timeZoneId: String = options.getOrElse(
    DateTimeUtils.TIMEZONE_OPTION.toLowerCase(Locale.ROOT),
    SQLConf.get.sessionLocalTimeZone)

  /* Implicitly defaults to UTC if unable to parse */
  lazy val timeZone: TimeZone = DateTimeUtils.getTimeZone(timeZoneId)
  lazy val timeString: UTF8String = UTF8String.fromString(options.apply(strategy()))

  def thresholdTime(): Long = {
    DateTimeUtils
      .stringToTimestamp(timeString, timeZone.toZoneId)
      .getOrElse(
        throw new AnalysisException(
          s"The timestamp provided for the '${strategy()}'" +
            s" option is invalid.  The expected format is 'YYYY-MM-DDTHH:mm:ss'. " +
            s" Provided timestamp:  " +
            s"${options.apply(strategy())}"))
  }

  def localTime(micros: Long): Long =
    DateTimeUtils.fromUTCTime(micros, timeZoneId)

  def accept(fileStatus: FileStatus): Boolean
  def accept(path: Path): Boolean
  def strategy(): String
}

/**
 * Filter used to determine whether file was modified
 * before the provided timestamp.
 *
 @param sparkSession SparkSession
 @param hadoopConf Hadoop Configuration object
 @param options Map containing options
 */
class ModifiedBeforeFilter(
    sparkSession: SparkSession,
    hadoopConf: Configuration,
    options: CaseInsensitiveMap[String])
    extends ModifiedDateFilter(sparkSession, hadoopConf, options)
    with FileIndexFilter {
  override def accept(fileStatus: FileStatus): Boolean =
    /* We standardize on microseconds wherever possible */
    thresholdTime - localTime(
      DateTimeUtils
      /* getModificationTime returns in milliseconds */
        .millisToMicros(fileStatus.getModificationTime)) > 0

  override def accept(path: Path): Boolean = true
  override def strategy(): String = "modifiedbefore"
}
case object ModifiedBeforeFilter extends PathFilterObject {
  def get(
      sparkSession: SparkSession,
      configuration: Configuration,
      options: CaseInsensitiveMap[String]): ModifiedBeforeFilter = {
    new ModifiedBeforeFilter(sparkSession, configuration, options)
  }
  def strategy(): String = "modifiedbefore"
}

/**
 * Filter used to determine whether file was modified
 * after the provided timestamp.
 *
@param sparkSession SparkSession
@param hadoopConf Hadoop Configuration object
@param options Map containing options
 */
case class ModifiedAfterFilter(
    sparkSession: SparkSession,
    hadoopConf: Configuration,
    options: CaseInsensitiveMap[String])
    extends ModifiedDateFilter(sparkSession, hadoopConf, options)
    with FileIndexFilter {
  override def accept(fileStatus: FileStatus): Boolean =
    (localTime(
      DateTimeUtils
      /* getModificationTime returns in milliseconds */
        .millisToMicros(fileStatus.getModificationTime))
    /* We standardize on microseconds wherever possible */
      - thresholdTime > 0)

  override def accept(path: Path): Boolean = true
  override def strategy(): String = "modifiedafter"
}

case object ModifiedAfterFilter extends PathFilterObject {
  def get(
      sparkSession: SparkSession,
      configuration: Configuration,
      options: CaseInsensitiveMap[String]): ModifiedAfterFilter = {
    new ModifiedAfterFilter(sparkSession, configuration, options)
  }
  def strategy(): String = "modifiedafter"
}

class PathGlobFilter(
    sparkSession: SparkSession,
    conf: Configuration,
    options: CaseInsensitiveMap[String])
    extends GlobFilter(options.get("pathGlobFilter").get)
    with FileIndexFilter {

  override def accept(fileStatus: FileStatus): Boolean =
    accept(fileStatus.getPath)
  override def strategy(): String = "pathGlobFilter"
}
case object PathGlobFilter extends PathFilterObject {
  def get(
      sparkSession: SparkSession,
      configuration: Configuration,
      options: CaseInsensitiveMap[String]): PathGlobFilter = {
    new PathGlobFilter(sparkSession, configuration, options)
  }
  def strategy(): String = "pathglobfilter"
}

trait FileIndexFilter extends PathFilter with Serializable {
  def accept(fileStatus: FileStatus): Boolean
  def strategy(): String
}

trait PathFilterObject {
  def get(
      sparkSession: SparkSession,
      configuration: Configuration,
      options: CaseInsensitiveMap[String]): FileIndexFilter
  def strategy(): String
}

trait PathFilterStrategy extends FileIndexFilter {
  def accept(path: Path): Boolean
  def accept(fileStatus: FileStatus): Boolean
  def strategy(): String
}

case object PathFilterStrategies {
  var cache = Iterable[PathFilterObject]()

  def get(
      sparkSession: SparkSession,
      conf: Configuration,
      options: CaseInsensitiveMap[String]): Iterable[FileIndexFilter] =
    (options.keys)
      .map(option => {
        cache
          .filter(pathFilter => pathFilter.strategy() == option)
          .map(filter => filter.get(sparkSession, conf, options))
          .headOption
          .getOrElse(null)
      })
      .filter(_ != null)

  def register(filter: PathFilterObject): Unit = {
    cache = cache.++(Iterable[PathFilterObject](filter))
  }
}

object PathFilterFactory {
  PathFilterStrategies.register(ModifiedAfterFilter)
  PathFilterStrategies.register(ModifiedBeforeFilter)
  PathFilterStrategies.register(PathGlobFilter)
  def create(
      sparkSession: SparkSession,
      conf: Configuration,
      parameters: CaseInsensitiveMap[String]): Iterable[FileIndexFilter] = {
    PathFilterStrategies.get(sparkSession, conf, parameters)
  }
}
