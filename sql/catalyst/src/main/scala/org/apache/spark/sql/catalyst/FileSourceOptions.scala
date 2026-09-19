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
package org.apache.spark.sql.catalyst

import java.util.regex.{Pattern, PatternSyntaxException}

import scala.util.control.NonFatal

import org.apache.hadoop.fs.GlobPattern

import org.apache.spark.sql.catalyst.FileSourceOptions.{ARCHIVE_PATH_FILTER, IGNORE_CORRUPT_FILES, IGNORE_MISSING_FILES, IGNORED_PATH_SEGMENT_REGEX}
import org.apache.spark.sql.catalyst.util.{CaseInsensitiveMap, DateFormatter}
import org.apache.spark.sql.internal.{LegacyBehaviorPolicy, SQLConf}

/**
 * Common options for the file-based data source.
 */
class FileSourceOptions(
    @transient private val parameters: CaseInsensitiveMap[String])
  extends Serializable {

  def this(parameters: Map[String, String]) = this(CaseInsensitiveMap(parameters))

  protected def commonTimestampFormat =
    if (SQLConf.get.legacyTimeParserPolicy == LegacyBehaviorPolicy.LEGACY) {
      s"${DateFormatter.defaultPattern}'T'HH:mm:ss.SSSXXX"
    } else {
      if (SQLConf.get.supportSecondOffsetFormat) {
        s"${DateFormatter.defaultPattern}'T'HH:mm:ss[.SSS][XXXXX]"
      } else {
        s"${DateFormatter.defaultPattern}'T'HH:mm:ss[.SSS][XXX]"
      }
    }

  val ignoreCorruptFiles: Boolean = parameters.get(IGNORE_CORRUPT_FILES).map(_.toBoolean)
    .getOrElse(SQLConf.get.ignoreCorruptFiles)

  val ignoreMissingFiles: Boolean = parameters.get(IGNORE_MISSING_FILES).map(_.toBoolean)
    .getOrElse(SQLConf.get.ignoreMissingFiles)

  /**
   * Whether a read under these options fails rather than skipping missing or corrupt input files.
   */
  def hasStrictFileReads: Boolean = !ignoreCorruptFiles && !ignoreMissingFiles

  /**
   * Whether the data source may read tar archives (.tar/.tar.gz/.tgz) by streaming their entries.
   * Gated by [[SQLConf.ARCHIVE_FORMAT_READER_ENABLED]] and resolved at construction (on the driver,
   * where SQLConf is instantiated) so the value is stable once the options are serialized to
   * executors. Only the CSV data source currently honors this.
   */
  val archiveFormatEnabled: Boolean = SQLConf.get.getConf(SQLConf.ARCHIVE_FORMAT_READER_ENABLED)

  val ignoredPathSegmentRegex: String =
    parameters.getOrElse(IGNORED_PATH_SEGMENT_REGEX, SQLConf.get.ignoredPathSegmentRegex)

  /**
   * The effective [[ignoredPathSegmentRegex]] compiled and validated once here, so the ~5 listing
   * call sites can reuse a single [[Pattern]] instead of re-compiling. An empty value disables the
   * generic hidden-file filter (see [[FileSourceOptions.compileIgnoredPathSegmentRegex]]).
   */
  lazy val ignoredPathSegmentRegexPattern: Pattern =
    FileSourceOptions.compileIgnoredPathSegmentRegex(ignoredPathSegmentRegex)

  /**
   * Glob selecting which inner archive entries to read, matched against each entry's full path
   * within the archive (e.g. `subdir/*`, `*/*.csv`). An empty value disables the filter, matching
   * how an empty [[ignoredPathSegmentRegex]] is treated. Validated here so an invalid glob fails on
   * the driver.
   */
  val archivePathFilter: Option[String] = {
    val glob = parameters.get(ARCHIVE_PATH_FILTER).filter(_.nonEmpty)
    glob.foreach(FileSourceOptions.compileArchivePathFilter)
    glob
  }

  /**
   * The effective [[archivePathFilter]] is compiled once per instance of this class, so the archive
   * reads sharing one options object reuse a single matcher rather than re-compiling per archive.
   * `transient` because Hadoop's `GlobPattern` is not serializable, so an executor recompiles from
   * [[archivePathFilter]] on first use. Paths that cannot reach this value -- the schema-inference
   * RDDs, which would have to capture the matcher in a closure, and the parallel footer/schema
   * readers, whose signatures are fixed -- carry [[archivePathFilter]] instead and compile it
   * themselves.
   */
  @transient lazy val archivePathFilterPattern: Option[GlobPattern] =
    archivePathFilter.map(FileSourceOptions.compileArchivePathFilter)
}

object FileSourceOptions {
  val IGNORE_CORRUPT_FILES = "ignoreCorruptFiles"
  val IGNORE_MISSING_FILES = "ignoreMissingFiles"
  val IGNORED_PATH_SEGMENT_REGEX = "ignoredPathSegmentRegex"
  val ARCHIVE_PATH_FILTER = "archivePathFilter"

  // A regex that never matches any name, used when the filter is disabled by an empty value.
  private val DISABLED_FILTER_PATTERN = Pattern.compile("(?!)")

  /**
   * Compiles the effective `ignoredPathSegmentRegex`. An empty string disables the generic
   * hidden-file filter (logically an "empty" regex matches nothing, so the returned pattern never
   * matches). A non-empty value must be a valid Java regular expression; an invalid one is reported
   * as a clear [[IllegalArgumentException]] rather than a raw [[PatternSyntaxException]] surfacing
   * deep in file listing.
   */
  def compileIgnoredPathSegmentRegex(regex: String): Pattern = {
    if (regex.isEmpty) {
      DISABLED_FILTER_PATTERN
    } else {
      try {
        Pattern.compile(regex)
      } catch {
        case e: PatternSyntaxException =>
          throw new IllegalArgumentException(
            s"The '$IGNORED_PATH_SEGMENT_REGEX' value '$regex' is not a valid Java regular " +
            "expression. Use an empty string to disable hidden-file filtering.", e)
      }
    }
  }

  /** Compiles `archivePathFilter` into a glob matcher, reporting an invalid glob clearly. */
  def compileArchivePathFilter(glob: String): GlobPattern = {
    try {
      new GlobPattern(glob)
    } catch {
      case NonFatal(e) =>
        throw new IllegalArgumentException(
          s"The '$ARCHIVE_PATH_FILTER' value '$glob' is not a valid glob.", e)
    }
  }
}
