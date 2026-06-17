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

import org.apache.spark.sql.catalyst.FileSourceOptions.{IGNORE_CORRUPT_FILES, IGNORE_MISSING_FILES, IGNORED_PATH_SEGMENT_REGEX}
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
   * Whether the data source may read tar archives (.tar/.tar.gz/.tgz) by streaming their entries.
   * Gated by [[SQLConf.ARCHIVE_FORMAT_READER_ENABLED]] and resolved at construction (on the driver,
   * where SQLConf is instantiated) so the value is stable once the options are serialized to
   * executors. Only the CSV data source currently honors this.
   */
  val archiveFormatEnabled: Boolean = SQLConf.get.getConf(SQLConf.ARCHIVE_FORMAT_READER_ENABLED)

  val ignoredPathSegmentRegex: String =
    parameters.getOrElse(IGNORED_PATH_SEGMENT_REGEX, SQLConf.get.ignoredPathSegmentRegex)
  require(ignoredPathSegmentRegex.nonEmpty, "The 'ignoredPathSegmentRegex' option must be a non-empty " +
    "regular expression. Use '(?!)' to disable hidden-file filtering.")
}

object FileSourceOptions {
  val IGNORE_CORRUPT_FILES = "ignoreCorruptFiles"
  val IGNORE_MISSING_FILES = "ignoreMissingFiles"
  val IGNORED_PATH_SEGMENT_REGEX = "ignoredPathSegmentRegex"
}
