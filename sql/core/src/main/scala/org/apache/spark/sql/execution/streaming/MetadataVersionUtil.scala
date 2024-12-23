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

package org.apache.spark.sql.execution.streaming

object MetadataVersionUtil {
  /**
   * Parse the log version from the given `text` -- will throw exception when the parsed version
   * exceeds `maxSupportedVersion`, or when `text` is malformed.
   */
  def validateVersion(text: String, maxSupportedVersion: Int): Int = {
    val version: Int = extractVersion(text)
    if (version > maxSupportedVersion) {
      throw new IllegalStateException(s"UnsupportedLogVersion: maximum supported log version " +
        s"is v${maxSupportedVersion}, but encountered v$version. The log file was produced " +
        s"by a newer version of Spark and cannot be read by this version. Please upgrade.")
    }
    version
  }

  /**
   * Parse the log version from the given `text` -- will throw exception when the parsed version
   * does not equal to `matchVersion`, or when `text` is malformed.
   */
  def validateVersionExactMatch(text: String, matchVersion: Int): Int = {
    val version: Int = extractVersion(text)
    if (version != matchVersion) {
      throw new IllegalStateException(s"UnsupportedLogVersion: the only supported log version " +
        s"is v${matchVersion}, but encountered v$version.")
    }
    version
  }

  /**
   * Parse the log version from the given `text` -- will throw exception when the parsed version
   * when `text` is malformed (such as "xyz", "v", "v-1", "v123xyz" etc.)
   */
  private def extractVersion(text: String): Int = {
    val version: Int = if (text.nonEmpty && text(0) == 'v') {
      try {
        text.substring(1, text.length).toInt
      } catch {
        case _: NumberFormatException =>
          throw new IllegalStateException(s"Log file was malformed: failed to read correct log " +
            s"version from $text.")
      }
    } else {
      throw new IllegalStateException(s"Log file was malformed: failed to read correct log " +
        s"version from $text.")
    }
    if (version < 0) {
      throw new IllegalStateException(s"Log file was malformed: failed to read correct log " +
        s"version from $text.")
    }
    version
  }
}
