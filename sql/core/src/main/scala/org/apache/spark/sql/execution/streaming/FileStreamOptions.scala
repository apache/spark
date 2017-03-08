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

import scala.util.Try

import org.apache.spark.internal.Logging
import org.apache.spark.sql.catalyst.util.CaseInsensitiveMap
import org.apache.spark.util.Utils

/**
 * User specified options for file streams.
 */
class FileStreamOptions(parameters: CaseInsensitiveMap) extends Logging {

  def this(parameters: Map[String, String]) = this(new CaseInsensitiveMap(parameters))

  val maxFilesPerTrigger: Option[Int] = parameters.get("maxFilesPerTrigger").map { str =>
    Try(str.toInt).toOption.filter(_ > 0).getOrElse {
      throw new IllegalArgumentException(
        s"Invalid value '$str' for option 'maxFilesPerTrigger', must be a positive integer")
    }
  }

  /**
   * Maximum age of a file that can be found in this directory, before it is ignored. For the
   * first batch all files will be considered valid. If `latestFirst` is set to `true` and
   * `maxFilesPerTrigger` is set, then this parameter will be ignored, because old files that are
   * valid, and should be processed, may be ignored. Please refer to SPARK-19813 for details.
   *
   * The max age is specified with respect to the timestamp of the latest file, and not the
   * timestamp of the current system. That this means if the last file has timestamp 1000, and the
   * current system time is 2000, and max age is 200, the system will purge files older than
   * 800 (rather than 1800) from the internal state.
   *
   * Default to a week.
   */
  val maxFileAgeMs: Long =
    Utils.timeStringAsMs(parameters.getOrElse("maxFileAge", "7d"))

  /** Options as specified by the user, in a case-insensitive map, without "path" set. */
  val optionMapWithoutPath: Map[String, String] =
    parameters.filterKeys(_ != "path")

  /**
   * Whether to scan latest files first. If it's true, when the source finds unprocessed files in a
   * trigger, it will first process the latest files.
   */
  val latestFirst: Boolean = parameters.get("latestFirst").map { str =>
    try {
      str.toBoolean
    } catch {
      case _: IllegalArgumentException =>
        throw new IllegalArgumentException(
          s"Invalid value '$str' for option 'latestFirst', must be 'true' or 'false'")
    }
  }.getOrElse(false)
}
