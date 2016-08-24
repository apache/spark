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
import org.apache.spark.sql.execution.datasources.CaseInsensitiveMap
import org.apache.spark.util.Utils

/**
 * User specified options for file streams.
 */
class FileStreamOptions(@transient private val parameters: Map[String, String])
  extends Logging with Serializable {

  val maxFilesPerTrigger: Option[Int] = parameters.get("maxFilesPerTrigger").map { str =>
    Try(str.toInt).toOption.filter(_ > 0).getOrElse {
      throw new IllegalArgumentException(
        s"Invalid value '$str' for option 'maxFilesPerTrigger', must be a positive integer")
    }
  }

  /**
   * Maximum age of a file that can be found in this directory, before it is deleted.
   * Default to a week.
   */
  val maxFileAgeMs: Long =
    Utils.timeStringAsMs(parameters.getOrElse("maxFileAge", "7d"))

  /** Options as specified by the user, in a case-insensitive map, without "path" set. */
  val optionMapWithoutPath: Map[String, String] =
    new CaseInsensitiveMap(parameters).filterKeys(_ != "path")
}


object FileStreamOptions {

  def apply(): FileStreamOptions = new FileStreamOptions(Map.empty)

  def apply(paramName: String, paramValue: String): FileStreamOptions = {
    new FileStreamOptions(Map(paramName -> paramValue))
  }
}
