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

package org.apache.spark.sql.streaming

import scala.util.matching.Regex

import org.apache.spark.sql.AnalysisException

/**
 * Shared validation for user-assigned streaming source and sink names. Names must be non-null,
 * non-empty, and contain only alphanumeric characters and underscores.
 */
private[sql] object StreamingNameValidator {
  private val validNamePattern: Regex = "^[a-zA-Z0-9_]+$".r

  /**
   * Validates the given streaming entity name. Throws an [[IllegalArgumentException]] if the name
   * is null or empty, and invokes `onInvalid` to build the [[AnalysisException]] to throw if the
   * name does not match the allowed character set.
   *
   * @param name
   *   the source/sink name to validate
   * @param entityKind
   *   a human-readable label (e.g. "Source", "Sink") used in null/empty messages
   * @param onInvalid
   *   builds the AnalysisException to throw when `name` has invalid characters
   */
  def validate(name: String, entityKind: String)(onInvalid: String => AnalysisException): Unit = {
    require(name != null, s"$entityKind name cannot be null")
    require(name.nonEmpty, s"$entityKind name cannot be empty")
    if (!validNamePattern.pattern.matcher(name).matches()) {
      throw onInvalid(name)
    }
  }
}
