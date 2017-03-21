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

package org.apache.spark.sql.catalyst.util

import org.apache.spark.internal.Logging

object ParseMode extends Enumeration with Logging {
  type ParseMode = Value

  /**
   * This mode permissively parses the records.
   */
  val Permissive = Value("PERMISSIVE")

  /**
   * This mode ignores the whole corrupted records.
   */
  val DropMalformed = Value("DROPMALFORMED")

  /**
   * This mode throws an exception when it meets corrupted records.
   */
  val FailFast = Value("FAILFAST")

  /**
   * Returns `ParseMode` enum from the given string.
   */
  def fromString(mode: String): ParseMode = mode.toUpperCase match {
    case "PERMISSIVE" => ParseMode.Permissive
    case "DROPMALFORMED" => ParseMode.DropMalformed
    case "FAILFAST" => ParseMode.FailFast
    case _ =>
      logWarning(s"$mode is not a valid parse mode. Using $Permissive.")
      ParseMode.Permissive
  }
}
