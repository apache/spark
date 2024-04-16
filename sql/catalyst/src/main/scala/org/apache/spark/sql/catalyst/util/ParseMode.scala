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

import java.util.Locale

import org.apache.spark.internal.{Logging, MDC}
import org.apache.spark.internal.LogKey.PARSE_MODE

sealed trait ParseMode {
  /**
   * String name of the parse mode.
   */
  def name: String
}

/**
 * This mode permissively parses the records.
 */
case object PermissiveMode extends ParseMode { val name = "PERMISSIVE" }

/**
 * This mode ignores the whole corrupted records.
 */
case object DropMalformedMode extends ParseMode { val name = "DROPMALFORMED" }

/**
 * This mode throws an exception when it meets corrupted records.
 */
case object FailFastMode extends ParseMode { val name = "FAILFAST" }

object ParseMode extends Logging {
  /**
   * Returns the parse mode from the given string.
   */
  def fromString(mode: String): ParseMode = Option(mode).map {
    v => v.toUpperCase(Locale.ROOT) match {
      case PermissiveMode.name => PermissiveMode
      case DropMalformedMode.name => DropMalformedMode
      case FailFastMode.name => FailFastMode
      case _ =>
        logWarning(log"${MDC(PARSE_MODE, v)} is not a valid parse mode. " +
          log"Using ${MDC(PARSE_MODE, PermissiveMode.name)}.")
        PermissiveMode
    }
  }.getOrElse {
    logWarning(log"mode is null and not a valid parse mode. " +
      log"Using ${MDC(PARSE_MODE, PermissiveMode.name)}.")
    PermissiveMode
  }
}
