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

import org.apache.spark.sql.catalyst.FileSourceOptions.{IGNORE_CORRUPT_FILES, IGNORE_MISSING_FILES}
import org.apache.spark.sql.catalyst.util.CaseInsensitiveMap
import org.apache.spark.sql.internal.SQLConf

/**
 * Common options for the file-based data source.
 */
class FileSourceOptions(
    @transient private val parameters: CaseInsensitiveMap[String])
  extends Serializable {

  def this(parameters: Map[String, String]) = this(CaseInsensitiveMap(parameters))

  val ignoreCorruptFiles: Boolean = parameters.get(IGNORE_CORRUPT_FILES).map(_.toBoolean)
    .getOrElse(SQLConf.get.ignoreCorruptFiles)

  val ignoreMissingFiles: Boolean = parameters.get(IGNORE_MISSING_FILES).map(_.toBoolean)
    .getOrElse(SQLConf.get.ignoreMissingFiles)
}

object FileSourceOptions {
  val IGNORE_CORRUPT_FILES = "ignoreCorruptFiles"
  val IGNORE_MISSING_FILES = "ignoreMissingFiles"
}

/**
 * Trait about how to register an option and retrieve all registered options
 * for a file-based data source
 */
trait FileSourceOptionsSet {
  private val validOptions = collection.mutable.Map[String, Option[String]]()

  /**
   * Register a new Option. If two options are alternative to each other, each of them needs to be
   * registered individually
   * @param name The primary option name
   * @param alternative Alternative option name if any
   */
  protected def newOption(name: String, alternative: Option[String] = None): String = {
    validOptions += (name -> alternative)
    name
  }

  /**
   * @return All valid file source options
   */
  def getAllValidOptions: scala.collection.Set[String] = validOptions.keySet

  /**
   * @param name Option name to be validated
   * @return if the given Option name is valid
   */
  def isValidOption(name: String): Boolean = validOptions.contains(name)

  /**
   * @param name Option name
   * @return Alternative option name if any
   */
  def getAlternativeOption(name: String): Option[String] = validOptions.getOrElse(name, None)
}
