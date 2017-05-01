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

package org.apache.spark.sql.hive.execution

import java.util.Locale

import org.apache.spark.sql.catalyst.util.CaseInsensitiveMap

/**
 * Options for the Hive data source. Note that rule `DetermineHiveSerde` will extract Hive
 * serde/format information from these options.
 */
class HiveOptions(@transient private val parameters: CaseInsensitiveMap[String])
  extends Serializable {
  import HiveOptions._

  def this(parameters: Map[String, String]) = this(CaseInsensitiveMap(parameters))

  val fileFormat = parameters.get(FILE_FORMAT).map(_.toLowerCase(Locale.ROOT))
  val inputFormat = parameters.get(INPUT_FORMAT)
  val outputFormat = parameters.get(OUTPUT_FORMAT)

  if (inputFormat.isDefined != outputFormat.isDefined) {
    throw new IllegalArgumentException("Cannot specify only inputFormat or outputFormat, you " +
      "have to specify both of them.")
  }

  def hasInputOutputFormat: Boolean = inputFormat.isDefined

  if (fileFormat.isDefined && inputFormat.isDefined) {
    throw new IllegalArgumentException("Cannot specify fileFormat and inputFormat/outputFormat " +
      "together for Hive data source.")
  }

  val serde = parameters.get(SERDE)

  if (fileFormat.isDefined && serde.isDefined) {
    if (!Set("sequencefile", "textfile", "rcfile").contains(fileFormat.get)) {
      throw new IllegalArgumentException(
        s"fileFormat '${fileFormat.get}' already specifies a serde.")
    }
  }

  val containsDelimiters = delimiterOptions.keys.exists(parameters.contains)

  if (containsDelimiters) {
    if (serde.isDefined) {
      throw new IllegalArgumentException("Cannot specify delimiters with a custom serde.")
    }
    if (fileFormat.isEmpty) {
      throw new IllegalArgumentException("Cannot specify delimiters without fileFormat.")
    }
    if (fileFormat.get != "textfile") {
      throw new IllegalArgumentException("Cannot specify delimiters as they are only compatible " +
        s"with fileFormat 'textfile', not ${fileFormat.get}.")
    }
  }

  for (lineDelim <- parameters.get("lineDelim") if lineDelim != "\n") {
    throw new IllegalArgumentException("Hive data source only support newline '\\n' as " +
      s"line delimiter, but given: $lineDelim.")
  }

  def serdeProperties: Map[String, String] = parameters.filterKeys {
    k => !lowerCasedOptionNames.contains(k.toLowerCase(Locale.ROOT))
  }.map { case (k, v) => delimiterOptions.getOrElse(k, k) -> v }
}

object HiveOptions {
  private val lowerCasedOptionNames = collection.mutable.Set[String]()

  private def newOption(name: String): String = {
    lowerCasedOptionNames += name.toLowerCase(Locale.ROOT)
    name
  }

  val FILE_FORMAT = newOption("fileFormat")
  val INPUT_FORMAT = newOption("inputFormat")
  val OUTPUT_FORMAT = newOption("outputFormat")
  val SERDE = newOption("serde")

  // A map from the public delimiter option keys to the underlying Hive serde property keys.
  val delimiterOptions = Map(
    "fieldDelim" -> "field.delim",
    "escapeDelim" -> "escape.delim",
    // The following typo is inherited from Hive...
    "collectionDelim" -> "colelction.delim",
    "mapkeyDelim" -> "mapkey.delim",
    "lineDelim" -> "line.delim").map { case (k, v) => k.toLowerCase(Locale.ROOT) -> v }
}
