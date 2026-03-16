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

package org.apache.spark.sql.pipelines.graph

import java.util.Locale

import scala.collection.mutable
import scala.util.control.NonFatal

/**
 * Interface for validating and accessing Pipeline-specific table properties.
 */
object PipelinesTableProperties {

  /** Prefix used for all table properties. */
  val pipelinesPrefix = "pipelines."

  /** Map of keys to property entries. */
  private val entries: mutable.HashMap[String, PipelineTableProperty[_]] = mutable.HashMap.empty

  /** Whether the table should be reset when a Reset is triggered. */
  val resetAllowed: PipelineTableProperty[Boolean] =
    buildProp("pipelines.reset.allowed", default = true)

  /**
   * Validates that all known pipeline properties are valid and can be parsed.
   * Also warn the user if they try to set an unknown pipelines property, or any
   * property that looks suspiciously similar to a known pipeline property.
   *
   * @param rawProps Raw table properties to validate and canonicalize
   * @param warnFunction Function to warn users of potential issues. Fatal errors are still thrown.
   * @return a map of table properties, with canonical-case keys for valid pipelines properties,
   *         and excluding invalid pipelines properties.
   */
  def validateAndCanonicalize(
      rawProps: Map[String, String],
      warnFunction: String => Unit): Map[String, String] = {
    rawProps.flatMap {
      case (originalCaseKey, value) =>
        val k = originalCaseKey.toLowerCase(Locale.ROOT)

        // Make sure all pipelines properties are valid
        if (k.startsWith(pipelinesPrefix)) {
          entries.get(k) match {
            case Some(prop) =>
              prop.fromMap(rawProps) // Make sure the value can be retrieved
              Option(prop.key -> value) // Canonicalize the case
            case None =>
              warnFunction(s"Unknown pipelines table property '$originalCaseKey'.")
              // exclude this property - the pipeline won't recognize it anyway,
              // and setting it would allow users to use `pipelines.xyz` for their custom
              // purposes - which we don't want to encourage.
              None
          }
        } else {
          // Make sure they weren't trying to set a pipelines property
          val similarProp = entries.get(s"${pipelinesPrefix}$k")
          similarProp.foreach { c =>
            warnFunction(
              s"You are trying to set table property '$originalCaseKey', which has a similar name" +
              s" to Pipelines table property '${c.key}'. If you are trying to set the Pipelines " +
              s"table property, please include the correct prefix."
            )
          }
          Option(originalCaseKey -> value)
        }
    }
  }

  /** Registers a pipelines table property with the specified key and default value. */
  private def buildProp[T](
      key: String,
      default: String,
      fromString: String => T): PipelineTableProperty[T] = {
    val prop = PipelineTableProperty(key, default, fromString)
    entries.put(key.toLowerCase(Locale.ROOT), prop)
    prop
  }

  private def buildProp(key: String, default: Boolean): PipelineTableProperty[Boolean] =
    buildProp(key, default.toString, _.toBoolean)
}

case class PipelineTableProperty[T](key: String, default: String, fromString: String => T) {
  def fromMap(rawProps: Map[String, String]): T = parseFromString(rawProps.getOrElse(key, default))

  private def parseFromString(value: String): T = {
    try {
      fromString(value)
    } catch {
      case NonFatal(_) =>
        throw new IllegalArgumentException(
          s"Could not parse value '$value' for table property '$key'"
        )
    }
  }
}
