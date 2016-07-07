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

package org.apache.spark.internal.config

import java.util.{Map => JMap}

import org.apache.spark.SparkConf

/**
 * An entry contains all meta information for a configuration.
 *
 * @param key the key for the configuration
 * @param defaultValue the default value for the configuration
 * @param valueConverter how to convert a string to the value. It should throw an exception if the
 *                       string does not have the required format.
 * @param stringConverter how to convert a value to a string that the user can use it as a valid
 *                        string value. It's usually `toString`. But sometimes, a custom converter
 *                        is necessary. E.g., if T is List[String], `a, b, c` is better than
 *                        `List(a, b, c)`.
 * @param doc the documentation for the configuration
 * @param isPublic if this configuration is public to the user. If it's `false`, this
 *                 configuration is only used internally and we should not expose it to users.
 * @tparam T the value type
 */
private[spark] abstract class ConfigEntry[T] (
    val key: String,
    val valueConverter: String => T,
    val stringConverter: T => String,
    val doc: String,
    val isPublic: Boolean,
    private val expandVars: Boolean) {

  import ConfigEntry._

  registerEntry(this)

  def defaultValueString: String

  def readFrom(conf: JMap[String, String], getenv: String => String): T

  def defaultValue: Option[T] = None

  override def toString: String = {
    s"ConfigEntry(key=$key, defaultValue=$defaultValueString, doc=$doc, public=$isPublic)"
  }

  protected def readAndExpand(
      conf: JMap[String, String],
      getenv: String => String,
      usedRefs: Set[String] = Set()): Option[String] = {
    Option(conf.get(key)).map { value =>
      if (expandVars) expand(value, conf, getenv, usedRefs) else value
    }
  }

}

private class ConfigEntryWithDefault[T] (
    key: String,
    _defaultValue: T,
    valueConverter: String => T,
    stringConverter: T => String,
    doc: String,
    isPublic: Boolean,
    private val expandVars: Boolean)
    extends ConfigEntry(key, valueConverter, stringConverter, doc, isPublic, expandVars) {

  override def defaultValue: Option[T] = Some(_defaultValue)

  override def defaultValueString: String = stringConverter(_defaultValue)

  def readFrom(conf: JMap[String, String], getenv: String => String): T = {
    readAndExpand(conf, getenv).map(valueConverter).getOrElse(_defaultValue)
  }

}

/**
 * A config entry that does not have a default value.
 */
private[spark] class OptionalConfigEntry[T](
    key: String,
    val rawValueConverter: String => T,
    val rawStringConverter: T => String,
    doc: String,
    isPublic: Boolean,
    private val expandVars: Boolean)
    extends ConfigEntry[Option[T]](key, s => Some(rawValueConverter(s)),
      v => v.map(rawStringConverter).orNull, doc, isPublic, expandVars) {

  override def defaultValueString: String = "<undefined>"

  override def readFrom(conf: JMap[String, String], getenv: String => String): Option[T] = {
    readAndExpand(conf, getenv).map(rawValueConverter)
  }

}

/**
 * A config entry whose default value is defined by another config entry.
 */
private class FallbackConfigEntry[T] (
    key: String,
    doc: String,
    isPublic: Boolean,
    private[config] val fallback: ConfigEntry[T],
    private val expandVars: Boolean)
    extends ConfigEntry[T](
        key,
        fallback.valueConverter,
        fallback.stringConverter,
        doc,
        isPublic,
        expandVars) {

  override def defaultValueString: String = s"<value of ${fallback.key}>"

  override def readFrom(conf: JMap[String, String], getenv: String => String): T = {
    Option(conf.get(key)).map(valueConverter).getOrElse(fallback.readFrom(conf, getenv))
  }

}

private object ConfigEntry {

  private val knownConfigs = new java.util.concurrent.ConcurrentHashMap[String, ConfigEntry[_]]()

  private val REF_RE = "\\$\\{(?:(\\w+?):)?(\\S+?)\\}".r.pattern

  def registerEntry(entry: ConfigEntry[_]): Unit = {
    val existing = knownConfigs.putIfAbsent(entry.key, entry)
    require(existing == null, s"Config entry ${entry.key} already registered!")
  }

  def findEntry(key: String): ConfigEntry[_] = knownConfigs.get(key)

  /**
   * Expand the `value` according to the rules explained in `ConfigBuilder.withVariableExpansion`.
   */
  def expand(
      value: String,
      conf: JMap[String, String],
      getenv: String => String,
      usedRefs: Set[String]): String = {
    val matcher = REF_RE.matcher(value)
    val result = new StringBuilder()
    var end = 0

    while (end < value.length() && matcher.find(end)) {
      val prefix = matcher.group(1)
      val name = matcher.group(2)

      result.append(value.substring(end, matcher.start()))

      val replacement = prefix match {
        case null =>
          require(!usedRefs.contains(name), s"Circular reference in $value: $name")
          Option(findEntry(name))
            .flatMap(_.readAndExpand(conf, getenv, usedRefs = usedRefs + name))
            .orElse(Option(conf.get(name)))
            .orElse(defaultValueString(name))
        case "system" => sys.props.get(name)
        case "env" => Option(getenv(name))
        case _ => throw new IllegalArgumentException(s"Invalid prefix: $prefix")
      }

      result.append(replacement.getOrElse(matcher.group(0)))
      end = matcher.end()
    }

    if (end < value.length()) {
      result.append(value.substring(end, value.length()))
    }
    result.toString()
  }

  private def defaultValueString(key: String): Option[String] = {
    findEntry(key) match {
      case e: ConfigEntryWithDefault[_] => Some(e.defaultValueString)
      case e: FallbackConfigEntry[_] => defaultValueString(e.fallback.key)
      case _ => None
    }
  }

}
