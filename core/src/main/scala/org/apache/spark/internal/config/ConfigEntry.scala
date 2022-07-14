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

// ====================================================================================
//                      The guideline for naming configurations
// ====================================================================================
/*
In general, the config name should be a noun that describes its basic purpose. It's
recommended to add prefix to the config name to make the scope clearer. For example,
`spark.scheduler.mode` clearly indicates that this config is for the scheduler.

A config name can have multiple prefixes that are structured, which is similar to a
qualified Java class name. Each prefix behaves like a namespace. We should only create
a namespace if it's meaningful and can be shared by multiple configs. For example,
`buffer.inMemoryThreshold` is preferred over `buffer.in.memory.threshold`.

The followings are best practices of naming configs for some common cases:
1. When adding configs for a big feature, it's better to create an umbrella config that
   can turn the feature on/off, with a name like `featureName.enabled`. The other configs
   of this feature should be put under the `featureName` namespace. For example:
     - spark.sql.cbo.enabled
     - spark.sql.cbo.starSchemaDetection
     - spark.sql.cbo.joinReorder.enabled
     - spark.sql.cbo.joinReorder.dp.threshold
2. When adding a boolean config, the name should be a verb that describes what
   happens if this config is set to true, e.g. `spark.shuffle.sort.useRadixSort`.
3. When adding a config to specify a time duration, it's better to put the time unit
   in the config name. For example, `featureName.timeoutMs`, which clearly indicates
   that the time unit is millisecond. The config entry should be created with
   `ConfigBuilder#timeConf`, to support time strings like `2 minutes`.
*/

/**
 * An entry contains all meta information for a configuration.
 *
 * When applying variable substitution to config values, only references starting with "spark." are
 * considered in the default namespace. For known Spark configuration keys (i.e. those created using
 * `ConfigBuilder`), references will also consider the default value when it exists.
 *
 * Variable expansion is also applied to the default values of config entries that have a default
 * value declared as a string.
 *
 * @param key the key for the configuration
 * @param prependedKey the key for the configuration which will be prepended
 * @param prependSeparator the separator which is used for prepending
 * @param valueConverter how to convert a string to the value. It should throw an exception if the
 *                       string does not have the required format.
 * @param stringConverter how to convert a value to a string that the user can use it as a valid
 *                        string value. It's usually `toString`. But sometimes, a custom converter
 *                        is necessary. E.g., if T is List[String], `a, b, c` is better than
 *                        `List(a, b, c)`.
 * @param doc the documentation for the configuration
 * @param isPublic if this configuration is public to the user. If it's `false`, this
 *                 configuration is only used internally and we should not expose it to users.
 * @param version the spark version when the configuration was released.
 * @tparam T the value type
 */
private[spark] abstract class ConfigEntry[T] (
    val key: String,
    val prependedKey: Option[String],
    val prependSeparator: String,
    val alternatives: List[String],
    val valueConverter: String => T,
    val stringConverter: T => String,
    val doc: String,
    val isPublic: Boolean,
    val version: String) {

  import ConfigEntry._

  registerEntry(this)

  def defaultValueString: String

  protected def readString(reader: ConfigReader): Option[String] = {
    val values = Seq(
      prependedKey.flatMap(reader.get(_)),
      alternatives.foldLeft(reader.get(key))((res, nextKey) => res.orElse(reader.get(nextKey)))
    ).flatten
    if (values.nonEmpty) {
      Some(values.mkString(prependSeparator))
    } else {
      None
    }
  }

  def readFrom(reader: ConfigReader): T

  def defaultValue: Option[T] = None

  override def toString: String = {
    s"ConfigEntry(key=$key, defaultValue=$defaultValueString, doc=$doc, " +
      s"public=$isPublic, version=$version)"
  }
}

private class ConfigEntryWithDefault[T] (
    key: String,
    prependedKey: Option[String],
    prependSeparator: String,
    alternatives: List[String],
    _defaultValue: T,
    valueConverter: String => T,
    stringConverter: T => String,
    doc: String,
    isPublic: Boolean,
    version: String)
  extends ConfigEntry(
    key,
    prependedKey,
    prependSeparator,
    alternatives,
    valueConverter,
    stringConverter,
    doc,
    isPublic,
    version
  ) {

  override def defaultValue: Option[T] = Some(_defaultValue)

  override def defaultValueString: String = stringConverter(_defaultValue)

  def readFrom(reader: ConfigReader): T = {
    readString(reader).map(valueConverter).getOrElse(_defaultValue)
  }
}

private class ConfigEntryWithDefaultFunction[T] (
    key: String,
    prependedKey: Option[String],
    prependSeparator: String,
    alternatives: List[String],
    _defaultFunction: () => T,
    valueConverter: String => T,
    stringConverter: T => String,
    doc: String,
    isPublic: Boolean,
    version: String)
  extends ConfigEntry(
    key,
    prependedKey,
    prependSeparator,
    alternatives,
    valueConverter,
    stringConverter,
    doc,
    isPublic,
    version
  ) {

  override def defaultValue: Option[T] = Some(_defaultFunction())

  override def defaultValueString: String = stringConverter(_defaultFunction())

  def readFrom(reader: ConfigReader): T = {
    readString(reader).map(valueConverter).getOrElse(_defaultFunction())
  }
}

private class ConfigEntryWithDefaultString[T] (
    key: String,
    prependedKey: Option[String],
    prependSeparator: String,
    alternatives: List[String],
    _defaultValue: String,
    valueConverter: String => T,
    stringConverter: T => String,
    doc: String,
    isPublic: Boolean,
    version: String)
  extends ConfigEntry(
    key,
    prependedKey,
    prependSeparator,
    alternatives,
    valueConverter,
    stringConverter,
    doc,
    isPublic,
    version
  ) {

  override def defaultValue: Option[T] = Some(valueConverter(_defaultValue))

  override def defaultValueString: String = _defaultValue

  def readFrom(reader: ConfigReader): T = {
    val value = readString(reader).getOrElse(reader.substitute(_defaultValue))
    valueConverter(value)
  }
}


/**
 * A config entry that does not have a default value.
 */
private[spark] class OptionalConfigEntry[T](
    key: String,
    prependedKey: Option[String],
    prependSeparator: String,
    alternatives: List[String],
    val rawValueConverter: String => T,
    val rawStringConverter: T => String,
    doc: String,
    isPublic: Boolean,
    version: String)
  extends ConfigEntry[Option[T]](
    key,
    prependedKey,
    prependSeparator,
    alternatives,
    s => Some(rawValueConverter(s)),
    v => v.map(rawStringConverter).orNull,
    doc,
    isPublic,
    version
  ) {

  override def defaultValueString: String = ConfigEntry.UNDEFINED

  override def readFrom(reader: ConfigReader): Option[T] = {
    readString(reader).map(rawValueConverter)
  }
}

/**
 * A config entry whose default value is defined by another config entry.
 */
private[spark] class FallbackConfigEntry[T] (
    key: String,
    prependedKey: Option[String],
    prependSeparator: String,
    alternatives: List[String],
    doc: String,
    isPublic: Boolean,
    version: String,
    val fallback: ConfigEntry[T])
  extends ConfigEntry[T](
    key,
    prependedKey,
    prependSeparator,
    alternatives,
    fallback.valueConverter,
    fallback.stringConverter,
    doc,
    isPublic,
    version
  ) {

  override def defaultValueString: String = s"<value of ${fallback.key}>"

  override def readFrom(reader: ConfigReader): T = {
    readString(reader).map(valueConverter).getOrElse(fallback.readFrom(reader))
  }
}

private[spark] object ConfigEntry {

  val UNDEFINED = "<undefined>"

  private[spark] val knownConfigs =
    new java.util.concurrent.ConcurrentHashMap[String, ConfigEntry[_]]()

  def registerEntry(entry: ConfigEntry[_]): Unit = {
    val existing = knownConfigs.putIfAbsent(entry.key, entry)
    require(existing == null, s"Config entry ${entry.key} already registered!")
  }

  def findEntry(key: String): ConfigEntry[_] = knownConfigs.get(key)

}
