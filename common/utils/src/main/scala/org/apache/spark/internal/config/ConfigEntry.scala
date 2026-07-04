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

import org.apache.spark.SparkException
import org.apache.spark.config.ConfigRegistry
import org.apache.spark.config.protobuf.{BindingPolicy, ConfigEntry => ProtoConfigEntry, ValueType, Visibility}
import org.apache.spark.util.SparkEnvUtils

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
 * @param bindingPolicy optional policy for SQL configs: SESSION or PERSISTED.
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
    val version: String,
    val bindingPolicy: Option[ConfigBindingPolicy.Value] = None) {

  import ConfigEntry._

  registerEntry(this)

  def defaultValueString: String

  protected def readString(reader: ConfigReader): Option[String] = {
    // SPARK-48678: performance optimization: this code could be expressed more succinctly
    // using flatten and mkString, but doing so adds lots of Scala collections perf. overhead.
    val maybePrependedValue: Option[String] = prependedKey.flatMap(reader.get)
    val maybeValue: Option[String] = alternatives
      .foldLeft(reader.get(key))((res, nextKey) => res.orElse(reader.get(nextKey)))
    (maybePrependedValue, maybeValue) match {
      case (Some(prependedValue), Some(value)) => Some(s"$prependedValue$prependSeparator$value")
      case _ => maybeValue.orElse(maybePrependedValue)
    }
  }

  def readFrom(reader: ConfigReader): T

  def defaultValue: Option[T] = None

  override def toString: String = {
    s"ConfigEntry(key=$key, defaultValue=$defaultValueString, doc=$doc, " +
      s"public=$isPublic, version=$version, bindingPolicy=$bindingPolicy)"
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
    version: String,
    bindingPolicy: Option[ConfigBindingPolicy.Value] = None)
  extends ConfigEntry(
    key,
    prependedKey,
    prependSeparator,
    alternatives,
    valueConverter,
    stringConverter,
    doc,
    isPublic,
    version,
    bindingPolicy
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
    version: String,
    bindingPolicy: Option[ConfigBindingPolicy.Value] = None)
  extends ConfigEntry(
    key,
    prependedKey,
    prependSeparator,
    alternatives,
    valueConverter,
    stringConverter,
    doc,
    isPublic,
    version,
    bindingPolicy
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
    version: String,
    bindingPolicy: Option[ConfigBindingPolicy.Value] = None)
  extends ConfigEntry(
    key,
    prependedKey,
    prependSeparator,
    alternatives,
    valueConverter,
    stringConverter,
    doc,
    isPublic,
    version,
    bindingPolicy
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
    version: String,
    bindingPolicy: Option[ConfigBindingPolicy.Value] = None)
  extends ConfigEntry[Option[T]](
    key,
    prependedKey,
    prependSeparator,
    alternatives,
    s => Some(rawValueConverter(s)),
    v => v.map(rawStringConverter).orNull,
    doc,
    isPublic,
    version,
    bindingPolicy
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
    bindingPolicy: Option[ConfigBindingPolicy.Value] = None,
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
    version,
    bindingPolicy
  ) {

  override def defaultValueString: String = s"<value of ${fallback.key}>"

  override def readFrom(reader: ConfigReader): T = {
    readString(reader).map(valueConverter).getOrElse(fallback.readFrom(reader))
  }
}

/**
 * Marker trait for config entries backed by proto definitions.
 */
private[spark] sealed trait ProtoBackedBase

/**
 * A proto-backed config entry with a default value.
 */
private[spark] class ProtoBackedConfigEntry[T](
    protoEntry: ProtoConfigEntry,
    valueConverter: String => T,
    stringConverter: T => String)
  extends ConfigEntry[T](
    key = protoEntry.getKey,
    prependedKey = None,
    prependSeparator = "",
    alternatives = Nil,
    valueConverter = valueConverter,
    stringConverter = stringConverter,
    doc = protoEntry.getDoc,
    isPublic = protoEntry.getVisibility != Visibility.VISIBILITY_INTERNAL,
    version = protoEntry.getVersion,
    bindingPolicy = ProtoBackedConfigEntry.toBindingPolicy(protoEntry)
  ) with ProtoBackedBase {

  override def defaultValueString: String =
    defaultValue.map(stringConverter).getOrElse(ConfigEntry.UNDEFINED)

  // The default is derived from immutable proto fields (and the fixed testing flag), so convert it
  // once and cache rather than re-parsing the string on every default-fallback read.
  private lazy val cachedDefaultValue: Option[T] = {
    val defaultStrOpt = if (SparkEnvUtils.isTesting && protoEntry.hasTestDefault) {
      Some(protoEntry.getTestDefault)
    } else if (protoEntry.hasDefaultValue) {
      Some(protoEntry.getDefaultValue)
    } else {
      None
    }
    defaultStrOpt.map(valueConverter)
  }

  override def defaultValue: Option[T] = cachedDefaultValue

  override def readFrom(reader: ConfigReader): T = {
    readString(reader).map(valueConverter).getOrElse(defaultValue.get)
  }

  def checkValue(validator: T => Boolean, errorMsg: String): ProtoBackedConfigEntry[T] = {
    new ProtoBackedConfigEntry[T](
      protoEntry,
      str => {
        val v = valueConverter(str)
        if (!validator(v)) {
          throw ConfigHelpers.configRequirementError(key, str, errorMsg)
        }
        v
      },
      stringConverter
    )
  }
}

/**
 * A proto-backed config entry that does not have a default value.
 */
private[spark] class ProtoBackedOptionalConfigEntry[T](
    protoEntry: ProtoConfigEntry,
    rawValueConverter: String => T,
    rawStringConverter: T => String)
  extends ConfigEntry[Option[T]](
    key = protoEntry.getKey,
    prependedKey = None,
    prependSeparator = "",
    alternatives = Nil,
    valueConverter = s => Some(rawValueConverter(s)),
    stringConverter = v => v.map(rawStringConverter).orNull,
    doc = protoEntry.getDoc,
    isPublic = protoEntry.getVisibility != Visibility.VISIBILITY_INTERNAL,
    version = protoEntry.getVersion,
    bindingPolicy = ProtoBackedConfigEntry.toBindingPolicy(protoEntry)
  ) with ProtoBackedBase {

  override def defaultValueString: String = ConfigEntry.UNDEFINED

  override def readFrom(reader: ConfigReader): Option[T] = {
    readString(reader).map(rawValueConverter)
  }
}

private[spark] object ProtoBackedConfigEntry {
  def toBindingPolicy(
      protoEntry: ProtoConfigEntry): Option[ConfigBindingPolicy.Value] = {
    protoEntry.getBindingPolicy match {
      case BindingPolicy.BINDING_POLICY_SESSION => Some(ConfigBindingPolicy.SESSION)
      case BindingPolicy.BINDING_POLICY_PERSISTED => Some(ConfigBindingPolicy.PERSISTED)
      case BindingPolicy.BINDING_POLICY_NOT_APPLICABLE => Some(ConfigBindingPolicy.NOT_APPLICABLE)
      case _ => None
    }
  }
}

private[spark] object ConfigEntry {

  val UNDEFINED = "<undefined>"

  private[spark] val knownConfigs =
    new java.util.concurrent.ConcurrentHashMap[String, ConfigEntry[_]]()

  // Register all proto-backed configs at object initialization.
  // These can be overwritten later by modules that need to add validation.
  ConfigRegistry.allConfigs().forEach { protoEntry =>
    createProtoBackedConfigEntry(protoEntry)
  }

  def registerEntry(entry: ConfigEntry[_]): Unit = {
    val existing = knownConfigs.putIfAbsent(entry.key, entry)
    if (existing != null) {
      // A key registered twice is normally a bug (typo or accidental duplicate), so we fail loudly.
      // The one exception is the enhancement pattern: a proto-backed config is registered eagerly
      // at object init, then a Scala entry built via `buildConfFromConfigFile` (e.g. to add
      // `checkValue`) may intentionally replace it. We allow the overwrite only when the existing
      // entry is proto-backed; this deliberately trades the duplicate-detection net for that key,
      // so the Scala side must still reference a key that is genuinely defined in a .textproto
      // file.
      require(existing.isInstanceOf[ProtoBackedBase],
        s"Config entry ${entry.key} already registered!")
      knownConfigs.put(entry.key, entry)
    }
  }

  def findEntry(key: String): ConfigEntry[_] = knownConfigs.get(key)

  def listAllEntries(): java.util.Collection[ConfigEntry[_]] = knownConfigs.values()

  def findProtoBackedEntry(key: String): ProtoBackedConfigEntry[_] = {
    knownConfigs.get(key) match {
      case entry: ProtoBackedConfigEntry[_] => entry
      case _ => null
    }
  }

  def findProtoDefinedEntry(key: String): ConfigEntry[_] = {
    if (ConfigRegistry.containsConfig(key)) knownConfigs.get(key) else null
  }

  def listAllProtoDefinedConfigs(): java.util.Collection[ConfigEntry[_]] = {
    new java.util.AbstractCollection[ConfigEntry[_]]() {
      override def iterator(): java.util.Iterator[ConfigEntry[_]] = {
        val keys = ConfigRegistry.allKeys().iterator()
        new java.util.Iterator[ConfigEntry[_]]() {
          override def hasNext: Boolean = keys.hasNext
          override def next(): ConfigEntry[_] = knownConfigs.get(keys.next())
        }
      }

      override def size(): Int = ConfigRegistry.allKeys().size()
    }
  }

  private def createProtoBackedConfigEntry(
      protoEntry: ProtoConfigEntry): ConfigEntry[_] = {
    // `test_default` only overrides `default_value` in test environments; a config that sets
    // `test_default` without `default_value` would be a has-default entry under test but an
    // optional entry in production, diverging the entry's shape (and `buildConfFromConfigFile`'s
    // success/failure) between the two. Reject it here so the misconfiguration fails consistently.
    require(!protoEntry.hasTestDefault || protoEntry.hasDefaultValue,
      s"Config entry ${protoEntry.getKey} sets test_default without default_value; " +
        "a config with a test default must also declare a default value.")
    // With the invariant above, `test_default` implies `default_value`, so having a default is
    // equivalent to having a `default_value` in every environment.
    val hasDefault = protoEntry.hasDefaultValue
    protoEntry.getValueType match {
      case ValueType.VALUE_TYPE_BOOL =>
        createTypedEntry[Boolean](protoEntry, hasDefault, _.toBoolean, _.toString)
      case ValueType.VALUE_TYPE_INT =>
        createTypedEntry[Int](protoEntry, hasDefault, _.toInt, _.toString)
      case ValueType.VALUE_TYPE_LONG =>
        createTypedEntry[Long](protoEntry, hasDefault, _.toLong, _.toString)
      case ValueType.VALUE_TYPE_DOUBLE =>
        createTypedEntry[Double](protoEntry, hasDefault, _.toDouble, _.toString)
      case ValueType.VALUE_TYPE_STRING =>
        createTypedEntry[String](protoEntry, hasDefault, identity[String], identity[String])
      case other =>
        throw SparkException.internalError(s"Unsupported value type: $other")
    }
  }

  private def createTypedEntry[T](
      protoEntry: ProtoConfigEntry,
      hasDefault: Boolean,
      valueConverter: String => T,
      stringConverter: T => String): ConfigEntry[_] = {
    if (hasDefault) {
      new ProtoBackedConfigEntry[T](protoEntry, valueConverter, stringConverter)
    } else {
      new ProtoBackedOptionalConfigEntry[T](protoEntry, valueConverter, stringConverter)
    }
  }
}
