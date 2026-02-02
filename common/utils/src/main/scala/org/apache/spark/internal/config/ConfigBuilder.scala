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

import java.util.Locale
import java.util.concurrent.TimeUnit
import java.util.regex.PatternSyntaxException

import scala.util.matching.Regex

import org.apache.spark.SparkIllegalArgumentException
import org.apache.spark.network.util.{ByteUnit, JavaUtils}
import org.apache.spark.util.SparkStringUtils

private object ConfigHelpers {

  def toNumber[T](s: String, converter: String => T, key: String, configType: String): T = {
    try {
      converter(s.trim)
    } catch {
      case _: NumberFormatException => throw configTypeMismatchError(key, s, configType)
    }
  }

  def toBoolean(s: String, key: String): Boolean = {
    try {
      s.trim.toBoolean
    } catch {
      case _: IllegalArgumentException => throw configTypeMismatchError(key, s, "boolean")
    }
  }

  def toEnum[E <: Enumeration](s: String, enumClass: E, key: String): enumClass.Value = {
    try {
      enumClass.withName(s.trim.toUpperCase(Locale.ROOT))
    } catch {
      case _: NoSuchElementException =>
        throw configOutOfRangeOfOptionsError(key, s, enumClass.values)
    }
  }

  def toEnum[E <: Enum[E]](s: String, enumClass: Class[E], key: String): E = {
    enumClass.getEnumConstants.find(_.name().equalsIgnoreCase(s.trim)) match {
      case Some(enum) => enum
      case None => throw configOutOfRangeOfOptionsError(key, s, enumClass.getEnumConstants)
    }
  }

  def stringToSeq[T](str: String, converter: String => T): Seq[T] = {
    SparkStringUtils.stringToSeq(str).map(converter)
  }

  def seqToString[T](v: Seq[T], stringConverter: T => String): String = {
    v.map(stringConverter).mkString(",")
  }

  def timeFromString(str: String, unit: TimeUnit, key: String): Long = {
    try {
      JavaUtils.timeStringAs(str, unit)
    } catch {
      case _: NumberFormatException => throw configTypeMismatchError(key, str, s"time in $unit")
    }
  }

  def timeToString(v: Long, unit: TimeUnit): String = s"${TimeUnit.MILLISECONDS.convert(v, unit)}ms"

  def byteFromString(str: String, unit: ByteUnit, key: String): Long = {
    try {
      val (input, multiplier) =
        if (str.nonEmpty && str.charAt(0) == '-') {
          (str.substring(1), -1)
        } else {
          (str, 1)
        }
      multiplier * JavaUtils.byteStringAs(input, unit)
    } catch {
      case _: NumberFormatException | _: IllegalArgumentException =>
        throw configTypeMismatchError(key, str, s"bytes in $unit")
    }
  }

  def byteToString(v: Long, unit: ByteUnit, key: String): String = {
    try {
      s"${unit.convertTo(v, ByteUnit.BYTE)}b"
    } catch {
      case _: IllegalArgumentException =>
        throw configTypeMismatchError(key, v.toString, s"bytes in $unit")
    }
  }

  def regexFromString(str: String, key: String): Regex = {
    try str.r catch {
      case _: PatternSyntaxException => throw configTypeMismatchError(key, str, "regex")
    }
  }

  def configTypeMismatchError(
      key: String, value: String, configType: String): SparkIllegalArgumentException = {
    new SparkIllegalArgumentException(
      errorClass = "INVALID_CONF_VALUE.TYPE_MISMATCH",
      messageParameters = Map(
        "confName" -> key,
        "confValue" -> value,
        "confType" -> configType))
  }

  def configOutOfRangeOfOptionsError(
      key: String, value: String, options: Iterable[AnyRef]): SparkIllegalArgumentException = {
    new SparkIllegalArgumentException(
      errorClass = "INVALID_CONF_VALUE.OUT_OF_RANGE_OF_OPTIONS",
      messageParameters = Map(
        "confName" -> key,
        "confValue" -> value,
        "confOptions" -> options.mkString(", ")))
  }

  def configRequirementError(
      key: String, value: String, requirement: String): SparkIllegalArgumentException = {
    new SparkIllegalArgumentException(
      errorClass = "INVALID_CONF_VALUE.REQUIREMENT",
      messageParameters = Map(
        "confName" -> key,
        "confValue" -> value,
        "confRequirement" -> requirement))
  }
}

/**
 * A type-safe config builder. Provides methods for transforming the input data (which can be
 * used, e.g., for validation) and creating the final config entry.
 *
 * One of the methods that return a [[ConfigEntry]] must be called to create a config entry that
 * can be used with [[SparkConf]].
 */
private[spark] class TypedConfigBuilder[T](
  val parent: ConfigBuilder,
  val converter: String => T,
  val stringConverter: T => String) {

  import ConfigHelpers._

  def this(parent: ConfigBuilder, converter: String => T) = {
    this(parent, converter, { v: T => v.toString })
  }

  /** Apply a transformation to the user-provided values of the config entry. */
  def transform(fn: T => T): TypedConfigBuilder[T] = {
    new TypedConfigBuilder(parent, s => fn(converter(s)), stringConverter)
  }

  /** Checks if the user-provided value for the config matches the validator. */
  def checkValue(validator: T => Boolean, errorMsg: String): TypedConfigBuilder[T] = {
    transform { v =>
      if (!validator(v)) {
        throw configRequirementError(parent.key, stringConverter(v), errorMsg)
      }
      v
    }
  }

  /** Checks if the user-provided value for the config matches the validator.
   * If it doesn't match, raise Spark's exception with the given error class. */
  def checkValue(
      validator: T => Boolean,
      errorClass: String,
      parameters: T => Map[String, String]): TypedConfigBuilder[T] = {
    transform { v =>
      if (!validator(v)) {
        throw new SparkIllegalArgumentException(
          errorClass = "INVALID_CONF_VALUE." + errorClass,
          messageParameters = parameters(v) ++ Map(
            "confValue" -> v.toString,
            "confName" -> parent.key))
      }
      v
    }
  }

  /** Check that user-provided values for the config match a pre-defined set. */
  def checkValues(validValues: Set[T]): TypedConfigBuilder[T] = {
    transform { v =>
      if (!validValues.contains(v)) {
        throw configOutOfRangeOfOptionsError(
          parent.key, stringConverter(v), validValues.map(stringConverter))
      }
      v
    }
  }

  /** Turns the config entry into a sequence of values of the underlying type. */
  def toSequence: TypedConfigBuilder[Seq[T]] = {
    new TypedConfigBuilder(parent, stringToSeq(_, converter), seqToString(_, stringConverter))
  }

  /** Creates a [[ConfigEntry]] that does not have a default value. */
  def createOptional: OptionalConfigEntry[T] = {
    val entry = new OptionalConfigEntry[T](parent.key, parent._prependedKey,
      parent._prependSeparator, parent._alternatives, converter, stringConverter, parent._doc,
      parent._public, parent._version)
    parent._onCreate.foreach(_(entry))
    entry
  }

  /** Creates a [[ConfigEntry]] that has a default value. */
  def createWithDefault(default: T): ConfigEntry[T] = {
    assert(default != null, "Use createOptional.")
    // Treat "String" as a special case, so that both createWithDefault and createWithDefaultString
    // behave the same w.r.t. variable expansion of default values.
    default match {
      case str: String => createWithDefaultString(str)
      case _ =>
        val transformedDefault = converter(stringConverter(default))
        val entry = new ConfigEntryWithDefault[T](parent.key, parent._prependedKey,
          parent._prependSeparator, parent._alternatives, transformedDefault, converter,
          stringConverter, parent._doc, parent._public, parent._version)
        parent._onCreate.foreach(_ (entry))
        entry
    }
  }

  /** Creates a [[ConfigEntry]] with a function to determine the default value */
  def createWithDefaultFunction(defaultFunc: () => T): ConfigEntry[T] = {
    val entry = new ConfigEntryWithDefaultFunction[T](parent.key, parent._prependedKey,
      parent._prependSeparator, parent._alternatives, defaultFunc, converter, stringConverter,
      parent._doc, parent._public, parent._version)
    parent._onCreate.foreach(_ (entry))
    entry
  }

  /**
   * Creates a [[ConfigEntry]] that has a default value. The default value is provided as a
   * [[String]] and must be a valid value for the entry.
   */
  def createWithDefaultString(default: String): ConfigEntry[T] = {
    val entry = new ConfigEntryWithDefaultString[T](parent.key, parent._prependedKey,
      parent._prependSeparator, parent._alternatives, default, converter, stringConverter,
      parent._doc, parent._public, parent._version)
    parent._onCreate.foreach(_(entry))
    entry
  }

}

/**
 * Basic builder for Spark configurations. Provides methods for creating type-specific builders.
 *
 * @see TypedConfigBuilder
 */
private[spark] case class ConfigBuilder(key: String) {

  import ConfigHelpers._

  private[config] var _prependedKey: Option[String] = None
  private[config] var _prependSeparator: String = ""
  private[config] var _public = true
  private[config] var _doc = ""
  private[config] var _version = ""
  private[config] var _onCreate: Option[ConfigEntry[_] => Unit] = None
  private[config] var _alternatives = List.empty[String]

  def internal(): ConfigBuilder = {
    _public = false
    this
  }

  def doc(s: String): ConfigBuilder = {
    _doc = s
    this
  }

  def version(v: String): ConfigBuilder = {
    _version = v
    this
  }

  /**
   * Registers a callback for when the config entry is finally instantiated. Currently used by
   * SQLConf to keep track of SQL configuration entries.
   */
  def onCreate(callback: ConfigEntry[_] => Unit): ConfigBuilder = {
    _onCreate = Option(callback)
    this
  }

  def withPrepended(key: String, separator: String = " "): ConfigBuilder = {
    _prependedKey = Option(key)
    _prependSeparator = separator
    this
  }

  def withAlternative(key: String): ConfigBuilder = {
    _alternatives = _alternatives :+ key
    this
  }

  def intConf: TypedConfigBuilder[Int] = {
    checkPrependConfig
    new TypedConfigBuilder(this, toNumber(_, _.toInt, key, "int"))
  }

  def longConf: TypedConfigBuilder[Long] = {
    checkPrependConfig
    new TypedConfigBuilder(this, toNumber(_, _.toLong, key, "long"))
  }

  def doubleConf: TypedConfigBuilder[Double] = {
    checkPrependConfig
    new TypedConfigBuilder(this, toNumber(_, _.toDouble, key, "double"))
  }

  def booleanConf: TypedConfigBuilder[Boolean] = {
    checkPrependConfig
    new TypedConfigBuilder(this, toBoolean(_, key))
  }

  def stringConf: TypedConfigBuilder[String] = {
    new TypedConfigBuilder(this, v => v)
  }

  def enumConf(e: Enumeration): TypedConfigBuilder[e.Value] = {
    checkPrependConfig
    new TypedConfigBuilder(this, toEnum(_, e, key))
  }

  def enumConf[E <: Enum[E]](e: Class[E]): TypedConfigBuilder[E] = {
    checkPrependConfig
    new TypedConfigBuilder(this, toEnum(_, e, key))
  }

  def timeConf(unit: TimeUnit): TypedConfigBuilder[Long] = {
    checkPrependConfig
    new TypedConfigBuilder(this, timeFromString(_, unit, key), timeToString(_, unit))
  }

  def bytesConf(unit: ByteUnit): TypedConfigBuilder[Long] = {
    checkPrependConfig
    new TypedConfigBuilder(this, byteFromString(_, unit, key), byteToString(_, unit, key))
  }

  def fallbackConf[T](fallback: ConfigEntry[T]): ConfigEntry[T] = {
    val entry = new FallbackConfigEntry(key, _prependedKey, _prependSeparator, _alternatives, _doc,
      _public, _version, fallback)
    _onCreate.foreach(_(entry))
    entry
  }

  def regexConf: TypedConfigBuilder[Regex] = {
    checkPrependConfig
    new TypedConfigBuilder(this, regexFromString(_, this.key), _.toString)
  }

  private def checkPrependConfig = {
    if (_prependedKey.isDefined) {
      throw new IllegalArgumentException(s"$key type must be string if prepend used")
    }
  }
}
