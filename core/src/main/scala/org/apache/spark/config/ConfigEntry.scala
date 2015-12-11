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

package org.apache.spark.config

import java.util.concurrent.TimeUnit

import org.apache.spark.SparkConf
import org.apache.spark.network.util.{ByteUnit, JavaUtils}

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
 * @param doc the document for the configuration
 * @param isPublic if this configuration is public to the user. If it's `false`, this
 *                 configuration is only used internally and we should not expose it to the user.
 * @tparam T the value type
 */
private[spark] class ConfigEntry[T] (
    val key: String,
    val defaultValue: Option[T],
    val valueConverter: String => T,
    val stringConverter: T => String,
    val doc: String,
    val isPublic: Boolean) {

  def defaultValueString: String = defaultValue.map(stringConverter).getOrElse("<undefined>")

  /**
   * Returns a new ConfigEntry that wraps the value in an Option, for config entries that do
   * not require a value.
   */
  def optional: OptionalConfigEntry[T] = {
    require(!defaultValue.isDefined, s"$this has a default value, cannot be optional.")
    new OptionalConfigEntry(key, valueConverter, stringConverter, doc, isPublic)
  }

  def readFrom(conf: SparkConf): T = {
    conf.getOption(key).map(valueConverter).orElse(defaultValue).getOrElse(
      throw new NoSuchElementException(s"$key is not set."))
  }

  override def toString: String = {
    s"ConfigEntry(key=$key, defaultValue=$defaultValueString, doc=$doc, public=$isPublic)"
  }
}

private[spark] class OptionalConfigEntry[T](
    override val key: String,
    val _valueConverter: String => T,
    val _stringConverter: T => String,
    override val doc: String,
    override val isPublic: Boolean)
    extends ConfigEntry[Option[T]](key, None, s => Some(_valueConverter(s)),
      null, doc, isPublic) {

  override def readFrom(conf: SparkConf): Option[T] = {
    conf.getOption(key).map(_valueConverter)
  }

}

private class FallbackConfigEntry[T] (
    override val key: String,
    override val doc: String,
    override val isPublic: Boolean,
    private val fallback: ConfigEntry[T])
    extends ConfigEntry[T](key, None, fallback.valueConverter, fallback.stringConverter,
      doc, isPublic) {

  override def defaultValueString: String = {
    defaultValue.map(stringConverter).getOrElse(s"<value of ${fallback.key}>")
  }

  override def readFrom(conf: SparkConf): T = {
    conf.getOption(key).map(valueConverter).getOrElse(fallback.readFrom(conf))
  }
}

private[spark] object ConfigEntry {

  def intConf(
      key: String,
      defaultValue: Option[Int] = None,
      doc: String = "",
      isPublic: Boolean = true): ConfigEntry[Int] =
    new ConfigEntry(key, defaultValue, { v =>
      try {
        v.toInt
      } catch {
        case _: NumberFormatException =>
          throw new IllegalArgumentException(s"$key should be int, but was $v")
      }
    }, _.toString, doc, isPublic)

  def longConf(
      key: String,
      defaultValue: Option[Long] = None,
      doc: String = "",
      isPublic: Boolean = true): ConfigEntry[Long] =
    new ConfigEntry(key, defaultValue, { v =>
      try {
        v.toLong
      } catch {
        case _: NumberFormatException =>
          throw new IllegalArgumentException(s"$key should be long, but was $v")
      }
    }, _.toString, doc, isPublic)

  def doubleConf(
      key: String,
      defaultValue: Option[Double] = None,
      doc: String = "",
      isPublic: Boolean = true): ConfigEntry[Double] =
    new ConfigEntry(key, defaultValue, { v =>
      try {
        v.toDouble
      } catch {
        case _: NumberFormatException =>
          throw new IllegalArgumentException(s"$key should be double, but was $v")
      }
    }, _.toString, doc, isPublic)

  def booleanConf(
      key: String,
      defaultValue: Option[Boolean] = None,
      doc: String = "",
      isPublic: Boolean = true): ConfigEntry[Boolean] =
    new ConfigEntry(key, defaultValue, { v =>
      try {
        v.toBoolean
      } catch {
        case _: IllegalArgumentException =>
          throw new IllegalArgumentException(s"$key should be boolean, but was $v")
      }
    }, _.toString, doc, isPublic)

  def stringConf(
      key: String,
      defaultValue: Option[String] = None,
      doc: String = "",
      isPublic: Boolean = true): ConfigEntry[String] =
    new ConfigEntry(key, defaultValue, v => v, v => v, doc, isPublic)

  def enumConf[T](
      key: String,
      valueConverter: String => T,
      validValues: Set[T],
      defaultValue: Option[T] = None,
      doc: String = "",
      isPublic: Boolean = true): ConfigEntry[T] =
    new ConfigEntry(key, defaultValue, v => {
      val _v = valueConverter(v)
      if (!validValues.contains(_v)) {
        throw new IllegalArgumentException(
          s"The value of $key should be one of ${validValues.mkString(", ")}, but was $v")
      }
      _v
    }, _.toString, doc, isPublic)

  def seqConf[T](
      key: String,
      valueConverter: String => T,
      defaultValue: Option[Seq[T]] = None,
      doc: String = "",
      isPublic: Boolean = true): ConfigEntry[Seq[T]] = {
    def stringToSeq(str: String): Seq[T] = {
      str.split(",").map(_.trim()).filter(_.nonEmpty).map(valueConverter)
    }

    new ConfigEntry(
      key, defaultValue, stringToSeq, _.mkString(","), doc, isPublic)
  }

  def stringSeqConf(
      key: String,
      defaultValue: Option[Seq[String]] = None,
      doc: String = "",
      isPublic: Boolean = true): ConfigEntry[Seq[String]] = {
    seqConf(key, s => s, defaultValue, doc, isPublic)
  }

  def timeConf(
      key: String,
      unit: TimeUnit,
      defaultValue: Option[String] = None,
      doc: String = "",
      isPublic: Boolean = true): ConfigEntry[Long] = {
    def timeFromString(str: String): Long = JavaUtils.timeStringAs(str, unit)
    def timeToString(v: Long): String = TimeUnit.MILLISECONDS.convert(v, unit) + "ms"

    new ConfigEntry(
      key, defaultValue.map(timeFromString), timeFromString, timeToString, doc, isPublic)
  }

  def bytesConf(
      key: String,
      unit: ByteUnit,
      defaultValue: Option[String] = None,
      doc: String = "",
      isPublic: Boolean = true): ConfigEntry[Long] = {
    def byteFromString(str: String): Long = JavaUtils.byteStringAs(str, unit)
    def byteToString(v: Long): String = unit.convertTo(v, ByteUnit.BYTE) + "b"

    new ConfigEntry(
      key, defaultValue.map(byteFromString), byteFromString, byteToString, doc, isPublic)
  }

  def fallbackConf[T](
      key: String,
      fallback: ConfigEntry[T],
      doc: String = "",
      isPublic: Boolean = true): ConfigEntry[T] = {
    new FallbackConfigEntry(key, doc, isPublic, fallback)
  }

}
