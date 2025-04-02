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
package org.apache.spark.sql

import org.apache.spark.annotation.Stable
import org.apache.spark.internal.config.{ConfigEntry, OptionalConfigEntry}

/**
 * Runtime configuration interface for Spark. To access this, use `SparkSession.conf`.
 *
 * Options set here are automatically propagated to the Hadoop configuration during I/O.
 *
 * @since 2.0.0
 */
@Stable
abstract class RuntimeConfig {

  /**
   * Sets the given Spark runtime configuration property.
   *
   * @since 2.0.0
   */
  def set(key: String, value: String): Unit

  /**
   * Sets the given Spark runtime configuration property.
   *
   * @since 2.0.0
   */
  def set(key: String, value: Boolean): Unit = {
    set(key, value.toString)
  }

  /**
   * Sets the given Spark runtime configuration property.
   *
   * @since 2.0.0
   */
  def set(key: String, value: Long): Unit = {
    set(key, value.toString)
  }

  /**
   * Sets the given Spark runtime configuration property.
   */
  private[sql] def set[T](entry: ConfigEntry[T], value: T): Unit

  /**
   * Returns the value of Spark runtime configuration property for the given key. If the key is
   * not set yet, return its default value if possible, otherwise `NoSuchElementException` will be
   * thrown.
   *
   * @throws java.util.NoSuchElementException
   *   if the key is not set and does not have a default value
   * @since 2.0.0
   */
  @throws[NoSuchElementException]("if the key is not set and there is no default value")
  def get(key: String): String

  /**
   * Returns the value of Spark runtime configuration property for the given key. If the key is
   * not set yet, return the user given `default`. This is useful when its default value defined
   * by Apache Spark is not the desired one.
   *
   * @since 2.0.0
   */
  def get(key: String, default: String): String

  /**
   * Returns the value of Spark runtime configuration property for the given key. If the key is
   * not set yet, return `defaultValue` in [[ConfigEntry]].
   */
  @throws[NoSuchElementException]("if the key is not set")
  private[sql] def get[T](entry: ConfigEntry[T]): T

  /**
   * Returns the value of Spark runtime configuration property for the given key. If the key is
   * not set yet, return None.
   */
  private[sql] def get[T](entry: OptionalConfigEntry[T]): Option[T]

  /**
   * Returns the value of Spark runtime configuration property for the given key. If the key is
   * not set yet, return the user given `default`.
   */
  private[sql] def get[T](entry: ConfigEntry[T], default: T): T

  /**
   * Returns whether a particular key is set.
   */
  private[sql] def contains(key: String): Boolean

  /**
   * Returns all properties set in this conf.
   *
   * @since 2.0.0
   */
  def getAll: Map[String, String]

  /**
   * Returns the value of Spark runtime configuration property for the given key. If the key is
   * not set yet, return its default value if possible, otherwise `None` will be returned.
   *
   * @since 2.0.0
   */
  def getOption(key: String): Option[String]

  /**
   * Resets the configuration property for the given key.
   *
   * @since 2.0.0
   */
  def unset(key: String): Unit

  /**
   * Indicates whether the configuration property with the given key is modifiable in the current
   * session.
   *
   * @return
   *   `true` if the configuration property is modifiable. For static SQL, Spark Core, invalid
   *   (not existing) and other non-modifiable configuration properties, the returned value is
   *   `false`.
   * @since 2.4.0
   */
  def isModifiable(key: String): Boolean
}
