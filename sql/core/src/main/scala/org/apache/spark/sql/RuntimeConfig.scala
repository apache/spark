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

import org.apache.spark.sql.internal.SQLConf

/**
 * Runtime configuration interface for Spark. To access this, use [[SparkSession.conf]].
 *
 * Options set here are automatically propagated to the Hadoop configuration during I/O.
 *
 * @since 2.0.0
 */
class RuntimeConfig private[sql](sqlConf: SQLConf = new SQLConf) {

  /**
   * Sets the given Spark runtime configuration property.
   *
   * @since 2.0.0
   */
  def set(key: String, value: String): RuntimeConfig = {
    sqlConf.setConfString(key, value)
    this
  }

  /**
   * Sets the given Spark runtime configuration property.
   *
   * @since 2.0.0
   */
  def set(key: String, value: Boolean): RuntimeConfig = {
    set(key, value.toString)
  }

  /**
   * Sets the given Spark runtime configuration property.
   *
   * @since 2.0.0
   */
  def set(key: String, value: Long): RuntimeConfig = {
    set(key, value.toString)
  }

  /**
   * Returns the value of Spark runtime configuration property for the given key.
   *
   * @throws NoSuchElementException if the key is not set and does not have a default value
   * @since 2.0.0
   */
  @throws[NoSuchElementException]("if the key is not set")
  def get(key: String): String = {
    sqlConf.getConfString(key)
  }

  /**
   * Returns the value of Spark runtime configuration property for the given key.
   *
   * @since 2.0.0
   */
  def getOption(key: String): Option[String] = {
    try Option(get(key)) catch {
      case _: NoSuchElementException => None
    }
  }

  /**
   * Resets the configuration property for the given key.
   *
   * @since 2.0.0
   */
  def unset(key: String): Unit = {
    sqlConf.unsetConf(key)
  }
}
