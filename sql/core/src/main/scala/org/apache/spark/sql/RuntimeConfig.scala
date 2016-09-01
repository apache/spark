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

import scala.collection.mutable.HashMap

import org.apache.spark.internal.config.{ConfigEntry, OptionalConfigEntry}
import org.apache.spark.scheduler.{LiveListenerBus, SparkListenerSessionUpdate}
import org.apache.spark.sql.internal.SQLConf


/**
 * Runtime configuration interface for Spark. To access this, use [[SparkSession.conf]].
 *
 * Options set here are automatically propagated to the Hadoop configuration during I/O.
 *
 * @since 2.0.0
 */
class RuntimeConfig private[sql](sqlConf: SQLConf = new SQLConf,
                                 listenerBus: Option[LiveListenerBus] = None) {

  /**
   * Sets the given Spark runtime configuration property.
   *
   * @since 2.0.0
   */
  def set(key: String, value: String): Unit = {
    sqlConf.setConfString(key, value)
    postSessionUpdate()
  }

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
  def get(key: String, default: String): String = {
    sqlConf.getConfString(key, default)
  }

  /**
   * Returns the value of Spark runtime configuration property for the given key.
   */
  @throws[NoSuchElementException]("if the key is not set")
  protected[sql] def get[T](entry: ConfigEntry[T]): T = {
    sqlConf.getConf(entry)
  }

  protected[sql] def get[T](entry: OptionalConfigEntry[T]): Option[T] = {
    sqlConf.getConf(entry)
  }

  /**
   * Returns the value of Spark runtime configuration property for the given key.
   */
  protected[sql] def get[T](entry: ConfigEntry[T], default: T): T = {
    sqlConf.getConf(entry, default)
  }

  /**
   * Returns all properties set in this conf.
   *
   * @since 2.0.0
   */
  def getAll: Map[String, String] = {
    sqlConf.getAllConfs
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
    postSessionUpdate()
  }

  /**
   * Returns whether a particular key is set.
   */
  protected[sql] def contains(key: String): Boolean = {
    sqlConf.contains(key)
  }

  /**
   * Set the sqlConf multiple times while call postSessionUpdate only once.
   * This is faster than calling set multiple times which posts session update every time.
   */
  protected[sql] def setBatch(options: HashMap[String, String]): Unit = {
    options.foreach { case (k, v) => sqlConf.setConfString(k, v) }
    postSessionUpdate()
  }

  private def postSessionUpdate() {
    if (listenerBus.isDefined) {
      val sessionUpdate = SparkListenerSessionUpdate(getAll)
      listenerBus.get.post(sessionUpdate)
    }
  }

}
