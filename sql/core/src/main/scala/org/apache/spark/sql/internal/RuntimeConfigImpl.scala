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

package org.apache.spark.sql.internal

import scala.jdk.CollectionConverters._

import org.apache.spark.SPARK_DOC_ROOT
import org.apache.spark.annotation.Stable
import org.apache.spark.internal.config.{ConfigEntry, DEFAULT_PARALLELISM}
import org.apache.spark.sql.RuntimeConfig
import org.apache.spark.sql.errors.QueryCompilationErrors

/**
 * Runtime configuration interface for Spark. To access this, use `SparkSession.conf`.
 *
 * Options set here are automatically propagated to the Hadoop configuration during I/O.
 *
 * @since 2.0.0
 */
@Stable
class RuntimeConfigImpl private[sql](val sqlConf: SQLConf = new SQLConf) extends RuntimeConfig {

  /** @inheritdoc */
  def set(key: String, value: String): Unit = {
    requireNonStaticConf(key)
    sqlConf.setConfString(key, value)
  }

  /** @inheritdoc */
  @throws[NoSuchElementException]("if the key is not set and there is no default value")
  def get(key: String): String = {
    sqlConf.getConfString(key)
  }

  /** @inheritdoc */
  def get(key: String, default: String): String = {
    sqlConf.getConfString(key, default)
  }

  /** @inheritdoc */
  def getAll: Map[String, String] = {
    sqlConf.getAllConfs
  }

  private[sql] def getAllAsJava: java.util.Map[String, String] = {
    getAll.asJava
  }

  /** @inheritdoc */
  def getOption(key: String): Option[String] = {
    try Option(get(key)) catch {
      case _: NoSuchElementException => None
    }
  }

  /** @inheritdoc */
  def unset(key: String): Unit = {
    requireNonStaticConf(key)
    sqlConf.unsetConf(key)
  }

  /** @inheritdoc */
  def isModifiable(key: String): Boolean = sqlConf.isModifiable(key)

  /**
   * Returns whether a particular key is set.
   */
  private[sql] def contains(key: String): Boolean = {
    sqlConf.contains(key)
  }

  private[sql] def requireNonStaticConf(key: String): Unit = {
    // We documented `spark.default.parallelism` by SPARK-48773, however this config
    // is actually a static config so now a spark.conf.set("spark.default.parallelism")
    // will fail. Before SPARK-48773 it does not, then this becomes a behavior change.
    // Technically the current behavior is correct, however it still forms a behavior change.
    // To address the change, we need a check here and do not fail on default parallelism
    // setting through spark session conf to maintain the same behavior.
    if (key == DEFAULT_PARALLELISM.key) {
      return
    }
    if (SQLConf.isStaticConfigKey(key)) {
      throw QueryCompilationErrors.cannotModifyValueOfStaticConfigError(key)
    }
    if (sqlConf.setCommandRejectsSparkCoreConfs &&
        ConfigEntry.findEntry(key) != null && !SQLConf.containsConfigKey(key)) {
      throw QueryCompilationErrors.cannotModifyValueOfSparkConfigError(key, SPARK_DOC_ROOT)
    }
  }
}
