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

import java.util.Properties

import scala.collection.JavaConverters._

object SQLConf {
  val COMPRESS_CACHED = "spark.sql.inMemoryColumnarStorage.compressed"
  val AUTO_BROADCASTJOIN_THRESHOLD = "spark.sql.autoBroadcastJoinThreshold"
  val DEFAULT_SIZE_IN_BYTES = "spark.sql.defaultSizeInBytes"
  val AUTO_CONVERT_JOIN_SIZE = "spark.sql.auto.convert.join.size"
  val SHUFFLE_PARTITIONS = "spark.sql.shuffle.partitions"
  val JOIN_BROADCAST_TABLES = "spark.sql.join.broadcastTables"
  val CODEGEN_ENABLED = "spark.sql.codegen"

  object Deprecated {
    val MAPRED_REDUCE_TASKS = "mapred.reduce.tasks"
  }
}

/**
 * A trait that enables the setting and getting of mutable config parameters/hints.
 *
 * In the presence of a SQLContext, these can be set and queried by passing SET commands
 * into Spark SQL's query functions (sql(), hql(), etc.). Otherwise, users of this trait can
 * modify the hints by programmatically calling the setters and getters of this trait.
 *
 * SQLConf is thread-safe (internally synchronized, so safe to be used in multiple threads).
 */
trait SQLConf {
  import SQLConf._

  @transient protected[spark] val settings = java.util.Collections.synchronizedMap(
    new java.util.HashMap[String, String]())

  /** ************************ Spark SQL Params/Hints ******************* */
  // TODO: refactor so that these hints accessors don't pollute the name space of SQLContext?

  /** When true tables cached using the in-memory columnar caching will be compressed. */
  private[spark] def useCompression: Boolean = get(COMPRESS_CACHED, "false").toBoolean

  /** Number of partitions to use for shuffle operators. */
  private[spark] def numShufflePartitions: Int = get(SHUFFLE_PARTITIONS, "200").toInt

  /**
   * When set to true, Spark SQL will use the Scala compiler at runtime to generate custom bytecode
   * that evaluates expressions found in queries.  In general this custom code runs much faster
   * than interpreted evaluation, but there are significant start-up costs due to compilation.
   * As a result codegen is only benificial when queries run for a long time, or when the same
   * expressions are used multiple times.
   *
   * Defaults to false as this feature is currently experimental.
   */
  private[spark] def codegenEnabled: Boolean =
    if (get(CODEGEN_ENABLED, "false") == "true") true else false

  /**
   * Upper bound on the sizes (in bytes) of the tables qualified for the auto conversion to
   * a broadcast value during the physical executions of join operations.  Setting this to -1
   * effectively disables auto conversion.
   *
   * Hive setting: hive.auto.convert.join.noconditionaltask.size, whose default value is also 10000.
   */
  private[spark] def autoBroadcastJoinThreshold: Int =
    get(AUTO_BROADCASTJOIN_THRESHOLD, "10000").toInt

  /**
   * The default size in bytes to assign to a logical operator's estimation statistics.  By default,
   * it is set to a larger value than `autoConvertJoinSize`, hence any logical operator without a
   * properly implemented estimation of this statistic will not be incorrectly broadcasted in joins.
   */
  private[spark] def defaultSizeInBytes: Long =
    getOption(DEFAULT_SIZE_IN_BYTES).map(_.toLong).getOrElse(autoBroadcastJoinThreshold + 1)

  /** ********************** SQLConf functionality methods ************ */

  def set(props: Properties): Unit = {
    settings.synchronized {
      props.asScala.foreach { case (k, v) => settings.put(k, v) }
    }
  }

  def set(key: String, value: String): Unit = {
    require(key != null, "key cannot be null")
    require(value != null, s"value cannot be null for key: $key")
    settings.put(key, value)
  }

  def get(key: String): String = {
    Option(settings.get(key)).getOrElse(throw new NoSuchElementException(key))
  }

  def get(key: String, defaultValue: String): String = {
    Option(settings.get(key)).getOrElse(defaultValue)
  }

  def getAll: Array[(String, String)] = settings.synchronized { settings.asScala.toArray }

  def getOption(key: String): Option[String] = Option(settings.get(key))

  def contains(key: String): Boolean = settings.containsKey(key)

  def toDebugString: String = {
    settings.synchronized {
      settings.asScala.toArray.sorted.map{ case (k, v) => s"$k=$v" }.mkString("\n")
    }
  }

  private[spark] def clear() {
    settings.clear()
  }
}

