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

/**
 * SQLConf holds mutable config parameters and hints.  These can be set and
 * queried either by passing SET commands into Spark SQL's DSL
 * functions (sql(), hql(), etc.), or by programmatically using setters and
 * getters of this class.
 *
 * SQLConf is thread-safe (internally synchronized so safe to be used in multiple threads).
 */
trait SQLConf {

  /** ************************ Spark SQL Params/Hints ******************* */
  // TODO: refactor so that these hints accessors don't pollute the name space of SQLContext?

  /** Number of partitions to use for shuffle operators. */
  private[spark] def numShufflePartitions: Int = get("spark.sql.shuffle.partitions", "200").toInt

  /**
   * Upper bound on the sizes (in bytes) of the tables qualified for the auto conversion to
   * a broadcast value during the physical executions of join operations.  Setting this to 0
   * effectively disables auto conversion.
   * Hive setting: hive.auto.convert.join.noconditionaltask.size.
   */
  private[spark] def autoConvertJoinSize: Int =
    get("spark.sql.auto.convert.join.size", "10000").toInt

  /** A comma-separated list of table names marked to be broadcasted during joins. */
  private[spark] def joinBroadcastTables: String = get("spark.sql.join.broadcastTables", "")

  /** ********************** SQLConf functionality methods ************ */

  @transient
  private val settings = java.util.Collections.synchronizedMap(
    new java.util.HashMap[String, String]())

  def set(props: Properties): Unit = {
    props.asScala.foreach { case (k, v) => this.settings.put(k, v) }
  }

  def set(key: String, value: String): Unit = {
    require(key != null, "key cannot be null")
    require(value != null, s"value cannot be null for ${key}")
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
