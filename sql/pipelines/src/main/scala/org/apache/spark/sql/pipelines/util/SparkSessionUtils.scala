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

package org.apache.spark.sql.pipelines.util

import org.apache.spark.internal.{Logging, LogKeys}
import org.apache.spark.internal.config.ConfigEntry
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.internal.SQLConf

object SparkSessionUtils extends Logging {

  /**
   * Sets all SQL configurations specified in `pairs`, calls `f`, and then restores all SQL
   * configurations.
   *
   * Configurations that cannot be changed on a running session (static SQL configurations and
   * Spark core configurations) are skipped: a pipeline's `configuration` is also used to create
   * the session, which is the only place where those can take effect.
   */
  def withSqlConf[T](spark: SparkSession, pairs: (String, String)*)(f: => T): T = {
    val conf = spark.conf
    val (runtimePairs, nonRuntimePairs) = pairs.partition { case (k, _) => isRuntimeConf(k) }
    nonRuntimePairs.foreach { case (k, v) =>
      if (!conf.getOption(k).contains(v)) {
        logWarning(log"Ignoring ${MDC(LogKeys.CONFIG, k)}=${MDC(LogKeys.VALUE, v)} because it " +
          log"cannot be modified on a running session. Set it when the session is created, " +
          log"for example with --conf on the spark-pipelines command line.")
      }
    }
    val (keys, values) = runtimePairs.unzip
    val currentValues = keys.map(conf.getOption)
    keys.lazyZip(values).foreach((k, v) => conf.set(k, v))
    try f
    finally {
      keys.zip(currentValues).foreach {
        case (key, Some(value)) => conf.set(key, value)
        case (key, None) => conf.unset(key)
      }
    }
  }

  /** Whether `key` can be changed on a running session with `spark.conf.set`. */
  private def isRuntimeConf(key: String): Boolean = {
    !SQLConf.isStaticConfigKey(key) &&
      (SQLConf.containsConfigKey(key) || ConfigEntry.findEntry(key) == null)
  }
}
