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

import scala.util.Try

import org.apache.spark.internal.{Logging, LogKeys}
import org.apache.spark.sql.classic.SparkSession
import org.apache.spark.sql.internal.{SharedState, StaticSQLConf}

object SparkSessionUtils extends Logging {

  /**
   * Sets all SQL configurations specified in `pairs`, calls `f`, and then restores all SQL
   * configurations.
   *
   * Every pair that a running session cannot take is ignored. These are the pairs
   * `spark.conf.set` rejects: static SQL configurations and, unless
   * `spark.sql.legacy.setCommandRejectsSparkCoreConfs` is false, Spark core configurations.
   * This applies to each pair the helper receives, whether it comes from a pipeline's
   * `configuration` or from flow-level settings such as Python `spark_conf` and SQL `SET`
   * statements in pipeline source files. Such settings can only be given to the session when it
   * is created. When the requested value differs from the session's value, a warning is logged on
   * the driver, with the value redacted the same way the session redacts options.
   */
  def withSqlConf[T](spark: SparkSession, pairs: (String, String)*)(f: => T): T = {
    val conf = spark.conf
    // Each pair is checked with the rule `spark.conf.set` itself applies, right before it is set,
    // so the skip cannot drift from what the session rejects.
    val previousValues = pairs.flatMap { case (k, v) =>
      if (Try(conf.requireNonStaticConf(k)).isSuccess) {
        val previous = conf.getOption(k)
        conf.set(k, v)
        Some(k -> previous)
      } else {
        if (!isInEffect(spark, k, v)) {
          val (_, redacted) = spark.sessionState.conf.redactOptions(Seq(k -> v)).head
          logWarning(log"Ignoring ${MDC(LogKeys.CONFIG, k)}=${MDC(LogKeys.VALUE, redacted)} " +
            log"because it cannot be modified on a running session. Set it when the session is " +
            log"created, for example with --conf on the spark-pipelines command line.")
        }
        None
      }
    }
    try f
    finally {
      previousValues.reverse.foreach {
        case (key, Some(value)) => conf.set(key, value)
        case (key, None) => conf.unset(key)
      }
    }
  }

  /**
   * Whether the session already holds `value` for `key`, so that skipping the pair changes
   * nothing.
   */
  private def isInEffect(spark: SparkSession, key: String, value: String): Boolean = {
    val requested = if (key == StaticSQLConf.WAREHOUSE_PATH.key) {
      // The session stores the warehouse directory qualified (see `SharedState`), so qualify the
      // requested directory the same way before comparing. Keep the raw value if that fails.
      Try(SharedState.qualifyWarehousePath(spark.sessionState.newHadoopConf(), value))
        .getOrElse(value)
    } else {
      value
    }
    spark.conf.getOption(key).contains(requested)
  }
}
