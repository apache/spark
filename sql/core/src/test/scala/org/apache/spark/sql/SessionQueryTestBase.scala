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

// scalastyle:off funsuite
import org.scalatest.funsuite.AnyFunSuite

import org.apache.spark.CheckErrorHelper
// scalastyle:on

/**
 * TODO should be moved to sql/api
 *
 * base for fully sql/core independent tests, i.e. this trait could be moved to sql/api and then
 * used in sql/connect/client.
 */
trait SessionQueryTestBase
  extends AnyFunSuite
    with SparkSessionProvider
    with CheckAnswerHelper
    with CheckErrorHelper
    with QueryCleanupHelper {

  /**
   * Sets all configurations specified in `pairs`, calls `f`, and then restores all configurations.
   *
   * Use this instead of `withSQLConf` as [[internal.SQLConf SQLConf]] is not part of Spark's public
   * API.
   */
  protected def withConf[T](pairs: (String, String)*)(f: => T): T = {
    val (keys, values) = pairs.unzip
    val currentValues = keys.map { key =>
      if (spark.conf.contains(key)) {
        Some(spark.conf.get(key))
      } else {
        None
      }
    }
    keys.lazyZip(values).foreach { (k, v) =>
      spark.conf.set(k, v)
    }
    try f finally {
      keys.zip(currentValues).foreach {
        case (key, Some(value)) => spark.conf.set(key, value)
        case (key, None) => spark.conf.unset(key)
      }
    }
  }

  /**
   * Whether the bound session is a Spark Connect session (`false` for classic), so that tests can
   * handle and document session-specific behaviour.
   *
   * {{{
   *   test(...) {
   *     val df = // query with connect-specific behaviour
   *     if (isConnect) {
   *       checkError(...)
   *     } else {
   *       checkAnswer(df, ...)
   *     }
   *   }
   * }}}
   */
  def isConnect: Boolean
}
