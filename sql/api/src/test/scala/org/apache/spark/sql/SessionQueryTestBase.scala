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
// scalastyle:on

trait SessionQueryTestBase
  extends AnyFunSuite
    with SparkSessionProvider
    with CheckAnswerHelper
    with QueryCleanupHelper {

  /**
   * Documents used session so that tests can handle and document session-specific behaviour
   *
   * {{{
   *   test(...) {
   *     val df = // query with connect-specific behaviour
   *     if (sessionType = 'connect') {
   *       checkError(...)
   *     } else {
   *       checkAnswer(df, ...)
   *     }
   *   }
   * }}}
   */
  def sessionType: String
}
