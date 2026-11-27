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
package org.apache.spark.sql.errors

import org.scalatest.Suite

import org.apache.spark.CheckErrorHelper

/**
 * The interval divisions that raise in every mode, shared by the suites that check their query
 * context with and without ANSI mode (SPARK-61352). They read a column, so that they are not
 * constant-folded on the driver.
 */
trait IntervalErrorContextTestUtils extends CheckErrorHelper { self: Suite =>
  protected val ymDivide = "CAST(CAST(id - 2147483648 AS INT) AS INTERVAL MONTH) / -1"
  protected val dtDivide = "CAST(id + 1 AS INTERVAL DAY) / CAST(id AS INT)"

  /** The context of `fragment` in `SELECT $query FROM range(1)`. */
  protected def expectedContext(query: String, fragment: String): ExpectedContext = {
    val start = "SELECT ".length + query.indexOf(fragment)
    ExpectedContext(fragment = fragment, start = start, stop = start + fragment.length - 1)
  }
}
