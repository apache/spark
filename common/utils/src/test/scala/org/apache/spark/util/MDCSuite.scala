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

package org.apache.spark.util

import scala.jdk.CollectionConverters._

import org.scalatest.funsuite.AnyFunSuite // scalastyle:ignore funsuite

import org.apache.spark.internal.{Logging, MDC}
import org.apache.spark.internal.LogKey.EXIT_CODE

class MDCSuite
    extends AnyFunSuite // scalastyle:ignore funsuite
    with Logging {

  test("check MDC message") {
    val log = log"This is a log, exitcode ${MDC(EXIT_CODE, 10086)}"
    assert(log.message === "This is a log, exitcode 10086")
    assert(log.context === Map("exit_code" -> "10086").asJava)
  }

  test("custom object as MDC value") {
    val cov = CustomObjectValue("spark", 10086)
    val log = log"This is a log, exitcode ${MDC(EXIT_CODE, cov)}"
    assert(log.message === "This is a log, exitcode CustomObjectValue: spark, 10086")
    assert(log.context === Map("exit_code" -> "CustomObjectValue: spark, 10086").asJava)
  }

  case class CustomObjectValue(key: String, value: Int) {
    override def toString: String = {
      "CustomObjectValue: " + key + ", " + value
    }
  }
}
