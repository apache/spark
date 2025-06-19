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

import org.scalatest.BeforeAndAfterAll
import org.scalatest.funsuite.AnyFunSuite // scalastyle:ignore funsuite

import org.apache.spark.internal.{Logging, MDC}
import org.apache.spark.internal.LogKeys.{EXIT_CODE, OFFSET, RANGE}

class MDCSuite
    extends AnyFunSuite // scalastyle:ignore funsuite
    with Logging
    with BeforeAndAfterAll {

  override def beforeAll(): Unit = {
    Logging.enableStructuredLogging()
  }

  override def afterAll(): Unit = {
    Logging.disableStructuredLogging()
  }

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

  test("null as MDC value") {
    val log = log"This is a log, exitcode ${MDC(EXIT_CODE, null)}"
    assert(log.message === "This is a log, exitcode null")
    assert(log.context === Map("exit_code" -> null).asJava)
  }

  test("the class of value cannot be MDC") {
    val log = log"This is a log, exitcode ${MDC(EXIT_CODE, "123456")}"
    val e = intercept[IllegalArgumentException] {
      MDC(RANGE, log)
    }
    assert(e.getMessage ===
      "requirement failed: the class of value cannot be MessageWithContext")
  }

  test("check MDC stripMargin") {
    val log =
      log"""
           |The current available offset range is ${MDC(RANGE, "12 - 34")}.
           | Offset ${MDC(OFFSET, "666")}. is out of range""".stripMargin
    val expected =
      s"""
         |The current available offset range is 12 - 34.
         | Offset 666. is out of range""".stripMargin
    assert(log.message === expected)
    assert(log.context === Map("range" -> "12 - 34", "offset" -> "666").asJava)
  }

  case class CustomObjectValue(key: String, value: Int) {
    override def toString: String = {
      "CustomObjectValue: " + key + ", " + value
    }
  }
}
