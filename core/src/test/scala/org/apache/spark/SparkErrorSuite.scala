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

package org.apache.spark

import java.io.File
import java.util.IllegalFormatException

import com.fasterxml.jackson.core.JsonParser.Feature.STRICT_DUPLICATE_DETECTION
import com.fasterxml.jackson.core.`type`.TypeReference
import com.fasterxml.jackson.databind.json.JsonMapper
import com.fasterxml.jackson.module.scala.DefaultScalaModule

import org.apache.spark.SparkError._
import org.apache.spark.internal.config.SHOW_SPARK_ERROR_FIELDS

class SparkErrorSuite extends SparkFunSuite {
  def withShowSparkErrorFieldsSet(enabled: Boolean)(fx: => Unit): Unit = {
    System.setProperty(SHOW_SPARK_ERROR_FIELDS.key, enabled.toString)
    fx
    System.clearProperty(SHOW_SPARK_ERROR_FIELDS.key)
  }

  def checkIfUnique(ss: Seq[String]): Unit = {
    val dups = ss.groupBy(identity).mapValues(_.size).filter(_._2 > 1).keys.toSeq
    assert(dups.isEmpty)
  }

  def checkCondition(ss: Seq[String], fx: String => Boolean): Unit = {
    ss.foreach { s =>
      assert(fx(s))
    }
  }

  test("No duplicate error classes") {
    // Enabling this feature incurs performance overhead (20-30%)
    val mapper = JsonMapper.builder()
      .addModule(DefaultScalaModule)
      .enable(STRICT_DUPLICATE_DETECTION)
      .build()
    mapper.readValue(errorClassesUrl, new TypeReference[Map[String, ErrorInfo]]() {})
  }

  test("SQLSTATE have length 5") {
    val sqlStates = errorClassToInfoMap.values.toSeq.flatMap(_.sqlState)
    checkCondition(sqlStates, s => s.length == 5)
  }

  test("Message formats are non-null and unique") {
    val messageFormats = errorClassToInfoMap.values.toSeq.map(_.messageFormat)
    checkCondition(messageFormats, s => s != null)
    checkIfUnique(messageFormats)
  }

  test("Ingested map's contents match error info file") {
    val expectedErrorClassToInfoMap =
      mapper.readValue(errorClassesUrl, new TypeReference[Map[String, ErrorInfo]]() {})
    assert(expectedErrorClassToInfoMap == errorClassToInfoMap)
  }

  test("Round trip") {
    val tmpFile = File.createTempFile("rewritten", ".json")
    mapper.writeValue(tmpFile, errorClassToInfoMap)
    val rereadErrorClassToInfoMap = mapper.readValue(
      tmpFile, new TypeReference[Map[String, ErrorInfo]]() {})
    assert(rereadErrorClassToInfoMap == errorClassToInfoMap)
  }

  test("Check if error class is missing") {
    val ex1 = intercept[IllegalArgumentException] {
      getMessage("", Seq.empty)
    }
    assert(ex1.getMessage == "Cannot find error class ''")

    val ex2 = intercept[IllegalArgumentException] {
      getMessage("LOREM_IPSUM_ERROR", Seq.empty)
    }
    assert(ex2.getMessage == "Cannot find error class 'LOREM_IPSUM_ERROR'")
  }

  test("Check if message parameters match message format") {
    // Requires 2 args
    intercept[IllegalFormatException] {
      getMessage("MISSING_COLUMN_ERROR", Seq.empty)
    }

    // Requires 2 args
    intercept[IllegalFormatException] {
      getMessage("MISSING_COLUMN_ERROR", Seq("foo"))
    }

    // Does not fail with too many args (expects 0 args)
    assert(getMessage("DIVIDE_BY_ZERO_ERROR", Seq("foo", "bar")) == "divide by zero")
  }

  test("Error fields are only added if requested") {
    withShowSparkErrorFieldsSet(enabled = false) {
      assert(getMessage("MISSING_COLUMN_ERROR", Seq("foo", "bar")) ==
        "cannot resolve 'foo' given input columns: [bar]")
    }

    withShowSparkErrorFieldsSet(enabled = true) {
      assert(getMessage("MISSING_COLUMN_ERROR", Seq("foo", "bar")) ==
        "MISSING_COLUMN_ERROR: cannot resolve 'foo' given input columns: [bar] (SQLSTATE 42000)")
    }
  }

  test("Try catching SparkError") {
    try {
      throw new SparkException(
        errorClass = "WRITING_JOB_ABORTED_ERROR",
        messageParameters = Seq.empty,
        cause = null)
    } catch {
      case e: SparkError =>
        assert(e.errorClass.contains("WRITING_JOB_ABORTED_ERROR"))
        assert(e.sqlState.contains("40000"))
      case _: Throwable =>
        // Should not end up here
        assert(false)
    }
  }
}
