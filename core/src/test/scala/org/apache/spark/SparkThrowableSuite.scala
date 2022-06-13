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
import java.nio.charset.StandardCharsets
import java.nio.file.Files
import java.util.IllegalFormatException

import com.fasterxml.jackson.annotation.JsonInclude.Include
import com.fasterxml.jackson.core.JsonParser.Feature.STRICT_DUPLICATE_DETECTION
import com.fasterxml.jackson.core.`type`.TypeReference
import com.fasterxml.jackson.core.util.{DefaultIndenter, DefaultPrettyPrinter}
import com.fasterxml.jackson.databind.SerializationFeature
import com.fasterxml.jackson.databind.json.JsonMapper
import com.fasterxml.jackson.module.scala.DefaultScalaModule
import org.apache.commons.io.{FileUtils, IOUtils}

import org.apache.spark.SparkThrowableHelper._
import org.apache.spark.util.Utils

/**
 * Test suite for Spark Throwables.
 */
class SparkThrowableSuite extends SparkFunSuite {

  /* Used to regenerate the error class file. Run:
   {{{
      SPARK_GENERATE_GOLDEN_FILES=1 build/sbt \
        "core/testOnly *SparkThrowableSuite -- -t \"Error classes are correctly formatted\""
   }}}
   */
  private val regenerateGoldenFiles: Boolean = System.getenv("SPARK_GENERATE_GOLDEN_FILES") == "1"
  private val errorClassDir = getWorkspaceFilePath(
    "core", "src", "main", "resources", "error").toFile

  override def beforeAll(): Unit = {
    super.beforeAll()
  }

  def checkIfUnique(ss: Seq[Any]): Unit = {
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

  test("Error classes are correctly formatted") {
    val errorClassFileContents = IOUtils.toString(errorClassesUrl.openStream())
    val mapper = JsonMapper.builder()
      .addModule(DefaultScalaModule)
      .enable(SerializationFeature.INDENT_OUTPUT)
      .build()
    val prettyPrinter = new DefaultPrettyPrinter()
      .withArrayIndenter(DefaultIndenter.SYSTEM_LINEFEED_INSTANCE)
    val rewrittenString = mapper.configure(SerializationFeature.ORDER_MAP_ENTRIES_BY_KEYS, true)
      .setSerializationInclusion(Include.NON_ABSENT)
      .writer(prettyPrinter)
      .writeValueAsString(errorClassToInfoMap)

    if (regenerateGoldenFiles) {
      if (rewrittenString.trim != errorClassFileContents.trim) {
        val errorClassesFile = new File(errorClassDir, new File(errorClassesUrl.getPath).getName)
        logInfo(s"Regenerating error class file $errorClassesFile")
        Files.delete(errorClassesFile.toPath)
        FileUtils.writeStringToFile(errorClassesFile, rewrittenString, StandardCharsets.UTF_8)
      }
    } else {
      assert(rewrittenString.trim == errorClassFileContents.trim)
    }
  }

  test("SQLSTATE invariants") {
    val sqlStates = errorClassToInfoMap.values.toSeq.flatMap(_.sqlState)
    val errorClassReadMe = Utils.getSparkClassLoader.getResource("error/README.md")
    val errorClassReadMeContents = IOUtils.toString(errorClassReadMe.openStream())
    val sqlStateTableRegex =
      "(?s)<!-- SQLSTATE table start -->(.+)<!-- SQLSTATE table stop -->".r
    val sqlTable = sqlStateTableRegex.findFirstIn(errorClassReadMeContents).get
    val sqlTableRows = sqlTable.split("\n").filter(_.startsWith("|")).drop(2)
    val validSqlStates = sqlTableRows.map(_.slice(1, 6)).toSet
    // Sanity check
    assert(Set("07000", "42000", "HZ000").subsetOf(validSqlStates))
    assert(validSqlStates.forall(_.length == 5), validSqlStates)
    checkCondition(sqlStates, s => validSqlStates.contains(s))
  }

  test("Message invariants") {
    val messageSeq = errorClassToInfoMap.values.toSeq.flatMap { i =>
      Seq(i.message) ++ i.subClass.getOrElse(Map.empty).values.toSeq.map(_.message)
    }
    messageSeq.foreach { message =>
      message.foreach { msg =>
        assert(!msg.contains("\n"))
        assert(msg.trim == msg)
      }
    }
  }

  test("Message format invariants") {
    val messageFormats = errorClassToInfoMap.values.toSeq.flatMap { i =>
      Seq(i.messageFormat) ++ i.subClass.getOrElse(Map.empty).values.toSeq.map(_.messageFormat)
    }
    checkCondition(messageFormats, s => s != null)
    checkIfUnique(messageFormats)
  }

  test("Round trip") {
    val tmpFile = File.createTempFile("rewritten", ".json")
    val mapper = JsonMapper.builder()
      .addModule(DefaultScalaModule)
      .enable(SerializationFeature.INDENT_OUTPUT)
      .build()
    mapper.writeValue(tmpFile, errorClassToInfoMap)
    val rereadErrorClassToInfoMap = mapper.readValue(
      tmpFile, new TypeReference[Map[String, ErrorInfo]]() {})
    assert(rereadErrorClassToInfoMap == errorClassToInfoMap)
  }

  test("Check if error class is missing") {
    val ex1 = intercept[IllegalArgumentException] {
      getMessage("", null, Array.empty)
    }
    assert(ex1.getMessage == "Cannot find error class ''")

    val ex2 = intercept[IllegalArgumentException] {
      getMessage("LOREM_IPSUM", null, Array.empty)
    }
    assert(ex2.getMessage == "Cannot find error class 'LOREM_IPSUM'")
  }

  test("Check if message parameters match message format") {
    // Requires 2 args
    intercept[IllegalFormatException] {
      getMessage("MISSING_COLUMN", null, Array.empty)
    }

    // Does not fail with too many args (expects 0 args)
    assert(getMessage("DIVIDE_BY_ZERO", null, Array("foo", "bar", "baz")) ==
      "[DIVIDE_BY_ZERO] Division by zero. " +
      "Use `try_divide` to tolerate divisor being 0 and return NULL instead. " +
        "If necessary set foo to \"false\" " +
        "(except for ANSI interval type) to bypass this error.")
  }

  test("Error message is formatted") {
    assert(getMessage("MISSING_COLUMN", null, Array("foo", "bar, baz")) ==
      "[MISSING_COLUMN] Column 'foo' does not exist. Did you mean one of the following? [bar, baz]")
  }

  test("Try catching legacy SparkError") {
    try {
      throw new SparkException("Arbitrary legacy message")
    } catch {
      case e: SparkThrowable =>
        assert(e.getErrorClass == null)
        assert(e.getSqlState == null)
      case _: Throwable =>
        // Should not end up here
        assert(false)
    }
  }

  test("Try catching SparkError with error class") {
    try {
      throw new SparkException(
        errorClass = "WRITING_JOB_ABORTED",
        messageParameters = Array.empty,
        cause = null)
    } catch {
      case e: SparkThrowable =>
        assert(e.getErrorClass == "WRITING_JOB_ABORTED")
        assert(e.getSqlState == "40000")
      case _: Throwable =>
        // Should not end up here
        assert(false)
    }
  }

  test("Try catching internal SparkError") {
    try {
      throw new SparkException(
        errorClass = "INTERNAL_ERROR",
        messageParameters = Array("this is an internal error"),
        cause = null
      )
    } catch {
      case e: SparkThrowable =>
        assert(e.isInternalError)
        assert(e.getSqlState == null)
      case _: Throwable =>
        // Should not end up here
        assert(false)
    }
  }
}
