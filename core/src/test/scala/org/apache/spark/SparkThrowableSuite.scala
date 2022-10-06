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
  private val errorJsonFilePath = getWorkspaceFilePath(
    "core", "src", "main", "resources", "error", "error-classes.json")

  private val errorReader = new ErrorClassesJsonReader(Seq(errorJsonFilePath.toUri.toURL))

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
    mapper.readValue(errorJsonFilePath.toUri.toURL, new TypeReference[Map[String, ErrorInfo]]() {})
  }

  test("Error classes are correctly formatted") {
    val errorClassFileContents = IOUtils.toString(errorJsonFilePath.toUri.toURL.openStream())
    val mapper = JsonMapper.builder()
      .addModule(DefaultScalaModule)
      .enable(SerializationFeature.INDENT_OUTPUT)
      .build()
    val prettyPrinter = new DefaultPrettyPrinter()
      .withArrayIndenter(DefaultIndenter.SYSTEM_LINEFEED_INSTANCE)
    val rewrittenString = mapper.configure(SerializationFeature.ORDER_MAP_ENTRIES_BY_KEYS, true)
      .setSerializationInclusion(Include.NON_ABSENT)
      .writer(prettyPrinter)
      .writeValueAsString(errorReader.errorInfoMap)

    if (regenerateGoldenFiles) {
      if (rewrittenString.trim != errorClassFileContents.trim) {
        val errorClassesFile = errorJsonFilePath.toFile
        logInfo(s"Regenerating error class file $errorClassesFile")
        Files.delete(errorClassesFile.toPath)
        FileUtils.writeStringToFile(errorClassesFile, rewrittenString, StandardCharsets.UTF_8)
      }
    } else {
      assert(rewrittenString.trim == errorClassFileContents.trim)
    }
  }

  test("SQLSTATE invariants") {
    val sqlStates = errorReader.errorInfoMap.values.toSeq.flatMap(_.sqlState)
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
    val messageSeq = errorReader.errorInfoMap.values.toSeq.flatMap { i =>
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
    val messageFormats = errorReader.errorInfoMap
      .filterKeys(!_.startsWith("_LEGACY_ERROR_TEMP_"))
      .values.toSeq.flatMap { i => Seq(i.messageTemplate) }
    checkCondition(messageFormats, s => s != null)
    checkIfUnique(messageFormats)
  }

  test("Round trip") {
    val tmpFile = File.createTempFile("rewritten", ".json")
    val mapper = JsonMapper.builder()
      .addModule(DefaultScalaModule)
      .enable(SerializationFeature.INDENT_OUTPUT)
      .build()
    mapper.writeValue(tmpFile, errorReader.errorInfoMap)
    val rereadErrorClassToInfoMap = mapper.readValue(
      tmpFile, new TypeReference[Map[String, ErrorInfo]]() {})
    assert(rereadErrorClassToInfoMap == errorReader.errorInfoMap)
  }

  test("Check if error class is missing") {
    val ex1 = intercept[SparkException] {
      getMessage("", null, Map.empty[String, String])
    }
    assert(ex1.getMessage.contains("Cannot find main error class"))

    val ex2 = intercept[SparkException] {
      getMessage("LOREM_IPSUM", null, Map.empty[String, String])
    }
    assert(ex2.getMessage.contains("Cannot find main error class"))
  }

  test("Check if message parameters match message format") {
    // Requires 2 args
    val e = intercept[SparkException] {
      getMessage("UNRESOLVED_COLUMN", "WITHOUT_SUGGESTION", Map.empty[String, String])
    }
    assert(e.getErrorClass === "INTERNAL_ERROR")
    assert(e.getMessageParameters().get("message").contains("Undefined error message parameter"))

    // Does not fail with too many args (expects 0 args)
    assert(getMessage("DIVIDE_BY_ZERO", null, Map("config" -> "foo", "a" -> "bar")) ==
      "[DIVIDE_BY_ZERO] Division by zero. " +
      "Use `try_divide` to tolerate divisor being 0 and return NULL instead. " +
        "If necessary set foo to \"false\" " +
        "to bypass this error.")
  }

  test("Error message is formatted") {
    assert(
      getMessage(
        "UNRESOLVED_COLUMN",
        "WITH_SUGGESTION",
        Map("objectName" -> "`foo`", "proposal" -> "`bar`, `baz`")
      ) ==
      "[UNRESOLVED_COLUMN.WITH_SUGGESTION] A column or function parameter with " +
        "name `foo` cannot be resolved. Did you mean one of the following? [`bar`, `baz`]"
    )

    assert(
      getMessage(
        "UNRESOLVED_COLUMN",
        "WITH_SUGGESTION",
        Map(
          "objectName" -> "`foo`",
          "proposal" -> "`bar`, `baz`"),
        ""
      ) ==
      "[UNRESOLVED_COLUMN.WITH_SUGGESTION] A column or function parameter with " +
        "name `foo` cannot be resolved. Did you mean one of the following? [`bar`, `baz`]"
    )
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
        errorClass = "CANNOT_PARSE_DECIMAL",
        messageParameters = Map.empty,
        cause = null)
    } catch {
      case e: SparkThrowable =>
        assert(e.getErrorClass == "CANNOT_PARSE_DECIMAL")
        assert(e.getSqlState == "42000")
      case _: Throwable =>
        // Should not end up here
        assert(false)
    }
  }

  test("Try catching internal SparkError") {
    try {
      throw new SparkException(
        errorClass = "INTERNAL_ERROR",
        messageParameters = Map("message" -> "this is an internal error"),
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

  test("Get message in the specified format") {
    import ErrorMessageFormat._
    class TestQueryContext extends QueryContext {
      override val objectName = "v1"
      override val objectType = "VIEW"
      override val startIndex = 2
      override val stopIndex = -1
      override val fragment = "1 / 0"
    }
    val e = new SparkArithmeticException(
      errorClass = "DIVIDE_BY_ZERO",
      errorSubClass = None,
      messageParameters = Map("config" -> "CONFIG"),
      context = Array(new TestQueryContext),
      summary = "Query summary")

    assert(SparkThrowableHelper.getMessage(e, PRETTY) ===
      "[DIVIDE_BY_ZERO] Division by zero. Use `try_divide` to tolerate divisor being 0 " +
      "and return NULL instead. If necessary set CONFIG to \"false\" to bypass this error." +
      "\nQuery summary")
    // scalastyle:off line.size.limit
    assert(SparkThrowableHelper.getMessage(e, MINIMAL) ===
      """{
        |  "errorClass" : "DIVIDE_BY_ZERO",
        |  "sqlState" : "22012",
        |  "messageParameters" : {
        |    "config" : "CONFIG"
        |  },
        |  "queryContext" : [ {
        |    "objectType" : "VIEW",
        |    "objectName" : "v1",
        |    "startIndex" : 3,
        |    "fragment" : "1 / 0"
        |  } ]
        |}""".stripMargin)
    assert(SparkThrowableHelper.getMessage(e, STANDARD) ===
      """{
        |  "errorClass" : "DIVIDE_BY_ZERO",
        |  "messageTemplate" : "Division by zero. Use `try_divide` to tolerate divisor being 0 and return NULL instead. If necessary set <config> to \"false\" to bypass this error.",
        |  "sqlState" : "22012",
        |  "messageParameters" : {
        |    "config" : "CONFIG"
        |  },
        |  "queryContext" : [ {
        |    "objectType" : "VIEW",
        |    "objectName" : "v1",
        |    "startIndex" : 3,
        |    "fragment" : "1 / 0"
        |  } ]
        |}""".stripMargin)
      // scalastyle:on line.size.limit
    // STANDARD w/ errorSubClass but w/o queryContext
    val e2 = new SparkIllegalArgumentException(
      errorClass = "UNSUPPORTED_SAVE_MODE",
      errorSubClass = Some("EXISTENT_PATH"),
      messageParameters = Map("saveMode" -> "UNSUPPORTED_MODE"))
    assert(SparkThrowableHelper.getMessage(e2, STANDARD) ===
      """{
        |  "errorClass" : "UNSUPPORTED_SAVE_MODE",
        |  "errorSubClass" : "EXISTENT_PATH",
        |  "messageTemplate" : "The save mode <saveMode> is not supported for: an existent path.",
        |  "messageParameters" : {
        |    "saveMode" : "UNSUPPORTED_MODE"
        |  }
        |}""".stripMargin)
    // Legacy mode when an exception does not have any error class
    class LegacyException extends Throwable with SparkThrowable {
      override def getErrorClass: String = null
      override def getMessage: String = "Test message"
    }
    val e3 = new LegacyException
    assert(SparkThrowableHelper.getMessage(e3, MINIMAL) ===
      """{
        |  "errorClass" : "LEGACY",
        |  "messageParameters" : {
        |    "message" : "Test message"
        |  }
        |}""".stripMargin)
  }

  test("overwrite error classes") {
    withTempDir { dir =>
      val json = new File(dir, "errors.json")
      FileUtils.writeStringToFile(json,
        """
          |{
          |  "DIVIDE_BY_ZERO" : {
          |    "message" : [
          |      "abc"
          |    ]
          |  }
          |}
          |""".stripMargin)
      val reader = new ErrorClassesJsonReader(Seq(errorJsonFilePath.toUri.toURL, json.toURL))
      assert(reader.getErrorMessage("DIVIDE_BY_ZERO", Map.empty) == "abc")
    }
  }
}
