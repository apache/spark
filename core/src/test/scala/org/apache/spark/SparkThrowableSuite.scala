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

import scala.util.Properties.lineSeparator

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
        "core/testOnly *SparkThrowableSuite -- -t \"Error conditions are correctly formatted\""
   }}}
   */
  private val regenerateCommand = "SPARK_GENERATE_GOLDEN_FILES=1 build/sbt " +
    "\"core/testOnly *SparkThrowableSuite -- -t \\\"Error classes match with document\\\"\""

  private val errorJsonFilePath = getWorkspaceFilePath(
    "common", "utils", "src", "main", "resources", "error", "error-conditions.json")

  private val errorReader = new ErrorClassesJsonReader(Seq(errorJsonFilePath.toUri.toURL))

  override def beforeAll(): Unit = {
    super.beforeAll()
  }

  def checkIfUnique(ss: Seq[Any]): Unit = {
    val dups = ss.groupBy(identity).transform((_, v) => v.size).filter(_._2 > 1).keys.toSeq
    assert(dups.isEmpty, s"Duplicate error classes: ${dups.mkString(", ")}")
  }

  def checkCondition(ss: Seq[String], fx: String => Boolean): Unit = {
    ss.foreach { s =>
      assert(fx(s), s)
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

  test("Error conditions are correctly formatted") {
    val errorConditionFileContents =
      IOUtils.toString(errorJsonFilePath.toUri.toURL.openStream(), StandardCharsets.UTF_8)
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
      if (rewrittenString.trim != errorConditionFileContents.trim) {
        val errorConditionsFile = errorJsonFilePath.toFile
        logInfo(s"Regenerating error conditions file $errorConditionsFile")
        Files.delete(errorConditionsFile.toPath)
        FileUtils.writeStringToFile(
          errorConditionsFile,
          rewrittenString + lineSeparator,
          StandardCharsets.UTF_8)
      }
    } else {
      assert(rewrittenString.trim == errorConditionFileContents.trim)
    }
  }

  test("SQLSTATE is mandatory") {
    val errorConditionsNoSqlState = errorReader.errorInfoMap.filter {
      case (error: String, info: ErrorInfo) =>
        !error.startsWith("_LEGACY_ERROR_TEMP") && info.sqlState.isEmpty
    }.keys.toSeq
    assert(errorConditionsNoSqlState.isEmpty,
      s"Error classes without SQLSTATE: ${errorConditionsNoSqlState.mkString(", ")}")
  }

  test("Error class and error state / SQLSTATE invariants") {
    val errorClassesJson = Utils.getSparkClassLoader.getResource("error/error-classes.json")
    val errorStatesJson = Utils.getSparkClassLoader.getResource("error/error-states.json")
    val mapper = JsonMapper.builder()
      .addModule(DefaultScalaModule)
      .enable(STRICT_DUPLICATE_DETECTION)
      .build()
    val errorClasses = mapper.readValue(
      errorClassesJson, new TypeReference[Map[String, String]]() {})
    val errorStates = mapper.readValue(
      errorStatesJson, new TypeReference[Map[String, ErrorStateInfo]]() {})
    val errorConditionStates = errorReader.errorInfoMap.values.toSeq.flatMap(_.sqlState).toSet
    assert(Set("22012", "22003", "42601").subsetOf(errorStates.keySet))
    assert(errorClasses.keySet.filter(!_.matches("[A-Z0-9]{2}")).isEmpty)
    assert(errorStates.keySet.filter(!_.matches("[A-Z0-9]{5}")).isEmpty)
    assert(errorStates.keySet.map(_.substring(0, 2)).diff(errorClasses.keySet).isEmpty)
    assert(errorConditionStates.diff(errorStates.keySet).isEmpty)
  }

  test("Message invariants") {
    val messageSeq = errorReader.errorInfoMap.values.toSeq.flatMap { i =>
      Seq(i.message) ++ i.subClass.getOrElse(Map.empty).values.toSeq.map(_.message)
    }
    messageSeq.foreach { message =>
      message.foreach { msg =>
        // Error messages in the JSON file should not contain newline characters:
        // newlines are delineated as different elements in the array.
        assert(!msg.contains("\n"))
        assert(msg.trim == msg)
      }
    }
  }

  test("Message format invariants") {
    val messageFormats = errorReader.errorInfoMap
      .filter { case (k, _) => !k.startsWith("_LEGACY_ERROR_") }
      .filter { case (k, _) => !k.startsWith("INTERNAL_ERROR") }
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
    val rereadErrorConditionToInfoMap = mapper.readValue(
      tmpFile, new TypeReference[Map[String, ErrorInfo]]() {})
    assert(rereadErrorConditionToInfoMap == errorReader.errorInfoMap)
  }

  test("Error class names should contain only capital letters, numbers and underscores") {
    val allowedChars = "[A-Z0-9_]*"
    errorReader.errorInfoMap.foreach { e =>
      assert(e._1.matches(allowedChars), s"Error class: ${e._1} is invalid")
      e._2.subClass.map { s =>
        s.keys.foreach { k =>
          assert(k.matches(allowedChars), s"Error sub-class: $k is invalid")
        }
      }
    }
  }

  test("Check if error class is missing") {
    val ex1 = intercept[SparkException] {
      getMessage("", Map.empty[String, String])
    }
    assert(ex1.getMessage.contains("Cannot find main error class"))

    val ex2 = intercept[SparkException] {
      getMessage("LOREM_IPSUM", Map.empty[String, String])
    }
    assert(ex2.getMessage.contains("Cannot find main error class"))
  }

  test("Check if message parameters match message format") {
    // Requires 2 args
    val e = intercept[SparkException] {
      getMessage("UNRESOLVED_COLUMN.WITHOUT_SUGGESTION", Map.empty[String, String])
    }
    assert(e.getErrorClass === "INTERNAL_ERROR")
    assert(e.getMessageParameters().get("message").contains("Undefined error message parameter"))
  }

  test("Error message is formatted") {
    assert(
      getMessage(
        "UNRESOLVED_COLUMN.WITH_SUGGESTION",
        Map("objectName" -> "`foo`", "proposal" -> "`bar`, `baz`")
      ) ==
      "[UNRESOLVED_COLUMN.WITH_SUGGESTION] A column, variable, or function parameter with " +
        "name `foo` cannot be resolved. Did you mean one of the following? [`bar`, `baz`]." +
      " SQLSTATE: 42703"
    )

    assert(
      getMessage(
        "UNRESOLVED_COLUMN.WITH_SUGGESTION",
        Map(
          "objectName" -> "`foo`",
          "proposal" -> "`bar`, `baz`"),
        ""
      ) ==
      "[UNRESOLVED_COLUMN.WITH_SUGGESTION] A column, variable, or function parameter with " +
        "name `foo` cannot be resolved. Did you mean one of the following? [`bar`, `baz`]." +
        " SQLSTATE: 42703"
    )
  }

  test("Error message does not do substitution on values") {
    assert(
      getMessage(
        "UNRESOLVED_COLUMN.WITH_SUGGESTION",
        Map("objectName" -> "`foo`", "proposal" -> "`${bar}`, `baz`")
      ) ==
        "[UNRESOLVED_COLUMN.WITH_SUGGESTION] A column, variable, or function parameter with " +
          "name `foo` cannot be resolved. Did you mean one of the following? [`${bar}`, `baz`]." +
          " SQLSTATE: 42703"
    )
  }

  test("Try catching legacy SparkError") {
    try {
      throw new SparkException("Arbitrary legacy message")
    } catch {
      case e: SparkThrowable =>
        assert(e.getErrorClass == null)
        assert(!e.isInternalError)
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
        assert(!e.isInternalError)
        assert(e.getSqlState == "22018")
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
        assert(e.getSqlState.startsWith("XX"))
      case _: Throwable =>
        // Should not end up here
        assert(false)
    }
  }

  test("Get message in the specified format") {
    import ErrorMessageFormat._
    class TestQueryContext extends QueryContext {
      override val contextType = QueryContextType.SQL
      override val objectName = "v1"
      override val objectType = "VIEW"
      override val startIndex = 2
      override val stopIndex = -1
      override val fragment = "1 / 0"
      override def callSite: String = throw new UnsupportedOperationException
      override val summary = ""
    }
    val e = new SparkArithmeticException(
      errorClass = "DIVIDE_BY_ZERO",
      messageParameters = Map("config" -> "CONFIG"),
      context = Array(new TestQueryContext),
      summary = "Query summary")

    assert(SparkThrowableHelper.getMessage(e, PRETTY) ===
      "[DIVIDE_BY_ZERO] Division by zero. Use `try_divide` to tolerate divisor being 0 " +
        "and return NULL instead. If necessary set CONFIG to \"false\" to bypass this error." +
        " SQLSTATE: 22012" +
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
      errorClass = "UNSUPPORTED_SAVE_MODE.EXISTENT_PATH",
      messageParameters = Map("saveMode" -> "UNSUPPORTED_MODE"))
    assert(SparkThrowableHelper.getMessage(e2, STANDARD) ===
      """{
        |  "errorClass" : "UNSUPPORTED_SAVE_MODE.EXISTENT_PATH",
        |  "messageTemplate" : "The save mode <saveMode> is not supported for: an existent path.",
        |  "sqlState" : "0A000",
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

    class TestQueryContext2 extends QueryContext {
      override val contextType = QueryContextType.DataFrame
      override def objectName: String = throw new UnsupportedOperationException
      override def objectType: String = throw new UnsupportedOperationException
      override def startIndex: Int = throw new UnsupportedOperationException
      override def stopIndex: Int = throw new UnsupportedOperationException
      override val fragment: String = "div"
      override val callSite: String = "SimpleApp$.main(SimpleApp.scala:9)"
      override val summary = ""
    }
    val e4 = new SparkArithmeticException(
      errorClass = "DIVIDE_BY_ZERO",
      messageParameters = Map("config" -> "CONFIG"),
      context = Array(new TestQueryContext2),
      summary = "Query summary")

    assert(SparkThrowableHelper.getMessage(e4, PRETTY) ===
        "[DIVIDE_BY_ZERO] Division by zero. Use `try_divide` to tolerate divisor being 0 " +
            "and return NULL instead. If necessary set CONFIG to \"false\" to bypass this error." +
            " SQLSTATE: 22012\nQuery summary")
    // scalastyle:off line.size.limit
    assert(SparkThrowableHelper.getMessage(e4, MINIMAL) ===
        """{
          |  "errorClass" : "DIVIDE_BY_ZERO",
          |  "sqlState" : "22012",
          |  "messageParameters" : {
          |    "config" : "CONFIG"
          |  },
          |  "queryContext" : [ {
          |    "fragment" : "div",
          |    "callSite" : "SimpleApp$.main(SimpleApp.scala:9)"
          |  } ]
          |}""".stripMargin)
    assert(SparkThrowableHelper.getMessage(e4, STANDARD) ===
        """{
          |  "errorClass" : "DIVIDE_BY_ZERO",
          |  "messageTemplate" : "Division by zero. Use `try_divide` to tolerate divisor being 0 and return NULL instead. If necessary set <config> to \"false\" to bypass this error.",
          |  "sqlState" : "22012",
          |  "messageParameters" : {
          |    "config" : "CONFIG"
          |  },
          |  "queryContext" : [ {
          |    "fragment" : "div",
          |    "callSite" : "SimpleApp$.main(SimpleApp.scala:9)"
          |  } ]
          |}""".stripMargin)
    // scalastyle:on line.size.limit
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
          |""".stripMargin, StandardCharsets.UTF_8)
      val reader = new ErrorClassesJsonReader(Seq(errorJsonFilePath.toUri.toURL, json.toURI.toURL))
      assert(reader.getErrorMessage("DIVIDE_BY_ZERO", Map.empty) == "abc")
    }
  }

  test("prohibit dots in error class names") {
    withTempDir { dir =>
      val json = new File(dir, "errors.json")
      FileUtils.writeStringToFile(json,
        """
          |{
          |  "DIVIDE.BY_ZERO" : {
          |    "message" : [
          |      "abc"
          |    ]
          |  }
          |}
          |""".stripMargin, StandardCharsets.UTF_8)
      val e = intercept[SparkException] {
        new ErrorClassesJsonReader(Seq(errorJsonFilePath.toUri.toURL, json.toURI.toURL))
      }
      assert(e.getErrorClass === "INTERNAL_ERROR")
      assert(e.getMessage.contains("DIVIDE.BY_ZERO"))
    }

    withTempDir { dir =>
      val json = new File(dir, "errors.json")
      FileUtils.writeStringToFile(json,
        """
          |{
          |  "DIVIDE" : {
          |    "message" : [
          |      "abc"
          |    ],
          |    "subClass" : {
          |      "BY.ZERO" : {
          |        "message" : [
          |          "def"
          |        ]
          |      }
          |    }
          |  }
          |}
          |""".stripMargin, StandardCharsets.UTF_8)
      val e = intercept[SparkException] {
        new ErrorClassesJsonReader(Seq(errorJsonFilePath.toUri.toURL, json.toURI.toURL))
      }
      assert(e.getErrorClass === "INTERNAL_ERROR")
      assert(e.getMessage.contains("BY.ZERO"))
    }
  }

  test("handle null values in message parameters") {
    withTempDir { dir =>
      val json = new File(dir, "errors.json")
      FileUtils.writeStringToFile(json,
        """
          |{
          |  "MISSING_PARAMETER" : {
          |    "message" : [
          |      "Parameter <param> is missing."
          |    ]
          |  }
          |}
          |""".stripMargin, StandardCharsets.UTF_8)

      val reader = new ErrorClassesJsonReader(Seq(errorJsonFilePath.toUri.toURL, json.toURI.toURL))
      // Attempt to get the error message with a null parameter
      val errorMessage = reader.getErrorMessage("MISSING_PARAMETER", Map("param" -> null))

      assert(errorMessage.contains("Parameter null is missing."))
    }
  }

  test("detect unused message parameters") {
    checkError(
      exception = intercept[SparkException] {
        SparkThrowableHelper.getMessage(
          errorClass = "CANNOT_UP_CAST_DATATYPE",
          messageParameters = Map(
            "expression" -> "CAST('aaa' AS LONG)",
            "sourceType" -> "STRING",
            "targetType" -> "LONG",
            "op" -> "CAST", // unused parameter
            "details" -> "implicit cast"
          ))
      },
      errorClass = "INTERNAL_ERROR",
      parameters = Map(
        "message" ->
          ("Found unused message parameters of the error class 'CANNOT_UP_CAST_DATATYPE'. " +
          "Its error message format has 4 placeholders, but the passed message parameters map " +
          "has 5 items. Consider to add placeholders to the error format or " +
          "remove unused message parameters.")
      )
    )
  }
}
