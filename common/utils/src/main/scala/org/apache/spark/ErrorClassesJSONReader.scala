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

import java.net.URL

import scala.jdk.CollectionConverters._

import com.fasterxml.jackson.annotation.JsonIgnore
import com.fasterxml.jackson.core.`type`.TypeReference
import com.fasterxml.jackson.databind.json.JsonMapper
import com.fasterxml.jackson.module.scala.DefaultScalaModule
import org.apache.commons.text.StringSubstitutor

import org.apache.spark.annotation.DeveloperApi

/**
 * A reader to load error information from one or more JSON files. Note that, if one error appears
 * in more than one JSON files, the latter wins.
 * Please read common/utils/src/main/resources/error/README.md for more details.
 */
@DeveloperApi
class ErrorClassesJsonReader(jsonFileURLs: Seq[URL]) {
  assert(jsonFileURLs.nonEmpty)

  // Exposed for testing
  private[spark] val errorInfoMap =
    jsonFileURLs.map(ErrorClassesJsonReader.readAsMap).reduce(_ ++ _)

  def getErrorMessage(errorClass: String, messageParameters: Map[String, Any]): String = {
    val messageTemplate = getMessageTemplate(errorClass)
    val sanitizedParameters = messageParameters.map {
      case (key, null) => key -> "null"
      case (key, value) => key -> value
    }
    val sub = new StringSubstitutor(sanitizedParameters.asJava)
    sub.setEnableUndefinedVariableException(true)
    sub.setDisableSubstitutionInValues(true)
    val errorMessage = try {
      sub.replace(ErrorClassesJsonReader.TEMPLATE_REGEX.replaceAllIn(
        messageTemplate, "\\$\\{$1\\}"))
    } catch {
      case i: IllegalArgumentException => throw SparkException.internalError(
        s"Undefined error message parameter for error class: '$errorClass', " +
          s"MessageTemplate: $messageTemplate, " +
          s"Parameters: $messageParameters", i)
    }
    if (util.SparkEnvUtils.isTesting) {
      val placeHoldersNum = ErrorClassesJsonReader.TEMPLATE_REGEX.findAllIn(messageTemplate).length
      if (placeHoldersNum < sanitizedParameters.size &&
          !ErrorClassesJsonReader.MORE_PARAMS_ALLOWLIST.contains(errorClass)) {
        throw SparkException.internalError(
          s"Found unused message parameters of the error class '$errorClass'. " +
          s"Its error message format has $placeHoldersNum placeholders, " +
          s"but the passed message parameters map has ${sanitizedParameters.size} items. " +
          "Consider to add placeholders to the error format or remove unused message parameters.")
      }
    }
    errorMessage
  }

  def getMessageParameters(errorClass: String): Seq[String] = {
    val messageTemplate = getMessageTemplate(errorClass)
    val matches = ErrorClassesJsonReader.TEMPLATE_REGEX.findAllIn(messageTemplate).toSeq
    matches.map(m => m.stripSuffix(">").stripPrefix("<"))
  }

  def getMessageTemplate(errorClass: String): String = {
    val errorClasses = errorClass.split("\\.")
    assert(errorClasses.length == 1 || errorClasses.length == 2)

    val mainErrorClass = errorClasses.head
    val subErrorClass = errorClasses.tail.headOption
    val errorInfo = errorInfoMap.getOrElse(
      mainErrorClass,
      throw SparkException.internalError(s"Cannot find main error class '$errorClass'"))
    assert(errorInfo.subClass.isDefined == subErrorClass.isDefined)

    if (subErrorClass.isEmpty) {
      errorInfo.messageTemplate
    } else {
      val errorSubInfo = errorInfo.subClass.get.getOrElse(
        subErrorClass.get,
        throw SparkException.internalError(s"Cannot find sub error class '$errorClass'"))
      errorInfo.messageTemplate + " " + errorSubInfo.messageTemplate
    }
  }

  def getSqlState(errorClass: String): String = {
    Option(errorClass)
      .flatMap(_.split('.').headOption)
      .flatMap(errorInfoMap.get)
      .flatMap(_.sqlState)
      .orNull
  }

  def isValidErrorClass(errorClass: String): Boolean = {
    val errorClasses = errorClass.split("\\.")
    errorClasses match {
      case Array(mainClass) => errorInfoMap.contains(mainClass)
      case Array(mainClass, subClass) => errorInfoMap.get(mainClass).exists { info =>
        info.subClass.get.contains(subClass)
      }
      case _ => false
    }
  }
}

private object ErrorClassesJsonReader {
  private val TEMPLATE_REGEX = "<([a-zA-Z0-9_-]+)>".r

  private val MORE_PARAMS_ALLOWLIST = Array("CAST_INVALID_INPUT", "CAST_OVERFLOW")

  private val mapper: JsonMapper = JsonMapper.builder()
    .addModule(DefaultScalaModule)
    .build()
  private def readAsMap(url: URL): Map[String, ErrorInfo] = {
    val map = mapper.readValue(url, new TypeReference[Map[String, ErrorInfo]]() {})
    val errorClassWithDots = map.collectFirst {
      case (errorClass, _) if errorClass.contains('.') => errorClass
      case (_, ErrorInfo(_, Some(map), _)) if map.keys.exists(_.contains('.')) =>
        map.keys.collectFirst { case s if s.contains('.') => s }.get
    }
    if (errorClassWithDots.isEmpty) {
      map
    } else {
      throw SparkException.internalError(
        s"Found the (sub-)error class with dots: ${errorClassWithDots.get}")
    }
  }
}

/**
 * Information associated with an error class.
 *
 * @param sqlState SQLSTATE associated with this class.
 * @param subClass SubClass associated with this class.
 * @param message Message format with optional placeholders (e.g. &lt;parm&gt;).
 *                The error message is constructed by concatenating the lines with newlines.
 */
private case class ErrorInfo(
    message: Seq[String],
    subClass: Option[Map[String, ErrorSubInfo]],
    sqlState: Option[String]) {
  // For compatibility with multi-line error messages
  @JsonIgnore
  val messageTemplate: String = message.mkString("\n")
}

/**
 * Information associated with an error subclass.
 *
 * @param message Message format with optional placeholders (e.g. &lt;parm&gt;).
 *                The error message is constructed by concatenating the lines with newlines.
 */
private case class ErrorSubInfo(message: Seq[String]) {
  // For compatibility with multi-line error messages
  @JsonIgnore
  val messageTemplate: String = message.mkString("\n")
}

/**
 * Information associated with an error state / SQLSTATE.
 *
 * @param description What the error state means.
 * @param origin The DBMS where this error state was first defined.
 * @param standard Whether this error state is part of the SQL standard.
 * @param usedBy What database systems use this error state.
 */
private case class ErrorStateInfo(
    description: String,
    origin: String,
    standard: String,
    usedBy: List[String])
