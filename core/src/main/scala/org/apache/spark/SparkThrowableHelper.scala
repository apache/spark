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

import scala.collection.immutable.SortedMap

import com.fasterxml.jackson.annotation.JsonIgnore
import com.fasterxml.jackson.core.`type`.TypeReference
import com.fasterxml.jackson.databind.json.JsonMapper
import com.fasterxml.jackson.module.scala.DefaultScalaModule
import org.json4s.{JInt, JString}
import org.json4s.JsonAST.{JArray, JObject}
import org.json4s.JsonDSL._
import org.json4s.jackson.JsonMethods.{compact, render}

import org.apache.spark.util.Utils

/**
 * Information associated with an error subclass.
 *
 * @param message C-style message format compatible with printf.
 *                The error message is constructed by concatenating the lines with newlines.
 */
private[spark] case class ErrorSubInfo(message: Seq[String]) {
  // For compatibility with multi-line error messages
  @JsonIgnore
  val messageFormat: String = message.mkString("\n")
}

/**
 * Information associated with an error class.
 *
 * @param sqlState SQLSTATE associated with this class.
 * @param subClass SubClass associated with this class.
 * @param message C-style message format compatible with printf.
 *                The error message is constructed by concatenating the lines with newlines.
 */
private[spark] case class ErrorInfo(
    message: Seq[String],
    subClass: Option[Map[String, ErrorSubInfo]],
    sqlState: Option[String]) {
  // For compatibility with multi-line error messages
  @JsonIgnore
  val messageFormat: String = message.mkString("\n")
}

object ErrorMessageFormat extends Enumeration {
  val PRETTY, MINIMAL, STANDARD = Value
}

/**
 * Companion object used by instances of [[SparkThrowable]] to access error class information and
 * construct error messages.
 */
private[spark] object SparkThrowableHelper {
  val errorClassesUrl: URL =
    Utils.getSparkClassLoader.getResource("error/error-classes.json")
  val errorClassToInfoMap: SortedMap[String, ErrorInfo] = {
    val mapper: JsonMapper = JsonMapper.builder()
      .addModule(DefaultScalaModule)
      .build()
    mapper.readValue(errorClassesUrl, new TypeReference[SortedMap[String, ErrorInfo]]() {})
  }

  def getMessage(
      errorClass: String,
      errorSubClass: String,
      messageParameters: Array[String]): String = {
    getMessage(errorClass, errorSubClass, messageParameters, "")
  }

  def getMessage(
      errorClass: String,
      errorSubClass: String,
      messageParameters: Array[String],
      context: String): String = {
    val errorInfo = errorClassToInfoMap.getOrElse(errorClass,
      throw new IllegalArgumentException(s"Cannot find error class '$errorClass'"))
    val (displayClass, displayMessageParameters, displayFormat) = if (errorInfo.subClass.isEmpty) {
      (errorClass, messageParameters, errorInfo.messageFormat)
    } else {
      val subClasses = errorInfo.subClass.get
      if (errorSubClass == null) {
        throw new IllegalArgumentException(s"Subclass required for error class '$errorClass'")
      }
      val errorSubInfo = subClasses.getOrElse(errorSubClass,
        throw new IllegalArgumentException(s"Cannot find sub error class '$errorSubClass'"))
      (errorClass + "." + errorSubClass, messageParameters,
        errorInfo.messageFormat + " " + errorSubInfo.messageFormat)
    }
    val displayMessage = String.format(
      displayFormat.replaceAll("<[a-zA-Z0-9_-]+>", "%s"),
      displayMessageParameters : _*)
    val displayQueryContext = (if (context.isEmpty) "" else "\n") + context

    s"[$displayClass] $displayMessage$displayQueryContext"
  }

  def getParameterNames(errorClass: String, errorSubCLass: String): Array[String] = {
    val errorInfo = errorClassToInfoMap.getOrElse(errorClass,
      throw new IllegalArgumentException(s"Cannot find error class '$errorClass'"))
    if (errorInfo.subClass.isEmpty && errorSubCLass != null) {
      throw new IllegalArgumentException(s"'$errorClass' has no subclass")
    }
    if (errorInfo.subClass.isDefined && errorSubCLass == null) {
      throw new IllegalArgumentException(s"'$errorClass' requires subclass")
    }
    var parameterizedMessage = errorInfo.messageFormat
    if (errorInfo.subClass.isDefined) {
      val givenSubClass = errorSubCLass
      val errorSubInfo = errorInfo.subClass.get.getOrElse(givenSubClass,
        throw new IllegalArgumentException(s"Cannot find sub error class '$givenSubClass'"))
      parameterizedMessage = parameterizedMessage + errorSubInfo.messageFormat
    }
    val pattern = "<[a-zA-Z0-9_-]+>".r
    val matches = pattern.findAllIn(parameterizedMessage)
    val parameterSeq = matches.toArray
    val parameterNames = parameterSeq.map(p => p.stripPrefix("<").stripSuffix(">"))
    parameterNames
  }

  def getSqlState(errorClass: String): String = {
    Option(errorClass).flatMap(errorClassToInfoMap.get).flatMap(_.sqlState).orNull
  }

  def isInternalError(errorClass: String): Boolean = {
    errorClass == "INTERNAL_ERROR"
  }

  def getMessage(e: SparkThrowable with Throwable, format: ErrorMessageFormat.Value): String = {
    import ErrorMessageFormat._
    format match {
      case PRETTY => e.getMessage
      case MINIMAL | STANDARD if e.getErrorClass == null =>
        val jValue = ("errorClass" -> "legacy") ~
          ("messageParameters" -> JObject(List("message" -> JString(e.getMessage)))) ~
          ("queryContext" -> JArray(List.empty))
        compact(render(jValue))
      case MINIMAL | STANDARD =>
        val message = if (format == STANDARD) Some(e.getMessage) else None
        assert(e.getParameterNames.size == e.getMessageParameters.size,
          "Number of message parameter names and values must be the same")
        val jValue = ("errorClass" -> e.getErrorClass) ~
          ("errorSubClass" -> Option(e.getErrorSubClass)) ~
          ("message" -> message) ~
          ("sqlState" -> Option(e.getSqlState)) ~
          ("messageParameters" ->
            JObject((e.getParameterNames zip e.getMessageParameters.map(JString)).toList)) ~
          ("queryContext" -> JArray(
            e.getQueryContext.map(c => JObject(
              "objectType" -> JString(c.objectType()),
              "objectName" -> JString(c.objectName()),
              "startIndex" -> JInt(c.startIndex()),
              "stopIndex" -> JInt(c.stopIndex()),
              "fragment" -> JString(c.fragment()))).toList)
          )
        compact(render(jValue))
    }
  }
}
