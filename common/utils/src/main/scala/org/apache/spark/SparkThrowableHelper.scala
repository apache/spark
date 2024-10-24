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

import scala.jdk.CollectionConverters._

import org.apache.spark.util.JsonUtils.toJsonString
import org.apache.spark.util.SparkClassUtils

private[spark] object ErrorMessageFormat extends Enumeration {
  val PRETTY, MINIMAL, STANDARD = Value
}

/**
 * Companion object used by instances of [[SparkThrowable]] to access error class information and
 * construct error messages.
 */
private[spark] object SparkThrowableHelper {
  val errorReader = new ErrorClassesJsonReader(
    // Note that though we call them "error classes" here, the proper name is "error conditions",
    // hence why the name of the JSON file is different. We will address this inconsistency as part
    // of this ticket: https://issues.apache.org/jira/browse/SPARK-47429
    Seq(SparkClassUtils.getSparkClassLoader.getResource("error/error-conditions.json")))

  def getMessage(
      errorClass: String,
      messageParameters: Map[String, String]): String = {
    getMessage(errorClass, messageParameters, "")
  }

  def getMessage(
      errorClass: String,
      messageParameters: java.util.Map[String, String]): String = {
    getMessage(errorClass, messageParameters.asScala.toMap, "")
  }

  def getMessage(
      errorClass: String,
      messageParameters: Map[String, String],
      context: String): String = {
    val displayMessage = errorReader.getErrorMessage(errorClass, messageParameters)
    val sqlState = getSqlState(errorClass)
    val displaySqlState = if (sqlState == null) "" else s" SQLSTATE: $sqlState"
    val displayQueryContext = (if (context.isEmpty) "" else "\n") + context
    val prefix = if (errorClass.startsWith("_LEGACY_ERROR_")) "" else s"[$errorClass] "
    s"$prefix$displayMessage$displaySqlState$displayQueryContext"
  }

  def getSqlState(errorClass: String): String = {
    errorReader.getSqlState(errorClass)
  }

  def isValidErrorClass(errorClass: String): Boolean = {
    errorReader.isValidErrorClass(errorClass)
  }

  def getMessageParameters(errorClass: String): Seq[String] = {
    errorReader.getMessageParameters(errorClass)
  }

  def isInternalError(errorClass: String): Boolean = {
    errorClass != null && errorClass.startsWith("INTERNAL_ERROR")
  }

  def getMessage(e: SparkThrowable with Throwable, format: ErrorMessageFormat.Value): String = {
    import ErrorMessageFormat._
    format match {
      case PRETTY => e.getMessage
      case MINIMAL | STANDARD if e.getCondition == null =>
        toJsonString { generator =>
          val g = generator.useDefaultPrettyPrinter()
          g.writeStartObject()
          g.writeStringField("errorClass", "LEGACY")
          g.writeObjectFieldStart("messageParameters")
          g.writeStringField("message", e.getMessage)
          g.writeEndObject()
          g.writeEndObject()
        }
      case MINIMAL | STANDARD =>
        val errorClass = e.getCondition
        toJsonString { generator =>
          val g = generator.useDefaultPrettyPrinter()
          g.writeStartObject()
          g.writeStringField("errorClass", errorClass)
          if (format == STANDARD) {
            g.writeStringField("messageTemplate", errorReader.getMessageTemplate(errorClass))
          }
          val sqlState = e.getSqlState
          if (sqlState != null) g.writeStringField("sqlState", sqlState)
          val messageParameters = e.getMessageParameters
          if (!messageParameters.isEmpty) {
            g.writeObjectFieldStart("messageParameters")
            messageParameters.asScala
              .toMap // To remove duplicates
              .toSeq.sortBy(_._1)
              .foreach { case (name, value) =>
                g.writeStringField(name, value.replaceAll("#\\d+", "#x")) }
            g.writeEndObject()
          }
          val queryContext = e.getQueryContext
          if (!queryContext.isEmpty) {
            g.writeArrayFieldStart("queryContext")
            e.getQueryContext.foreach { c =>
              g.writeStartObject()
              c.contextType() match {
                case QueryContextType.SQL =>
                  g.writeStringField("objectType", c.objectType())
                  g.writeStringField("objectName", c.objectName())
                  val startIndex = c.startIndex() + 1
                  if (startIndex > 0) g.writeNumberField("startIndex", startIndex)
                  val stopIndex = c.stopIndex() + 1
                  if (stopIndex > 0) g.writeNumberField("stopIndex", stopIndex)
                  g.writeStringField("fragment", c.fragment())
                case QueryContextType.DataFrame =>
                  g.writeStringField("fragment", c.fragment())
                  g.writeStringField("callSite", c.callSite())
              }
              g.writeEndObject()
            }
            g.writeEndArray()
          }
          g.writeEndObject()
        }
    }
  }
}
