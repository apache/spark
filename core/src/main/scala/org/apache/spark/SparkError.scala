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

import com.fasterxml.jackson.annotation.JsonIgnore
import com.fasterxml.jackson.core.`type`.TypeReference
import com.fasterxml.jackson.databind.json.JsonMapper
import com.fasterxml.jackson.module.scala.DefaultScalaModule

import org.apache.spark.internal.config.{
  SHOW_SPARK_ERROR_FIELDS, SHOW_STACKTRACE_FOR_USER_FACING_ERRORS}
import org.apache.spark.util.Utils

/**
 * Information associated with an error class.
 *
 * @param sqlState SQLSTATE associated with this class.
 * @param messageFormatLines C-style message format compatible with printf.
 *                           The error message is constructed by concatenating the lines with
 *                           linebreaks.
 */
case class ErrorInfo(sqlState: Option[String], messageFormatLines: Seq[String]) {
  @JsonIgnore
  val messageFormat: String = messageFormatLines.mkString("\n")
}

/**
 * Companion object used by instances of [[SparkError]] to access error class information and
 * construct error messages.
 */
object SparkError {
  val errorClassesUrl: URL = Utils.getSparkClassLoader.getResource("error/error-classes.json")
  val mapper: JsonMapper = JsonMapper.builder()
    .addModule(DefaultScalaModule)
    .build()
  val errorClassToInfoMap: Map[String, ErrorInfo] =
    mapper.readValue(errorClassesUrl, new TypeReference[Map[String, ErrorInfo]]() {})

  def getMessage(errorClass: String, messageParameters: Seq[String]): String = {
    val errorInfo = errorClassToInfoMap.getOrElse(errorClass,
      throw new IllegalArgumentException(s"Cannot find error class '$errorClass'"))
    val errorMessage = String.format(errorInfo.messageFormat, messageParameters: _*)
    if (new SparkConf().get(SHOW_SPARK_ERROR_FIELDS)) {
      val sqlStateStr = if (errorInfo.sqlState.isDefined) {
        s" (SQLSTATE ${errorInfo.sqlState.get})"
      } else {
        ""
      }
      s"$errorClass: $errorMessage$sqlStateStr"
    } else {
      errorMessage
    }
  }
}

/**
 * Trait mixed into exceptions thrown from Spark.
 * - For backwards compatibility, existing exception types can be thrown with an arbitrary error
 *   message with no error class. See [[SparkException]].
 * - To promote standardization, exceptions should be thrown with an error class and message
 *   parameters to construct an error message with [[SparkError.getMessage()]]. New exception types
 *   should not accept arbitrary error messages. See [[SparkArithmeticException]].
 */
trait SparkError extends Throwable {
  // Should be provided during Exception invocation
  val errorClass: Option[String] = None
  val messageParameters: Seq[String] = Seq.empty

  // Derived from error class
  val errorInfo: Option[ErrorInfo] = errorClass.flatMap(SparkError.errorClassToInfoMap.get)
  // None if the error class or SQLSTATE are not set
  val sqlState: Option[String] = errorInfo.flatMap(_.sqlState)
  // None if the error class is not set
  val messageFormat: Option[String] = errorInfo.map(_.messageFormat)
}
