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

package org.apache.spark.sql

import scala.jdk.CollectionConverters._

import org.apache.spark.{QueryContext, SparkThrowable, SparkThrowableHelper}
import org.apache.spark.annotation.Stable
import org.apache.spark.sql.catalyst.trees.{Origin, WithOrigin}

/**
 * Thrown when a query fails to analyze, usually because the query itself is invalid.
 *
 * @since 1.3.0
 */
@Stable
class AnalysisException protected (
    val message: String,
    val line: Option[Int] = None,
    val startPosition: Option[Int] = None,
    val cause: Option[Throwable] = None,
    val errorClass: Option[String] = None,
    val messageParameters: Map[String, String] = Map.empty,
    val context: Array[QueryContext] = Array.empty,
    val sqlState: Option[String] = None,
    val messageTemplate: Option[String] = None)
    extends Exception(message, cause.orNull)
    with SparkThrowable
    with Serializable
    with WithOrigin {

  def this(
      message: String,
      line: Option[Int],
      startPosition: Option[Int],
      cause: Option[Throwable],
      errorClass: Option[String],
      messageParameters: Map[String, String],
      context: Array[QueryContext]) =
    this(message, line, startPosition, cause, errorClass, messageParameters, context, None, None)

  def this(errorClass: String, messageParameters: Map[String, String], cause: Option[Throwable]) =
    this(
      SparkThrowableHelper.getMessage(errorClass, messageParameters),
      errorClass = Some(errorClass),
      messageParameters = messageParameters,
      cause = cause)

  def this(
      errorClass: String,
      messageParameters: Map[String, String],
      context: Array[QueryContext],
      cause: Option[Throwable]) =
    this(
      SparkThrowableHelper.getMessage(errorClass, messageParameters),
      errorClass = Some(errorClass),
      messageParameters = messageParameters,
      context = context,
      cause = cause)

  /**
   * External constructor for callers that want to supply error fields directly, without requiring
   * a local JSON definition for the error class.
   *
   * If `message` is provided (Some), it is used verbatim. Otherwise, the message is rendered from
   * (errorClass, sqlState, messageTemplate, messageParameters).
   *
   * `messageTemplate` is always persisted into the exception so clients can read it via
   * SparkThrowable.getDefaultMessageTemplate().
   */
  def this(
      errorClass: String,
      sqlState: String,
      messageTemplate: String,
      messageParameters: Map[String, String],
      cause: Option[Throwable],
      message: Option[String]) =
    this(
      message = message.getOrElse(
        SparkThrowableHelper
          .getMessage(errorClass, sqlState, messageTemplate, messageParameters)),
      cause = cause,
      errorClass = Option(errorClass),
      messageParameters = messageParameters,
      sqlState = Option(sqlState),
      messageTemplate = Option(messageTemplate))

  def this(
      errorClass: String,
      messageParameters: Map[String, String],
      context: Array[QueryContext],
      summary: String) =
    this(
      SparkThrowableHelper.getMessage(errorClass, messageParameters, summary),
      errorClass = Some(errorClass),
      messageParameters = messageParameters,
      cause = null,
      context = context)

  def this(errorClass: String, messageParameters: Map[String, String]) =
    this(errorClass = errorClass, messageParameters = messageParameters, cause = None)

  def this(errorClass: String, messageParameters: Map[String, String], origin: Origin) =
    this(
      SparkThrowableHelper.getMessage(errorClass, messageParameters),
      line = origin.line,
      startPosition = origin.startPosition,
      errorClass = Some(errorClass),
      messageParameters = messageParameters,
      context = origin.getQueryContext)

  def this(
      errorClass: String,
      messageParameters: Map[String, String],
      origin: Origin,
      cause: Option[Throwable]) =
    this(
      SparkThrowableHelper.getMessage(errorClass, messageParameters),
      line = origin.line,
      startPosition = origin.startPosition,
      errorClass = Some(errorClass),
      messageParameters = messageParameters,
      context = origin.getQueryContext,
      cause = cause)

  def copy(
      message: String,
      line: Option[Int],
      startPosition: Option[Int],
      cause: Option[Throwable],
      errorClass: Option[String],
      messageParameters: Map[String, String],
      context: Array[QueryContext]): AnalysisException =
    new AnalysisException(
      message,
      line,
      startPosition,
      cause,
      errorClass,
      messageParameters,
      context,
      this.sqlState,
      this.messageTemplate)

  def copy(
      message: String = this.message,
      line: Option[Int] = this.line,
      startPosition: Option[Int] = this.startPosition,
      cause: Option[Throwable] = this.cause,
      errorClass: Option[String] = this.errorClass,
      messageParameters: Map[String, String] = this.messageParameters,
      context: Array[QueryContext] = this.context,
      sqlState: Option[String] = this.sqlState,
      messageTemplate: Option[String] = this.messageTemplate): AnalysisException =
    new AnalysisException(
      message,
      line,
      startPosition,
      cause,
      errorClass,
      messageParameters,
      context,
      sqlState,
      messageTemplate)

  def withPosition(origin: Origin): AnalysisException = {
    val newException = this.copy(
      line = origin.line,
      startPosition = origin.startPosition,
      context = origin.getQueryContext)
    newException.setStackTrace(getStackTrace)
    newException
  }

  override def getDefaultMessageTemplate: String =
    messageTemplate.getOrElse(super.getDefaultMessageTemplate)

  override def getSqlState: String = sqlState.getOrElse(super.getSqlState)

  override def getMessage: String = getSimpleMessage

  // Outputs an exception without the logical plan.
  // For testing only
  def getSimpleMessage: String = if (line.isDefined || startPosition.isDefined) {
    val lineAnnotation = line.map(l => s" line $l").getOrElse("")
    val positionAnnotation = startPosition.map(p => s" pos $p").getOrElse("")
    s"$message;$lineAnnotation$positionAnnotation"
  } else {
    message
  }

  override def getMessageParameters: java.util.Map[String, String] = messageParameters.asJava

  override def getCondition: String = errorClass.orNull

  override def getQueryContext: Array[QueryContext] = context

  override lazy val origin: Origin = Origin(line, startPosition)
}
