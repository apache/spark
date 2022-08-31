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

import org.apache.spark.{SparkThrowable, SparkThrowableHelper}
import org.apache.spark.annotation.Stable
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.catalyst.trees.Origin

/**
 * Thrown when a query fails to analyze, usually because the query itself is invalid.
 *
 * @since 1.3.0
 */
@Stable
class AnalysisException protected[sql] (
    val message: String,
    val line: Option[Int] = None,
    val startPosition: Option[Int] = None,
    // Some plans fail to serialize due to bugs in scala collections.
    @transient val plan: Option[LogicalPlan] = None,
    val cause: Option[Throwable] = None,
    val errorClass: Option[String] = None,
    val errorSubClass: Option[String] = None,
    val messageParameters: Array[String] = Array.empty)
  extends Exception(message, cause.orNull) with SparkThrowable with Serializable {

    // Needed for binary compatibility
    protected[sql] def this(message: String,
           line: Option[Int],
           startPosition: Option[Int],
           plan: Option[LogicalPlan],
           cause: Option[Throwable],
           errorClass: Option[String],
           messageParameters: Array[String]) =
    this(message = message,
         line = line,
         startPosition = startPosition,
         plan = plan,
         cause = cause,
         errorClass,
         errorSubClass = None,
         messageParameters = messageParameters)

  def this(errorClass: String, messageParameters: Array[String], cause: Option[Throwable]) =
    this(
      SparkThrowableHelper.getMessage(errorClass, null, messageParameters),
      errorClass = Some(errorClass),
      errorSubClass = None,
      messageParameters = messageParameters,
      cause = cause)

  def this(errorClass: String, messageParameters: Array[String]) =
    this(errorClass = errorClass, messageParameters = messageParameters, cause = None)

  def this(
      errorClass: String,
      messageParameters: Array[String],
      origin: Origin) =
    this(
      SparkThrowableHelper.getMessage(errorClass, null, messageParameters),
      line = origin.line,
      startPosition = origin.startPosition,
      errorClass = Some(errorClass),
      errorSubClass = None,
      messageParameters = messageParameters)

  def this(
      errorClass: String,
      errorSubClass: String,
      messageParameters: Array[String]) =
    this(
      SparkThrowableHelper.getMessage(errorClass, errorSubClass, messageParameters),
      errorClass = Some(errorClass),
      errorSubClass = Some(errorSubClass),
      messageParameters = messageParameters)

  def this(
      errorClass: String,
      errorSubClass: String,
      messageParameters: Array[String],
      origin: Origin) =
    this(
      SparkThrowableHelper.getMessage(errorClass, errorSubClass, messageParameters),
      line = origin.line,
      startPosition = origin.startPosition,
      errorClass = Some(errorClass),
      errorSubClass = Option(errorSubClass),
      messageParameters = messageParameters)

  def copy(
      message: String = this.message,
      line: Option[Int] = this.line,
      startPosition: Option[Int] = this.startPosition,
      plan: Option[LogicalPlan] = this.plan,
      cause: Option[Throwable] = this.cause,
      errorClass: Option[String] = this.errorClass,
      messageParameters: Array[String] = this.messageParameters): AnalysisException =
    new AnalysisException(message, line, startPosition, plan, cause, errorClass, errorSubClass,
      messageParameters)

  def withPosition(line: Option[Int], startPosition: Option[Int]): AnalysisException = {
    val newException = this.copy(line = line, startPosition = startPosition)
    newException.setStackTrace(getStackTrace)
    newException
  }

  override def getMessage: String = {
    val planAnnotation = Option(plan).flatten.map(p => s";\n$p").getOrElse("")
    getSimpleMessage + planAnnotation
  }

  // Outputs an exception without the logical plan.
  // For testing only
  def getSimpleMessage: String = if (line.isDefined || startPosition.isDefined) {
    val lineAnnotation = line.map(l => s" line $l").getOrElse("")
    val positionAnnotation = startPosition.map(p => s" pos $p").getOrElse("")
    s"$message;$lineAnnotation$positionAnnotation"
  } else {
    message
  }

  override def getMessageParameters: Array[String] = messageParameters
  override def getErrorClass: String = errorClass.orNull
  override def getErrorSubClass: String = errorSubClass.orNull
}
