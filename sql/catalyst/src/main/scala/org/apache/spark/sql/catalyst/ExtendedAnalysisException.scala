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
package org.apache.spark.sql.catalyst

import org.apache.spark.QueryContext
import org.apache.spark.sql.AnalysisException
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan

/**
 * Internal [[AnalysisException]] that also captures a [[LogicalPlan]].
 */
class ExtendedAnalysisException private(
    message: String,
    line: Option[Int] = None,
    startPosition: Option[Int] = None,
    // Some plans fail to serialize due to bugs in scala collections.
    @transient val plan: Option[LogicalPlan] = None,
    cause: Option[Throwable] = None,
    errorClass: Option[String] = None,
    messageParameters: Map[String, String] = Map.empty,
    context: Array[QueryContext] = Array.empty)
  extends AnalysisException(
    message,
    line,
    startPosition,
    cause,
    errorClass,
    messageParameters,
    context) {

  def this(e: AnalysisException, plan: LogicalPlan) = {
    this(
      e.message,
      e.line,
      e.startPosition,
      Option(plan),
      e.cause,
      e.errorClass,
      e.messageParameters,
      e.context)
    setStackTrace(e.getStackTrace)
  }

  override def copy(
      message: String,
      line: Option[Int],
      startPosition: Option[Int],
      cause: Option[Throwable],
      errorClass: Option[String],
      messageParameters: Map[String, String],
      context: Array[QueryContext]): ExtendedAnalysisException = {
    new ExtendedAnalysisException(message, line, startPosition, plan, cause, errorClass,
      messageParameters, context)
  }

  override def getMessage: String = {
    val planAnnotation = Option(plan).flatten.map(p => s";\n$p").getOrElse("")
    getSimpleMessage + planAnnotation
  }

  override def toString: String = {
    val message = getLocalizedMessage
    if (message != null) {
      ExtendedAnalysisException.name + ": " + message
    } else {
      ExtendedAnalysisException.name
    }
  }
}

object ExtendedAnalysisException {
  private val name = classOf[AnalysisException].getName
}
