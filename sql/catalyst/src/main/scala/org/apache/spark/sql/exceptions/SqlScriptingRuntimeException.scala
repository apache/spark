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

package org.apache.spark.sql.exceptions

import scala.jdk.CollectionConverters._

import org.apache.spark.{QueryContext, SparkThrowable, SparkThrowableHelper}
import org.apache.spark.sql.catalyst.trees.Origin
import org.apache.spark.sql.exceptions.SqlScriptingRuntimeException.errorMessageWithLineNumber

class SqlScriptingRuntimeException (
    condition: String,
    sqlState: String,
    message: String,
    cause: Throwable,
    val origin: Origin,
    messageParameters: Map[String, String] = Map.empty,
    isBuiltinError: Boolean = false)
  extends RuntimeException(
    errorMessageWithLineNumber(
      origin,
      condition,
      sqlState,
      message,
      messageParameters,
      isBuiltinError),
    cause)
  with SparkThrowable {

  def getCondition: String = condition

  override def getSqlState: String = sqlState

  override def getMessageParameters: java.util.Map[String, String] = messageParameters.asJava

  override def getQueryContext: Array[QueryContext] = origin.getQueryContext
}

private object SqlScriptingRuntimeException {

  private def errorMessageWithLineNumber(
      origin: Origin,
      condition: String,
      sqlState: String,
      message: String,
      messageParameters: Map[String, String],
      isBuiltinError: Boolean): String = {

    val lineNumberPrefix = origin.line.map(l => s"{LINE:$l} ").getOrElse("")
    val msgString = if (isBuiltinError) {
      SparkThrowableHelper.getMessage(condition, messageParameters)
    } else {
      s"[$condition] " + message + s" SQLSTATE: $sqlState"
    }
    lineNumberPrefix + msgString
  }
}
