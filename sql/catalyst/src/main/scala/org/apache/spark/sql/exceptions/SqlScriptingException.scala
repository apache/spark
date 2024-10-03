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

import scala.jdk.CollectionConverters.MapHasAsJava

import org.apache.spark.{SparkThrowable, SparkThrowableHelper}
import org.apache.spark.sql.catalyst.trees.Origin
import org.apache.spark.sql.exceptions.SqlScriptingException.errorMessageWithLineNumber

class SqlScriptingException (
    errorClass: String,
    cause: Throwable,
    val origin: Origin,
    messageParameters: Map[String, String] = Map.empty)
  extends Exception(
    errorMessageWithLineNumber(Option(origin), errorClass, messageParameters),
    cause)
  with SparkThrowable {

  override def getErrorClass: String = errorClass
  override def getMessageParameters: java.util.Map[String, String] = messageParameters.asJava
}

private object SqlScriptingException {

  private def errorMessageWithLineNumber(
      origin: Option[Origin],
      errorClass: String,
      messageParameters: Map[String, String]): String = {
    val prefix = origin.flatMap(o => o.line.map(l => s"{LINE:$l} ")).getOrElse("")
    prefix + SparkThrowableHelper.getMessage(errorClass, messageParameters)
  }
}
