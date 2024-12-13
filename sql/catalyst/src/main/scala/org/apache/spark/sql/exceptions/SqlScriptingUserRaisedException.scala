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

import org.apache.spark.SparkThrowable

class SqlScriptingUserRaisedException (
    condition: Option[String] = None,
    sqlState: Option[String] = None,
    message: String,
    messageParameters: Map[String, String] = Map.empty)
  extends Exception(
    message,
    null)
    with SparkThrowable {

  override def getCondition: String = condition.getOrElse("USER_RAISED_EXCEPTION")

  override def getSqlState: String = sqlState.getOrElse(super.getSqlState)

  override def getMessageParameters: java.util.Map[String, String] = messageParameters.asJava
}
