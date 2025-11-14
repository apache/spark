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

package org.apache.spark.api.python

import java.util

import org.apache.spark.{BreakingChangeInfo, QueryContext, SparkThrowable}

/**
 * Utility object that provides convenient accessors for extracting
 * detailed information from a [[SparkThrowable]] instance.
 *
 * This object is primarily used in PySpark
 * to retrieve structured error metadata because Py4J does not work
 * with default methods.
 */
private[spark] object PythonErrorUtils {
  def getCondition(e: SparkThrowable): String = e.getCondition
  def getErrorClass(e: SparkThrowable): String = e.getCondition
  def getSqlState(e: SparkThrowable): String = e.getSqlState
  def isInternalError(e: SparkThrowable): Boolean = e.isInternalError
  def getBreakingChangeInfo(e: SparkThrowable): BreakingChangeInfo = e.getBreakingChangeInfo
  def getMessageParameters(e: SparkThrowable): util.Map[String, String] = e.getMessageParameters
  def getDefaultMessageTemplate(e: SparkThrowable): String = e.getDefaultMessageTemplate
  def getQueryContext(e: SparkThrowable): Array[QueryContext] = e.getQueryContext
}
