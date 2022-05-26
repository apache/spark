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

package org.apache.spark.sql.errors

import org.apache.spark.SparkThrowable
import org.apache.spark.sql.catalyst.parser.ParseException
import org.apache.spark.sql.test.SharedSparkSession

trait QueryErrorsSuiteBase extends SharedSparkSession {
  def checkErrorClass(
      exception: Exception with SparkThrowable,
      errorClass: String,
      errorSubClass: Option[String] = None,
      msg: String,
      sqlState: Option[String] = None,
      matchMsg: Boolean = false): Unit = {
    assert(exception.getErrorClass === errorClass)
    sqlState.foreach(state => exception.getSqlState === state)
    val fullErrorClass = if (errorSubClass.isDefined) {
      errorClass + "." + errorSubClass.get
    } else {
     errorClass
    }
    if (matchMsg) {
      assert(exception.getMessage.matches(s"""\\[$fullErrorClass\\] """ + msg))
    } else {
      assert(exception.getMessage === s"""[$fullErrorClass] """ + msg)
    }
  }

  def validateParsingError(
      sqlText: String,
      errorClass: String,
      errorSubClass: Option[String] = None,
      sqlState: String,
      message: String): Unit = {
    val exception = intercept[ParseException] {
      sql(sqlText)
    }
    checkParsingError(exception, errorClass, errorSubClass, sqlState, message)
  }

  def checkParsingError(
      exception: Exception with SparkThrowable,
      errorClass: String,
      errorSubClass: Option[String] = None,
      sqlState: String,
      message: String): Unit = {
    val fullErrorClass = if (errorSubClass.isDefined) {
      errorClass + "." + errorSubClass.get
    } else {
      errorClass
    }
    assert(exception.getErrorClass === errorClass)
    assert(exception.getSqlState === sqlState)
    assert(exception.getMessage === s"""\n[$fullErrorClass] """ + message)
  }
}
