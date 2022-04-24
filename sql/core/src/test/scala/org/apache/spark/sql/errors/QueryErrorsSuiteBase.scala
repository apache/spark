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
      msg: String,
      sqlState: Option[String] = None,
      matchMsg: Boolean = false): Unit = {
    assert(exception.getErrorClass === errorClass)
    sqlState.foreach(state => exception.getSqlState === state)
    if (matchMsg) {
      assert(exception.getMessage.matches(s"""\\[$errorClass\\] """ + msg))
    } else {
      assert(exception.getMessage === s"""[$errorClass] """ + msg)
    }
  }

  def validateParsingError(
      sqlText: String,
      errorClass: String,
      sqlState: String,
      message: String): Unit = {
    val e = intercept[ParseException] {
      sql(sqlText)
    }
    assert(e.getErrorClass === errorClass)
    assert(e.getSqlState === sqlState)
    assert(e.getMessage === s"""\n[$errorClass] """ + message)
  }
}
