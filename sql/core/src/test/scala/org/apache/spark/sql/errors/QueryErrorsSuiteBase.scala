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

import org.apache.spark.QueryContext
import org.apache.spark.sql.catalyst.parser.ParseException
import org.apache.spark.sql.test.SharedSparkSession

trait QueryErrorsSuiteBase extends SharedSparkSession {

  def validateParsingError(
      sqlText: String,
      errorClass: String,
      errorSubClass: Option[String] = None,
      sqlState: String,
      parameters: Map[String, String] = Map.empty): Unit = {
    checkError(
      exception = intercept[ParseException](sql(sqlText)),
      errorClass = errorClass,
      errorSubClass = errorSubClass,
      sqlState = Some(sqlState),
      parameters = parameters)
  }

  case class ExpectedContext(
      objectType: String,
      objectName: String,
      startIndex: Int,
      stopIndex: Int,
      fragment: String) extends QueryContext

  object ExpectedContext {
    def apply(fragment: String, start: Int, stop: Int): ExpectedContext = {
      ExpectedContext("", "", start, stop, fragment)
    }
  }
}
