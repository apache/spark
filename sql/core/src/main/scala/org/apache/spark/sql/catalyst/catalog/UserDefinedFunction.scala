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

package org.apache.spark.sql.catalyst.catalog

import org.apache.spark.sql.catalyst.FunctionIdentifier
import org.apache.spark.sql.catalyst.parser.ParserInterface
import org.apache.spark.sql.catalyst.util.CharVarcharUtils
import org.apache.spark.sql.types.{DataType, StructType}

/**
 * The base class for all user defined functions registered via SQL queries.
 */
trait UserDefinedFunction {

  /**
   * Qualified name of the function
   */
  def name: FunctionIdentifier

  /**
   * Additional properties to be serialized for the function.
   * Use this to preserve the runtime configuration that should be used during the function
   * execution, such as SQL configs etc. See [[SQLConf]] for more info.
   */
  def properties: Map[String, String]

  /**
   * Owner of the function
   */
  def owner: Option[String]

  /**
   * Function creation time in milliseconds since the linux epoch
   */
  def createTimeMs: Long

  /**
   * The language of the user defined function.
   */
  def language: RoutineLanguage
}

object UserDefinedFunction {
  def parseTableSchema(text: String, parser: ParserInterface): StructType = {
    val parsed = parser.parseTableSchema(text)
    CharVarcharUtils.failIfHasCharVarchar(parsed).asInstanceOf[StructType]
  }

  def parseDataType(text: String, parser: ParserInterface): DataType = {
    val dataType = parser.parseDataType(text)
    CharVarcharUtils.failIfHasCharVarchar(dataType)
  }
}
