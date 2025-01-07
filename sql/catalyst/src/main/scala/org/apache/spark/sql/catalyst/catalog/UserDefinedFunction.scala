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

import com.fasterxml.jackson.annotation.JsonInclude.Include
import com.fasterxml.jackson.databind.{DeserializationFeature, ObjectMapper}
import com.fasterxml.jackson.module.scala.{DefaultScalaModule, ScalaObjectMapper}

import org.apache.spark.SparkException
import org.apache.spark.sql.catalyst.FunctionIdentifier
import org.apache.spark.sql.catalyst.expressions.ExpressionInfo
import org.apache.spark.sql.catalyst.parser.ParserInterface
import org.apache.spark.sql.catalyst.util.CharVarcharUtils
import org.apache.spark.sql.types.{DataType, StructType}

/**
 * The base class for all user defined functions registered via SQL.
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

  /**
   * Convert the function to a [[CatalogFunction]].
   */
  def toCatalogFunction: CatalogFunction

  /**
   * Convert the SQL function to an [[ExpressionInfo]].
   */
  def toExpressionInfo: ExpressionInfo
}

object UserDefinedFunction {
  val SQL_CONFIG_PREFIX = "sqlConfig."
  val INDEX_LENGTH: Int = 3

  // The default Hive Metastore SQL schema length for function resource uri.
  private val HIVE_FUNCTION_RESOURCE_URI_LENGTH_THRESHOLD: Int = 4000

  def parseTableSchema(text: String, parser: ParserInterface): StructType = {
    val parsed = parser.parseTableSchema(text)
    CharVarcharUtils.failIfHasCharVarchar(parsed).asInstanceOf[StructType]
  }

  def parseDataType(text: String, parser: ParserInterface): DataType = {
    val dataType = parser.parseDataType(text)
    CharVarcharUtils.failIfHasCharVarchar(dataType)
  }

  private val _mapper: ObjectMapper = getObjectMapper

  /**
   * A shared [[ObjectMapper]] for serializations.
   */
  def mapper: ObjectMapper = _mapper

  /**
   * Convert the given properties to a list of function resources.
   */
  def propertiesToFunctionResources(
      props: Map[String, String],
      name: FunctionIdentifier): Seq[FunctionResource] = {
    val blob = mapper.writeValueAsString(props)
    val threshold = HIVE_FUNCTION_RESOURCE_URI_LENGTH_THRESHOLD - INDEX_LENGTH
    blob.grouped(threshold).zipWithIndex.map { case (part, i) =>
      // Add a sequence number to the part and pad it to a given length.
      // E.g. 1 will become "001" if the given length is 3.
      val index = s"%0${INDEX_LENGTH}d".format(i)
      if (index.length > INDEX_LENGTH) {
        throw UserDefinedFunctionErrors.routinePropertyTooLarge(name.funcName)
      }
      FunctionResource(FileResource, index + part)
    }.toSeq
  }

  /**
   * Get a object mapper to serialize and deserialize function properties.
   */
  private def getObjectMapper: ObjectMapper = {
    val mapper = new ObjectMapper with ScalaObjectMapper
    mapper.setSerializationInclusion(Include.NON_ABSENT)
    mapper.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false)
    mapper.registerModule(DefaultScalaModule)
    mapper
  }

  /**
   * Convert a [[CatalogFunction]] into a corresponding UDF.
   */
  def fromCatalogFunction(function: CatalogFunction, parser: ParserInterface)
  : UserDefinedFunction = {
    val className = function.className
    if (SQLFunction.isSQLFunction(className)) {
      SQLFunction.fromCatalogFunction(function, parser)
    } else {
      throw SparkException.internalError(s"Unsupported function type $className")
    }
  }

  /**
   * Verify if the function is a [[UserDefinedFunction]].
   */
  def isUserDefinedFunction(className: String): Boolean = SQLFunction.isSQLFunction(className)
}
