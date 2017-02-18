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

package org.apache.spark.sql.catalyst.json

import scala.util.{Failure, Success, Try}

import com.fasterxml.jackson.core.{JsonParser, JsonToken}
import org.json4s.jackson.JsonMethods.parse

import org.apache.spark.sql.AnalysisException
import org.apache.spark.sql.catalyst.expressions.{Expression, Literal}
import org.apache.spark.sql.types._

object JacksonUtils {
  /**
   * Advance the parser until a null or a specific token is found
   */
  def nextUntil(parser: JsonParser, stopOn: JsonToken): Boolean = {
    parser.nextToken() match {
      case null => false
      case x => x != stopOn
    }
  }

  /**
   * Verify if the schema is supported in JSON parsing.
   */
  def verifySchema(schema: StructType): Unit = {
    def verifyType(name: String, dataType: DataType): Unit = dataType match {
      case NullType | BooleanType | ByteType | ShortType | IntegerType | LongType | FloatType |
           DoubleType | StringType | TimestampType | DateType | BinaryType | _: DecimalType =>

      case st: StructType => st.foreach(field => verifyType(field.name, field.dataType))

      case at: ArrayType => verifyType(name, at.elementType)

      case mt: MapType => verifyType(name, mt.keyType)

      case udt: UserDefinedType[_] => verifyType(name, udt.sqlType)

      case _ =>
        throw new UnsupportedOperationException(
          s"Unable to convert column $name of type ${dataType.simpleString} to JSON.")
    }

    schema.foreach(field => verifyType(field.name, field.dataType))
  }

  private def validateStringLiteral(exp: Expression): String = exp match {
    case Literal(s, StringType) => s.toString
    case e => throw new AnalysisException("Must be a string literal, but: " + e)
  }

  def validateSchemaLiteral(exp: Expression): StructType =
    DataType.fromJson(validateStringLiteral(exp)).asInstanceOf[StructType]

  /**
   * Convert a literal including a json option string (e.g., '{"mode": "PERMISSIVE", ...}')
   * to Map-type data.
   */
  def validateOptionsLiteral(exp: Expression): Map[String, String] = {
    implicit val formats = org.json4s.DefaultFormats
    val json = validateStringLiteral(exp)
    Try(parse(json).extract[Map[String, String]]) match {
      case Success(m) => m
      case Failure(_) =>
        throw new AnalysisException(s"""The format must be '{"key": "value", ...}', but ${json}"""")
    }
  }
}
