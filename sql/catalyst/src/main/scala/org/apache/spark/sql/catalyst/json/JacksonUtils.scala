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

import com.fasterxml.jackson.core.{JsonParser, JsonToken}

import org.apache.spark.sql.catalyst.analysis.TypeCheckResult
import org.apache.spark.sql.catalyst.analysis.TypeCheckResult.{DataTypeMismatch, TypeCheckSuccess}
import org.apache.spark.sql.errors.QueryErrorsBase
import org.apache.spark.sql.types._

object JacksonUtils extends QueryErrorsBase {
  /**
   * Advance the parser until a null or a specific token is found
   */
  def nextUntil(parser: JsonParser, stopOn: JsonToken): Boolean = {
    parser.nextToken() match {
      case null => false
      case x => x != stopOn
    }
  }

  def verifyType(name: String, dataType: DataType): TypeCheckResult = {
    dataType match {
      case NullType | _: AtomicType | CalendarIntervalType => TypeCheckSuccess

      case st: StructType =>
        st.foldLeft(TypeCheckSuccess: TypeCheckResult) { case (currResult, field) =>
          if (currResult.isFailure) currResult else verifyType(field.name, field.dataType)
        }

      case at: ArrayType => verifyType(name, at.elementType)

      // For MapType, its keys are treated as a string (i.e. calling `toString`) basically when
      // generating JSON, so we only care if the values are valid for JSON.
      case mt: MapType => verifyType(name, mt.valueType)

      case udt: UserDefinedType[_] => verifyType(name, udt.sqlType)

      case _ =>
        DataTypeMismatch(
          errorSubClass = "CANNOT_CONVERT_TO_JSON",
          messageParameters = Map(
            "name" -> toSQLId(name),
            "type" -> toSQLType(dataType)))
    }
  }
}
