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

package org.apache.spark.sql.catalyst.expressions

import org.apache.spark.sql.AnalysisException
import org.apache.spark.sql.catalyst.util.ArrayBasedMapData
import org.apache.spark.sql.types.{MapType, StringType, StructType}

object ExprUtils {

  def evalSchemaExpr(exp: Expression): StructType = exp match {
    case Literal(s, StringType) => StructType.fromDDL(s.toString)
    case e => throw new AnalysisException(
      s"Schema should be specified in DDL format as a string literal instead of ${e.sql}")
  }

  def convertToMapData(exp: Expression): Map[String, String] = exp match {
    case m: CreateMap
      if m.dataType.acceptsType(MapType(StringType, StringType, valueContainsNull = false)) =>
      val arrayMap = m.eval().asInstanceOf[ArrayBasedMapData]
      ArrayBasedMapData.toScalaMap(arrayMap).map { case (key, value) =>
        key.toString -> value.toString
      }
    case m: CreateMap =>
      throw new AnalysisException(
        s"A type of keys and values in map() must be string, but got ${m.dataType.catalogString}")
    case _ =>
      throw new AnalysisException("Must use a map() function for options")
  }
}
