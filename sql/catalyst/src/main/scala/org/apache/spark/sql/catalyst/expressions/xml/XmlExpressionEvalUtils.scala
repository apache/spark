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
package org.apache.spark.sql.catalyst.expressions.xml

import org.apache.spark.sql.catalyst.util.DropMalformedMode
import org.apache.spark.sql.catalyst.xml.{XmlInferSchema, XmlOptions}
import org.apache.spark.sql.errors.QueryCompilationErrors
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.types.{ArrayType, DataType, StructType}
import org.apache.spark.unsafe.types.UTF8String

case class SchemaOfXmlEvaluator(options: Map[String, String]) {

  @transient
  private lazy val xmlOptions = new XmlOptions(options, "UTC")

  @transient
  private lazy val xmlInferSchema = {
    if (xmlOptions.parseMode == DropMalformedMode) {
      throw QueryCompilationErrors.parseModeUnsupportedError("schema_of_xml", xmlOptions.parseMode)
    }
    new XmlInferSchema(xmlOptions, caseSensitive = SQLConf.get.caseSensitiveAnalysis)
  }

  final def evaluate(xml: UTF8String): Any = {
    val dataType = xmlInferSchema.infer(xml.toString).get match {
      case st: StructType =>
        xmlInferSchema.canonicalizeType(st).getOrElse(StructType(Nil))
      case at: ArrayType if at.elementType.isInstanceOf[StructType] =>
        xmlInferSchema
          .canonicalizeType(at.elementType)
          .map(ArrayType(_, containsNull = at.containsNull))
          .getOrElse(ArrayType(StructType(Nil), containsNull = at.containsNull))
      case other: DataType =>
        xmlInferSchema.canonicalizeType(other).getOrElse(SQLConf.get.defaultStringType)
    }

    UTF8String.fromString(dataType.sql)
  }
}
