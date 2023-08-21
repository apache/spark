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
package org.apache.spark.sql.execution.datasources.xml

import org.apache.spark.sql.catalyst.CatalystTypeConverters
import org.apache.spark.sql.catalyst.expressions.{ExpectsInputTypes, Expression, UnaryExpression}
import org.apache.spark.sql.catalyst.expressions.codegen.CodegenFallback
import org.apache.spark.sql.catalyst.util.GenericArrayData
import org.apache.spark.sql.execution.datasources.xml.parsers.StaxXmlParser
import org.apache.spark.sql.types._
import org.apache.spark.unsafe.types.UTF8String

case class XmlDataToCatalyst(
    child: Expression,
    schema: DataType,
    options: XmlOptions)
  extends UnaryExpression with CodegenFallback with ExpectsInputTypes {

  override lazy val dataType: DataType = schema

  @transient
  lazy val rowSchema: StructType = schema match {
    case st: StructType => st
    case ArrayType(st: StructType, _) => st
  }

  override def nullSafeEval(xml: Any): Any = xml match {
    case string: UTF8String =>
      CatalystTypeConverters.convertToCatalyst(
        StaxXmlParser.parseColumn(string.toString, rowSchema, options))
    case string: String =>
      StaxXmlParser.parseColumn(string, rowSchema, options)
    case arr: GenericArrayData =>
      CatalystTypeConverters.convertToCatalyst(
        arr.array.map(s => StaxXmlParser.parseColumn(s.toString, rowSchema, options)))
    case arr: Array[_] =>
      arr.map(s => StaxXmlParser.parseColumn(s.toString, rowSchema, options))
    case _ => null
  }

  override def inputTypes: Seq[DataType] = schema match {
    case _: StructType => Seq(StringType)
    case ArrayType(_: StructType, _) => Seq(ArrayType(StringType))
  }

  // Overrides, in Spark 3.2.0+
  protected def withNewChildInternal(newChild: Expression): XmlDataToCatalyst = copy(newChild)
}
