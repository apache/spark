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

import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.expressions.codegen.CodegenFallback
import org.apache.spark.sql.types.{AbstractDataType, BooleanType, DataType, StringType}
import org.apache.spark.unsafe.types.UTF8String


@ExpressionDescription(
  usage = "_FUNC_(xml, xpath) - Evaluates a boolean xpath expression.",
  extended = "> SELECT _FUNC_('<a><b>1</b></a>','a/b');\ntrue")
case class XPathBoolean(xml: Expression, path: Expression)
  extends BinaryExpression with ExpectsInputTypes with CodegenFallback {

  @transient private lazy val xpathUtil = new UDFXPathUtil

  // If the path is a constant, cache the path string so that we don't need to convert path
  // from UTF8String to String for every row.
  @transient lazy val pathLiteral: String = path match {
    case Literal(str: UTF8String, _) => str.toString
    case _ => null
  }

  override def prettyName: String = "xpath_boolean"

  override def dataType: DataType = BooleanType

  override def inputTypes: Seq[AbstractDataType] = Seq(StringType, StringType)

  override def left: Expression = xml
  override def right: Expression = path

  override protected def nullSafeEval(xml: Any, path: Any): Any = {
    val xmlString = xml.asInstanceOf[UTF8String].toString
    if (pathLiteral ne null) {
      xpathUtil.evalBoolean(xmlString, pathLiteral)
    } else {
      xpathUtil.evalBoolean(xmlString, path.asInstanceOf[UTF8String].toString)
    }
  }
}
