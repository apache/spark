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
import org.apache.spark.sql.types._
import org.apache.spark.unsafe.types.UTF8String

/**
 * Base class for xpath_boolean, xpath_double, xpath_int, etc.
 */
abstract class XPathExtract extends BinaryExpression with ExpectsInputTypes with CodegenFallback {

  @transient private[this] lazy val xpathUtil = new UDFXPathUtil

  // If the path is a constant, cache the path string so that we don't need to convert path
  // from UTF8String to String for every row.
  @transient private[this] lazy val pathLiteral: String = path match {
    case Literal(str: UTF8String, _) => str.toString
    case _ => null
  }

  override def left: Expression = xml
  override def right: Expression = path

  override def inputTypes: Seq[AbstractDataType] = Seq(StringType, StringType)

  override protected def nullSafeEval(xml: Any, path: Any): Any = {
    val xmlString = xml.asInstanceOf[UTF8String].toString
    if (pathLiteral ne null) {
      xpathEval(xpathUtil, xmlString, pathLiteral)
    } else {
      xpathEval(xpathUtil, xmlString, path.asInstanceOf[UTF8String].toString)
    }
  }

  /** Concrete implementations need to override the following three methods. */
  def xml: Expression
  def path: Expression
  def xpathEval(xpathUtil: UDFXPathUtil, xml: String, path: String): Any
}

@ExpressionDescription(
  usage = "_FUNC_(xml, xpath) - Evaluates a boolean xpath expression.",
  extended = "> SELECT _FUNC_('<a><b>1</b></a>','a/b');\ntrue")
case class XPathBoolean(xml: Expression, path: Expression) extends XPathExtract {
  override def prettyName: String = "xpath_boolean"
  override def dataType: DataType = BooleanType

  def xpathEval(xpathUtil: UDFXPathUtil, xml: String, path: String): Any = {
    xpathUtil.evalBoolean(xml, path)
  }
}

@ExpressionDescription(
  usage = "_FUNC_(xml, xpath) - Returns a short value that matches the xpath expression",
  extended = "> SELECT _FUNC_('<a><b>1</b><b>2</b></a>','sum(a/b)');\n3")
case class XPathShort(xml: Expression, path: Expression) extends XPathExtract {
  override def prettyName: String = "xpath_short"
  override def dataType: DataType = ShortType

  def xpathEval(xpathUtil: UDFXPathUtil, xml: String, path: String): Any = {
    val ret = xpathUtil.evalNumber(xml, path)
    if (ret eq null) null else ret.shortValue()
  }
}

@ExpressionDescription(
  usage = "_FUNC_(xml, xpath) - Returns an integer value that matches the xpath expression",
  extended = "> SELECT _FUNC_('<a><b>1</b><b>2</b></a>','sum(a/b)');\n3")
case class XPathInt(xml: Expression, path: Expression) extends XPathExtract {
  override def prettyName: String = "xpath_int"
  override def dataType: DataType = IntegerType

  def xpathEval(xpathUtil: UDFXPathUtil, xml: String, path: String): Any = {
    val ret = xpathUtil.evalNumber(xml, path)
    if (ret eq null) null else ret.intValue()
  }
}

@ExpressionDescription(
  usage = "_FUNC_(xml, xpath) - Returns a long value that matches the xpath expression",
  extended = "> SELECT _FUNC_('<a><b>1</b><b>2</b></a>','sum(a/b)');\n3")
case class XPathLong(xml: Expression, path: Expression) extends XPathExtract {
  override def prettyName: String = "xpath_long"
  override def dataType: DataType = LongType

  def xpathEval(xpathUtil: UDFXPathUtil, xml: String, path: String): Any = {
    val ret = xpathUtil.evalNumber(xml, path)
    if (ret eq null) null else ret.longValue()
  }
}

@ExpressionDescription(
  usage = "_FUNC_(xml, xpath) - Returns a float value that matches the xpath expression",
  extended = "> SELECT _FUNC_('<a><b>1</b><b>2</b></a>','sum(a/b)');\n3.0")
case class XPathFloat(xml: Expression, path: Expression) extends XPathExtract {
  override def prettyName: String = "xpath_float"
  override def dataType: DataType = FloatType

  def xpathEval(xpathUtil: UDFXPathUtil, xml: String, path: String): Any = {
    val ret = xpathUtil.evalNumber(xml, path)
    if (ret eq null) null else ret.floatValue()
  }
}

@ExpressionDescription(
  usage = "_FUNC_(xml, xpath) - Returns a double value that matches the xpath expression",
  extended = "> SELECT _FUNC_('<a><b>1</b><b>2</b></a>','sum(a/b)');\n3.0")
case class XPathDouble(xml: Expression, path: Expression) extends XPathExtract {
  override def prettyName: String = "xpath_float"
  override def dataType: DataType = DoubleType

  def xpathEval(xpathUtil: UDFXPathUtil, xml: String, path: String): Any = {
    val ret = xpathUtil.evalNumber(xml, path)
    if (ret eq null) null else ret.doubleValue()
  }
}

// scalastyle:off line.size.limit
@ExpressionDescription(
  usage = "_FUNC_(xml, xpath) - Returns the text contents of the first xml node that matches the xpath expression",
  extended = "> SELECT _FUNC_('<a><b>b</b><c>cc</c></a>','a/c');\ncc")
// scalastyle:on line.size.limit
case class XPathString(xml: Expression, path: Expression) extends XPathExtract {
  override def prettyName: String = "xpath_string"
  override def dataType: DataType = StringType

  def xpathEval(xpathUtil: UDFXPathUtil, xml: String, path: String): Any = {
    UTF8String.fromString(xpathUtil.evalString(xml, path))
  }
}
