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

import org.apache.spark.sql.catalyst.analysis.{FunctionRegistry, TypeCheckResult}
import org.apache.spark.sql.catalyst.analysis.TypeCheckResult.DataTypeMismatch
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.expressions.Cast._
import org.apache.spark.sql.catalyst.expressions.objects.Invoke
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.internal.types.StringTypeWithCollation
import org.apache.spark.sql.types._
import org.apache.spark.unsafe.types.UTF8String

/**
 * Base class for xpath_boolean, xpath_double, xpath_int, etc.
 *
 * This is not the world's most efficient implementation due to type conversion, but works.
 */
abstract class XPathExtract
  extends BinaryExpression with RuntimeReplaceable with ExpectsInputTypes {
  override def left: Expression = xml
  override def right: Expression = path

  /** XPath expressions are always nullable, e.g. if the xml string is empty. */
  override def nullable: Boolean = true

  override def inputTypes: Seq[AbstractDataType] =
    Seq(StringTypeWithCollation, StringTypeWithCollation)

  override def checkInputDataTypes(): TypeCheckResult = {
    if (!path.foldable) {
      DataTypeMismatch(
        errorSubClass = "NON_FOLDABLE_INPUT",
        messageParameters = Map(
          "inputName" -> toSQLId("path"),
          "inputType" -> toSQLType(StringTypeWithCollation),
          "inputExpr" -> toSQLExpr(path)
        )
      )
    } else {
      super.checkInputDataTypes()
    }
  }

  /** Concrete implementations need to override the following three methods. */
  def xml: Expression
  def path: Expression

  @transient private lazy val pathUTF8String = path.eval().asInstanceOf[UTF8String]
  @transient private lazy val evaluator = XPathEvaluatorFactory.create(dataType, pathUTF8String)

  override def replacement: Expression = Invoke(
    Literal.create(evaluator, ObjectType(classOf[XPathEvaluator])),
    "evaluate",
    dataType,
    Seq(xml),
    Seq(xml.dataType))
}

// scalastyle:off line.size.limit
@ExpressionDescription(
  usage = "_FUNC_(xml, xpath) - Returns true if the XPath expression evaluates to true, or if a matching node is found.",
  examples = """
    Examples:
      > SELECT _FUNC_('<a><b>1</b></a>','a/b');
       true
  """,
  since = "2.0.0",
  group = "xml_funcs")
// scalastyle:on line.size.limit
case class XPathBoolean(xml: Expression, path: Expression) extends XPathExtract with Predicate {

  override def prettyName: String = "xpath_boolean"

  override protected def withNewChildrenInternal(
    newLeft: Expression, newRight: Expression): XPathBoolean = copy(xml = newLeft, path = newRight)
}

// scalastyle:off line.size.limit
@ExpressionDescription(
  usage = "_FUNC_(xml, xpath) - Returns a short integer value, or the value zero if no match is found, or a match is found but the value is non-numeric.",
  examples = """
    Examples:
      > SELECT _FUNC_('<a><b>1</b><b>2</b></a>', 'sum(a/b)');
       3
  """,
  since = "2.0.0",
  group = "xml_funcs")
// scalastyle:on line.size.limit
case class XPathShort(xml: Expression, path: Expression) extends XPathExtract {
  override def prettyName: String = "xpath_short"
  override def dataType: DataType = ShortType

  override protected def withNewChildrenInternal(
    newLeft: Expression, newRight: Expression): XPathShort = copy(xml = newLeft, path = newRight)
}

// scalastyle:off line.size.limit
@ExpressionDescription(
  usage = "_FUNC_(xml, xpath) - Returns an integer value, or the value zero if no match is found, or a match is found but the value is non-numeric.",
  examples = """
    Examples:
      > SELECT _FUNC_('<a><b>1</b><b>2</b></a>', 'sum(a/b)');
       3
  """,
  since = "2.0.0",
  group = "xml_funcs")
// scalastyle:on line.size.limit
case class XPathInt(xml: Expression, path: Expression) extends XPathExtract {
  override def prettyName: String = "xpath_int"
  override def dataType: DataType = IntegerType

  override protected def withNewChildrenInternal(
    newLeft: Expression, newRight: Expression): Expression = copy(xml = newLeft, path = newRight)
}

// scalastyle:off line.size.limit
@ExpressionDescription(
  usage = "_FUNC_(xml, xpath) - Returns a long integer value, or the value zero if no match is found, or a match is found but the value is non-numeric.",
  examples = """
    Examples:
      > SELECT _FUNC_('<a><b>1</b><b>2</b></a>', 'sum(a/b)');
       3
  """,
  since = "2.0.0",
  group = "xml_funcs")
// scalastyle:on line.size.limit
case class XPathLong(xml: Expression, path: Expression) extends XPathExtract {
  override def prettyName: String = "xpath_long"
  override def dataType: DataType = LongType

  override protected def withNewChildrenInternal(
    newLeft: Expression, newRight: Expression): XPathLong = copy(xml = newLeft, path = newRight)
}

// scalastyle:off line.size.limit
@ExpressionDescription(
  usage = "_FUNC_(xml, xpath) - Returns a float value, the value zero if no match is found, or NaN if a match is found but the value is non-numeric.",
  examples = """
    Examples:
      > SELECT _FUNC_('<a><b>1</b><b>2</b></a>', 'sum(a/b)');
       3.0
  """,
  since = "2.0.0",
  group = "xml_funcs")
// scalastyle:on line.size.limit
case class XPathFloat(xml: Expression, path: Expression) extends XPathExtract {
  override def prettyName: String = "xpath_float"
  override def dataType: DataType = FloatType

  override protected def withNewChildrenInternal(
    newLeft: Expression, newRight: Expression): XPathFloat = copy(xml = newLeft, path = newRight)
}

// scalastyle:off line.size.limit
@ExpressionDescription(
  usage = "_FUNC_(xml, xpath) - Returns a double value, the value zero if no match is found, or NaN if a match is found but the value is non-numeric.",
  examples = """
    Examples:
      > SELECT _FUNC_('<a><b>1</b><b>2</b></a>', 'sum(a/b)');
       3.0
  """,
  since = "2.0.0",
  group = "xml_funcs")
// scalastyle:on line.size.limit
case class XPathDouble(xml: Expression, path: Expression) extends XPathExtract {
  override def prettyName: String =
    getTagValue(FunctionRegistry.FUNC_ALIAS).getOrElse("xpath_double")
  override def dataType: DataType = DoubleType

  override protected def withNewChildrenInternal(
    newLeft: Expression, newRight: Expression): XPathDouble = copy(xml = newLeft, path = newRight)
}

// scalastyle:off line.size.limit
@ExpressionDescription(
  usage = "_FUNC_(xml, xpath) - Returns the text contents of the first xml node that matches the XPath expression.",
  examples = """
    Examples:
      > SELECT _FUNC_('<a><b>b</b><c>cc</c></a>','a/c');
       cc
  """,
  since = "2.0.0",
  group = "xml_funcs")
// scalastyle:on line.size.limit
case class XPathString(xml: Expression, path: Expression) extends XPathExtract {
  override def prettyName: String = "xpath_string"
  override def dataType: DataType = SQLConf.get.defaultStringType

  override protected def withNewChildrenInternal(
    newLeft: Expression, newRight: Expression): Expression = copy(xml = newLeft, path = newRight)
}

// scalastyle:off line.size.limit
@ExpressionDescription(
  usage = "_FUNC_(xml, xpath) - Returns a string array of values within the nodes of xml that match the XPath expression.",
  examples = """
    Examples:
      > SELECT _FUNC_('<a><b>b1</b><b>b2</b><b>b3</b><c>c1</c><c>c2</c></a>','a/b/text()');
       ["b1","b2","b3"]
      > SELECT _FUNC_('<a><b>b1</b><b>b2</b><b>b3</b><c>c1</c><c>c2</c></a>','a/b');
       [null,null,null]
  """,
  since = "2.0.0",
  group = "xml_funcs")
// scalastyle:on line.size.limit
case class XPathList(xml: Expression, path: Expression) extends XPathExtract {
  override def prettyName: String = "xpath"
  override def dataType: DataType = ArrayType(SQLConf.get.defaultStringType)

  override protected def withNewChildrenInternal(
    newLeft: Expression, newRight: Expression): XPathList = copy(xml = newLeft, path = newRight)
}
