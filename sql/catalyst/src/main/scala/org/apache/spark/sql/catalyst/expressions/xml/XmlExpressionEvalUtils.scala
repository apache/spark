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

import java.io.CharArrayWriter

import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.{Expression, ExprUtils}
import org.apache.spark.sql.catalyst.util.{FailFastMode, FailureSafeParser, GenericArrayData, PermissiveMode}
import org.apache.spark.sql.catalyst.xml.{StaxXmlGenerator, StaxXmlParser, ValidatorUtil, XmlInferSchema, XmlOptions}
import org.apache.spark.sql.errors.QueryCompilationErrors
import org.apache.spark.sql.types._
import org.apache.spark.unsafe.types.{UTF8String, VariantVal}

object XmlExpressionEvalUtils {

  def schemaOfXml(xmlInferSchema: XmlInferSchema, xml: UTF8String): UTF8String = {
    val dataType = xmlInferSchema.infer(xml.toString).get match {
      case st: StructType =>
        xmlInferSchema.canonicalizeType(st).getOrElse(StructType(Nil))
      case at: ArrayType if at.elementType.isInstanceOf[StructType] =>
        xmlInferSchema
          .canonicalizeType(at.elementType)
          .map(ArrayType(_, containsNull = at.containsNull))
          .getOrElse(ArrayType(StructType(Nil), containsNull = at.containsNull))
      case other: DataType =>
        xmlInferSchema.canonicalizeType(other).getOrElse(StringType)
    }

    UTF8String.fromString(dataType.sql)
  }
}

trait XPathEvaluator {

  protected val path: UTF8String

  @transient protected lazy val xpathUtil: UDFXPathUtil = new UDFXPathUtil

  final def evaluate(xml: UTF8String): Any = {
    if (xml == null || xml.toString.isEmpty || path == null || path.toString.isEmpty) return null
    doEvaluate(xml)
  }

  def doEvaluate(xml: UTF8String): Any
}

case class XPathBooleanEvaluator(path: UTF8String) extends XPathEvaluator {
  override def doEvaluate(xml: UTF8String): Any = {
    xpathUtil.evalBoolean(xml.toString, path.toString)
  }
}

case class XPathShortEvaluator(path: UTF8String) extends XPathEvaluator {
  override def doEvaluate(xml: UTF8String): Any = {
    val ret = xpathUtil.evalNumber(xml.toString, path.toString)
    if (ret eq null) null.asInstanceOf[Short] else ret.shortValue()
  }
}

case class XPathIntEvaluator(path: UTF8String) extends XPathEvaluator {
  override def doEvaluate(xml: UTF8String): Any = {
    val ret = xpathUtil.evalNumber(xml.toString, path.toString)
    if (ret eq null) null.asInstanceOf[Int] else ret.intValue()
  }
}

case class XPathLongEvaluator(path: UTF8String) extends XPathEvaluator {
  override def doEvaluate(xml: UTF8String): Any = {
    val ret = xpathUtil.evalNumber(xml.toString, path.toString)
    if (ret eq null) null.asInstanceOf[Long] else ret.longValue()
  }
}

case class XPathFloatEvaluator(path: UTF8String) extends XPathEvaluator {
  override def doEvaluate(xml: UTF8String): Any = {
    val ret = xpathUtil.evalNumber(xml.toString, path.toString)
    if (ret eq null) null.asInstanceOf[Float] else ret.floatValue()
  }
}

case class XPathDoubleEvaluator(path: UTF8String) extends XPathEvaluator {
  override def doEvaluate(xml: UTF8String): Any = {
    val ret = xpathUtil.evalNumber(xml.toString, path.toString)
    if (ret eq null) null.asInstanceOf[Double] else ret.doubleValue()
  }
}

case class XPathStringEvaluator(path: UTF8String) extends XPathEvaluator {
  override def doEvaluate(xml: UTF8String): Any = {
    val ret = xpathUtil.evalString(xml.toString, path.toString)
    UTF8String.fromString(ret)
  }
}

case class XPathListEvaluator(path: UTF8String) extends XPathEvaluator {
  override def doEvaluate(xml: UTF8String): Any = {
    val nodeList = xpathUtil.evalNodeList(xml.toString, path.toString)
    if (nodeList ne null) {
      val ret = new Array[AnyRef](nodeList.getLength)
      var i = 0
      while (i < nodeList.getLength) {
        ret(i) = UTF8String.fromString(nodeList.item(i).getNodeValue)
        i += 1
      }
      new GenericArrayData(ret)
    } else {
      null
    }
  }
}

case class XmlToStructsEvaluator(
    options: Map[String, String],
    nullableSchema: DataType,
    nameOfCorruptRecord: String,
    timeZoneId: Option[String],
    child: Expression
) {
  @transient lazy val parsedOptions = new XmlOptions(options, timeZoneId.get, nameOfCorruptRecord)

  // This converts parsed rows to the desired output by the given schema.
  @transient
  private lazy val converter =
    (rows: Iterator[InternalRow]) => if (rows.hasNext) rows.next() else null

  // Parser that parse XML strings as internal rows
  @transient
  private lazy val parser = {
    val mode = parsedOptions.parseMode
    if (mode != PermissiveMode && mode != FailFastMode) {
      throw QueryCompilationErrors.parseModeUnsupportedError("from_xml", mode)
    }

    // The parser is only used when the input schema is StructType
    val schema = nullableSchema.asInstanceOf[StructType]
    ExprUtils.verifyColumnNameOfCorruptRecord(schema, parsedOptions.columnNameOfCorruptRecord)
    val rawParser = new StaxXmlParser(schema, parsedOptions)

    val xsdSchema = Option(parsedOptions.rowValidationXSDPath).map(ValidatorUtil.getSchema)

    new FailureSafeParser[String](
      input => rawParser.doParseColumn(input, mode, xsdSchema),
      mode,
      schema,
      parsedOptions.columnNameOfCorruptRecord)
  }

  final def evaluate(xml: UTF8String): Any = {
    if (xml == null) return null
    nullableSchema match {
      case _: VariantType => StaxXmlParser.parseVariant(xml.toString, parsedOptions)
      case _: StructType => converter(parser.parse(xml.toString))
    }
  }
}

case class StructsToXmlEvaluator(
    options: Map[String, String],
    inputSchema: DataType,
    timeZoneId: Option[String]) {

  @transient
  lazy val writer = new CharArrayWriter()

  @transient
  lazy val gen =
    new StaxXmlGenerator(inputSchema, writer, new XmlOptions(options, timeZoneId.get), false)

  // This converts rows to the XML output according to the given schema.
  @transient
  lazy val converter: Any => UTF8String = {
    def getAndReset(): UTF8String = {
      gen.flush()
      val xmlString = writer.toString
      writer.reset()
      UTF8String.fromString(xmlString)
    }

    inputSchema match {
      case _: StructType =>
        (row: Any) =>
          gen.write(row.asInstanceOf[InternalRow])
          getAndReset()
      case _: VariantType =>
        (v: Any) =>
          gen.write(v.asInstanceOf[VariantVal])
          getAndReset()
    }
  }

  final def evaluate(value: Any): Any = {
    if (value == null) return null
    converter(value)
  }
}
