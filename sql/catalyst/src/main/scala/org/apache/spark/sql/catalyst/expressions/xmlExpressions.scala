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

import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.analysis.TypeCheckResult
import org.apache.spark.sql.catalyst.analysis.TypeCheckResult.DataTypeMismatch
import org.apache.spark.sql.catalyst.expressions.{ExpectsInputTypes, Expression, ExpressionDescription, ExprUtils, NullIntolerant, TimeZoneAwareExpression, UnaryExpression}
import org.apache.spark.sql.catalyst.expressions.codegen.CodegenFallback
import org.apache.spark.sql.catalyst.util.{ArrayData, FailFastMode, FailureSafeParser, GenericArrayData, PermissiveMode}
import org.apache.spark.sql.catalyst.xml.{StaxXmlParser, ValidatorUtil, XmlInferSchema, XmlOptions}
import org.apache.spark.sql.errors.{QueryCompilationErrors, QueryErrorsBase}
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.types._
import org.apache.spark.unsafe.types.UTF8String

/**
 * Converts an XML input string to a [[StructType]] with the specified schema.
 * It is assumed that the XML input string constitutes a single record; so the
 * [[rowTag]] option will be not applicable.
 */
// scalastyle:off line.size.limit
@ExpressionDescription(
  usage = "_FUNC_(xmlStr, schema[, options]) - Returns a struct value with the given `xmlStr` and `schema`.",
  examples = """
    Examples:
      > SELECT _FUNC_('<p><a>1</a><b>0.8</b></p>', 'a INT, b DOUBLE');
       {"a":1,"b":0.8}
      > SELECT _FUNC_('<p><time>26/08/2015</time></p>', 'time Timestamp', map('timestampFormat', 'dd/MM/yyyy'));
       {"time":2015-08-26 00:00:00}
      > SELECT _FUNC_('<p><teacher>Alice</teacher><student><name>Bob</name><rank>1</rank></student><student><name>Charlie</name><rank>2</rank></student></p>', 'STRUCT<teacher: STRING, student: ARRAY<STRUCT<name: STRING, rank: INT>>>');
       {"teacher":"Alice","student":[{"name":"Bob","rank":1},{"name":"Charlie","rank":2}]}
  """,
  group = "xml_funcs",
  since = "4.0.0")
// scalastyle:on line.size.limit
case class XmlToStructs(
    schema: DataType,
    options: Map[String, String],
    child: Expression,
    timeZoneId: Option[String] = None)
  extends UnaryExpression
  with TimeZoneAwareExpression
  with CodegenFallback
  with ExpectsInputTypes
  with NullIntolerant
  with QueryErrorsBase {

  def this(child: Expression, schema: Expression, options: Map[String, String]) =
    this(
      schema = ExprUtils.evalSchemaExpr(schema),
      options = options,
      child = child,
      timeZoneId = None)

  override def nullable: Boolean = true

  // The XML input data might be missing certain fields. We force the nullability
  // of the user-provided schema to avoid data corruptions.
  val nullableSchema = schema.asNullable

  def this(child: Expression, schema: Expression) = this(child, schema, Map.empty[String, String])

  def this(child: Expression, schema: Expression, options: Expression) =
    this(
      schema = ExprUtils.evalSchemaExpr(schema),
      options = ExprUtils.convertToMapData(options),
      child = child,
      timeZoneId = None)

  // This converts parsed rows to the desired output by the given schema.
  @transient
  lazy val converter = nullableSchema match {
    case _: StructType =>
      (rows: Iterator[InternalRow]) => if (rows.hasNext) rows.next() else null
    case _: ArrayType =>
      (rows: Iterator[InternalRow]) => if (rows.hasNext) rows.next().getArray(0) else null
    case _: MapType =>
      (rows: Iterator[InternalRow]) => if (rows.hasNext) rows.next().getMap(0) else null
  }

  val nameOfCorruptRecord = SQLConf.get.getConf(SQLConf.COLUMN_NAME_OF_CORRUPT_RECORD)

  @transient lazy val parser = {
    val parsedOptions = new XmlOptions(options, timeZoneId.get, nameOfCorruptRecord)
    val mode = parsedOptions.parseMode
    if (mode != PermissiveMode && mode != FailFastMode) {
      throw QueryCompilationErrors.parseModeUnsupportedError("from_xml", mode)
    }
    val (parserSchema, actualSchema) = nullableSchema match {
      case s: StructType =>
        ExprUtils.verifyColumnNameOfCorruptRecord(s, parsedOptions.columnNameOfCorruptRecord)
        (s, StructType(s.filterNot(_.name == parsedOptions.columnNameOfCorruptRecord)))
      case other =>
        (StructType(Array(StructField("value", other))), other)
    }

    val rowSchema: StructType = schema match {
      case st: StructType => st
      case ArrayType(st: StructType, _) => st
    }
    val rawParser = new StaxXmlParser(rowSchema, parsedOptions)
    val xsdSchema = Option(parsedOptions.rowValidationXSDPath).map(ValidatorUtil.getSchema)

    new FailureSafeParser[String](
      input => rawParser.doParseColumn(input, mode, xsdSchema),
      mode,
      parserSchema,
      parsedOptions.columnNameOfCorruptRecord)
  }

  override def dataType: DataType = nullableSchema

  override def withTimeZone(timeZoneId: String): TimeZoneAwareExpression = {
    copy(timeZoneId = Option(timeZoneId))
  }
  override def nullSafeEval(xml: Any): Any = xml match {
    case arr: GenericArrayData =>
      new GenericArrayData(arr.array.map(s => converter(parser.parse(s.toString))))
    case arr: ArrayData =>
      new GenericArrayData(arr.array.map(s => converter(parser.parse(s.toString))))
    case _ =>
      val str = xml.asInstanceOf[UTF8String].toString
      converter(parser.parse(str))
  }

  override def inputTypes: Seq[AbstractDataType] = StringType :: Nil

  override def sql: String = schema match {
    case _: MapType => "entries"
    case _ => super.sql
  }

  override def prettyName: String = "from_xml"

  protected def withNewChildInternal(newChild: Expression): XmlToStructs =
    copy(child = newChild)
}

/**
 * A function infers schema of XML string.
 */
@ExpressionDescription(
  usage = "_FUNC_(xml[, options]) - Returns schema in the DDL format of XML string.",
  examples = """
    Examples:
      > SELECT _FUNC_('<p><a>1</a></p>');
       STRUCT<a: BIGINT>
      > SELECT _FUNC_('<p><a attr="2">1</a><a>3</a></p>', map('excludeAttribute', 'true'));
       STRUCT<a: ARRAY<BIGINT>>
  """,
  since = "4.0.0",
  group = "xml_funcs")
case class SchemaOfXml(
    child: Expression,
    options: Map[String, String])
  extends UnaryExpression with CodegenFallback with QueryErrorsBase {

  def this(child: Expression) = this(child, Map.empty[String, String])

  def this(child: Expression, options: Expression) = this(
    child = child,
    options = ExprUtils.convertToMapData(options))

  override def dataType: DataType = StringType

  override def nullable: Boolean = false

  @transient
  private lazy val xmlOptions = new XmlOptions(options, "UTC")

  @transient
  private lazy val xmlFactory = xmlOptions.buildXmlFactory()

  @transient
  private lazy val xml = child.eval().asInstanceOf[UTF8String]

  override def checkInputDataTypes(): TypeCheckResult = {
    if (child.foldable && xml != null) {
      super.checkInputDataTypes()
    } else if (!child.foldable) {
      DataTypeMismatch(
        errorSubClass = "NON_FOLDABLE_INPUT",
        messageParameters = Map(
          "inputName" -> toSQLId("xml"),
          "inputType" -> toSQLType(child.dataType),
          "inputExpr" -> toSQLExpr(child)))
    } else {
      DataTypeMismatch(
        errorSubClass = "UNEXPECTED_NULL",
        messageParameters = Map("exprName" -> "xml"))
    }
  }

  override def eval(v: InternalRow): Any = {
    val dataType = XmlInferSchema.infer(xml.toString, xmlOptions).get match {
      case st: StructType =>
        XmlInferSchema.canonicalizeType(st).getOrElse(StructType(Nil))
      case at: ArrayType if at.elementType.isInstanceOf[StructType] =>
        XmlInferSchema
          .canonicalizeType(at.elementType)
          .map(ArrayType(_, containsNull = at.containsNull))
          .getOrElse(ArrayType(StructType(Nil), containsNull = at.containsNull))
      case other: DataType =>
        XmlInferSchema.canonicalizeType(other).getOrElse(StringType)
    }

    UTF8String.fromString(dataType.sql)
  }

  override def prettyName: String = "schema_of_xml"

  override protected def withNewChildInternal(newChild: Expression): SchemaOfXml =
    copy(child = newChild)
}
