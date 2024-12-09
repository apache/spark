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

import java.io.CharArrayWriter

import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.analysis.TypeCheckResult
import org.apache.spark.sql.catalyst.analysis.TypeCheckResult.{DataTypeMismatch, TypeCheckSuccess}
import org.apache.spark.sql.catalyst.csv._
import org.apache.spark.sql.catalyst.expressions.codegen.{CodegenContext, CodeGenerator, ExprCode}
import org.apache.spark.sql.catalyst.expressions.codegen.Block.BlockHelper
import org.apache.spark.sql.catalyst.expressions.csv.{CsvToStructsEvaluator, SchemaOfCsvEvaluator}
import org.apache.spark.sql.catalyst.expressions.objects.Invoke
import org.apache.spark.sql.catalyst.util.TypeUtils._
import org.apache.spark.sql.errors.QueryErrorsBase
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.internal.types.StringTypeWithCollation
import org.apache.spark.sql.types._
import org.apache.spark.unsafe.types.UTF8String

/**
 * Converts a CSV input string to a [[StructType]] with the specified schema.
 */
// scalastyle:off line.size.limit
@ExpressionDescription(
  usage = "_FUNC_(csvStr, schema[, options]) - Returns a struct value with the given `csvStr` and `schema`.",
  examples = """
    Examples:
      > SELECT _FUNC_('1, 0.8', 'a INT, b DOUBLE');
       {"a":1,"b":0.8}
      > SELECT _FUNC_('26/08/2015', 'time Timestamp', map('timestampFormat', 'dd/MM/yyyy'));
       {"time":2015-08-26 00:00:00}
  """,
  since = "3.0.0",
  group = "csv_funcs")
// scalastyle:on line.size.limit
case class CsvToStructs(
    schema: StructType,
    options: Map[String, String],
    child: Expression,
    timeZoneId: Option[String] = None,
    requiredSchema: Option[StructType] = None)
  extends UnaryExpression
  with TimeZoneAwareExpression
  with ExpectsInputTypes {

  override def nullable: Boolean = child.nullable

  override def nullIntolerant: Boolean = true

  // Used in `FunctionRegistry`
  def this(child: Expression, schema: Expression, options: Map[String, String]) =
    this(
      schema = ExprUtils.evalSchemaExpr(schema),
      options = options,
      child = child,
      timeZoneId = None)

  def this(child: Expression, schema: Expression) = this(child, schema, Map.empty[String, String])

  def this(child: Expression, schema: Expression, options: Expression) =
    this(
      schema = ExprUtils.evalSchemaExpr(schema),
      options = ExprUtils.convertToMapData(options),
      child = child,
      timeZoneId = None)

  override def dataType: DataType = requiredSchema.getOrElse(schema).asNullable

  override def withTimeZone(timeZoneId: String): TimeZoneAwareExpression = {
    copy(timeZoneId = Option(timeZoneId))
  }

  override def inputTypes: Seq[AbstractDataType] =
    StringTypeWithCollation(supportsTrimCollation = true) :: Nil

  override def prettyName: String = "from_csv"

  // The CSV input data might be missing certain fields. We force the nullability
  // of the user-provided schema to avoid data corruptions.
  private val nullableSchema: StructType = schema.asNullable

  @transient
  private val nameOfCorruptRecord = SQLConf.get.getConf(SQLConf.COLUMN_NAME_OF_CORRUPT_RECORD)

  @transient
  private lazy val evaluator: CsvToStructsEvaluator = CsvToStructsEvaluator(
    options, nullableSchema, nameOfCorruptRecord, timeZoneId, requiredSchema)

  override def nullSafeEval(input: Any): Any = {
    evaluator.evaluate(input.asInstanceOf[UTF8String])
  }

  override def doGenCode(ctx: CodegenContext, ev: ExprCode): ExprCode = {
    val refEvaluator = ctx.addReferenceObj("evaluator", evaluator)
    val eval = child.genCode(ctx)
    val resultType = CodeGenerator.boxedType(dataType)
    val resultTerm = ctx.freshName("result")
    ev.copy(code =
      code"""
         |${eval.code}
         |$resultType $resultTerm = ($resultType) $refEvaluator.evaluate(${eval.value});
         |boolean ${ev.isNull} = $resultTerm == null;
         |${CodeGenerator.javaType(dataType)} ${ev.value} = ${CodeGenerator.defaultValue(dataType)};
         |if (!${ev.isNull}) {
         |  ${ev.value} = $resultTerm;
         |}
         |""".stripMargin)
  }

  override protected def withNewChildInternal(newChild: Expression): CsvToStructs =
    copy(child = newChild)
}

/**
 * A function infers schema of CSV string.
 */
@ExpressionDescription(
  usage = "_FUNC_(csv[, options]) - Returns schema in the DDL format of CSV string.",
  examples = """
    Examples:
      > SELECT _FUNC_('1,abc');
       STRUCT<_c0: INT, _c1: STRING>
  """,
  since = "3.0.0",
  group = "csv_funcs")
case class SchemaOfCsv(
    child: Expression,
    options: Map[String, String])
  extends UnaryExpression with RuntimeReplaceable with QueryErrorsBase {

  def this(child: Expression) = this(child, Map.empty[String, String])

  def this(child: Expression, options: Expression) = this(
    child = child,
    options = ExprUtils.convertToMapData(options))

  override def dataType: DataType = SQLConf.get.defaultStringType

  override def nullable: Boolean = false

  @transient
  private lazy val csv = child.eval().asInstanceOf[UTF8String]

  override def checkInputDataTypes(): TypeCheckResult = {
    if (child.foldable && csv != null) {
      super.checkInputDataTypes()
    } else if (!child.foldable) {
      DataTypeMismatch(
        errorSubClass = "NON_FOLDABLE_INPUT",
        messageParameters = Map(
          "inputName" -> toSQLId("csv"),
          "inputType" -> toSQLType(child.dataType),
          "inputExpr" -> toSQLExpr(child)))
    } else {
      DataTypeMismatch(
        errorSubClass = "UNEXPECTED_NULL",
        messageParameters = Map("exprName" -> "csv"))
    }
  }

  override def prettyName: String = "schema_of_csv"

  override protected def withNewChildInternal(newChild: Expression): SchemaOfCsv =
    copy(child = newChild)

  @transient
  private lazy val evaluator: SchemaOfCsvEvaluator = SchemaOfCsvEvaluator(options)

  override def replacement: Expression = Invoke(
    Literal.create(evaluator, ObjectType(classOf[SchemaOfCsvEvaluator])),
    "evaluate",
    dataType,
    Seq(child),
    Seq(child.dataType),
    returnNullable = false)
}

/**
 * Converts a [[StructType]] to a CSV output string.
 */
// scalastyle:off line.size.limit
@ExpressionDescription(
  usage = "_FUNC_(expr[, options]) - Returns a CSV string with a given struct value",
  examples = """
    Examples:
      > SELECT _FUNC_(named_struct('a', 1, 'b', 2));
       1,2
      > SELECT _FUNC_(named_struct('time', to_timestamp('2015-08-26', 'yyyy-MM-dd')), map('timestampFormat', 'dd/MM/yyyy'));
       26/08/2015
  """,
  since = "3.0.0",
  group = "csv_funcs")
// scalastyle:on line.size.limit
case class StructsToCsv(
     options: Map[String, String],
     child: Expression,
     timeZoneId: Option[String] = None)
  extends UnaryExpression with TimeZoneAwareExpression with ExpectsInputTypes {
  override def nullIntolerant: Boolean = true
  override def nullable: Boolean = true

  def this(options: Map[String, String], child: Expression) = this(options, child, None)

  // Used in `FunctionRegistry`
  def this(child: Expression) = this(Map.empty, child, None)

  def this(child: Expression, options: Expression) =
    this(
      options = ExprUtils.convertToMapData(options),
      child = child,
      timeZoneId = None)

  override def checkInputDataTypes(): TypeCheckResult = {
    child.dataType match {
      case schema: StructType if schema.map(_.dataType).forall(
        dt => isSupportedDataType(dt)) => TypeCheckSuccess
      case _ => DataTypeMismatch(
        errorSubClass = "UNSUPPORTED_INPUT_TYPE",
        messageParameters = Map(
          "functionName" -> toSQLId(prettyName),
          "dataType" -> toSQLType(child.dataType)
        )
      )
    }
  }

  private def isSupportedDataType(dataType: DataType): Boolean = dataType match {
    case _: VariantType => false
    case array: ArrayType => isSupportedDataType(array.elementType)
    case map: MapType => isSupportedDataType(map.keyType) && isSupportedDataType(map.valueType)
    case st: StructType => st.map(_.dataType).forall(dt => isSupportedDataType(dt))
    case udt: UserDefinedType[_] => isSupportedDataType(udt.sqlType)
    case _ => true
  }

  @transient
  lazy val writer = new CharArrayWriter()

  @transient
  lazy val inputSchema: StructType = child.dataType.asInstanceOf[StructType]

  @transient
  lazy val gen = new UnivocityGenerator(
    inputSchema, writer, new CSVOptions(options, columnPruning = true, timeZoneId.get))

  // This converts rows to the CSV output according to the given schema.
  @transient
  lazy val converter: Any => UTF8String = {
    (row: Any) => UTF8String.fromString(gen.writeToString(row.asInstanceOf[InternalRow]))
  }

  override def dataType: DataType = SQLConf.get.defaultStringType

  override def withTimeZone(timeZoneId: String): TimeZoneAwareExpression =
    copy(timeZoneId = Option(timeZoneId))

  override def nullSafeEval(value: Any): Any = converter(value)

  override def inputTypes: Seq[AbstractDataType] = StructType :: Nil

  override def prettyName: String = "to_csv"

  override protected def withNewChildInternal(newChild: Expression): StructsToCsv =
    copy(child = newChild)

  override protected def doGenCode(ctx: CodegenContext, ev: ExprCode): ExprCode = {
    val structsToCsv = ctx.addReferenceObj("structsToCsv", this)
    nullSafeCodeGen(ctx, ev,
      eval => s"${ev.value} = (UTF8String) $structsToCsv.converter().apply($eval);")
  }
}
