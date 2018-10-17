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
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.csv._
import org.apache.spark.sql.catalyst.expressions.codegen.CodegenFallback
import org.apache.spark.sql.catalyst.util._
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
       {"a":1, "b":0.8}
      > SELECT _FUNC_('26/08/2015', 'time Timestamp', map('timestampFormat', 'dd/MM/yyyy'))
       {"time":2015-08-26 00:00:00.0}
  """,
  since = "3.0.0")
// scalastyle:on line.size.limit
case class CsvToStructs(
    schema: StructType,
    options: Map[String, String],
    child: Expression,
    timeZoneId: Option[String] = None)
  extends UnaryExpression
    with TimeZoneAwareExpression
    with CodegenFallback
    with ExpectsInputTypes
    with NullIntolerant {

  override def nullable: Boolean = child.nullable

  // The CSV input data might be missing certain fields. We force the nullability
  // of the user-provided schema to avoid data corruptions.
  val nullableSchema: StructType = schema.asNullable

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

  // This converts parsed rows to the desired output by the given schema.
  @transient
  lazy val converter = (rows: Iterator[InternalRow]) => {
    if (rows.hasNext) {
      val result = rows.next()
      // CSV's parser produces one record only.
      assert(!rows.hasNext)
      result
    } else {
      throw new IllegalArgumentException("Expected one row from CSV parser.")
    }
  }

  @transient lazy val parser = {
    val parsedOptions = new CSVOptions(options, columnPruning = true, timeZoneId.get)
    val mode = parsedOptions.parseMode
    if (mode != PermissiveMode && mode != FailFastMode) {
      throw new AnalysisException(s"from_csv() doesn't support the ${mode.name} mode. " +
        s"Acceptable modes are ${PermissiveMode.name} and ${FailFastMode.name}.")
    }
    val actualSchema =
      StructType(nullableSchema.filterNot(_.name == parsedOptions.columnNameOfCorruptRecord))
    val rawParser = new UnivocityParser(actualSchema, actualSchema, parsedOptions)
    new FailureSafeParser[String](
      input => Seq(rawParser.parse(input)),
      mode,
      nullableSchema,
      parsedOptions.columnNameOfCorruptRecord,
      parsedOptions.multiLine)
  }

  override def dataType: DataType = nullableSchema

  override def withTimeZone(timeZoneId: String): TimeZoneAwareExpression = {
    copy(timeZoneId = Option(timeZoneId))
  }

  override def nullSafeEval(input: Any): Any = {
    val csv = input.asInstanceOf[UTF8String].toString
    converter(parser.parse(csv))
  }

  override def inputTypes: Seq[AbstractDataType] = StringType :: Nil
}
