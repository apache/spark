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

import org.apache.spark.sql.catalyst.analysis.TypeCheckResult
import org.apache.spark.sql.catalyst.util.ArrayBasedMapData
import org.apache.spark.sql.errors.QueryCompilationErrors
import org.apache.spark.sql.types.{MapType, NullType, StringType}
import org.apache.spark.unsafe.types.UTF8String
import org.apache.spark.util.Utils

/**
 * Converts a binary column of Avro format into its corresponding Catalyst value.
 * This is a thin wrapper over the [[AvroDataToCatalyst]] class to create a SQL function.
 *
 * @param child the Catalyst binary input column.
 * @param jsonFormatSchema the Avro schema in JSON string format.
 * @param options the options to use when performing the conversion.
 *
 * @since 4.0.0
 */
// scalastyle:off line.size.limit
@ExpressionDescription(
  usage = """
    _FUNC_(child, jsonFormatSchema, options) - Converts a binary Avro value into a Catalyst value.
    """,
  examples = """
    Examples:
      > SELECT _FUNC_(s, '{"type": "record", "name": "struct", "fields": [{ "name": "u", "type": ["int","string"] }]}', map()) IS NULL AS result FROM (SELECT NAMED_STRUCT('u', NAMED_STRUCT('member0', member0, 'member1', member1)) AS s FROM VALUES (1, NULL), (NULL,  'a') tab(member0, member1));
       [false]
  """,
  note = """
    The specified schema must match actual schema of the read data, otherwise the behavior
    is undefined: it may fail or return arbitrary result.
    To deserialize the data with a compatible and evolved schema, the expected Avro schema can be
    set via the corresponding option.
  """,
  group = "misc_funcs",
  since = "4.0.0"
)
// scalastyle:on line.size.limit
case class FromAvro(child: Expression, jsonFormatSchema: Expression, options: Expression)
  extends TernaryExpression with RuntimeReplaceable {
  override def first: Expression = child
  override def second: Expression = jsonFormatSchema
  override def third: Expression = options

  override def withNewChildrenInternal(
      newFirst: Expression, newSecond: Expression, newThird: Expression): Expression = {
    copy(child = newFirst, jsonFormatSchema = newSecond, options = newThird)
  }

  override def checkInputDataTypes(): TypeCheckResult = {
    val schemaCheck = jsonFormatSchema.dataType match {
      case _: StringType |
           _: NullType
        if jsonFormatSchema.foldable =>
        None
      case _ =>
        Some(TypeCheckResult.TypeCheckFailure(
          "The second argument of the FROM_AVRO SQL function must be a constant string " +
            "containing the JSON representation of the schema to use for converting the value " +
            "from AVRO format"))
    }
    val optionsCheck = options.dataType match {
      case MapType(StringType, StringType, _) |
           MapType(NullType, NullType, _) |
           _: NullType
        if options.foldable =>
        None
      case _ =>
        Some(TypeCheckResult.TypeCheckFailure(
          "The third argument of the FROM_AVRO SQL function must be a constant map of strings to " +
            "strings containing the options to use for converting the value from AVRO format"))
    }
    schemaCheck.getOrElse(
      optionsCheck.getOrElse(
        TypeCheckResult.TypeCheckSuccess))
  }

  override lazy val replacement: Expression = {
    val schemaValue: String = jsonFormatSchema.eval() match {
      case s: UTF8String =>
        s.toString
      case null =>
        ""
    }
    val optionsValue: Map[String, String] = options.eval() match {
      case a: ArrayBasedMapData if a.keyArray.array.nonEmpty =>
        val keys: Array[String] = a.keyArray.array.map(_.toString)
        val values: Array[String] = a.valueArray.array.map(_.toString)
        keys.zip(values).toMap
      case _ =>
        Map.empty
    }
    val constructor = try {
      Utils.classForName("org.apache.spark.sql.avro.AvroDataToCatalyst").getConstructors().head
    } catch {
      case _: java.lang.ClassNotFoundException =>
        throw QueryCompilationErrors.avroNotLoadedSqlFunctionsUnusable(functionName = "FROM_AVRO")
    }
    val expr = constructor.newInstance(child, schemaValue, optionsValue)
    expr.asInstanceOf[Expression]
  }
}

/**
 * Converts a Catalyst binary input value into its corresponding AvroAvro format result.
 * This is a thin wrapper over the [[CatalystDataToAvro]] class to create a SQL function.
 *
 * @param child the Catalyst binary input column.
 * @param jsonFormatSchema the Avro schema in JSON string format.
 *
 * @since 4.0.0
 */
// scalastyle:off line.size.limit
@ExpressionDescription(
  usage = """
    _FUNC_(child, jsonFormatSchema) - Converts a Catalyst binary input value into its corresponding
      Avro format result.
  """,
  examples = """
    Examples:
      > SELECT _FUNC_(s, '{"type": "record", "name": "struct", "fields": [{ "name": "u", "type": ["int","string"] }]}', MAP()) IS NULL FROM (SELECT NULL AS s);
       [true]
  """,
  group = "misc_funcs",
  since = "4.0.0"
)
// scalastyle:on line.size.limit
case class ToAvro(child: Expression, jsonFormatSchema: Expression)
  extends BinaryExpression with RuntimeReplaceable {

  def this(child: Expression) = this(child, Literal(null))

  override def left: Expression = child

  override def right: Expression = jsonFormatSchema

  override def withNewChildrenInternal(newLeft: Expression, newRight: Expression): Expression = {
    copy(child = newLeft, jsonFormatSchema = newRight)
  }

  override def checkInputDataTypes(): TypeCheckResult = {
    jsonFormatSchema.dataType match {
      case _: StringType if jsonFormatSchema.foldable =>
        TypeCheckResult.TypeCheckSuccess
      case _: NullType =>
        // The 'jsonFormatSchema' argument is optional.
        TypeCheckResult.TypeCheckSuccess
      case _ =>
        TypeCheckResult.TypeCheckFailure(
          "The second argument of the TO_AVRO SQL function must be a constant string " +
            "containing the JSON representation of the schema to use for converting the value " +
            "to AVRO format")
    }
  }

  override lazy val replacement: Expression = {
    val schemaValue: Option[String] = jsonFormatSchema.eval() match {
      case null =>
        None
      case s: UTF8String =>
        Some(s.toString)
    }
    val constructor = try {
      Utils.classForName("org.apache.spark.sql.avro.CatalystDataToAvro").getConstructors().head
    } catch {
      case _: java.lang.ClassNotFoundException =>
        throw QueryCompilationErrors.avroNotLoadedSqlFunctionsUnusable(functionName = "TO_AVRO")
    }
    val expr = constructor.newInstance(child, schemaValue)
    expr.asInstanceOf[Expression]
  }
}
