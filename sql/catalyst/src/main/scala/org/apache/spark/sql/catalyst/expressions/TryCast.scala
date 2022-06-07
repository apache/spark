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

import org.apache.spark.sql.catalyst.expressions.Cast.{forceNullable, resolvableNullability}
import org.apache.spark.sql.catalyst.expressions.codegen._
import org.apache.spark.sql.catalyst.expressions.codegen.Block._
import org.apache.spark.sql.types.{ArrayType, DataType, MapType, StructType}

/**
 * A special version of [[AnsiCast]]. It performs the same operation (i.e. converts a value of
 * one data type into another data type), but returns a NULL value instead of raising an error
 * when the conversion can not be performed.
 *
 * When cast from/to timezone related types, we need timeZoneId, which will be resolved with
 * session local timezone by an analyzer [[ResolveTimeZone]].
 */
@ExpressionDescription(
  usage = """
    _FUNC_(expr AS type) - Casts the value `expr` to the target data type `type`.
      This expression is identical to CAST with configuration `spark.sql.ansi.enabled` as
      true, except it returns NULL instead of raising an error. Note that the behavior of this
      expression doesn't depend on configuration `spark.sql.ansi.enabled`.
  """,
  examples = """
    Examples:
      > SELECT _FUNC_('10' as int);
       10
      > SELECT _FUNC_(1234567890123L as int);
       null
  """,
  since = "3.2.0",
  group = "conversion_funcs")
case class TryCast(child: Expression, dataType: DataType, timeZoneId: Option[String] = None)
  extends CastBase {
  override def withTimeZone(timeZoneId: String): TimeZoneAwareExpression =
    copy(timeZoneId = Option(timeZoneId))

  // Here we force `ansiEnabled` as true so that we can reuse the evaluation code branches which
  // throw exceptions on conversion failures.
  override protected val ansiEnabled: Boolean = true

  override def nullable: Boolean = true

  // If the target data type is a complex type which can't have Null values, we should guarantee
  // that the casting between the element types won't produce Null results.
  override def canCast(from: DataType, to: DataType): Boolean = (from, to) match {
    case (ArrayType(fromType, fn), ArrayType(toType, tn)) =>
      canCast(fromType, toType) &&
        resolvableNullability(fn || forceNullable(fromType, toType), tn)

    case (MapType(fromKey, fromValue, fn), MapType(toKey, toValue, tn)) =>
      canCast(fromKey, toKey) &&
        (!forceNullable(fromKey, toKey)) &&
        canCast(fromValue, toValue) &&
        resolvableNullability(fn || forceNullable(fromValue, toValue), tn)

    case (StructType(fromFields), StructType(toFields)) =>
      fromFields.length == toFields.length &&
        fromFields.zip(toFields).forall {
          case (fromField, toField) =>
            canCast(fromField.dataType, toField.dataType) &&
              resolvableNullability(
                fromField.nullable || forceNullable(fromField.dataType, toField.dataType),
                toField.nullable)
        }

    case _ =>
      Cast.canAnsiCast(from, to)
  }

  override def cast(from: DataType, to: DataType): Any => Any = (input: Any) =>
    try {
      super.cast(from, to)(input)
    } catch {
      case _: Exception =>
        null
    }

  override def castCode(ctx: CodegenContext, input: ExprValue, inputIsNull: ExprValue,
    result: ExprValue, resultIsNull: ExprValue, resultType: DataType, cast: CastFunction): Block = {
    val javaType = JavaCode.javaType(resultType)
    code"""
      boolean $resultIsNull = $inputIsNull;
      $javaType $result = ${CodeGenerator.defaultValue(resultType)};
      if (!$inputIsNull) {
        try {
          ${cast(input, result, resultIsNull)}
        } catch (Exception e) {
          $resultIsNull = true;
        }
      }
    """
  }

  override def typeCheckFailureMessage: String =
    Cast.typeCheckFailureMessage(child.dataType, dataType, None)

  override protected def withNewChildInternal(newChild: Expression): TryCast =
    copy(child = newChild)

  override def toString: String = {
    s"try_cast($child as ${dataType.simpleString})"
  }

  override def sql: String = s"TRY_CAST(${child.sql} AS ${dataType.sql})"
}
