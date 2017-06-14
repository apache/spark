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

import java.util.UUID

import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.codegen._
import org.apache.spark.sql.catalyst.util.{BigDecimalUtils, DateTimeUtils}
import org.apache.spark.sql.types._
import org.apache.spark.unsafe.types.UTF8String

/**
 * Print the result of an expression to stderr (used for debugging codegen).
 */
case class PrintToStderr(child: Expression) extends UnaryExpression {

  override def dataType: DataType = child.dataType

  protected override def nullSafeEval(input: Any): Any = input

  private val outputPrefix = s"Result of ${child.simpleString} is "

  override def doGenCode(ctx: CodegenContext, ev: ExprCode): ExprCode = {
    val outputPrefixField = ctx.addReferenceObj("outputPrefix", outputPrefix)
    nullSafeCodeGen(ctx, ev, c =>
      s"""
         | System.err.println($outputPrefixField + $c);
         | ${ev.value} = $c;
       """.stripMargin)
  }
}

/**
 * A function throws an exception if 'condition' is not true.
 */
@ExpressionDescription(
  usage = "_FUNC_(expr) - Throws an exception if `expr` is not true.",
  extended = """
    Examples:
      > SELECT _FUNC_(0 < 1);
       NULL
  """)
case class AssertTrue(child: Expression) extends UnaryExpression with ImplicitCastInputTypes {

  override def nullable: Boolean = true

  override def inputTypes: Seq[DataType] = Seq(BooleanType)

  override def dataType: DataType = NullType

  override def prettyName: String = "assert_true"

  private val errMsg = s"'${child.simpleString}' is not true!"

  override def eval(input: InternalRow) : Any = {
    val v = child.eval(input)
    if (v == null || java.lang.Boolean.FALSE.equals(v)) {
      throw new RuntimeException(errMsg)
    } else {
      null
    }
  }

  override def doGenCode(ctx: CodegenContext, ev: ExprCode): ExprCode = {
    val eval = child.genCode(ctx)

    // Use unnamed reference that doesn't create a local field here to reduce the number of fields
    // because errMsgField is used only when the value is null or false.
    val errMsgField = ctx.addReferenceMinorObj(errMsg)
    ExprCode(code = s"""${eval.code}
       |if (${eval.isNull} || !${eval.value}) {
       |  throw new RuntimeException($errMsgField);
       |}""".stripMargin, isNull = "true", value = "null")
  }

  override def sql: String = s"assert_true(${child.sql})"
}

/**
 * Returns the current database of the SessionCatalog.
 */
@ExpressionDescription(
  usage = "_FUNC_() - Returns the current database.",
  extended = """
    Examples:
      > SELECT _FUNC_();
       default
  """)
case class CurrentDatabase() extends LeafExpression with Unevaluable {
  override def dataType: DataType = StringType
  override def foldable: Boolean = true
  override def nullable: Boolean = false
  override def prettyName: String = "current_database"
}

// scalastyle:off line.size.limit
@ExpressionDescription(
  usage = "_FUNC_() - Returns an universally unique identifier (UUID) string. The value is returned as a canonical UUID 36-character string.",
  extended = """
    Examples:
      > SELECT _FUNC_();
       46707d92-02f4-4817-8116-a4c3b23e6266
  """)
// scalastyle:on line.size.limit
case class Uuid() extends LeafExpression {

  override def deterministic: Boolean = false

  override def nullable: Boolean = false

  override def dataType: DataType = StringType

  override def eval(input: InternalRow): Any = UTF8String.fromString(UUID.randomUUID().toString)

  override def doGenCode(ctx: CodegenContext, ev: ExprCode): ExprCode = {
    ev.copy(code = s"final UTF8String ${ev.value} = " +
      s"UTF8String.fromString(java.util.UUID.randomUUID().toString());", isNull = "false")
  }
}

/**
 * Returns date truncated to the unit specified by the format or
 * numeric truncated to scale decimal places.
 */
// scalastyle:off line.size.limit
@ExpressionDescription(
  usage = """
      _FUNC_(data[, fmt]) - Returns `data` truncated by the format model `fmt`.
        If `data` is DateType/StringType, returns `data` with the time portion of the day truncated to the unit specified by the format model `fmt`.
        If `data` is DecimalType/DoubleType, returns `data` truncated to `fmt` decimal places.
  """,
  extended = """
    Examples:
      > SELECT _FUNC_('2009-02-12', 'MM');
       2009-02-01.
      > SELECT _FUNC_('2015-10-27', 'YEAR');
       2015-01-01
      > SELECT _FUNC_('2015-10-27');
       2015-10-01
      > SELECT _FUNC_(1234567891.1234567891, 4);
       1234567891.1234
      > SELECT _FUNC_(1234567891.1234567891, -4);
       1234560000
      > SELECT _FUNC_(1234567891.1234567891);
       1234567891
  """)
// scalastyle:on line.size.limit
case class Trunc(data: Expression, format: Expression)
  extends BinaryExpression with ImplicitCastInputTypes {

  def this(data: Expression) = {
    this(data, Literal(
      if (data.dataType.isInstanceOf[DecimalType] || data.dataType.isInstanceOf[DoubleType]) {
        0
      } else {
        "MM"
      }))
  }

  override def left: Expression = data
  override def right: Expression = format

  val isTruncNumber = format.dataType.isInstanceOf[IntegerType]

  override def dataType: DataType = data.dataType

  override def inputTypes: Seq[AbstractDataType] =
    Seq(TypeCollection(DateType, StringType, DoubleType, DecimalType),
      TypeCollection(StringType, IntegerType))

  override def nullable: Boolean = true

  override def prettyName: String = "trunc"

  private lazy val truncFormat: Int = if (isTruncNumber) {
    format.eval().asInstanceOf[Int]
  } else {
    DateTimeUtils.parseTruncLevel(format.eval().asInstanceOf[UTF8String])
  }

  override def eval(input: InternalRow): Any = {
    val d = data.eval(input)
    val form = format.eval()
    if (null == d || null == form) {
      null
    } else {
      if (isTruncNumber) {
        val scale = if (format.foldable) truncFormat else format.eval().asInstanceOf[Int]
        data.dataType match {
          case DoubleType => BigDecimalUtils.trunc(d.asInstanceOf[Double], scale)
          case DecimalType.Fixed(_, _) =>
            BigDecimalUtils.trunc(d.asInstanceOf[Decimal].toJavaBigDecimal, scale)
        }
      } else {
        val level = if (format.foldable) {
          truncFormat
        } else {
          DateTimeUtils.parseTruncLevel(format.eval().asInstanceOf[UTF8String])
        }
        if (level == -1) {
          // unknown format
          null
        } else {
          DateTimeUtils.truncDate(d.asInstanceOf[Int], level)
        }
      }
    }
  }

  override def doGenCode(ctx: CodegenContext, ev: ExprCode): ExprCode = {

    if (isTruncNumber) {
      val bdu = BigDecimalUtils.getClass.getName.stripSuffix("$")

      if (format.foldable) {
        val d = data.genCode(ctx)
        ev.copy(code = s"""
            ${d.code}
            boolean ${ev.isNull} = ${d.isNull};
            ${ctx.javaType(dataType)} ${ev.value} = ${ctx.defaultValue(dataType)};
            if (!${ev.isNull}) {
              ${ev.value} = $bdu.trunc(${d.value}, $truncFormat);
            }""")
      } else {
        nullSafeCodeGen(ctx, ev, (doubleVal, fmt) => {
          s"${ev.value} = $bdu.trunc($doubleVal, $fmt);"
        })
      }
    } else {
      val dtu = DateTimeUtils.getClass.getName.stripSuffix("$")

      if (format.foldable) {
        if (truncFormat == -1) {
          ev.copy(code = s"""
              boolean ${ev.isNull} = true;
              ${ctx.javaType(dataType)} ${ev.value} = ${ctx.defaultValue(dataType)};""")
        } else {
          val d = data.genCode(ctx)
          ev.copy(code = s"""
              ${d.code}
              boolean ${ev.isNull} = ${d.isNull};
              ${ctx.javaType(dataType)} ${ev.value} = ${ctx.defaultValue(dataType)};
              if (!${ev.isNull}) {
                ${ev.value} = $dtu.truncDate(${d.value}, $truncFormat);
              }""")
        }
      } else {
        nullSafeCodeGen(ctx, ev, (dateVal, fmt) => {
          val form = ctx.freshName("form")
          s"""
              int $form = $dtu.parseTruncLevel($fmt);
              if ($form == -1) {
                ${ev.isNull} = true;
              } else {
                ${ev.value} = $dtu.truncDate($dateVal, $form);
              }
            """
        })
      }
    }
  }
}
