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
import org.apache.spark.sql.catalyst.util.{DateTimeUtils, MathUtils}
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
  examples = """
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
  examples = """
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
  examples = """
    Examples:
      > SELECT _FUNC_();
       46707d92-02f4-4817-8116-a4c3b23e6266
  """)
// scalastyle:on line.size.limit
case class Uuid() extends LeafExpression {

  override lazy val deterministic: Boolean = false

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
      _FUNC_(data[, trunc_param]) - Returns `data` truncated by the format model `trunc_param`.
        If `data` is date/timestamp/string type, returns `data` with the time portion of the day truncated to the unit specified by the format model `trunc_param`. If `trunc_param` is omitted, then the default `trunc_param` is 'MM'.
        If `data` is decimal/double type, returns `data` truncated to `trunc_param` decimal places. If `trunc_param` is omitted, then the default `trunc_param` is 0.
  """,
  extended = """
    Examples:
      > SELECT _FUNC_('2009-02-12', 'MM');
       2009-02-01.
      > SELECT _FUNC_('2015-10-27', 'YEAR');
       2015-01-01
      > SELECT _FUNC_('1989-03-13');
       1989-03-01
      > SELECT _FUNC_(1234567891.1234567891, 4);
       1234567891.1234
      > SELECT _FUNC_(1234567891.1234567891, -4);
       1234560000
      > SELECT _FUNC_(1234567891.1234567891);
       1234567891
  """)
// scalastyle:on line.size.limit
case class Trunc(data: Expression, truncExpr: Expression)
  extends BinaryExpression with ExpectsInputTypes {

  def this(data: Expression) = {
    this(data, Literal(
      if (data.dataType.isInstanceOf[DateType] ||
        data.dataType.isInstanceOf[TimestampType] ||
        data.dataType.isInstanceOf[StringType]) {
      "MM"
     } else {
        0
      })
    )
  }

  override def left: Expression = data
  override def right: Expression = truncExpr

  private val isTruncNumber = truncExpr.dataType.isInstanceOf[IntegerType]
  private val isTruncDate = truncExpr.dataType.isInstanceOf[StringType]

  override def dataType: DataType = if (isTruncDate) DateType else data.dataType

  override def inputTypes: Seq[AbstractDataType] = data.dataType match {
    case NullType =>
      Seq(dataType, TypeCollection(StringType, IntegerType))
    case DateType | TimestampType | StringType =>
      Seq(TypeCollection(DateType, TimestampType, StringType), StringType)
    case DoubleType | DecimalType.Fixed(_, _) =>
      Seq(TypeCollection(DoubleType, DecimalType), IntegerType)
    case _ =>
      Seq(TypeCollection(DateType, StringType, TimestampType, DoubleType, DecimalType),
        TypeCollection(StringType, IntegerType))
  }

  override def nullable: Boolean = true

  override def prettyName: String = "trunc"


  private lazy val truncFormat: Int = if (isTruncNumber) {
    truncExpr.eval().asInstanceOf[Int]
  } else if (isTruncDate) {
    DateTimeUtils.parseTruncLevel(truncExpr.eval().asInstanceOf[UTF8String])
  } else {
    0
  }

  override def eval(input: InternalRow): Any = {
    val d = data.eval(input)
    val truncParam = truncExpr.eval()
    if (null == d || null == truncParam) {
      null
    } else {
      if (isTruncNumber) {
        val scale = if (truncExpr.foldable) truncFormat else truncExpr.eval().asInstanceOf[Int]
        data.dataType match {
          case DoubleType => MathUtils.trunc(d.asInstanceOf[Double], scale)
          case DecimalType.Fixed(_, _) =>
            MathUtils.trunc(d.asInstanceOf[Decimal].toJavaBigDecimal, scale)
        }
      } else if (isTruncDate) {
        val level = if (truncExpr.foldable) {
          truncFormat
        } else {
          DateTimeUtils.parseTruncLevel(truncExpr.eval().asInstanceOf[UTF8String])
        }
        if (level == -1) {
          // unknown format
          null
        } else {
          data.dataType match {
            case DateType => DateTimeUtils.truncDate(d.asInstanceOf[Int], level)
            case TimestampType =>
              val ts = DateTimeUtils.timestampToString(d.asInstanceOf[Long])
              val dt = DateTimeUtils.stringToDate(UTF8String.fromString(ts))
              if (dt.isDefined) DateTimeUtils.truncDate(dt.get, level) else null
            case StringType =>
              val dt = DateTimeUtils.stringToDate(d.asInstanceOf[UTF8String])
              if (dt.isDefined) DateTimeUtils.truncDate(dt.get, level) else null
          }
        }
      } else {
        null
      }
    }
  }

  override def doGenCode(ctx: CodegenContext, ev: ExprCode): ExprCode = {

    if (isTruncNumber) {
      val bdu = MathUtils.getClass.getName.stripSuffix("$")

      if (truncExpr.foldable) {
        val d = data.genCode(ctx)
        ev.copy(code = s"""
          ${d.code}
          boolean ${ev.isNull} = ${d.isNull};
          ${ctx.javaType(dataType)} ${ev.value} = ${ctx.defaultValue(dataType)};
          if (!${ev.isNull}) {
            ${ev.value} = $bdu.trunc(${d.value}, $truncFormat);
          }""")
      } else {
        nullSafeCodeGen(ctx, ev, (doubleVal, truncParam) =>
          s"${ev.value} = $bdu.trunc($doubleVal, $truncParam);")
      }
    } else if (isTruncDate) {
      val dtu = DateTimeUtils.getClass.getName.stripSuffix("$")

      if (truncExpr.foldable) {
        if (truncFormat == -1) {
          ev.copy(code = s"""
            boolean ${ev.isNull} = true;
            int ${ev.value} = ${ctx.defaultValue(DateType)};""")
        } else {
          val d = data.genCode(ctx)
          val dt = ctx.freshName("dt")
          val pre = s"""
            ${d.code}
            boolean ${ev.isNull} = ${d.isNull};
            int ${ev.value} = ${ctx.defaultValue(DateType)};"""
          data.dataType match {
            case DateType =>
              ev.copy(code = pre + s"""
                if (!${ev.isNull}) {
                  ${ev.value} = $dtu.truncDate(${d.value}, $truncFormat);
                }""")
            case TimestampType =>
              val ts = ctx.freshName("ts")
              ev.copy(code = pre + s"""
                String $ts = $dtu.timestampToString(${d.value});
                scala.Option<SQLDate> $dt = $dtu.stringToDate(UTF8String.fromString($ts));
                if (!${ev.isNull}) {
                  ${ev.value} = $dtu.truncDate((Integer)dt.get(), $truncFormat);
                }""")
            case StringType =>
              ev.copy(code = pre + s"""
                scala.Option<SQLDate> $dt = $dtu.stringToDate(${d.value});
                if (!${ev.isNull} && $dt.isDefined()) {
                  ${ev.value} = $dtu.truncDate((Integer)$dt.get(), $truncFormat);
                }""")
          }
        }
      } else {
        nullSafeCodeGen(ctx, ev, (dateVal, fmt) => {
          val truncParam = ctx.freshName("truncParam")
          val dt = ctx.freshName("dt")
          val pre = s"int $truncParam = $dtu.parseTruncLevel($fmt);"
          data.dataType match {
            case DateType =>
              pre + s"""
                if ($truncParam == -1) {
                  ${ev.isNull} = true;
                } else {
                  ${ev.value} = $dtu.truncDate($dateVal, $truncParam);
                }"""
            case TimestampType =>
              val ts = ctx.freshName("ts")
              pre + s"""
                String $ts = $dtu.timestampToString($dateVal);
                scala.Option<SQLDate> $dt = $dtu.stringToDate(UTF8String.fromString($ts));
                if ($truncParam == -1 || $dt.isEmpty()) {
                  ${ev.isNull} = true;
                } else {
                  ${ev.value} = $dtu.truncDate((Integer)$dt.get(), $truncParam);
                }"""
            case StringType =>
              pre + s"""
                scala.Option<SQLDate> $dt = $dtu.stringToDate($dateVal);
                ${ev.value} = ${ctx.defaultValue(DateType)};
                if ($truncParam == -1 || $dt.isEmpty()) {
                  ${ev.isNull} = true;
                } else {
                  ${ev.value} = $dtu.truncDate((Integer)$dt.get(), $truncParam);
                }"""
          }
        })
      }
    } else {
      nullSafeCodeGen(ctx, ev, (dataVal, fmt) => s"${ev.isNull} = true;")
    }
  }
}
