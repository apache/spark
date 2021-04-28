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

import java.nio.charset.StandardCharsets

import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.analysis.FunctionRegistry
import org.apache.spark.sql.catalyst.expressions.codegen.{CodegenContext, ExprCode, JavaCode}
import org.apache.spark.sql.catalyst.util.{ArrayData, DateFormatter, MapData, TimestampFormatter}
import org.apache.spark.sql.catalyst.util.IntervalStringStyles.HIVE_STYLE
import org.apache.spark.sql.catalyst.util.IntervalUtils.{toDayTimeIntervalString, toYearMonthIntervalString}
import org.apache.spark.sql.types._
import org.apache.spark.unsafe.types.{CalendarInterval, UTF8String}

case class ToPrettyString(expr: Expression, timeZoneId: Option[String] = None)
  extends UnaryExpression with ImplicitCastInputTypes with TimeZoneAwareExpression{
  import ToPrettyString._

  require(children.nonEmpty, s"$prettyName() should take at least 1 argument")
  override def withTimeZone(timeZoneId: String): TimeZoneAwareExpression =
    copy(timeZoneId = Option(timeZoneId))
  override def child: Expression = expr
  override def foldable: Boolean = child.foldable
  override def nullable: Boolean = child.nullable
  override def dataType: DataType = StringType
  override def inputTypes: Seq[AbstractDataType] = Seq(AnyDataType)

  private val timeFormatters: TimeFormatters =
    TimeFormatters(DateFormatter(zoneId), TimestampFormatter.getFractionFormatter(zoneId))

  override def nullSafeEval(input: Any): Any = {
    UTF8String.fromString(toHiveString((input, expr.dataType), false, timeFormatters))
  }

  override def doGenCode(ctx: CodegenContext, ev: ExprCode): ExprCode = {
    nullSafeCodeGen(ctx, ev, eval => {
      val toHiveString = ToPrettyString.getClass.getName.stripSuffix("$")
      val tuple2 = Tuple2.getClass.getName.stripSuffix("$")
      val dataType = JavaCode.global(
        ctx.addReferenceObj("dataType", expr.dataType),
        expr.dataType.getClass)
      val formatter = JavaCode.global(
        ctx.addReferenceObj("dateFormatter", timeFormatters),
        timeFormatters.getClass)
      s"""${ev.value} = UTF8String.fromString($toHiveString.toHiveString(
         |$tuple2.apply($eval, ${dataType}), false, $formatter));""".stripMargin
    })
  }

  override def prettyName: String = getTagValue(
    FunctionRegistry.FUNC_ALIAS).getOrElse("to_hive_string")

  override protected def withNewChildInternal(newChild: Expression): Expression =
    ToPrettyString(newChild)
}

object ToPrettyString {
  case class TimeFormatters(date: DateFormatter, timestamp: TimestampFormatter)

  def toHiveString(
    a: (Any, DataType),
    nested: Boolean,
    formatters: TimeFormatters): String = a match {
    case (null, _) => if (nested) "null" else "NULL"
    case (b, BooleanType) => b.toString
    case (d: Int, DateType) => formatters.date.format(d)
    case (t: Long, TimestampType) => formatters.timestamp.format(t)
    case (bin: Array[Byte], BinaryType) => new String(bin, StandardCharsets.UTF_8)
    case (decimal: Decimal, DecimalType()) => decimal.toJavaBigDecimal.toPlainString
    case (n, _: NumericType) => n.toString
    case (s: UTF8String, StringType) => if (nested) "\"" + s.toString + "\"" else s.toString
    case (interval: CalendarInterval, CalendarIntervalType) => interval.toString
    case (seq: ArrayData, ArrayType(typ, _)) =>
      seq.toArray(typ).toSeq.asInstanceOf[scala.collection.Seq[_]].map(v => (v, typ))
        .map(e => toHiveString(e, true, formatters)).mkString("[", ",", "]")
    case (m: MapData, MapType(kType, vType, _)) =>
      m.keyArray().toArray[Any](kType)
        .zip(m.valueArray().toArray[Any](vType)).toMap
        .map { case (key, value) =>
          toHiveString((key, kType), true, formatters) + ":" +
            toHiveString((value, vType), true, formatters)
        }.toSeq.sorted.mkString("{", ",", "}")
    case (struct: InternalRow, s @ StructType(fields: Array[StructField])) =>
      struct.toSeq(s).zip(fields).map { case (v, t) =>
        s""""${t.name}":${toHiveString((v, t.dataType), true, formatters)}"""
      }.mkString("{", ",", "}")
    case (months: Int, YearMonthIntervalType) =>
      toYearMonthIntervalString(months, HIVE_STYLE)
    case (micros: Long, DayTimeIntervalType) =>
      toDayTimeIntervalString(micros, HIVE_STYLE)
    case (other, _: UserDefinedType[_]) => other.toString
  }
}
