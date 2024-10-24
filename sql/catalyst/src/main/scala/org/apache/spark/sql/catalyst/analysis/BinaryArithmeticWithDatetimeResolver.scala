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

package org.apache.spark.sql.catalyst.analysis

import org.apache.spark.sql.catalyst.expressions.{
  Add,
  Cast,
  DateAdd,
  DateAddInterval,
  DateAddYMInterval,
  DateSub,
  DatetimeSub,
  Divide,
  DivideDTInterval,
  DivideInterval,
  DivideYMInterval,
  EvalMode,
  Expression,
  ExtractANSIIntervalDays,
  Multiply,
  MultiplyDTInterval,
  MultiplyInterval,
  MultiplyYMInterval,
  Subtract,
  SubtractDates,
  SubtractTimestamps,
  TimeAdd,
  TimestampAddYMInterval,
  UnaryMinus
}
import org.apache.spark.sql.types.{
  AnsiIntervalType,
  AnyTimestampTypeExpression,
  CalendarIntervalType,
  DateType,
  DayTimeIntervalType,
  NullType,
  StringType,
  TimestampNTZType,
  TimestampType,
  YearMonthIntervalType
}
import org.apache.spark.sql.types.DayTimeIntervalType.DAY

object BinaryArithmeticWithDatetimeResolver {
  def resolve(expr: Expression): Expression = expr match {
    case a @ Add(l, r, mode) =>
      (l.dataType, r.dataType) match {
        case (DateType, DayTimeIntervalType(DAY, DAY)) => DateAdd(l, ExtractANSIIntervalDays(r))
        case (DateType, _: DayTimeIntervalType) => TimeAdd(Cast(l, TimestampType), r)
        case (DayTimeIntervalType(DAY, DAY), DateType) => DateAdd(r, ExtractANSIIntervalDays(l))
        case (_: DayTimeIntervalType, DateType) => TimeAdd(Cast(r, TimestampType), l)
        case (DateType, _: YearMonthIntervalType) => DateAddYMInterval(l, r)
        case (_: YearMonthIntervalType, DateType) => DateAddYMInterval(r, l)
        case (TimestampType | TimestampNTZType, _: YearMonthIntervalType) =>
          TimestampAddYMInterval(l, r)
        case (_: YearMonthIntervalType, TimestampType | TimestampNTZType) =>
          TimestampAddYMInterval(r, l)
        case (CalendarIntervalType, CalendarIntervalType) |
             (_: DayTimeIntervalType, _: DayTimeIntervalType) =>
          a
        case (_: NullType, _: AnsiIntervalType) =>
          a.copy(left = Cast(a.left, a.right.dataType))
        case (_: AnsiIntervalType, _: NullType) =>
          a.copy(right = Cast(a.right, a.left.dataType))
        case (DateType, CalendarIntervalType) =>
          DateAddInterval(l, r, ansiEnabled = mode == EvalMode.ANSI)
        case (_, CalendarIntervalType | _: DayTimeIntervalType) => Cast(TimeAdd(l, r), l.dataType)
        case (CalendarIntervalType, DateType) =>
          DateAddInterval(r, l, ansiEnabled = mode == EvalMode.ANSI)
        case (CalendarIntervalType | _: DayTimeIntervalType, _) => Cast(TimeAdd(r, l), r.dataType)
        case (DateType, dt) if dt != StringType => DateAdd(l, r)
        case (dt, DateType) if dt != StringType => DateAdd(r, l)
        case _ => a
      }
    case s @ Subtract(l, r, mode) =>
      (l.dataType, r.dataType) match {
        case (DateType, DayTimeIntervalType(DAY, DAY)) =>
          DateAdd(l, UnaryMinus(ExtractANSIIntervalDays(r), mode == EvalMode.ANSI))
        case (DateType, _: DayTimeIntervalType) =>
          DatetimeSub(l, r, TimeAdd(Cast(l, TimestampType), UnaryMinus(r, mode == EvalMode.ANSI)))
        case (DateType, _: YearMonthIntervalType) =>
          DatetimeSub(l, r, DateAddYMInterval(l, UnaryMinus(r, mode == EvalMode.ANSI)))
        case (TimestampType | TimestampNTZType, _: YearMonthIntervalType) =>
          DatetimeSub(l, r, TimestampAddYMInterval(l, UnaryMinus(r, mode == EvalMode.ANSI)))
        case (CalendarIntervalType, CalendarIntervalType) |
             (_: DayTimeIntervalType, _: DayTimeIntervalType) =>
          s
        case (_: NullType, _: AnsiIntervalType) =>
          s.copy(left = Cast(s.left, s.right.dataType))
        case (_: AnsiIntervalType, _: NullType) =>
          s.copy(right = Cast(s.right, s.left.dataType))
        case (DateType, CalendarIntervalType) =>
          DatetimeSub(
            l,
            r,
            DateAddInterval(
              l,
              UnaryMinus(r, mode == EvalMode.ANSI),
              ansiEnabled = mode == EvalMode.ANSI
            )
          )
        case (_, CalendarIntervalType | _: DayTimeIntervalType) =>
          Cast(DatetimeSub(l, r, TimeAdd(l, UnaryMinus(r, mode == EvalMode.ANSI))), l.dataType)
        case _
          if AnyTimestampTypeExpression.unapply(l) ||
            AnyTimestampTypeExpression.unapply(r) =>
          SubtractTimestamps(l, r)
        case (_, DateType) => SubtractDates(l, r)
        case (DateType, dt) if dt != StringType => DateSub(l, r)
        case _ => s
      }
    case m @ Multiply(l, r, mode) =>
      (l.dataType, r.dataType) match {
        case (CalendarIntervalType, _) => MultiplyInterval(l, r, mode == EvalMode.ANSI)
        case (_, CalendarIntervalType) => MultiplyInterval(r, l, mode == EvalMode.ANSI)
        case (_: YearMonthIntervalType, _) => MultiplyYMInterval(l, r)
        case (_, _: YearMonthIntervalType) => MultiplyYMInterval(r, l)
        case (_: DayTimeIntervalType, _) => MultiplyDTInterval(l, r)
        case (_, _: DayTimeIntervalType) => MultiplyDTInterval(r, l)
        case _ => m
      }
    case d @ Divide(l, r, mode) =>
      (l.dataType, r.dataType) match {
        case (CalendarIntervalType, _) => DivideInterval(l, r, mode == EvalMode.ANSI)
        case (_: YearMonthIntervalType, _) => DivideYMInterval(l, r)
        case (_: DayTimeIntervalType, _) => DivideDTInterval(l, r)
        case _ => d
      }
  }
}
