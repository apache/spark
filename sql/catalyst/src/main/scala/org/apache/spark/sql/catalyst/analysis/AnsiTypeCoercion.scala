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

import org.apache.spark.sql.catalyst.analysis.TypeCoercion.numericPrecedence
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.types._
import org.apache.spark.sql.types.DayTimeIntervalType.DAY

/**
 * In Spark ANSI mode, the type coercion rules are based on the type precedence lists of the input
 * data types.
 * As per the section "Type precedence list determination" of "ISO/IEC 9075-2:2011
 * Information technology - Database languages - SQL - Part 2: Foundation (SQL/Foundation)",
 * the type precedence lists of primitive data types are as following:
 *   * Byte: Byte, Short, Int, Long, Decimal, Float, Double
 *   * Short: Short, Int, Long, Decimal, Float, Double
 *   * Int: Int, Long, Decimal, Float, Double
 *   * Long: Long, Decimal, Float, Double
 *   * Decimal: Float, Double, or any wider Numeric type
 *   * Float: Float, Double
 *   * Double: Double
 *   * String: String
 *   * Date: Date, Timestamp
 *   * Timestamp: Timestamp
 *   * Binary: Binary
 *   * Boolean: Boolean
 *   * Interval: Interval
 * As for complex data types, Spark will determine the precedent list recursively based on their
 * sub-types and nullability.
 *
 * With the definition of type precedent list, the general type coercion rules are as following:
 *   * Data type S is allowed to be implicitly cast as type T iff T is in the precedence list of S
 *   * Comparison is allowed iff the data type precedence list of both sides has at least one common
 *     element. When evaluating the comparison, Spark casts both sides as the tightest common data
 *     type of their precedent lists.
 *   * There should be at least one common data type among all the children's precedence lists for
 *     the following operators. The data type of the operator is the tightest common precedent
 *     data type.
 *       * In
 *       * Except
 *       * Intersect
 *       * Greatest
 *       * Least
 *       * Union
 *       * If
 *       * CaseWhen
 *       * CreateArray
 *       * Array Concat
 *       * Sequence
 *       * MapConcat
 *       * CreateMap
 *   * For complex types (struct, array, map), Spark recursively looks into the element type and
 *     applies the rules above.
 *  Note: this new type coercion system will allow implicit converting String type as other
 *  primitive types, in case of breaking too many existing Spark SQL queries. This is a special
 *  rule and it is not from the ANSI SQL standard.
 */
object AnsiTypeCoercion extends TypeCoercionBase {
  override def typeCoercionRules: List[Rule[LogicalPlan]] =
    UnpivotCoercion ::
    WidenSetOperationTypes ::
    new AnsiCombinedTypeCoercionRule(
      ResolveBinaryArithmetic ::
      InConversion ::
      PromoteStrings ::
      DecimalPrecision ::
      FunctionArgumentConversion ::
      ConcatCoercion ::
      MapZipWithCoercion ::
      EltCoercion ::
      CaseWhenCoercion ::
      IfCoercion ::
      StackCoercion ::
      Division ::
      IntegralDivision ::
      ImplicitTypeCasts ::
      DateTimeOperations ::
      WindowFrameCoercion ::
      GetDateFieldOperations:: Nil) :: Nil

  val findTightestCommonType: (DataType, DataType) => Option[DataType] = {
    case (t1, t2) if t1 == t2 => Some(t1)
    case (NullType, t1) => Some(t1)
    case (t1, NullType) => Some(t1)

    case (t1: IntegralType, t2: DecimalType) if t2.isWiderThan(t1) =>
      Some(t2)
    case (t1: DecimalType, t2: IntegralType) if t1.isWiderThan(t2) =>
      Some(t1)

    case (t1: NumericType, t2: NumericType)
        if !t1.isInstanceOf[DecimalType] && !t2.isInstanceOf[DecimalType] =>
      val index = numericPrecedence.lastIndexWhere(t => t == t1 || t == t2)
      val widerType = numericPrecedence(index)
      if (widerType == FloatType) {
        // If the input type is an Integral type and a Float type, simply return Double type as
        // the tightest common type to avoid potential precision loss on converting the Integral
        // type as Float type.
        Some(DoubleType)
      } else {
        Some(widerType)
      }

    case (d1: DatetimeType, d2: DatetimeType) => Some(findWiderDateTimeType(d1, d2))

    case (t1: DayTimeIntervalType, t2: DayTimeIntervalType) =>
      Some(DayTimeIntervalType(t1.startField.min(t2.startField), t1.endField.max(t2.endField)))
    case (t1: YearMonthIntervalType, t2: YearMonthIntervalType) =>
      Some(YearMonthIntervalType(t1.startField.min(t2.startField), t1.endField.max(t2.endField)))

    case (t1, t2) => findTypeForComplex(t1, t2, findTightestCommonType)
  }

  override def findWiderTypeForTwo(t1: DataType, t2: DataType): Option[DataType] = {
    findTightestCommonType(t1, t2)
      .orElse(findWiderTypeForDecimal(t1, t2))
      .orElse(findWiderTypeForString(t1, t2))
      .orElse(findTypeForComplex(t1, t2, findWiderTypeForTwo))
  }

  /** Promotes StringType to other data types. */
  @scala.annotation.tailrec
  private def findWiderTypeForString(dt1: DataType, dt2: DataType): Option[DataType] = {
    (dt1, dt2) match {
      case (StringType, _: IntegralType) => Some(LongType)
      case (StringType, _: FractionalType) => Some(DoubleType)
      case (StringType, NullType) => Some(StringType)
      // If a binary operation contains interval type and string, we can't decide which
      // interval type the string should be promoted as. There are many possible interval
      // types, such as year interval, month interval, day interval, hour interval, etc.
      case (StringType, _: AnsiIntervalType) => None
      case (StringType, a: AtomicType) => Some(a)
      case (other, StringType) if other != StringType => findWiderTypeForString(StringType, other)
      case _ => None
    }
  }

  override def findWiderCommonType(types: Seq[DataType]): Option[DataType] = {
    types.foldLeft[Option[DataType]](Some(NullType))((r, c) =>
      r match {
        case Some(d) => findWiderTypeForTwo(d, c)
        case _ => None
      })
  }

  override def implicitCast(e: Expression, expectedType: AbstractDataType): Option[Expression] = {
    implicitCast(e.dataType, expectedType).map { dt =>
      if (dt == e.dataType) e else Cast(e, dt)
    }
  }

  /**
   * In Ansi mode, the implicit cast is only allow when `expectedType` is in the type precedent
   * list of `inType`.
   */
  private def implicitCast(
      inType: DataType,
      expectedType: AbstractDataType): Option[DataType] = {
    (inType, expectedType) match {
      // If the expected type equals the input type, no need to cast.
      case _ if expectedType.acceptsType(inType) => Some(inType)

      // If input is a numeric type but not decimal, and we expect a decimal type,
      // cast the input to decimal.
      case (n: NumericType, DecimalType) => Some(DecimalType.forType(n))

      // Cast null type (usually from null literals) into target types
      // By default, the result type is `target.defaultConcreteType`. When the target type is
      // `TypeCollection`, there is another branch to find the "closet convertible data type" below.
      case (NullType, target) if !target.isInstanceOf[TypeCollection] =>
        Some(target.defaultConcreteType)

      // This type coercion system will allow implicit converting String type as other
      // primitive types, in case of breaking too many existing Spark SQL queries.
      case (StringType, a: AtomicType) =>
        Some(a)

      // If the target type is any Numeric type, convert the String type as Double type.
      case (StringType, NumericType) =>
        Some(DoubleType)

      // If the target type is any Decimal type, convert the String type as the default
      // Decimal type.
      case (StringType, DecimalType) =>
        Some(DecimalType.SYSTEM_DEFAULT)

      // If the target type is any timestamp type, convert the String type as the default
      // Timestamp type.
      case (StringType, AnyTimestampType) =>
        Some(AnyTimestampType.defaultConcreteType)

      case (DateType, AnyTimestampType) =>
        Some(AnyTimestampType.defaultConcreteType)

      case (_, target: DataType) =>
        if (Cast.canANSIStoreAssign(inType, target)) {
          Some(target)
        } else {
          None
        }

      // When we reach here, input type is not acceptable for any types in this type collection,
      // try to find the first one we can implicitly cast.
      case (_, TypeCollection(types)) =>
        types.flatMap(implicitCast(inType, _)).headOption

      case _ => None
    }
  }

  override def canCast(from: DataType, to: DataType): Boolean = Cast.canAnsiCast(from, to)

  object PromoteStrings extends TypeCoercionRule {
    private def castExpr(expr: Expression, targetType: DataType): Expression = {
      expr.dataType match {
        case NullType => Literal.create(null, targetType)
        case l if l != targetType => Cast(expr, targetType)
        case _ => expr
      }
    }

    override def transform: PartialFunction[Expression, Expression] = {
      // Skip nodes who's children have not been resolved yet.
      case e if !e.childrenResolved => e

      case b @ BinaryOperator(left, right)
        if findWiderTypeForString(left.dataType, right.dataType).isDefined =>
        val promoteType = findWiderTypeForString(left.dataType, right.dataType).get
        b.withNewChildren(Seq(castExpr(left, promoteType), castExpr(right, promoteType)))

      case Abs(e @ StringType(), failOnError) => Abs(Cast(e, DoubleType), failOnError)
      case m @ UnaryMinus(e @ StringType(), _) => m.withNewChildren(Seq(Cast(e, DoubleType)))
      case UnaryPositive(e @ StringType()) => UnaryPositive(Cast(e, DoubleType))

      case d @ DateAdd(left @ StringType(), _) =>
        d.copy(startDate = Cast(d.startDate, DateType))
      case d @ DateAdd(_, right @ StringType()) =>
        d.copy(days = Cast(right, IntegerType))
      case d @ DateSub(left @ StringType(), _) =>
        d.copy(startDate = Cast(d.startDate, DateType))
      case d @ DateSub(_, right @ StringType()) =>
        d.copy(days = Cast(right, IntegerType))

      case s @ SubtractDates(left @ StringType(), _, _) =>
        s.copy(left = Cast(s.left, DateType))
      case s @ SubtractDates(_, right @ StringType(), _) =>
        s.copy(right = Cast(s.right, DateType))
      case t @ TimeAdd(left @ StringType(), _, _) =>
        t.copy(start = Cast(t.start, TimestampType))
      case t @ SubtractTimestamps(left @ StringType(), _, _, _) =>
        t.copy(left = Cast(t.left, t.right.dataType))
      case t @ SubtractTimestamps(_, right @ StringType(), _, _) =>
        t.copy(right = Cast(right, t.left.dataType))
    }
  }

  /**
   * When getting a date field from a Timestamp column, cast the column as date type.
   *
   * This is Spark's hack to make the implementation simple. In the default type coercion rules,
   * the implicit cast rule does the work. However, The ANSI implicit cast rule doesn't allow
   * converting Timestamp type as Date type, so we need to have this additional rule
   * to make sure the date field extraction from Timestamp columns works.
   */
  object GetDateFieldOperations extends TypeCoercionRule {
    override def transform: PartialFunction[Expression, Expression] = {
      // Skip nodes who's children have not been resolved yet.
      case g if !g.childrenResolved => g

      case g: GetDateField if AnyTimestampType.unapply(g.child) =>
        g.withNewChildren(Seq(Cast(g.child, DateType)))
    }
  }

  /**
   * For [[Add]]:
   * 1. if both side are interval, stays the same;
   * 2. else if one side is date and the other is interval,
   * turns it to [[DateAddInterval]];
   * 3. else if one side is interval, turns it to [[TimeAdd]];
   * 4. else if one side is date, turns it to [[DateAdd]] ;
   * 5. else stays the same.
   *
   * For [[Subtract]]:
   * 1. if both side are interval, stays the same;
   * 2. else if the left side is date and the right side is interval,
   * turns it to [[DateAddInterval(l, -r)]];
   * 3. else if the right side is an interval, turns it to [[TimeAdd(l, -r)]];
   * 4. else if one side is timestamp, turns it to [[SubtractTimestamps]];
   * 5. else if the right side is date, turns it to [[DateDiff]]/[[SubtractDates]];
   * 6. else if the left side is date, turns it to [[DateSub]];
   * 7. else turns it to stays the same.
   *
   * For [[Multiply]]:
   * 1. If one side is interval, turns it to [[MultiplyInterval]];
   * 2. otherwise, stays the same.
   *
   * For [[Divide]]:
   * 1. If the left side is interval, turns it to [[DivideInterval]];
   * 2. otherwise, stays the same.
   */
  object ResolveBinaryArithmetic extends TypeCoercionRule {
    override val transform: PartialFunction[Expression, Expression] = {
      case a @ Add(l, r, mode) if a.childrenResolved => (l.dataType, r.dataType) match {
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
             (_: DayTimeIntervalType, _: DayTimeIntervalType) => a
        case (_: NullType, _: AnsiIntervalType) =>
          a.copy(left = Cast(a.left, a.right.dataType))
        case (_: AnsiIntervalType, _: NullType) =>
          a.copy(right = Cast(a.right, a.left.dataType))
        case (DateType, CalendarIntervalType) =>
          DateAddInterval(l, r, ansiEnabled = mode == EvalMode.ANSI)
        case (_: DatetimeType | _: AnsiIntervalType,
            CalendarIntervalType | _: DayTimeIntervalType) =>
          Cast(TimeAdd(l, r), l.dataType)
        case (CalendarIntervalType, DateType) =>
          DateAddInterval(r, l, ansiEnabled = mode == EvalMode.ANSI)
        case (CalendarIntervalType | _: DayTimeIntervalType,
            _: DatetimeType | _: AnsiIntervalType) =>
          Cast(TimeAdd(r, l), r.dataType)
        case (DateType, dt) if dt != StringType => DateAdd(l, r)
        case (dt, DateType) if dt != StringType => DateAdd(r, l)
        case _ => a
      }
      case s @ Subtract(l, r, mode) if s.childrenResolved => (l.dataType, r.dataType) match {
        case (DateType, DayTimeIntervalType(DAY, DAY)) =>
          DateAdd(l, UnaryMinus(ExtractANSIIntervalDays(r), mode == EvalMode.ANSI))
        case (DateType, _: DayTimeIntervalType) =>
          DatetimeSub(l, r, TimeAdd(Cast(l, TimestampType), UnaryMinus(r, mode == EvalMode.ANSI)))
        case (DateType, _: YearMonthIntervalType) =>
          DatetimeSub(l, r, DateAddYMInterval(l, UnaryMinus(r, mode == EvalMode.ANSI)))
        case (TimestampType | TimestampNTZType, _: YearMonthIntervalType) =>
          DatetimeSub(l, r, TimestampAddYMInterval(l, UnaryMinus(r, mode == EvalMode.ANSI)))
        case (CalendarIntervalType, CalendarIntervalType) |
             (_: DayTimeIntervalType, _: DayTimeIntervalType) => s
        case (_: NullType, _: AnsiIntervalType) =>
          s.copy(left = Cast(s.left, s.right.dataType))
        case (_: AnsiIntervalType, _: NullType) =>
          s.copy(right = Cast(s.right, s.left.dataType))
        case (DateType, CalendarIntervalType) =>
          DatetimeSub(l, r, DateAddInterval(l,
            UnaryMinus(r, mode == EvalMode.ANSI), ansiEnabled = mode == EvalMode.ANSI))
        case (_: DatetimeType | _: AnsiIntervalType,
            CalendarIntervalType | _: DayTimeIntervalType) =>
          Cast(DatetimeSub(l, r, TimeAdd(l, UnaryMinus(r, mode == EvalMode.ANSI))), l.dataType)
        case _ if AnyTimestampType.unapply(l) || AnyTimestampType.unapply(r) =>
          SubtractTimestamps(l, r)
        case (_, DateType) => SubtractDates(l, r)
        case (DateType, dt) if dt != StringType => DateSub(l, r)
        case _ => s
      }
      case m @ Multiply(l, r, mode) if m.childrenResolved => (l.dataType, r.dataType) match {
        case (CalendarIntervalType, _) => MultiplyInterval(l, r, mode == EvalMode.ANSI)
        case (_, CalendarIntervalType) => MultiplyInterval(r, l, mode == EvalMode.ANSI)
        case (_: YearMonthIntervalType, _) => MultiplyYMInterval(l, r)
        case (_, _: YearMonthIntervalType) => MultiplyYMInterval(r, l)
        case (_: DayTimeIntervalType, _) => MultiplyDTInterval(l, r)
        case (_, _: DayTimeIntervalType) => MultiplyDTInterval(r, l)
        case _ => m
      }
      case d @ Divide(l, r, mode) if d.childrenResolved => (l.dataType, r.dataType) match {
        case (CalendarIntervalType, _) => DivideInterval(l, r, mode == EvalMode.ANSI)
        case (_: YearMonthIntervalType, _) => DivideYMInterval(l, r)
        case (_: DayTimeIntervalType, _) => DivideDTInterval(l, r)
        case _ => d
      }
    }
  }

  object DateTimeOperations extends TypeCoercionRule {
    override val transform: PartialFunction[Expression, Expression] = {
      // Skip nodes who's children have not been resolved yet.
      case e if !e.childrenResolved => e

      case d @ DateAdd(AnyTimestampType(), _) => d.copy(startDate = Cast(d.startDate, DateType))
      case d @ DateSub(AnyTimestampType(), _) => d.copy(startDate = Cast(d.startDate, DateType))

      case s @ SubtractTimestamps(DateType(), AnyTimestampType(), _, _) =>
        s.copy(left = Cast(s.left, s.right.dataType))
      case s @ SubtractTimestamps(AnyTimestampType(), DateType(), _, _) =>
        s.copy(right = Cast(s.right, s.left.dataType))
      case s @ SubtractTimestamps(AnyTimestampType(), AnyTimestampType(), _, _)
        if s.left.dataType != s.right.dataType =>
        val newLeft = castIfNotSameType(s.left, TimestampNTZType)
        val newRight = castIfNotSameType(s.right, TimestampNTZType)
        s.copy(left = newLeft, right = newRight)
    }
  }

  // This is for generating a new rule id, so that we can run both default and Ansi
  // type coercion rules against one logical plan.
  class AnsiCombinedTypeCoercionRule(rules: Seq[TypeCoercionRule]) extends
    CombinedTypeCoercionRule(rules)
}
