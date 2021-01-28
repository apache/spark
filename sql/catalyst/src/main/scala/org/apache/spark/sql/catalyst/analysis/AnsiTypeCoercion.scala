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

import scala.annotation.tailrec

import org.apache.spark.sql.catalyst.analysis.TypeCoercion.numericPrecedence
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.expressions.aggregate._
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.types._

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
 *   * Decimal: Any wider Numeric type
 *   * Float: Float, Double
 *   * Double: Double
 *   * String: String
 *   * Date: Date, Timestamp
 *   * Timestamp: Timestamp
 *   * Binary: Binary
 *   * Boolean: Boolean
 *   * Interval: Interval
 * As for complex data types, Spark will determine the precedent list recursively based on their
 * sub-types.
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
 *       * Except(odd)
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
 *     applies the rules above. If the element nullability is converted from true to false, add
 *     runtime null check to the elements.
 *  Note: this new type coercion system will allow implicit converting String type literals as other
 *  primitive types, in case of breaking too many existing Spark SQL queries. This is a special
 *  rule and it is not from the ANSI SQL standard.
 */
object AnsiTypeCoercion extends TypeCoercionBase {
  override def typeCoercionRules: List[Rule[LogicalPlan]] =
    InConversion ::
      WidenSetOperationTypes ::
      PromoteStringLiterals ::
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
      StringLiteralCoercion ::
      Nil

  /**
   * Find the tightest common type of two types that might be used in a binary expression.
   */
  override def findTightestCommonType(t1: DataType, t2: DataType): Option[DataType] = {
    (t1, t2) match {
      case (t1, t2) if t1 == t2 => Some(t1)
      case (NullType, t1) => Some(t1)
      case (t1, NullType) => Some(t1)

      case (t1: NumericType, t2: NumericType) =>
        findTightestCommonNumericType(t1, t2)

      case (_: TimestampType, _: DateType) | (_: DateType, _: TimestampType) =>
        Some(TimestampType)

      case (t1, t2) => findTypeForComplex(t1, t2, findTightestCommonType)
    }
  }

  @tailrec
  private def findTightestCommonNumericType(t1: NumericType, t2: NumericType): Option[DataType] = {
    (t1, t2) match {
      case (i: IntegralType, d: DecimalType) =>
        if (d.isWiderThan(i)) {
          Some(t2)
        } else {
          findTightestCommonNumericType(DecimalType.forType(i), d)
        }

      case (t1: DecimalType, t2: IntegralType) =>
        findTightestCommonNumericType(t2, t1)

      case (t1: DecimalType, t2: DecimalType) =>
        Some(DecimalPrecision.widerDecimalType(t1, t2))

      case (_: FractionalType, _: DecimalType) | (_: DecimalType, _: FractionalType) =>
        Some(DoubleType)

      // Promote numeric types to the highest of the two
      case _ =>
        // The cases that t1 or t2 is DecimalType should be handled already.
        assert(!t1.isInstanceOf[DecimalType] && !t2.isInstanceOf[DecimalType])
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
    }

  }

  override def findWiderTypeForTwo(t1: DataType, t2: DataType): Option[DataType] = {
    findTightestCommonType(t1, t2)
  }

  override def findWiderCommonType(types: Seq[DataType]): Option[DataType] = {
    types.foldLeft[Option[DataType]](Some(NullType))((r, c) =>
      r match {
        case Some(d) => findWiderTypeForTwo(d, c)
        case _ => None
      })
  }

  override def implicitCast(e: Expression, expectedType: AbstractDataType): Option[Expression] = {
    (e, expectedType) match {
      // This type coercion system will allow implicit converting String type literals as other
      // primitive types, in case of breaking too many existing Spark SQL queries.
      case (_ @ StringType(), a: AtomicType) if e.foldable && a != BooleanType && a != StringType =>
        Some(Cast(e, a))

      case (_ @ StringType(), NumericType) if e.foldable =>
        Some(Cast(e, DoubleType))

      case _ =>
        implicitCast(e.dataType, expectedType).map { dt =>
          if (dt == e.dataType) e else Cast(e, dt)
        }
    }
  }

  /**
   * In Ansi mode, the implicit cast is only allow when `expectedType` is in the type precedent
   * list of `inType`.
   */
  private def implicitCast(inType: DataType, expectedType: AbstractDataType): Option[DataType] = {
    (inType, expectedType) match {
      // If the expected type equals the input type, no need to cast.
      case _ if expectedType.acceptsType(inType) => Some(inType)

      // Cast null type (usually from null literals) into target types
      case (NullType, target) => Some(target.defaultConcreteType)

      // If input is a numeric type but not decimal, and we expect a decimal type,
      // cast the input to decimal.
      case (d: NumericType, DecimalType) => Some(DecimalType.forType(d))

      case (n1: NumericType, n2: NumericType) =>
        val widerType = findTightestCommonNumericType(n1, n2)
        widerType match {
          // if the expected type is Float type, we should still return Float type.
          case Some(DoubleType) if n1 != DoubleType && n2 == FloatType => Some(FloatType)

          case Some(dt) if dt == n2 => Some(dt)

          case _ => None
        }

      case (DateType, TimestampType) => Some(TimestampType)

      // When we reach here, input type is not acceptable for any types in this type collection,
      // try to find the first one we can implicitly cast.
      case (_, TypeCollection(types)) =>
        types.flatMap(implicitCast(inType, _)).headOption

      // Implicit cast between array types.
      //
      // Compare the nullabilities of the from type and the to type, check whether the cast of
      // the nullability is resolvable by the following rules:
      // 1. If the nullability of the to type is true, the cast is always allowed;
      // 2. If the nullabilities of both the from type and the to type are false, the cast is
      //    allowed.
      // 3. Otherwise, the cast is not allowed
      case (ArrayType(fromType, containsNullFrom), ArrayType(toType: DataType, containsNullTo))
          if Cast.resolvableNullability(containsNullFrom, containsNullTo) =>
        implicitCast(fromType, toType).map(ArrayType(_, containsNullTo))

      // Implicit cast between Map types.
      // Follows the same semantics of implicit casting between two array types.
      // Refer to documentation above.
      case (MapType(fromKeyType, fromValueType, fn), MapType(toKeyType, toValueType, tn))
          if Cast.resolvableNullability(fn, tn) =>
        val newKeyType = implicitCast(fromKeyType, toKeyType)
        val newValueType = implicitCast(fromValueType, toValueType)
        if (newKeyType.isDefined && newValueType.isDefined) {
          Some(MapType(newKeyType.get, newValueType.get, tn))
        } else {
          None
        }

      case _ => None
    }
  }

  override def canCast(from: DataType, to: DataType): Boolean = AnsiCast.canCast(from, to)

  /**
   * Promotes string literals that appear in arithmetic expressions.
   */
  object PromoteStringLiterals extends TypeCoercionRule {
    private def castExpr(expr: Expression, targetType: DataType): Expression = {
      (expr.dataType, targetType) match {
        case (NullType, dt) => Literal.create(null, targetType)
        case (l, dt) if (l != dt) => Cast(expr, targetType)
        case _ => expr
      }
    }

    override protected def coerceTypes(
        plan: LogicalPlan): LogicalPlan = plan resolveExpressions {
      // Skip nodes who's children have not been resolved yet.
      case e if !e.childrenResolved => e

      case a @ BinaryArithmetic(left @ StringType(), right)
        if right.dataType != CalendarIntervalType && left.foldable =>
        a.makeCopy(Array(Cast(left, DoubleType), right))
      case a @ BinaryArithmetic(left, right @ StringType())
        if left.dataType != CalendarIntervalType && right.foldable =>
        a.makeCopy(Array(left, Cast(right, DoubleType)))

      // For equality between string and timestamp we cast the string to a timestamp
      // so that things like rounding of subsecond precision does not affect the comparison.
      case p @ Equality(left @ StringType(), right @ TimestampType()) if left.foldable =>
        p.makeCopy(Array(Cast(left, TimestampType), right))
      case p @ Equality(left @ TimestampType(), right @ StringType()) if right.foldable =>
        p.makeCopy(Array(left, Cast(right, TimestampType)))

      case p @ BinaryComparison(left @ StringType(), right @ AtomicType()) if left.foldable =>
        p.makeCopy(Array(castExpr(left, right.dataType), right))

      case p @ BinaryComparison(left @ AtomicType(), right @ StringType()) if right.foldable =>
        p.makeCopy(Array(left, castExpr(right, left.dataType)))

      case Abs(e @ StringType()) if e.foldable => Abs(Cast(e, DoubleType))
      case Sum(e @ StringType()) if e.foldable => Sum(Cast(e, DoubleType))
      case Average(e @ StringType()) if e.foldable => Average(Cast(e, DoubleType))
      case s @ StddevPop(e @ StringType(), _) if e.foldable =>
        s.withNewChildren(Seq(Cast(e, DoubleType)))
      case s @ StddevSamp(e @ StringType(), _) if e.foldable =>
        s.withNewChildren(Seq(Cast(e, DoubleType)))
      case m @ UnaryMinus(e @ StringType(), _) if e.foldable =>
        m.withNewChildren(Seq(Cast(e, DoubleType)))
      case UnaryPositive(e @ StringType()) if e.foldable =>
        UnaryPositive(Cast(e, DoubleType))
      case v @ VariancePop(e @ StringType(), _) if e.foldable =>
        v.withNewChildren(Seq(Cast(e, DoubleType)))
      case v @ VarianceSamp(e @ StringType(), _) if e.foldable =>
        v.withNewChildren(Seq(Cast(e, DoubleType)))
      case s @ Skewness(e @ StringType(), _) if e.foldable =>
        s.withNewChildren(Seq(Cast(e, DoubleType)))
      case k @ Kurtosis(e @ StringType(), _) if e.foldable =>
        k.withNewChildren(Seq(Cast(e, DoubleType)))
    }
  }
}
