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

import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.internal.types.{AbstractArrayType, AbstractStringType}
import org.apache.spark.sql.types._
import org.apache.spark.sql.types.UpCastRule.numericPrecedence

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
    ProcedureArgumentCoercion ::
    new AnsiCombinedTypeCoercionRule(
      CollationTypeCasts ::
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
      GetDateFieldOperations :: Nil) :: Nil

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
      .orElse(AnsiStringPromotionTypeCoercion.findWiderTypeForString(t1, t2))
      .orElse(findTypeForComplex(t1, t2, findWiderTypeForTwo))
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

      // If a function expects a StringType, no StringType instance should be implicitly cast to
      // StringType with a collation that's not accepted (aka. lockdown unsupported collations).
      case (_: StringType, _: StringType) => None
      case (_: StringType, _: AbstractStringType) => None

      // If a function expects integral type, fractional input is not allowed.
      case (_: FractionalType, IntegralType) => None

      // Ideally the implicit cast rule should be the same as `Cast.canANSIStoreAssign` so that it's
      // consistent with table insertion. To avoid breaking too many existing Spark SQL queries,
      // we make the system to allow implicitly converting String type as other primitive types.
      case (_: StringType, a @ (_: AtomicType | NumericType | DecimalType | AnyTimestampType)) =>
        Some(a.defaultConcreteType)

      case (ArrayType(fromType, _), AbstractArrayType(toType)) =>
        implicitCast(fromType, toType).map(ArrayType(_, true))

      // When the target type is `TypeCollection`, there is another branch to find the
      // "closet convertible data type" below.
      case (_, target) if !target.isInstanceOf[TypeCollection] =>
        val concreteType = target.defaultConcreteType
        if (Cast.canANSIStoreAssign(inType, concreteType)) {
          Some(concreteType)
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

    override def transform: PartialFunction[Expression, Expression] = {
      // Skip nodes who's children have not been resolved yet.
      case e if !e.childrenResolved => e
      case withChildrenResolved => AnsiStringPromotionTypeCoercion(withChildrenResolved)
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
      case withChildrenResolved => AnsiGetDateFieldOperationsTypeCoercion(withChildrenResolved)
    }
  }

  object DateTimeOperations extends TypeCoercionRule {
    override val transform: PartialFunction[Expression, Expression] = {
      // Skip nodes who's children have not been resolved yet.
      case e if !e.childrenResolved => e
      case withChildrenResolved => AnsiDateTimeOperationsTypeCoercion(withChildrenResolved)
    }
  }

  // This is for generating a new rule id, so that we can run both default and Ansi
  // type coercion rules against one logical plan.
  class AnsiCombinedTypeCoercionRule(rules: Seq[TypeCoercionRule]) extends
    CombinedTypeCoercionRule(rules)
}
