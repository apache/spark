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

import javax.annotation.Nullable

import scala.annotation.tailrec

import org.apache.spark.internal.Logging
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.plans.logical._
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.catalyst.trees.AlwaysProcess
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.internal.types.{AbstractArrayType, AbstractMapType, AbstractStringType}
import org.apache.spark.sql.types._
import org.apache.spark.sql.types.UpCastRule.numericPrecedence

/**
 * A collection of [[Rule]] that can be used to coerce differing types that participate in
 * operations into compatible ones.
 *
 * Notes about type widening / tightest common types: Broadly, there are two cases when we need
 * to widen data types (e.g. union, binary comparison). In case 1, we are looking for a common
 * data type for two or more data types, and in this case no loss of precision is allowed. Examples
 * include type inference in JSON (e.g. what's the column's data type if one row is an integer
 * while the other row is a long?). In case 2, we are looking for a widened data type with
 * some acceptable loss of precision (e.g. there is no common type for double and decimal because
 * double's range is larger than decimal, and yet decimal is more precise than double, but in
 * union we would cast the decimal into double).
 */
object TypeCoercion extends TypeCoercionBase {

  override def typeCoercionRules: List[Rule[LogicalPlan]] =
    UnpivotCoercion ::
    WidenSetOperationTypes ::
    ProcedureArgumentCoercion ::
    new CombinedTypeCoercionRule(
      CollationTypeCasts ::
      InConversion ::
      PromoteStrings ::
      DecimalPrecision ::
      BooleanEquality ::
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
      StringLiteralCoercion :: Nil) :: Nil

  override def canCast(from: DataType, to: DataType): Boolean = Cast.canCast(from, to)

  override val findTightestCommonType: (DataType, DataType) => Option[DataType] = {
      case (t1, t2) if t1 == t2 => Some(t1)
      case (NullType, t1) => Some(t1)
      case (t1, NullType) => Some(t1)

      case(s1: StringType, s2: StringType) => StringHelper.tightestCommonString(s1, s2)

      case (t1: IntegralType, t2: DecimalType) if t2.isWiderThan(t1) =>
        Some(t2)
      case (t1: DecimalType, t2: IntegralType) if t1.isWiderThan(t2) =>
        Some(t1)

      // Promote numeric types to the highest of the two
      case (t1: NumericType, t2: NumericType)
          if !t1.isInstanceOf[DecimalType] && !t2.isInstanceOf[DecimalType] =>
        val index = numericPrecedence.lastIndexWhere(t => t == t1 || t == t2)
        Some(numericPrecedence(index))

      case (d1: DatetimeType, d2: DatetimeType) => Some(findWiderDateTimeType(d1, d2))

      case (t1: DayTimeIntervalType, t2: DayTimeIntervalType) =>
        Some(DayTimeIntervalType(t1.startField.min(t2.startField), t1.endField.max(t2.endField)))
      case (t1: YearMonthIntervalType, t2: YearMonthIntervalType) =>
        Some(YearMonthIntervalType(t1.startField.min(t2.startField), t1.endField.max(t2.endField)))

      case (t1, t2) => findTypeForComplex(t1, t2, findTightestCommonType)
  }

  /** Promotes all the way to StringType. */
  private def stringPromotion(dt1: DataType, dt2: DataType): Option[DataType] = (dt1, dt2) match {
    // [SPARK-50060] If a binary operation contains two collated string types with different
    // collation IDs, we can't decide which collation ID the result should have.
    case (st1: StringType, st2: StringType) if st1.collationId != st2.collationId => None
    case (st: StringType, t2: AtomicType) if t2 != BinaryType && t2 != BooleanType => Some(st)
    case (t1: AtomicType, st: StringType) if t1 != BinaryType && t1 != BooleanType => Some(st)
    case _ => None
  }

  // Return whether a string literal can be promoted as the give data type in a binary comparison.
  private def canPromoteAsInBinaryComparison(dt: DataType) = dt match {
    // If a binary comparison contains interval type and string type, we can't decide which
    // interval type the string should be promoted as. There are many possible interval
    // types, such as year interval, month interval, day interval, hour interval, etc.
    case _: YearMonthIntervalType | _: DayTimeIntervalType => false
    // There is no need to add `Cast` for comparison between strings.
    case _: StringType => false
    case _: AtomicType => true
    case _ => false
  }

  /**
   * This function determines the target type of a comparison operator when one operand
   * is a String and the other is not. It also handles when one op is a Date and the
   * other is a Timestamp by making the target type to be String.
   */
  def findCommonTypeForBinaryComparison(
      dt1: DataType, dt2: DataType, conf: SQLConf): Option[DataType] = (dt1, dt2) match {
    case (st: StringType, DateType)
      => if (conf.castDatetimeToString) Some(st) else Some(DateType)
    case (DateType, st: StringType)
      => if (conf.castDatetimeToString) Some(st) else Some(DateType)
    case (st: StringType, TimestampType)
      => if (conf.castDatetimeToString) Some(st) else Some(TimestampType)
    case (TimestampType, st: StringType)
      => if (conf.castDatetimeToString) Some(st) else Some(TimestampType)
    case (st: StringType, NullType) => Some(st)
    case (NullType, st: StringType) => Some(st)

    // Cast to TimestampType when we compare DateType with TimestampType
    // i.e. TimeStamp('2017-03-01 00:00:00') eq Date('2017-03-01') = true
    case (TimestampType, DateType) => Some(TimestampType)
    case (DateType, TimestampType) => Some(TimestampType)

    // There is no proper decimal type we can pick,
    // using double type is the best we can do.
    // See SPARK-22469 for details.
    case (DecimalType.Fixed(_, s), _: StringType) if s > 0 => Some(DoubleType)
    case (_: StringType, DecimalType.Fixed(_, s)) if s > 0 => Some(DoubleType)

    case (s1: StringType, s2: StringType) => StringHelper.tightestCommonString(s1, s2)
    case (l: StringType, r: AtomicType) if canPromoteAsInBinaryComparison(r) => Some(r)
    case (l: AtomicType, r: StringType) if canPromoteAsInBinaryComparison(l) => Some(l)
    case (l, r) => None
  }

  override def findWiderTypeForTwo(t1: DataType, t2: DataType): Option[DataType] = {
    findTightestCommonType(t1, t2)
      .orElse(findWiderTypeForDecimal(t1, t2))
      .orElse(stringPromotion(t1, t2))
      .orElse(findTypeForComplex(t1, t2, findWiderTypeForTwo))
  }

  override def findWiderCommonType(types: Seq[DataType]): Option[DataType] = {
    // findWiderTypeForTwo doesn't satisfy the associative law, i.e. (a op b) op c may not equal
    // to a op (b op c). This is only a problem for StringType or nested StringType in ArrayType.
    // Excluding these types, findWiderTypeForTwo satisfies the associative law. For instance,
    // (TimestampType, IntegerType, StringType) should have StringType as the wider common type.
    val (stringTypes, nonStringTypes) = types.partition(hasStringType(_))
    (stringTypes.distinct ++ nonStringTypes).foldLeft[Option[DataType]](Some(NullType))((r, c) =>
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

  private def implicitCast(inType: DataType, expectedType: AbstractDataType): Option[DataType] = {
    // Note that ret is nullable to avoid typing a lot of Some(...) in this local scope.
    // We wrap immediately an Option after this.
    @Nullable val ret: DataType = (inType, expectedType) match {
      // If the expected type is already a parent of the input type, no need to cast.
      case _ if expectedType.acceptsType(inType) => inType

      // Cast null type (usually from null literals) into target types
      case (NullType, target) => target.defaultConcreteType

      case (s1: StringType, s2: StringType) =>
        if (s1.collationId == s2.collationId && StringHelper.isMoreConstrained(s1, s2)) {
          s2
        } else {
          null
        }
      // If the function accepts any numeric type and the input is a string, we follow the hive
      // convention and cast that input into a double
      case (_: StringType, NumericType) => NumericType.defaultConcreteType

      // Implicit cast among numeric types. When we reach here, input type is not acceptable.

      // If input is a numeric type but not decimal, and we expect a decimal type,
      // cast the input to decimal.
      case (d: NumericType, DecimalType) => DecimalType.forType(d)
      // For any other numeric types, implicitly cast to each other, e.g. long -> int, int -> long
      case (_: NumericType, target: NumericType) => target

      // Implicit cast between date time types
      case (_: DatetimeType, d: DatetimeType) => d
      case (_: DatetimeType, AnyTimestampType) => AnyTimestampType.defaultConcreteType

      // Implicit cast from/to string
      case (_: StringType, DecimalType) => DecimalType.SYSTEM_DEFAULT
      case (_: StringType, target: NumericType) => target
      case (_: StringType, datetime: DatetimeType) => datetime
      case (_: StringType, AnyTimestampType) => AnyTimestampType.defaultConcreteType
      case (_: StringType, BinaryType) => BinaryType
      // Cast any atomic type to string except if there are strings with different collations.
      case (any: AtomicType, st: StringType) if !any.isInstanceOf[StringType] => st
      case (any: AtomicType, st: AbstractStringType)
        if !any.isInstanceOf[StringType] =>
        st.defaultConcreteType

      // When we reach here, input type is not acceptable for any types in this type collection,
      // try to find the first one we can implicitly cast.
      case (_, TypeCollection(types)) =>
        types.flatMap(implicitCast(inType, _)).headOption.orNull

      // Implicit cast between array types.
      //
      // Compare the nullabilities of the from type and the to type, check whether the cast of
      // the nullability is resolvable by the following rules:
      // 1. If the nullability of the to type is true, the cast is always allowed;
      // 2. If the nullability of the to type is false, and the nullability of the from type is
      // true, the cast is never allowed;
      // 3. If the nullabilities of both the from type and the to type are false, the cast is
      // allowed only when Cast.forceNullable(fromType, toType) is false.
      case (ArrayType(fromType, fn), ArrayType(toType: DataType, true)) =>
        implicitCast(fromType, toType).map(ArrayType(_, true)).orNull

      case (ArrayType(fromType, fn), AbstractArrayType(toType)) =>
        implicitCast(fromType, toType).map(ArrayType(_, true)).orNull

      case (ArrayType(fromType, true), ArrayType(toType: DataType, false)) => null

      case (ArrayType(fromType, false), ArrayType(toType: DataType, false))
          if !Cast.forceNullable(fromType, toType) =>
        implicitCast(fromType, toType).map(ArrayType(_, false)).orNull

      // Implicit cast between Map types.
      // Follows the same semantics of implicit casting between two array types.
      // Refer to documentation above. Make sure that both key and values
      // can not be null after the implicit cast operation by calling forceNullable
      // method.
      case (MapType(fromKeyType, fromValueType, fn), MapType(toKeyType, toValueType, tn))
          if !Cast.forceNullable(fromKeyType, toKeyType) && Cast.resolvableNullability(fn, tn) =>
        if (Cast.forceNullable(fromValueType, toValueType) && !tn) {
          null
        } else {
          val newKeyType = implicitCast(fromKeyType, toKeyType).orNull
          val newValueType = implicitCast(fromValueType, toValueType).orNull
          if (newKeyType != null && newValueType != null) {
            MapType(newKeyType, newValueType, tn)
          } else {
            null
          }
        }

      case (MapType(fromKeyType, fromValueType, fn), AbstractMapType(toKeyType, toValueType)) =>
        val newKeyType = implicitCast(fromKeyType, toKeyType).orNull
        val newValueType = implicitCast(fromValueType, toValueType).orNull
        if (newKeyType != null && newValueType != null) {
          MapType(newKeyType, newValueType, fn)
        } else {
          null
        }

      case _ => null
    }
    Option(ret)
  }

  /**
   * The method finds a common type for data types that differ only in nullable flags, including
   * `nullable`, `containsNull` of [[ArrayType]] and `valueContainsNull` of [[MapType]].
   * If the input types are different besides nullable flags, None is returned.
   */
  def findCommonTypeDifferentOnlyInNullFlags(t1: DataType, t2: DataType): Option[DataType] = {
    if (t1 == t2) {
      Some(t1)
    } else {
      findTypeForComplex(t1, t2, findCommonTypeDifferentOnlyInNullFlags)
    }
  }

  def findCommonTypeDifferentOnlyInNullFlags(types: Seq[DataType]): Option[DataType] = {
    if (types.isEmpty) {
      None
    } else {
      types.tail.foldLeft[Option[DataType]](Some(types.head)) {
        case (Some(t1), t2) => findCommonTypeDifferentOnlyInNullFlags(t1, t2)
        case _ => None
      }
    }
  }

  /**
   * Whether the data type contains StringType.
   */
  @tailrec
  def hasStringType(dt: DataType): Boolean = dt match {
    case _: StringType => true
    case ArrayType(et, _) => hasStringType(et)
    // Add StructType if we support string promotion for struct fields in the future.
    case _ => false
  }

  /**
   * Promotes strings that appear in arithmetic expressions.
   */
  object PromoteStrings extends TypeCoercionRule {

    override def transform: PartialFunction[Expression, Expression] = {
      // Skip nodes who's children have not been resolved yet.
      case e if !e.childrenResolved => e
      case withChildrenResolved => StringPromotionTypeCoercion(withChildrenResolved)
    }
  }

  /**
   * Changes numeric values to booleans so that expressions like true = 1 can be evaluated.
   */
  object BooleanEquality extends TypeCoercionRule {
    override def transform: PartialFunction[Expression, Expression] = {
      // Skip nodes who's children have not been resolved yet.
      case e if !e.childrenResolved => e
      case withChildrenResolved => BooleanEqualityTypeCoercion(withChildrenResolved)
    }
  }

  object DateTimeOperations extends TypeCoercionRule {
    override val transform: PartialFunction[Expression, Expression] = {
      // Skip nodes who's children have not been resolved yet.
      case e if !e.childrenResolved => e
      case withChildrenResolved => DateTimeOperationsTypeCoercion(withChildrenResolved)
    }
  }
}

trait TypeCoercionRule extends Rule[LogicalPlan] with Logging {
  /**
   * Applies any changes to [[AttributeReference]] data types that are made by the transform method
   * to instances higher in the query tree.
   */
  def apply(plan: LogicalPlan): LogicalPlan = {
    val typeCoercionFn = transform
    plan.transformUpWithBeforeAndAfterRuleOnChildren(!_.analyzed, ruleId) {
      case (beforeMapChildren, afterMapChildren) =>
        if (!afterMapChildren.childrenResolved) {
          afterMapChildren
        } else {
          // Only propagate types if the children have changed.
          val withPropagatedTypes = if (beforeMapChildren ne afterMapChildren) {
            propagateTypes(afterMapChildren)
          } else {
            beforeMapChildren
          }
          withPropagatedTypes.transformExpressionsUpWithPruning(
            AlwaysProcess.fn, ruleId)(typeCoercionFn)
        }
    }
  }

  def transform: PartialFunction[Expression, Expression]

  private def propagateTypes(plan: LogicalPlan): LogicalPlan = {
    // Check if the inputs have changed.
    val references = AttributeMap(plan.references.collect {
      case a if a.resolved => a -> a
    })
    def sameButDifferent(a: Attribute): Boolean = {
      references.get(a).exists(b => b.dataType != a.dataType || b.nullable != a.nullable)
    }
    val inputMap = AttributeMap(plan.inputSet.collect {
      case a if a.resolved && sameButDifferent(a) => a -> a
    })
    if (inputMap.isEmpty) {
      // Nothing changed.
      plan
    } else {
      // Update the references if the dataType/nullability has changed.
      plan transformExpressions {
        case a: AttributeReference =>
          inputMap.getOrElse(a, a)
      }
    }
  }
}
