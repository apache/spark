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
import scala.collection.mutable

import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.expressions.aggregate._
import org.apache.spark.sql.catalyst.expressions.Literal.{FalseLiteral, TrueLiteral}
import org.apache.spark.sql.catalyst.plans.logical._
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.types._


/**
 * A collection of [[Rule]] that can be used to coerce differing types that participate in
 * operations into compatible ones.
 *
 * Most of these rules are based on Hive semantics, but they do not introduce any dependencies on
 * the hive codebase.
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
object HiveTypeCoercion {

  val typeCoercionRules =
    PropagateTypes ::
      InConversion ::
      WidenSetOperationTypes ::
      PromoteStrings ::
      DecimalPrecision ::
      BooleanEquality ::
      StringToIntegralCasts ::
      FunctionArgumentConversion ::
      CaseWhenCoercion ::
      IfCoercion ::
      Division ::
      PropagateTypes ::
      ImplicitTypeCasts ::
      DateTimeOperations ::
      Nil

  // See https://cwiki.apache.org/confluence/display/Hive/LanguageManual+Types.
  // The conversion for integral and floating point types have a linear widening hierarchy:
  private[sql] val numericPrecedence =
    IndexedSeq(
      ByteType,
      ShortType,
      IntegerType,
      LongType,
      FloatType,
      DoubleType)

  /**
   * Case 1 type widening (see the classdoc comment above for HiveTypeCoercion).
   *
   * Find the tightest common type of two types that might be used in a binary expression.
   * This handles all numeric types except fixed-precision decimals interacting with each other or
   * with primitive types, because in that case the precision and scale of the result depends on
   * the operation. Those rules are implemented in [[DecimalPrecision]].
   */
  val findTightestCommonTypeOfTwo: (DataType, DataType) => Option[DataType] = {
    case (t1, t2) if t1 == t2 => Some(t1)
    case (NullType, t1) => Some(t1)
    case (t1, NullType) => Some(t1)

    case (t1: IntegralType, t2: DecimalType) if t2.isWiderThan(t1) =>
      Some(t2)
    case (t1: DecimalType, t2: IntegralType) if t1.isWiderThan(t2) =>
      Some(t1)

    // Promote numeric types to the highest of the two
    case (t1: NumericType, t2: NumericType)
        if !t1.isInstanceOf[DecimalType] && !t2.isInstanceOf[DecimalType] =>
      val index = numericPrecedence.lastIndexWhere(t => t == t1 || t == t2)
      Some(numericPrecedence(index))

    case _ => None
  }

  /** Similar to [[findTightestCommonType]], but can promote all the way to StringType. */
  private def findTightestCommonTypeToString(left: DataType, right: DataType): Option[DataType] = {
    findTightestCommonTypeOfTwo(left, right).orElse((left, right) match {
      case (StringType, t2: AtomicType) if t2 != BinaryType && t2 != BooleanType => Some(StringType)
      case (t1: AtomicType, StringType) if t1 != BinaryType && t1 != BooleanType => Some(StringType)
      case _ => None
    })
  }

  /**
   * Similar to [[findTightestCommonType]], if can not find the TightestCommonType, try to use
   * [[findTightestCommonTypeToString]] to find the TightestCommonType.
   */
  private def findTightestCommonTypeAndPromoteToString(types: Seq[DataType]): Option[DataType] = {
    types.foldLeft[Option[DataType]](Some(NullType))((r, c) => r match {
      case None => None
      case Some(d) =>
        findTightestCommonTypeToString(d, c)
    })
  }

  /**
   * Find the tightest common type of a set of types by continuously applying
   * `findTightestCommonTypeOfTwo` on these types.
   */
  private def findTightestCommonType(types: Seq[DataType]): Option[DataType] = {
    types.foldLeft[Option[DataType]](Some(NullType))((r, c) => r match {
      case None => None
      case Some(d) => findTightestCommonTypeOfTwo(d, c)
    })
  }

  /**
   * Case 2 type widening (see the classdoc comment above for HiveTypeCoercion).
   *
   * i.e. the main difference with [[findTightestCommonTypeOfTwo]] is that here we allow some
   * loss of precision when widening decimal and double.
   */
  private def findWiderTypeForTwo(t1: DataType, t2: DataType): Option[DataType] = (t1, t2) match {
    case (t1: DecimalType, t2: DecimalType) =>
      Some(DecimalPrecision.widerDecimalType(t1, t2))
    case (t: IntegralType, d: DecimalType) =>
      Some(DecimalPrecision.widerDecimalType(DecimalType.forType(t), d))
    case (d: DecimalType, t: IntegralType) =>
      Some(DecimalPrecision.widerDecimalType(DecimalType.forType(t), d))
    case (_: FractionalType, _: DecimalType) | (_: DecimalType, _: FractionalType) =>
      Some(DoubleType)
    case _ =>
      findTightestCommonTypeToString(t1, t2)
  }

  private def findWiderCommonType(types: Seq[DataType]) = {
    types.foldLeft[Option[DataType]](Some(NullType))((r, c) => r match {
      case Some(d) => findWiderTypeForTwo(d, c)
      case None => None
    })
  }

  private def haveSameType(exprs: Seq[Expression]): Boolean =
    exprs.map(_.dataType).distinct.length == 1

  /**
   * Applies any changes to [[AttributeReference]] data types that are made by other rules to
   * instances higher in the query tree.
   */
  object PropagateTypes extends Rule[LogicalPlan] {
    def apply(plan: LogicalPlan): LogicalPlan = plan resolveOperators {

      // No propagation required for leaf nodes.
      case q: LogicalPlan if q.children.isEmpty => q

      // Don't propagate types from unresolved children.
      case q: LogicalPlan if !q.childrenResolved => q

      case q: LogicalPlan =>
        val inputMap = q.inputSet.toSeq.map(a => (a.exprId, a)).toMap
        q transformExpressions {
          case a: AttributeReference =>
            inputMap.get(a.exprId) match {
              // This can happen when a Attribute reference is born in a non-leaf node, for example
              // due to a call to an external script like in the Transform operator.
              // TODO: Perhaps those should actually be aliases?
              case None => a
              // Leave the same if the dataTypes match.
              case Some(newType) if a.dataType == newType.dataType => a
              case Some(newType) =>
                logDebug(s"Promoting $a to $newType in ${q.simpleString}")
                newType
            }
        }
    }
  }

  /**
   * Widens numeric types and converts strings to numbers when appropriate.
   *
   * Loosely based on rules from "Hadoop: The Definitive Guide" 2nd edition, by Tom White
   *
   * The implicit conversion rules can be summarized as follows:
   *   - Any integral numeric type can be implicitly converted to a wider type.
   *   - All the integral numeric types, FLOAT, and (perhaps surprisingly) STRING can be implicitly
   *     converted to DOUBLE.
   *   - TINYINT, SMALLINT, and INT can all be converted to FLOAT.
   *   - BOOLEAN types cannot be converted to any other type.
   *   - Any integral numeric type can be implicitly converted to decimal type.
   *   - two different decimal types will be converted into a wider decimal type for both of them.
   *   - decimal type will be converted into double if there float or double together with it.
   *
   * Additionally, all types when UNION-ed with strings will be promoted to strings.
   * Other string conversions are handled by PromoteStrings.
   *
   * Widening types might result in loss of precision in the following cases:
   * - IntegerType to FloatType
   * - LongType to FloatType
   * - LongType to DoubleType
   * - DecimalType to Double
   *
   * This rule is only applied to Union/Except/Intersect
   */
  object WidenSetOperationTypes extends Rule[LogicalPlan] {

    def apply(plan: LogicalPlan): LogicalPlan = plan resolveOperators {
      case p if p.analyzed => p

      case s @ SetOperation(left, right) if s.childrenResolved &&
          left.output.length == right.output.length && !s.resolved =>
        val newChildren: Seq[LogicalPlan] = buildNewChildrenWithWiderTypes(left :: right :: Nil)
        assert(newChildren.length == 2)
        s.makeCopy(Array(newChildren.head, newChildren.last))

      case s: Union if s.childrenResolved &&
          s.children.forall(_.output.length == s.children.head.output.length) && !s.resolved =>
        val newChildren: Seq[LogicalPlan] = buildNewChildrenWithWiderTypes(s.children)
        s.makeCopy(Array(newChildren))
    }

    /** Build new children with the widest types for each attribute among all the children */
    private def buildNewChildrenWithWiderTypes(children: Seq[LogicalPlan]): Seq[LogicalPlan] = {
      require(children.forall(_.output.length == children.head.output.length))

      // Get a sequence of data types, each of which is the widest type of this specific attribute
      // in all the children
      val targetTypes: Seq[DataType] =
        getWidestTypes(children, attrIndex = 0, mutable.Queue[DataType]())

      if (targetTypes.nonEmpty) {
        // Add an extra Project if the targetTypes are different from the original types.
        children.map(widenTypes(_, targetTypes))
      } else {
        // Unable to find a target type to widen, then just return the original set.
        children
      }
    }

    /** Get the widest type for each attribute in all the children */
    @tailrec private def getWidestTypes(
        children: Seq[LogicalPlan],
        attrIndex: Int,
        castedTypes: mutable.Queue[DataType]): Seq[DataType] = {
      // Return the result after the widen data types have been found for all the children
      if (attrIndex >= children.head.output.length) return castedTypes.toSeq

      // For the attrIndex-th attribute, find the widest type
      findWiderCommonType(children.map(_.output(attrIndex).dataType)) match {
        // If unable to find an appropriate widen type for this column, return an empty Seq
        case None => Seq.empty[DataType]
        // Otherwise, record the result in the queue and find the type for the next column
        case Some(widenType) =>
          castedTypes.enqueue(widenType)
          getWidestTypes(children, attrIndex + 1, castedTypes)
      }
    }

    /** Given a plan, add an extra project on top to widen some columns' data types. */
    private def widenTypes(plan: LogicalPlan, targetTypes: Seq[DataType]): LogicalPlan = {
      val casted = plan.output.zip(targetTypes).map {
        case (e, dt) if e.dataType != dt => Alias(Cast(e, dt), e.name)()
        case (e, _) => e
      }
      Project(casted, plan)
    }
  }

  /**
   * Promotes strings that appear in arithmetic expressions.
   */
  object PromoteStrings extends Rule[LogicalPlan] {
    def apply(plan: LogicalPlan): LogicalPlan = plan resolveExpressions {
      // Skip nodes who's children have not been resolved yet.
      case e if !e.childrenResolved => e

      case a @ BinaryArithmetic(left @ StringType(), right @ DecimalType.Expression(_, _)) =>
        a.makeCopy(Array(Cast(left, DecimalType.SYSTEM_DEFAULT), right))
      case a @ BinaryArithmetic(left @ DecimalType.Expression(_, _), right @ StringType()) =>
        a.makeCopy(Array(left, Cast(right, DecimalType.SYSTEM_DEFAULT)))

      case a @ BinaryArithmetic(left @ StringType(), right) =>
        a.makeCopy(Array(Cast(left, DoubleType), right))
      case a @ BinaryArithmetic(left, right @ StringType()) =>
        a.makeCopy(Array(left, Cast(right, DoubleType)))

      // For equality between string and timestamp we cast the string to a timestamp
      // so that things like rounding of subsecond precision does not affect the comparison.
      case p @ Equality(left @ StringType(), right @ TimestampType()) =>
        p.makeCopy(Array(Cast(left, TimestampType), right))
      case p @ Equality(left @ TimestampType(), right @ StringType()) =>
        p.makeCopy(Array(left, Cast(right, TimestampType)))

      // We should cast all relative timestamp/date/string comparison into string comparisons
      // This behaves as a user would expect because timestamp strings sort lexicographically.
      // i.e. TimeStamp(2013-01-01 00:00 ...) < "2014" = true
      case p @ BinaryComparison(left @ StringType(), right @ DateType()) =>
        p.makeCopy(Array(left, Cast(right, StringType)))
      case p @ BinaryComparison(left @ DateType(), right @ StringType()) =>
        p.makeCopy(Array(Cast(left, StringType), right))
      case p @ BinaryComparison(left @ StringType(), right @ TimestampType()) =>
        p.makeCopy(Array(left, Cast(right, StringType)))
      case p @ BinaryComparison(left @ TimestampType(), right @ StringType()) =>
        p.makeCopy(Array(Cast(left, StringType), right))

      // Comparisons between dates and timestamps.
      case p @ BinaryComparison(left @ TimestampType(), right @ DateType()) =>
        p.makeCopy(Array(Cast(left, StringType), Cast(right, StringType)))
      case p @ BinaryComparison(left @ DateType(), right @ TimestampType()) =>
        p.makeCopy(Array(Cast(left, StringType), Cast(right, StringType)))

      // Checking NullType
      case p @ BinaryComparison(left @ StringType(), right @ NullType()) =>
        p.makeCopy(Array(left, Literal.create(null, StringType)))
      case p @ BinaryComparison(left @ NullType(), right @ StringType()) =>
        p.makeCopy(Array(Literal.create(null, StringType), right))

      case p @ BinaryComparison(left @ StringType(), right) if right.dataType != StringType =>
        p.makeCopy(Array(Cast(left, DoubleType), right))
      case p @ BinaryComparison(left, right @ StringType()) if left.dataType != StringType =>
        p.makeCopy(Array(left, Cast(right, DoubleType)))

      case i @ In(a @ DateType(), b) if b.forall(_.dataType == StringType) =>
        i.makeCopy(Array(Cast(a, StringType), b))
      case i @ In(a @ TimestampType(), b) if b.forall(_.dataType == StringType) =>
        i.makeCopy(Array(a, b.map(Cast(_, TimestampType))))
      case i @ In(a @ DateType(), b) if b.forall(_.dataType == TimestampType) =>
        i.makeCopy(Array(Cast(a, StringType), b.map(Cast(_, StringType))))
      case i @ In(a @ TimestampType(), b) if b.forall(_.dataType == DateType) =>
        i.makeCopy(Array(Cast(a, StringType), b.map(Cast(_, StringType))))

      case Sum(e @ StringType()) => Sum(Cast(e, DoubleType))
      case Average(e @ StringType()) => Average(Cast(e, DoubleType))
      case StddevPop(e @ StringType()) => StddevPop(Cast(e, DoubleType))
      case StddevSamp(e @ StringType()) => StddevSamp(Cast(e, DoubleType))
      case VariancePop(e @ StringType()) => VariancePop(Cast(e, DoubleType))
      case VarianceSamp(e @ StringType()) => VarianceSamp(Cast(e, DoubleType))
      case Skewness(e @ StringType()) => Skewness(Cast(e, DoubleType))
      case Kurtosis(e @ StringType()) => Kurtosis(Cast(e, DoubleType))
    }
  }

  /**
   * Convert the value and in list expressions to the common operator type
   * by looking at all the argument types and finding the closest one that
   * all the arguments can be cast to. When no common operator type is found
   * the original expression will be returned and an Analysis Exception will
   * be raised at type checking phase.
   */
  object InConversion extends Rule[LogicalPlan] {
    def apply(plan: LogicalPlan): LogicalPlan = plan resolveExpressions {
      // Skip nodes who's children have not been resolved yet.
      case e if !e.childrenResolved => e

      case i @ In(a, b) if b.exists(_.dataType != a.dataType) =>
        findWiderCommonType(i.children.map(_.dataType)) match {
          case Some(finalDataType) => i.withNewChildren(i.children.map(Cast(_, finalDataType)))
          case None => i
        }
    }
  }

  /**
   * Changes numeric values to booleans so that expressions like true = 1 can be evaluated.
   */
  object BooleanEquality extends Rule[LogicalPlan] {
    private val trueValues = Seq(1.toByte, 1.toShort, 1, 1L, Decimal.ONE)
    private val falseValues = Seq(0.toByte, 0.toShort, 0, 0L, Decimal.ZERO)

    def apply(plan: LogicalPlan): LogicalPlan = plan resolveExpressions {
      // Skip nodes who's children have not been resolved yet.
      case e if !e.childrenResolved => e

      // Hive treats (true = 1) as true and (false = 0) as true,
      // all other cases are considered as false.

      // We may simplify the expression if one side is literal numeric values
      // TODO: Maybe these rules should go into the optimizer.
      case EqualTo(bool @ BooleanType(), Literal(value, _: NumericType))
        if trueValues.contains(value) => bool
      case EqualTo(bool @ BooleanType(), Literal(value, _: NumericType))
        if falseValues.contains(value) => Not(bool)
      case EqualTo(Literal(value, _: NumericType), bool @ BooleanType())
        if trueValues.contains(value) => bool
      case EqualTo(Literal(value, _: NumericType), bool @ BooleanType())
        if falseValues.contains(value) => Not(bool)
      case EqualNullSafe(bool @ BooleanType(), Literal(value, _: NumericType))
        if trueValues.contains(value) => And(IsNotNull(bool), bool)
      case EqualNullSafe(bool @ BooleanType(), Literal(value, _: NumericType))
        if falseValues.contains(value) => And(IsNotNull(bool), Not(bool))
      case EqualNullSafe(Literal(value, _: NumericType), bool @ BooleanType())
        if trueValues.contains(value) => And(IsNotNull(bool), bool)
      case EqualNullSafe(Literal(value, _: NumericType), bool @ BooleanType())
        if falseValues.contains(value) => And(IsNotNull(bool), Not(bool))

      case EqualTo(left @ BooleanType(), right @ NumericType()) =>
        EqualTo(Cast(left, right.dataType), right)
      case EqualTo(left @ NumericType(), right @ BooleanType()) =>
        EqualTo(left, Cast(right, left.dataType))
      case EqualNullSafe(left @ BooleanType(), right @ NumericType()) =>
        EqualNullSafe(Cast(left, right.dataType), right)
      case EqualNullSafe(left @ NumericType(), right @ BooleanType()) =>
        EqualNullSafe(left, Cast(right, left.dataType))
    }
  }

  /**
   * When encountering a cast from a string representing a valid fractional number to an integral
   * type the jvm will throw a `java.lang.NumberFormatException`.  Hive, in contrast, returns the
   * truncated version of this number.
   */
  object StringToIntegralCasts extends Rule[LogicalPlan] {
    def apply(plan: LogicalPlan): LogicalPlan = plan resolveExpressions {
      // Skip nodes who's children have not been resolved yet.
      case e if !e.childrenResolved => e

      case Cast(e @ StringType(), t: IntegralType) =>
        Cast(Cast(e, DecimalType.forType(LongType)), t)
    }
  }

  /**
   * This ensure that the types for various functions are as expected.
   */
  object FunctionArgumentConversion extends Rule[LogicalPlan] {
    def apply(plan: LogicalPlan): LogicalPlan = plan resolveExpressions {
      // Skip nodes who's children have not been resolved yet.
      case e if !e.childrenResolved => e

      case a @ CreateArray(children) if !haveSameType(children) =>
        val types = children.map(_.dataType)
        findTightestCommonTypeAndPromoteToString(types) match {
          case Some(finalDataType) => CreateArray(children.map(Cast(_, finalDataType)))
          case None => a
        }

      case m @ CreateMap(children) if m.keys.length == m.values.length &&
        (!haveSameType(m.keys) || !haveSameType(m.values)) =>
        val newKeys = if (haveSameType(m.keys)) {
          m.keys
        } else {
          val types = m.keys.map(_.dataType)
          findTightestCommonTypeAndPromoteToString(types) match {
            case Some(finalDataType) => m.keys.map(Cast(_, finalDataType))
            case None => m.keys
          }
        }

        val newValues = if (haveSameType(m.values)) {
          m.values
        } else {
          val types = m.values.map(_.dataType)
          findTightestCommonTypeAndPromoteToString(types) match {
            case Some(finalDataType) => m.values.map(Cast(_, finalDataType))
            case None => m.values
          }
        }

        CreateMap(newKeys.zip(newValues).flatMap { case (k, v) => Seq(k, v) })

      // Promote SUM, SUM DISTINCT and AVERAGE to largest types to prevent overflows.
      case s @ Sum(e @ DecimalType()) => s // Decimal is already the biggest.
      case Sum(e @ IntegralType()) if e.dataType != LongType => Sum(Cast(e, LongType))
      case Sum(e @ FractionalType()) if e.dataType != DoubleType => Sum(Cast(e, DoubleType))

      case s @ Average(e @ DecimalType()) => s // Decimal is already the biggest.
      case Average(e @ IntegralType()) if e.dataType != LongType =>
        Average(Cast(e, LongType))
      case Average(e @ FractionalType()) if e.dataType != DoubleType =>
        Average(Cast(e, DoubleType))

      // Hive lets you do aggregation of timestamps... for some reason
      case Sum(e @ TimestampType()) => Sum(Cast(e, DoubleType))
      case Average(e @ TimestampType()) => Average(Cast(e, DoubleType))

      // Coalesce should return the first non-null value, which could be any column
      // from the list. So we need to make sure the return type is deterministic and
      // compatible with every child column.
      case c @ Coalesce(es) if !haveSameType(es) =>
        val types = es.map(_.dataType)
        findWiderCommonType(types) match {
          case Some(finalDataType) => Coalesce(es.map(Cast(_, finalDataType)))
          case None => c
        }

      case g @ Greatest(children) if !haveSameType(children) =>
        val types = children.map(_.dataType)
        findTightestCommonType(types) match {
          case Some(finalDataType) => Greatest(children.map(Cast(_, finalDataType)))
          case None => g
        }

      case l @ Least(children) if !haveSameType(children) =>
        val types = children.map(_.dataType)
        findTightestCommonType(types) match {
          case Some(finalDataType) => Least(children.map(Cast(_, finalDataType)))
          case None => l
        }

      case NaNvl(l, r) if l.dataType == DoubleType && r.dataType == FloatType =>
        NaNvl(l, Cast(r, DoubleType))
      case NaNvl(l, r) if l.dataType == FloatType && r.dataType == DoubleType =>
        NaNvl(Cast(l, DoubleType), r)
    }
  }

  /**
   * Hive only performs integral division with the DIV operator. The arguments to / are always
   * converted to fractional types.
   */
  object Division extends Rule[LogicalPlan] {
    def apply(plan: LogicalPlan): LogicalPlan = plan resolveExpressions {
      // Skip nodes who has not been resolved yet,
      // as this is an extra rule which should be applied at last.
      case e if !e.resolved => e

      // Decimal and Double remain the same
      case d: Divide if d.dataType == DoubleType => d
      case d: Divide if d.dataType.isInstanceOf[DecimalType] => d

      case Divide(left, right) => Divide(Cast(left, DoubleType), Cast(right, DoubleType))
    }
  }

  /**
   * Coerces the type of different branches of a CASE WHEN statement to a common type.
   */
  object CaseWhenCoercion extends Rule[LogicalPlan] {
    def apply(plan: LogicalPlan): LogicalPlan = plan resolveExpressions {
      case c: CaseWhen if c.childrenResolved && !c.valueTypesEqual =>
        val maybeCommonType = findWiderCommonType(c.valueTypes)
        maybeCommonType.map { commonType =>
          var changed = false
          val newBranches = c.branches.map { case (condition, value) =>
            if (value.dataType.sameType(commonType)) {
              (condition, value)
            } else {
              changed = true
              (condition, Cast(value, commonType))
            }
          }
          val newElseValue = c.elseValue.map { value =>
            if (value.dataType.sameType(commonType)) {
              value
            } else {
              changed = true
              Cast(value, commonType)
            }
          }
          if (changed) CaseWhen(newBranches, newElseValue) else c
        }.getOrElse(c)
    }
  }

  /**
   * Coerces the type of different branches of If statement to a common type.
   */
  object IfCoercion extends Rule[LogicalPlan] {
    def apply(plan: LogicalPlan): LogicalPlan = plan resolveExpressions {
      case e if !e.childrenResolved => e
      // Find tightest common type for If, if the true value and false value have different types.
      case i @ If(pred, left, right) if left.dataType != right.dataType =>
        findWiderTypeForTwo(left.dataType, right.dataType).map { widestType =>
          val newLeft = if (left.dataType == widestType) left else Cast(left, widestType)
          val newRight = if (right.dataType == widestType) right else Cast(right, widestType)
          If(pred, newLeft, newRight)
        }.getOrElse(i)  // If there is no applicable conversion, leave expression unchanged.
      case If(Literal(null, NullType), left, right) =>
        If(Literal.create(null, BooleanType), left, right)
      case If(pred, left, right) if pred.dataType == NullType =>
        If(Cast(pred, BooleanType), left, right)
    }
  }

  /**
   * Turns Add/Subtract of DateType/TimestampType/StringType and CalendarIntervalType
   * to TimeAdd/TimeSub
   */
  object DateTimeOperations extends Rule[LogicalPlan] {

    private val acceptedTypes = Seq(DateType, TimestampType, StringType)

    def apply(plan: LogicalPlan): LogicalPlan = plan resolveExpressions {
      // Skip nodes who's children have not been resolved yet.
      case e if !e.childrenResolved => e

      case Add(l @ CalendarIntervalType(), r) if acceptedTypes.contains(r.dataType) =>
        Cast(TimeAdd(r, l), r.dataType)
      case Add(l, r @ CalendarIntervalType()) if acceptedTypes.contains(l.dataType) =>
        Cast(TimeAdd(l, r), l.dataType)
      case Subtract(l, r @ CalendarIntervalType()) if acceptedTypes.contains(l.dataType) =>
        Cast(TimeSub(l, r), l.dataType)
    }
  }

  /**
   * Casts types according to the expected input types for [[Expression]]s.
   */
  object ImplicitTypeCasts extends Rule[LogicalPlan] {
    def apply(plan: LogicalPlan): LogicalPlan = plan resolveExpressions {
      // Skip nodes who's children have not been resolved yet.
      case e if !e.childrenResolved => e

      case b @ BinaryOperator(left, right) if left.dataType != right.dataType =>
        findTightestCommonTypeOfTwo(left.dataType, right.dataType).map { commonType =>
          if (b.inputType.acceptsType(commonType)) {
            // If the expression accepts the tightest common type, cast to that.
            val newLeft = if (left.dataType == commonType) left else Cast(left, commonType)
            val newRight = if (right.dataType == commonType) right else Cast(right, commonType)
            b.withNewChildren(Seq(newLeft, newRight))
          } else {
            // Otherwise, don't do anything with the expression.
            b
          }
        }.getOrElse(b)  // If there is no applicable conversion, leave expression unchanged.

      case e: ImplicitCastInputTypes if e.inputTypes.nonEmpty =>
        val children: Seq[Expression] = e.children.zip(e.inputTypes).map { case (in, expected) =>
          // If we cannot do the implicit cast, just use the original input.
          implicitCast(in, expected).getOrElse(in)
        }
        e.withNewChildren(children)

      case e: ExpectsInputTypes if e.inputTypes.nonEmpty =>
        // Convert NullType into some specific target type for ExpectsInputTypes that don't do
        // general implicit casting.
        val children: Seq[Expression] = e.children.zip(e.inputTypes).map { case (in, expected) =>
          if (in.dataType == NullType && !expected.acceptsType(NullType)) {
            Literal.create(null, expected.defaultConcreteType)
          } else {
            in
          }
        }
        e.withNewChildren(children)
    }

    /**
     * Given an expected data type, try to cast the expression and return the cast expression.
     *
     * If the expression already fits the input type, we simply return the expression itself.
     * If the expression has an incompatible type that cannot be implicitly cast, return None.
     */
    def implicitCast(e: Expression, expectedType: AbstractDataType): Option[Expression] = {
      val inType = e.dataType

      // Note that ret is nullable to avoid typing a lot of Some(...) in this local scope.
      // We wrap immediately an Option after this.
      @Nullable val ret: Expression = (inType, expectedType) match {

        // If the expected type is already a parent of the input type, no need to cast.
        case _ if expectedType.acceptsType(inType) => e

        // Cast null type (usually from null literals) into target types
        case (NullType, target) => Cast(e, target.defaultConcreteType)

        // If the function accepts any numeric type and the input is a string, we follow the hive
        // convention and cast that input into a double
        case (StringType, NumericType) => Cast(e, NumericType.defaultConcreteType)

        // Implicit cast among numeric types. When we reach here, input type is not acceptable.

        // If input is a numeric type but not decimal, and we expect a decimal type,
        // cast the input to decimal.
        case (d: NumericType, DecimalType) => Cast(e, DecimalType.forType(d))
        // For any other numeric types, implicitly cast to each other, e.g. long -> int, int -> long
        case (_: NumericType, target: NumericType) => Cast(e, target)

        // Implicit cast between date time types
        case (DateType, TimestampType) => Cast(e, TimestampType)
        case (TimestampType, DateType) => Cast(e, DateType)

        // Implicit cast from/to string
        case (StringType, DecimalType) => Cast(e, DecimalType.SYSTEM_DEFAULT)
        case (StringType, target: NumericType) => Cast(e, target)
        case (StringType, DateType) => Cast(e, DateType)
        case (StringType, TimestampType) => Cast(e, TimestampType)
        case (StringType, BinaryType) => Cast(e, BinaryType)
        // Cast any atomic type to string.
        case (any: AtomicType, StringType) if any != StringType => Cast(e, StringType)

        // When we reach here, input type is not acceptable for any types in this type collection,
        // try to find the first one we can implicitly cast.
        case (_, TypeCollection(types)) => types.flatMap(implicitCast(e, _)).headOption.orNull

        // Else, just return the same input expression
        case _ => null
      }
      Option(ret)
    }
  }
}
