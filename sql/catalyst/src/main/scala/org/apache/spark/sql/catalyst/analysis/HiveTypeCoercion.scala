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
import org.apache.spark.sql.catalyst.plans.logical.{LogicalPlan, Project, Union}
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.types._

object HiveTypeCoercion {
  // See https://cwiki.apache.org/confluence/display/Hive/LanguageManual+Types.
  // The conversion for integral and floating point types have a linear widening hierarchy:
  private val numericPrecedence =
    IndexedSeq(
      ByteType,
      ShortType,
      IntegerType,
      LongType,
      FloatType,
      DoubleType,
      DecimalType.Unlimited)

  /**
   * Find the tightest common type of two types that might be used in a binary expression.
   * This handles all numeric types except fixed-precision decimals interacting with each other or
   * with primitive types, because in that case the precision and scale of the result depends on
   * the operation. Those rules are implemented in [[HiveTypeCoercion.DecimalPrecision]].
   */
  val findTightestCommonTypeOfTwo: (DataType, DataType) => Option[DataType] = {
    case (t1, t2) if t1 == t2 => Some(t1)
    case (NullType, t1) => Some(t1)
    case (t1, NullType) => Some(t1)

    // Promote numeric types to the highest of the two and all numeric types to unlimited decimal
    case (t1, t2) if Seq(t1, t2).forall(numericPrecedence.contains) =>
      val index = numericPrecedence.lastIndexWhere(t => t == t1 || t == t2)
      Some(numericPrecedence(index))

    // Fixed-precision decimals can up-cast into unlimited
    case (DecimalType.Unlimited, _: DecimalType) => Some(DecimalType.Unlimited)
    case (_: DecimalType, DecimalType.Unlimited) => Some(DecimalType.Unlimited)

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
   * Find the tightest common type of a set of types by continuously applying
   * `findTightestCommonTypeOfTwo` on these types.
   */
  private def findTightestCommonType(types: Seq[DataType]) = {
    types.foldLeft[Option[DataType]](Some(NullType))((r, c) => r match {
      case None => None
      case Some(d) => findTightestCommonTypeOfTwo(d, c)
    })
  }
}

/**
 * A collection of [[Rule Rules]] that can be used to coerce differing types that
 * participate in operations into compatible ones.  Most of these rules are based on Hive semantics,
 * but they do not introduce any dependencies on the hive codebase.  For this reason they remain in
 * Catalyst until we have a more standard set of coercions.
 */
trait HiveTypeCoercion {

  import HiveTypeCoercion._

  val typeCoercionRules =
    PropagateTypes ::
    ConvertNaNs ::
    InConversion ::
    WidenTypes ::
    PromoteStrings ::
    DecimalPrecision ::
    BooleanEquality ::
    StringToIntegralCasts ::
    FunctionArgumentConversion ::
    CaseWhenCoercion ::
    IfCoercion ::
    Division ::
    PropagateTypes ::
    ExpectedInputConversion ::
    Nil

  /**
   * Applies any changes to [[AttributeReference]] data types that are made by other rules to
   * instances higher in the query tree.
   */
  object PropagateTypes extends Rule[LogicalPlan] {
    def apply(plan: LogicalPlan): LogicalPlan = plan transform {
      // No propagation required for leaf nodes.
      case q: LogicalPlan if q.children.isEmpty => q

      // Don't propagate types from unresolved children.
      case q: LogicalPlan if !q.childrenResolved => q

      case q: LogicalPlan => q transformExpressions {
        case a: AttributeReference =>
          q.inputSet.find(_.exprId == a.exprId) match {
            // This can happen when a Attribute reference is born in a non-leaf node, for example
            // due to a call to an external script like in the Transform operator.
            // TODO: Perhaps those should actually be aliases?
            case None => a
            // Leave the same if the dataTypes match.
            case Some(newType) if a.dataType == newType.dataType => a
            case Some(newType) =>
              logDebug(s"Promoting $a to $newType in ${q.simpleString}}")
              newType
          }
      }
    }
  }

  /**
   * Converts string "NaN"s that are in binary operators with a NaN-able types (Float / Double) to
   * the appropriate numeric equivalent.
   */
  object ConvertNaNs extends Rule[LogicalPlan] {
    private val StringNaN = Literal("NaN")

    def apply(plan: LogicalPlan): LogicalPlan = plan transform {
      case q: LogicalPlan => q transformExpressions {
        // Skip nodes who's children have not been resolved yet.
        case e if !e.childrenResolved => e

        /* Double Conversions */
        case b @ BinaryExpression(StringNaN, right @ DoubleType()) =>
          b.makeCopy(Array(Literal(Double.NaN), right))
        case b @ BinaryExpression(left @ DoubleType(), StringNaN) =>
          b.makeCopy(Array(left, Literal(Double.NaN)))

        /* Float Conversions */
        case b @ BinaryExpression(StringNaN, right @ FloatType()) =>
          b.makeCopy(Array(Literal(Float.NaN), right))
        case b @ BinaryExpression(left @ FloatType(), StringNaN) =>
          b.makeCopy(Array(left, Literal(Float.NaN)))

        /* Use float NaN by default to avoid unnecessary type widening */
        case b @ BinaryExpression(left @ StringNaN, StringNaN) =>
          b.makeCopy(Array(left, Literal(Float.NaN)))
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
   *
   * Additionally, all types when UNION-ed with strings will be promoted to strings.
   * Other string conversions are handled by PromoteStrings.
   *
   * Widening types might result in loss of precision in the following cases:
   * - IntegerType to FloatType
   * - LongType to FloatType
   * - LongType to DoubleType
   */
  object WidenTypes extends Rule[LogicalPlan] {
    import HiveTypeCoercion._

    def apply(plan: LogicalPlan): LogicalPlan = plan transform {
      // TODO: unions with fixed-precision decimals
      case u @ Union(left, right) if u.childrenResolved && !u.resolved =>
        val castedInput = left.output.zip(right.output).map {
          // When a string is found on one side, make the other side a string too.
          case (lhs, rhs) if lhs.dataType == StringType && rhs.dataType != StringType =>
            (lhs, Alias(Cast(rhs, StringType), rhs.name)())
          case (lhs, rhs) if lhs.dataType != StringType && rhs.dataType == StringType =>
            (Alias(Cast(lhs, StringType), lhs.name)(), rhs)

          case (lhs, rhs) if lhs.dataType != rhs.dataType =>
            logDebug(s"Resolving mismatched union input ${lhs.dataType}, ${rhs.dataType}")
            findTightestCommonTypeOfTwo(lhs.dataType, rhs.dataType).map { widestType =>
              val newLeft =
                if (lhs.dataType == widestType) lhs else Alias(Cast(lhs, widestType), lhs.name)()
              val newRight =
                if (rhs.dataType == widestType) rhs else Alias(Cast(rhs, widestType), rhs.name)()

              (newLeft, newRight)
            }.getOrElse {
              // If there is no applicable conversion, leave expression unchanged.
              (lhs, rhs)
            }

          case other => other
        }

        val (castedLeft, castedRight) = castedInput.unzip

        val newLeft =
          if (castedLeft.map(_.dataType) != left.output.map(_.dataType)) {
            logDebug(s"Widening numeric types in union $castedLeft ${left.output}")
            Project(castedLeft, left)
          } else {
            left
          }

        val newRight =
          if (castedRight.map(_.dataType) != right.output.map(_.dataType)) {
            logDebug(s"Widening numeric types in union $castedRight ${right.output}")
            Project(castedRight, right)
          } else {
            right
          }

        Union(newLeft, newRight)

      // Also widen types for BinaryExpressions.
      case q: LogicalPlan => q transformExpressions {
        // Skip nodes who's children have not been resolved yet.
        case e if !e.childrenResolved => e

        case b @ BinaryExpression(left, right) if left.dataType != right.dataType =>
          findTightestCommonTypeOfTwo(left.dataType, right.dataType).map { widestType =>
            val newLeft = if (left.dataType == widestType) left else Cast(left, widestType)
            val newRight = if (right.dataType == widestType) right else Cast(right, widestType)
            b.makeCopy(Array(newLeft, newRight))
          }.getOrElse(b)  // If there is no applicable conversion, leave expression unchanged.
      }
    }
  }

  /**
   * Promotes strings that appear in arithmetic expressions.
   */
  object PromoteStrings extends Rule[LogicalPlan] {
    def apply(plan: LogicalPlan): LogicalPlan = plan transformAllExpressions {
      // Skip nodes who's children have not been resolved yet.
      case e if !e.childrenResolved => e

      case a @ BinaryArithmetic(left @ StringType(), r) =>
        a.makeCopy(Array(Cast(left, DoubleType), r))
      case a @ BinaryArithmetic(left, right @ StringType()) =>
        a.makeCopy(Array(left, Cast(right, DoubleType)))

      // we should cast all timestamp/date/string compare into string compare
      case p @ BinaryComparison(left @ StringType(), right @ DateType()) =>
        p.makeCopy(Array(left, Cast(right, StringType)))
      case p @ BinaryComparison(left @ DateType(), right @ StringType()) =>
        p.makeCopy(Array(Cast(left, StringType), right))
      case p @ BinaryComparison(left @ StringType(), right @ TimestampType()) =>
        p.makeCopy(Array(Cast(left, TimestampType), right))
      case p @ BinaryComparison(left @ TimestampType(), right @ StringType()) =>
        p.makeCopy(Array(left, Cast(right, TimestampType)))
      case p @ BinaryComparison(left @ TimestampType(), right @ DateType()) =>
        p.makeCopy(Array(Cast(left, StringType), Cast(right, StringType)))
      case p @ BinaryComparison(left @ DateType(), right @ TimestampType()) =>
        p.makeCopy(Array(Cast(left, StringType), Cast(right, StringType)))

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
      case Sqrt(e @ StringType()) => Sqrt(Cast(e, DoubleType))
    }
  }

  /**
   * Convert all expressions in in() list to the left operator type
   */
  object InConversion extends Rule[LogicalPlan] {
    def apply(plan: LogicalPlan): LogicalPlan = plan transformAllExpressions {
      // Skip nodes who's children have not been resolved yet.
      case e if !e.childrenResolved => e

      case i @ In(a, b) if b.exists(_.dataType != a.dataType) =>
        i.makeCopy(Array(a, b.map(Cast(_, a.dataType))))
    }
  }

  // scalastyle:off
  /**
   * Calculates and propagates precision for fixed-precision decimals. Hive has a number of
   * rules for this based on the SQL standard and MS SQL:
   * https://cwiki.apache.org/confluence/download/attachments/27362075/Hive_Decimal_Precision_Scale_Support.pdf
   * https://msdn.microsoft.com/en-us/library/ms190476.aspx
   *
   * In particular, if we have expressions e1 and e2 with precision/scale p1/s2 and p2/s2
   * respectively, then the following operations have the following precision / scale:
   *
   *   Operation    Result Precision                        Result Scale
   *   ------------------------------------------------------------------------
   *   e1 + e2      max(s1, s2) + max(p1-s1, p2-s2) + 1     max(s1, s2)
   *   e1 - e2      max(s1, s2) + max(p1-s1, p2-s2) + 1     max(s1, s2)
   *   e1 * e2      p1 + p2 + 1                             s1 + s2
   *   e1 / e2      p1 - s1 + s2 + max(6, s1 + p2 + 1)      max(6, s1 + p2 + 1)
   *   e1 % e2      min(p1-s1, p2-s2) + max(s1, s2)         max(s1, s2)
   *   e1 union e2  max(s1, s2) + max(p1-s1, p2-s2)         max(s1, s2)
   *   sum(e1)      p1 + 10                                 s1
   *   avg(e1)      p1 + 4                                  s1 + 4
   *
   * Catalyst also has unlimited-precision decimals. For those, all ops return unlimited precision.
   *
   * To implement the rules for fixed-precision types, we introduce casts to turn them to unlimited
   * precision, do the math on unlimited-precision numbers, then introduce casts back to the
   * required fixed precision. This allows us to do all rounding and overflow handling in the
   * cast-to-fixed-precision operator.
   *
   * In addition, when mixing non-decimal types with decimals, we use the following rules:
   * - BYTE gets turned into DECIMAL(3, 0)
   * - SHORT gets turned into DECIMAL(5, 0)
   * - INT gets turned into DECIMAL(10, 0)
   * - LONG gets turned into DECIMAL(20, 0)
   * - FLOAT and DOUBLE
   *   1. Union operation:
   *      FLOAT gets turned into DECIMAL(7, 7), DOUBLE gets turned into DECIMAL(15, 15) (this is the
   *      same as Hive)
   *   2. Other operation:
   *      FLOAT and DOUBLE cause fixed-length decimals to turn into DOUBLE (this is the same as Hive,
   *   but note that unlimited decimals are considered bigger than doubles in WidenTypes)
   */
  // scalastyle:on
  object DecimalPrecision extends Rule[LogicalPlan] {
    import scala.math.{max, min}

    // Conversion rules for integer types into fixed-precision decimals
    private val intTypeToFixed: Map[DataType, DecimalType] = Map(
      ByteType -> DecimalType(3, 0),
      ShortType -> DecimalType(5, 0),
      IntegerType -> DecimalType(10, 0),
      LongType -> DecimalType(20, 0)
    )

    private def isFloat(t: DataType): Boolean = t == FloatType || t == DoubleType

    // Conversion rules for float and double into fixed-precision decimals
    private val floatTypeToFixed: Map[DataType, DecimalType] = Map(
      FloatType -> DecimalType(7, 7),
      DoubleType -> DecimalType(15, 15)
    )

    def apply(plan: LogicalPlan): LogicalPlan = plan transform {
      // fix decimal precision for union
      case u @ Union(left, right) if u.childrenResolved && !u.resolved =>
        val castedInput = left.output.zip(right.output).map {
          case (lhs, rhs) if lhs.dataType != rhs.dataType =>
            (lhs.dataType, rhs.dataType) match {
              case (DecimalType.Fixed(p1, s1), DecimalType.Fixed(p2, s2)) =>
                // Union decimals with precision/scale p1/s2 and p2/s2  will be promoted to
                // DecimalType(max(s1, s2) + max(p1-s1, p2-s2), max(s1, s2))
                val fixedType = DecimalType(max(s1, s2) + max(p1 - s1, p2 - s2), max(s1, s2))
                (Alias(Cast(lhs, fixedType), lhs.name)(), Alias(Cast(rhs, fixedType), rhs.name)())
              case (t, DecimalType.Fixed(p, s)) if intTypeToFixed.contains(t) =>
                (Alias(Cast(lhs, intTypeToFixed(t)), lhs.name)(), rhs)
              case (DecimalType.Fixed(p, s), t) if intTypeToFixed.contains(t) =>
                (lhs, Alias(Cast(rhs, intTypeToFixed(t)), rhs.name)())
              case (t, DecimalType.Fixed(p, s)) if floatTypeToFixed.contains(t) =>
                (Alias(Cast(lhs, floatTypeToFixed(t)), lhs.name)(), rhs)
              case (DecimalType.Fixed(p, s), t) if floatTypeToFixed.contains(t) =>
                (lhs, Alias(Cast(rhs, floatTypeToFixed(t)), rhs.name)())
              case _ => (lhs, rhs)
            }
          case other => other
        }

        val (castedLeft, castedRight) = castedInput.unzip

        val newLeft =
          if (castedLeft.map(_.dataType) != left.output.map(_.dataType)) {
            Project(castedLeft, left)
          } else {
            left
          }

        val newRight =
          if (castedRight.map(_.dataType) != right.output.map(_.dataType)) {
            Project(castedRight, right)
          } else {
            right
          }

        Union(newLeft, newRight)

      // fix decimal precision for expressions
      case q => q.transformExpressions {
        // Skip nodes whose children have not been resolved yet
        case e if !e.childrenResolved => e

        case Add(e1 @ DecimalType.Expression(p1, s1), e2 @ DecimalType.Expression(p2, s2)) =>
          Cast(
            Add(Cast(e1, DecimalType.Unlimited), Cast(e2, DecimalType.Unlimited)),
            DecimalType(max(s1, s2) + max(p1 - s1, p2 - s2) + 1, max(s1, s2))
          )

        case Subtract(e1 @ DecimalType.Expression(p1, s1), e2 @ DecimalType.Expression(p2, s2)) =>
          Cast(
            Subtract(Cast(e1, DecimalType.Unlimited), Cast(e2, DecimalType.Unlimited)),
            DecimalType(max(s1, s2) + max(p1 - s1, p2 - s2) + 1, max(s1, s2))
          )

        case Multiply(e1 @ DecimalType.Expression(p1, s1), e2 @ DecimalType.Expression(p2, s2)) =>
          Cast(
            Multiply(Cast(e1, DecimalType.Unlimited), Cast(e2, DecimalType.Unlimited)),
            DecimalType(p1 + p2 + 1, s1 + s2)
          )

        case Divide(e1 @ DecimalType.Expression(p1, s1), e2 @ DecimalType.Expression(p2, s2)) =>
          Cast(
            Divide(Cast(e1, DecimalType.Unlimited), Cast(e2, DecimalType.Unlimited)),
            DecimalType(p1 - s1 + s2 + max(6, s1 + p2 + 1), max(6, s1 + p2 + 1))
          )

        case Remainder(e1 @ DecimalType.Expression(p1, s1), e2 @ DecimalType.Expression(p2, s2)) =>
          Cast(
            Remainder(Cast(e1, DecimalType.Unlimited), Cast(e2, DecimalType.Unlimited)),
            DecimalType(min(p1 - s1, p2 - s2) + max(s1, s2), max(s1, s2))
          )

        // When we compare 2 decimal types with different precisions, cast them to the smallest
        // common precision.
        case b @ BinaryComparison(e1 @ DecimalType.Expression(p1, s1),
                                  e2 @ DecimalType.Expression(p2, s2)) if p1 != p2 || s1 != s2 =>
          val resultType = DecimalType(max(p1, p2), max(s1, s2))
          b.makeCopy(Array(Cast(e1, resultType), Cast(e2, resultType)))

        // Promote integers inside a binary expression with fixed-precision decimals to decimals,
        // and fixed-precision decimals in an expression with floats / doubles to doubles
        case b @ BinaryExpression(left, right) if left.dataType != right.dataType =>
          (left.dataType, right.dataType) match {
            case (t, DecimalType.Fixed(p, s)) if intTypeToFixed.contains(t) =>
              b.makeCopy(Array(Cast(left, intTypeToFixed(t)), right))
            case (DecimalType.Fixed(p, s), t) if intTypeToFixed.contains(t) =>
              b.makeCopy(Array(left, Cast(right, intTypeToFixed(t))))
            case (t, DecimalType.Fixed(p, s)) if isFloat(t) =>
              b.makeCopy(Array(left, Cast(right, DoubleType)))
            case (DecimalType.Fixed(p, s), t) if isFloat(t) =>
              b.makeCopy(Array(Cast(left, DoubleType), right))
            case _ =>
              b
          }

        // TODO: MaxOf, MinOf, etc might want other rules

        // SUM and AVERAGE are handled by the implementations of those expressions
      }
    }

  }

  /**
   * Changes numeric values to booleans so that expressions like true = 1 can be evaluated.
   */
  object BooleanEquality extends Rule[LogicalPlan] {
    private val trueValues = Seq(1.toByte, 1.toShort, 1, 1L, Decimal(1))
    private val falseValues = Seq(0.toByte, 0.toShort, 0, 0L, Decimal(0))

    private def buildCaseKeyWhen(booleanExpr: Expression, numericExpr: Expression) = {
      CaseKeyWhen(numericExpr, Seq(
        Literal(trueValues.head), booleanExpr,
        Literal(falseValues.head), Not(booleanExpr),
        Literal(false)))
    }

    private def transform(booleanExpr: Expression, numericExpr: Expression) = {
      If(Or(IsNull(booleanExpr), IsNull(numericExpr)),
        Literal.create(null, BooleanType),
        buildCaseKeyWhen(booleanExpr, numericExpr))
    }

    private def transformNullSafe(booleanExpr: Expression, numericExpr: Expression) = {
      CaseWhen(Seq(
        And(IsNull(booleanExpr), IsNull(numericExpr)), Literal(true),
        Or(IsNull(booleanExpr), IsNull(numericExpr)), Literal(false),
        buildCaseKeyWhen(booleanExpr, numericExpr)
      ))
    }

    def apply(plan: LogicalPlan): LogicalPlan = plan transformAllExpressions {
      // Skip nodes who's children have not been resolved yet.
      case e if !e.childrenResolved => e

      // Hive treats (true = 1) as true and (false = 0) as true,
      // all other cases are considered as false.

      // We may simplify the expression if one side is literal numeric values
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
        transform(left , right)
      case EqualTo(left @ NumericType(), right @ BooleanType()) =>
        transform(right, left)
      case EqualNullSafe(left @ BooleanType(), right @ NumericType()) =>
        transformNullSafe(left, right)
      case EqualNullSafe(left @ NumericType(), right @ BooleanType()) =>
        transformNullSafe(right, left)
    }
  }

  /**
   * When encountering a cast from a string representing a valid fractional number to an integral
   * type the jvm will throw a `java.lang.NumberFormatException`.  Hive, in contrast, returns the
   * truncated version of this number.
   */
  object StringToIntegralCasts extends Rule[LogicalPlan] {
    def apply(plan: LogicalPlan): LogicalPlan = plan transformAllExpressions {
      // Skip nodes who's children have not been resolved yet.
      case e if !e.childrenResolved => e

      case Cast(e @ StringType(), t: IntegralType) =>
        Cast(Cast(e, DecimalType.Unlimited), t)
    }
  }

  /**
   * This ensure that the types for various functions are as expected.
   */
  object FunctionArgumentConversion extends Rule[LogicalPlan] {
    def apply(plan: LogicalPlan): LogicalPlan = plan transformAllExpressions {
      // Skip nodes who's children have not been resolved yet.
      case e if !e.childrenResolved => e

      case a @ CreateArray(children) if !a.resolved =>
        val commonType = a.childTypes.reduce(
          (a, b) => findTightestCommonTypeOfTwo(a, b).getOrElse(StringType))
        CreateArray(
          children.map(c => if (c.dataType == commonType) c else Cast(c, commonType)))

      // Promote SUM, SUM DISTINCT and AVERAGE to largest types to prevent overflows.
      case s @ Sum(e @ DecimalType()) => s // Decimal is already the biggest.
      case Sum(e @ IntegralType()) if e.dataType != LongType => Sum(Cast(e, LongType))
      case Sum(e @ FractionalType()) if e.dataType != DoubleType => Sum(Cast(e, DoubleType))

      case s @ SumDistinct(e @ DecimalType()) => s // Decimal is already the biggest.
      case SumDistinct(e @ IntegralType()) if e.dataType != LongType =>
        SumDistinct(Cast(e, LongType))
      case SumDistinct(e @ FractionalType()) if e.dataType != DoubleType =>
        SumDistinct(Cast(e, DoubleType))

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
      case Coalesce(es) if es.map(_.dataType).distinct.size > 1 =>
        val types = es.map(_.dataType)
        findTightestCommonType(types) match {
          case Some(finalDataType) => Coalesce(es.map(Cast(_, finalDataType)))
          case None =>
            sys.error(s"Could not determine return type of Coalesce for ${types.mkString(",")}")
        }
    }
  }

  /**
   * Hive only performs integral division with the DIV operator. The arguments to / are always
   * converted to fractional types.
   */
  object Division extends Rule[LogicalPlan] {
    def apply(plan: LogicalPlan): LogicalPlan = plan transformAllExpressions {
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
    import HiveTypeCoercion._

    def apply(plan: LogicalPlan): LogicalPlan = plan transformAllExpressions {
      case c: CaseWhenLike if c.childrenResolved && !c.valueTypesEqual =>
        logDebug(s"Input values for null casting ${c.valueTypes.mkString(",")}")
        val maybeCommonType = findTightestCommonType(c.valueTypes)
        maybeCommonType.map { commonType =>
          val castedBranches = c.branches.grouped(2).map {
            case Seq(when, value) if value.dataType != commonType =>
              Seq(when, Cast(value, commonType))
            case Seq(elseVal) if elseVal.dataType != commonType =>
              Seq(Cast(elseVal, commonType))
            case other => other
          }.reduce(_ ++ _)
          c match {
            case _: CaseWhen => CaseWhen(castedBranches)
            case CaseKeyWhen(key, _) => CaseKeyWhen(key, castedBranches)
          }
        }.getOrElse(c)

      case c: CaseKeyWhen if c.childrenResolved && !c.resolved =>
        val maybeCommonType = findTightestCommonType((c.key +: c.whenList).map(_.dataType))
        maybeCommonType.map { commonType =>
          val castedBranches = c.branches.grouped(2).map {
            case Seq(when, then) if when.dataType != commonType =>
              Seq(Cast(when, commonType), then)
            case other => other
          }.reduce(_ ++ _)
          CaseKeyWhen(Cast(c.key, commonType), castedBranches)
        }.getOrElse(c)
    }
  }

  /**
   * Coerces the type of different branches of If statement to a common type.
   */
  object IfCoercion extends Rule[LogicalPlan] {
    def apply(plan: LogicalPlan): LogicalPlan = plan transformAllExpressions {
      // Find tightest common type for If, if the true value and false value have different types.
      case i @ If(pred, left, right) if left.dataType != right.dataType =>
        findTightestCommonTypeToString(left.dataType, right.dataType).map { widestType =>
          val newLeft = if (left.dataType == widestType) left else Cast(left, widestType)
          val newRight = if (right.dataType == widestType) right else Cast(right, widestType)
          i.makeCopy(Array(pred, newLeft, newRight))
        }.getOrElse(i)  // If there is no applicable conversion, leave expression unchanged.

      // Convert If(null literal, _, _) into boolean type.
      // In the optimizer, we should short-circuit this directly into false value.
      case i @ If(pred, left, right) if pred.dataType == NullType =>
        i.makeCopy(Array(Literal.create(null, BooleanType), left, right))
    }
  }

  /**
   * Casts types according to the expected input types for Expressions that have the trait
   * `ExpectsInputTypes`.
   */
  object ExpectedInputConversion extends Rule[LogicalPlan] {

    def apply(plan: LogicalPlan): LogicalPlan = plan transformAllExpressions {
      // Skip nodes who's children have not been resolved yet.
      case e if !e.childrenResolved => e

      case e: ExpectsInputTypes if e.children.map(_.dataType) != e.expectedChildTypes =>
        val newC = (e.children, e.children.map(_.dataType), e.expectedChildTypes).zipped.map {
          case (child, actual, expected) =>
            if (actual == expected) child else Cast(child, expected)
        }
        e.withNewChildren(newC)
    }
  }
}
