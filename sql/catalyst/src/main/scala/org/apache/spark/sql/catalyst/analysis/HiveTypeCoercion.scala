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
  val findTightestCommonType: (DataType, DataType) => Option[DataType] = {
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
    BooleanComparisons ::
    BooleanCasts ::
    StringToIntegralCasts ::
    FunctionArgumentConversion ::
    CaseWhenCoercion ::
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
    val stringNaN = Literal("NaN")

    def apply(plan: LogicalPlan): LogicalPlan = plan transform {
      case q: LogicalPlan => q transformExpressions {
        // Skip nodes who's children have not been resolved yet.
        case e if !e.childrenResolved => e

        /* Double Conversions */
        case b: BinaryExpression if b.left == stringNaN && b.right.dataType == DoubleType =>
          b.makeCopy(Array(b.right, Literal(Double.NaN)))
        case b: BinaryExpression if b.left.dataType == DoubleType && b.right == stringNaN =>
          b.makeCopy(Array(Literal(Double.NaN), b.left))
        case b: BinaryExpression if b.left == stringNaN && b.right == stringNaN =>
          b.makeCopy(Array(Literal(Double.NaN), b.left))

        /* Float Conversions */
        case b: BinaryExpression if b.left == stringNaN && b.right.dataType == FloatType =>
          b.makeCopy(Array(b.right, Literal(Float.NaN)))
        case b: BinaryExpression if b.left.dataType == FloatType && b.right == stringNaN =>
          b.makeCopy(Array(Literal(Float.NaN), b.left))
        case b: BinaryExpression if b.left == stringNaN && b.right == stringNaN =>
          b.makeCopy(Array(Literal(Float.NaN), b.left))
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
          case (l, r) if l.dataType == StringType && r.dataType != StringType =>
            (l, Alias(Cast(r, StringType), r.name)())
          case (l, r) if l.dataType != StringType && r.dataType == StringType =>
            (Alias(Cast(l, StringType), l.name)(), r)

          case (l, r) if l.dataType != r.dataType =>
            logDebug(s"Resolving mismatched union input ${l.dataType}, ${r.dataType}")
            findTightestCommonType(l.dataType, r.dataType).map { widestType =>
              val newLeft =
                if (l.dataType == widestType) l else Alias(Cast(l, widestType), l.name)()
              val newRight =
                if (r.dataType == widestType) r else Alias(Cast(r, widestType), r.name)()

              (newLeft, newRight)
            }.getOrElse((l, r)) // If there is no applicable conversion, leave expression unchanged.
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

        case b: BinaryExpression if b.left.dataType != b.right.dataType =>
          findTightestCommonType(b.left.dataType, b.right.dataType).map { widestType =>
            val newLeft =
              if (b.left.dataType == widestType) b.left else Cast(b.left, widestType)
            val newRight =
              if (b.right.dataType == widestType) b.right else Cast(b.right, widestType)
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

      case a: BinaryArithmetic if a.left.dataType == StringType =>
        a.makeCopy(Array(Cast(a.left, DoubleType), a.right))
      case a: BinaryArithmetic if a.right.dataType == StringType =>
        a.makeCopy(Array(a.left, Cast(a.right, DoubleType)))

      // For equality between string and timestamp we cast the string to a timestamp
      // so that things like rounding of subsecond precision does not affect the comparison.
      case p @ Equality(left @ StringType(), right @ TimestampType()) =>
        p.makeCopy(Array(Cast(left, TimestampType), right))
      case p @ Equality(left @ TimestampType(), right @ StringType()) =>
        p.makeCopy(Array(left, Cast(right, TimestampType)))

      // We should cast all relative timestamp/date/string comparison into string comparisions
      // This behaves as a user would expect because timestamp strings sort lexicographically.
      // i.e. TimeStamp(2013-01-01 00:00 ...) < "2014" = true
      case p: BinaryComparison if p.left.dataType == StringType &&
                                  p.right.dataType == DateType =>
        p.makeCopy(Array(p.left, Cast(p.right, StringType)))
      case p: BinaryComparison if p.left.dataType == DateType &&
                                  p.right.dataType == StringType =>
        p.makeCopy(Array(Cast(p.left, StringType), p.right))
      case p: BinaryComparison if p.left.dataType == StringType &&
                                  p.right.dataType == TimestampType =>
        p.makeCopy(Array(p.left, Cast(p.right, StringType)))
      case p: BinaryComparison if p.left.dataType == TimestampType &&
                                  p.right.dataType == StringType =>
        p.makeCopy(Array(Cast(p.left, StringType), p.right))

      // Comparisons between dates and timestamps.
      case p: BinaryComparison if p.left.dataType == TimestampType &&
                                  p.right.dataType == DateType =>
        p.makeCopy(Array(Cast(p.left, StringType), Cast(p.right, StringType)))
      case p: BinaryComparison if p.left.dataType == DateType &&
                                  p.right.dataType == TimestampType =>
        p.makeCopy(Array(Cast(p.left, StringType), Cast(p.right, StringType)))

      case p: BinaryComparison if p.left.dataType == StringType &&
                                  p.right.dataType != StringType =>
        p.makeCopy(Array(Cast(p.left, DoubleType), p.right))
      case p: BinaryComparison if p.left.dataType != StringType &&
                                  p.right.dataType == StringType =>
        p.makeCopy(Array(p.left, Cast(p.right, DoubleType)))

      case i @ In(a, b) if a.dataType == DateType &&
                           b.forall(_.dataType == StringType) =>
        i.makeCopy(Array(Cast(a, StringType), b))
      case i @ In(a, b) if a.dataType == TimestampType &&
                           b.forall(_.dataType == StringType) =>
        i.makeCopy(Array(a, b.map(Cast(_, TimestampType))))
      case i @ In(a, b) if a.dataType == DateType &&
                           b.forall(_.dataType == TimestampType) =>
        i.makeCopy(Array(Cast(a, StringType), b.map(Cast(_, StringType))))
      case i @ In(a, b) if a.dataType == TimestampType &&
                           b.forall(_.dataType == DateType) =>
        i.makeCopy(Array(Cast(a, StringType), b.map(Cast(_, StringType))))

      case Sum(e) if e.dataType == StringType =>
        Sum(Cast(e, DoubleType))
      case Average(e) if e.dataType == StringType =>
        Average(Cast(e, DoubleType))
      case Sqrt(e) if e.dataType == StringType =>
        Sqrt(Cast(e, DoubleType))
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
    val intTypeToFixed: Map[DataType, DecimalType] = Map(
      ByteType -> DecimalType(3, 0),
      ShortType -> DecimalType(5, 0),
      IntegerType -> DecimalType(10, 0),
      LongType -> DecimalType(20, 0)
    )

    def isFloat(t: DataType): Boolean = t == FloatType || t == DoubleType

    // Conversion rules for float and double into fixed-precision decimals
    val floatTypeToFixed: Map[DataType, DecimalType] = Map(
      FloatType -> DecimalType(7, 7),
      DoubleType -> DecimalType(15, 15)
    )

    def apply(plan: LogicalPlan): LogicalPlan = plan transform {
      // fix decimal precision for union
      case u @ Union(left, right) if u.childrenResolved && !u.resolved =>
        val castedInput = left.output.zip(right.output).map {
          case (l, r) if l.dataType != r.dataType =>
            (l.dataType, r.dataType) match {
              case (DecimalType.Fixed(p1, s1), DecimalType.Fixed(p2, s2)) =>
                // Union decimals with precision/scale p1/s2 and p2/s2  will be promoted to
                // DecimalType(max(s1, s2) + max(p1-s1, p2-s2), max(s1, s2))
                val fixedType = DecimalType(max(s1, s2) + max(p1 - s1, p2 - s2), max(s1, s2))
                (Alias(Cast(l, fixedType), l.name)(), Alias(Cast(r, fixedType), r.name)())
              case (t, DecimalType.Fixed(p, s)) if intTypeToFixed.contains(t) =>
                (Alias(Cast(l, intTypeToFixed(t)), l.name)(), r)
              case (DecimalType.Fixed(p, s), t) if intTypeToFixed.contains(t) =>
                (l, Alias(Cast(r, intTypeToFixed(t)), r.name)())
              case (t, DecimalType.Fixed(p, s)) if floatTypeToFixed.contains(t) =>
                (Alias(Cast(l, floatTypeToFixed(t)), l.name)(), r)
              case (DecimalType.Fixed(p, s), t) if floatTypeToFixed.contains(t) =>
                (l, Alias(Cast(r, floatTypeToFixed(t)), r.name)())
              case _ => (l, r)
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

        case LessThan(e1 @ DecimalType.Expression(p1, s1),
                      e2 @ DecimalType.Expression(p2, s2)) if p1 != p2 || s1 != s2 =>
          LessThan(Cast(e1, DecimalType.Unlimited), Cast(e2, DecimalType.Unlimited))

        case LessThanOrEqual(e1 @ DecimalType.Expression(p1, s1),
                             e2 @ DecimalType.Expression(p2, s2)) if p1 != p2 || s1 != s2 =>
          LessThanOrEqual(Cast(e1, DecimalType.Unlimited), Cast(e2, DecimalType.Unlimited))

        case GreaterThan(e1 @ DecimalType.Expression(p1, s1),
                         e2 @ DecimalType.Expression(p2, s2)) if p1 != p2 || s1 != s2 =>
          GreaterThan(Cast(e1, DecimalType.Unlimited), Cast(e2, DecimalType.Unlimited))

        case GreaterThanOrEqual(e1 @ DecimalType.Expression(p1, s1),
                                e2 @ DecimalType.Expression(p2, s2)) if p1 != p2 || s1 != s2 =>
          GreaterThanOrEqual(Cast(e1, DecimalType.Unlimited), Cast(e2, DecimalType.Unlimited))

        // Promote integers inside a binary expression with fixed-precision decimals to decimals,
        // and fixed-precision decimals in an expression with floats / doubles to doubles
        case b: BinaryExpression if b.left.dataType != b.right.dataType =>
          (b.left.dataType, b.right.dataType) match {
            case (t, DecimalType.Fixed(p, s)) if intTypeToFixed.contains(t) =>
              b.makeCopy(Array(Cast(b.left, intTypeToFixed(t)), b.right))
            case (DecimalType.Fixed(p, s), t) if intTypeToFixed.contains(t) =>
              b.makeCopy(Array(b.left, Cast(b.right, intTypeToFixed(t))))
            case (t, DecimalType.Fixed(p, s)) if isFloat(t) =>
              b.makeCopy(Array(b.left, Cast(b.right, DoubleType)))
            case (DecimalType.Fixed(p, s), t) if isFloat(t) =>
              b.makeCopy(Array(Cast(b.left, DoubleType), b.right))
            case _ =>
              b
          }

        // TODO: MaxOf, MinOf, etc might want other rules

        // SUM and AVERAGE are handled by the implementations of those expressions
      }
    }

  }

  /**
   * Changes Boolean values to Bytes so that expressions like true < false can be Evaluated.
   */
  object BooleanComparisons extends Rule[LogicalPlan] {
    val trueValues = Seq(1, 1L, 1.toByte, 1.toShort, new java.math.BigDecimal(1)).map(Literal(_))
    val falseValues = Seq(0, 0L, 0.toByte, 0.toShort, new java.math.BigDecimal(0)).map(Literal(_))

    def apply(plan: LogicalPlan): LogicalPlan = plan transformAllExpressions {
      // Skip nodes who's children have not been resolved yet.
      case e if !e.childrenResolved => e

      // Hive treats (true = 1) as true and (false = 0) as true.
      case EqualTo(l @ BooleanType(), r) if trueValues.contains(r) => l
      case EqualTo(l, r @ BooleanType()) if trueValues.contains(l) => r
      case EqualTo(l @ BooleanType(), r) if falseValues.contains(r) => Not(l)
      case EqualTo(l, r @ BooleanType()) if falseValues.contains(l) => Not(r)

      // No need to change other EqualTo operators as that actually makes sense for boolean types.
      case e: EqualTo => e
      // No need to change the EqualNullSafe operators, too
      case e: EqualNullSafe => e
      // Otherwise turn them to Byte types so that there exists and ordering.
      case p: BinaryComparison if p.left.dataType == BooleanType &&
                                  p.right.dataType == BooleanType =>
        p.makeCopy(Array(Cast(p.left, ByteType), Cast(p.right, ByteType)))
    }
  }

  /**
   * Casts to/from [[BooleanType]] are transformed into comparisons since
   * the JVM does not consider Booleans to be numeric types.
   */
  object BooleanCasts extends Rule[LogicalPlan] {
    def apply(plan: LogicalPlan): LogicalPlan = plan transformAllExpressions {
      // Skip nodes who's children have not been resolved yet.
      case e if !e.childrenResolved => e
      // Skip if the type is boolean type already. Note that this extra cast should be removed
      // by optimizer.SimplifyCasts.
      case Cast(e, BooleanType) if e.dataType == BooleanType => e
      // DateType should be null if be cast to boolean.
      case Cast(e, BooleanType) if e.dataType == DateType => Cast(e, BooleanType)
      // If the data type is not boolean and is being cast boolean, turn it into a comparison
      // with the numeric value, i.e. x != 0. This will coerce the type into numeric type.
      case Cast(e, BooleanType) if e.dataType != BooleanType => Not(EqualTo(e, Literal(0)))
      // Stringify boolean if casting to StringType.
      // TODO Ensure true/false string letter casing is consistent with Hive in all cases.
      case Cast(e, StringType) if e.dataType == BooleanType =>
        If(e, Literal("true"), Literal("false"))
      // Turn true into 1, and false into 0 if casting boolean into other types.
      case Cast(e, dataType) if e.dataType == BooleanType =>
        Cast(If(e, Literal(1), Literal(0)), dataType)
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
          (a, b) => findTightestCommonType(a, b).getOrElse(StringType))
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
        val dt: Option[DataType] = Some(NullType)
        val types = es.map(_.dataType)
        val rt = types.foldLeft(dt)((r, c) => r match {
          case None => None
          case Some(d) => findTightestCommonType(d, c)
        })
        rt match {
          case Some(finaldt) => Coalesce(es.map(Cast(_, finaldt)))
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
      // Skip nodes who's children have not been resolved yet.
      case e if !e.childrenResolved => e

      // Decimal and Double remain the same
      case d: Divide if d.resolved && d.dataType == DoubleType => d
      case d: Divide if d.resolved && d.dataType.isInstanceOf[DecimalType] => d

      case Divide(l, r) if l.dataType.isInstanceOf[DecimalType] =>
        Divide(l, Cast(r, DecimalType.Unlimited))
      case Divide(l, r) if r.dataType.isInstanceOf[DecimalType] =>
        Divide(Cast(l, DecimalType.Unlimited), r)

      case Divide(l, r) => Divide(Cast(l, DoubleType), Cast(r, DoubleType))
    }
  }

  /**
   * Coerces the type of different branches of a CASE WHEN statement to a common type.
   */
  object CaseWhenCoercion extends Rule[LogicalPlan] {
    import HiveTypeCoercion._

    def apply(plan: LogicalPlan): LogicalPlan = plan transformAllExpressions {
      case cw: CaseWhenLike if !cw.resolved && cw.childrenResolved && !cw.valueTypesEqual =>
        logDebug(s"Input values for null casting ${cw.valueTypes.mkString(",")}")
        val commonType = cw.valueTypes.reduce { (v1, v2) =>
          findTightestCommonType(v1, v2).getOrElse(sys.error(
            s"Types in CASE WHEN must be the same or coercible to a common type: $v1 != $v2"))
        }
        val transformedBranches = cw.branches.sliding(2, 2).map {
          case Seq(when, value) if value.dataType != commonType =>
            Seq(when, Cast(value, commonType))
          case Seq(elseVal) if elseVal.dataType != commonType =>
            Seq(Cast(elseVal, commonType))
          case s => s
        }.reduce(_ ++ _)
        cw match {
          case _: CaseWhen =>
            CaseWhen(transformedBranches)
          case CaseKeyWhen(key, _) =>
            CaseKeyWhen(key, transformedBranches)
        }
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
