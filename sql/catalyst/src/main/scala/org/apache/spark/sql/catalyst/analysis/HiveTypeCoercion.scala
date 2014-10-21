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
import org.apache.spark.sql.catalyst.types._

object HiveTypeCoercion {
  // See https://cwiki.apache.org/confluence/display/Hive/LanguageManual+Types.
  // The conversion for integral and floating point types have a linear widening hierarchy:
  private val numericPrecedence =
    Seq(ByteType, ShortType, IntegerType, LongType, FloatType, DoubleType, DecimalType.Unlimited)

  /**
   * Find the tightest common type of two types that might be used in a binary expression.
   * This handles all numeric types except fixed-precision decimals interacting with each other or
   * with primitive types, because in that case the precision and scale of the result depends on
   * the operation. Those rules are implemented in [[HiveTypeCoercion.DecimalPrecision]].
   */
  def findTightestCommonType(t1: DataType, t2: DataType): Option[DataType] = {
    val valueTypes = Seq(t1, t2).filter(t => t != NullType)
    if (valueTypes.distinct.size > 1) {
      // Promote numeric types to the highest of the two and all numeric types to unlimited decimal
      if (numericPrecedence.contains(t1) && numericPrecedence.contains(t2)) {
        Some(numericPrecedence.filter(t => t == t1 || t == t2).last)
      } else if (t1.isInstanceOf[DecimalType] && t2.isInstanceOf[DecimalType]) {
        // Fixed-precision decimals can up-cast into unlimited
        if (t1 == DecimalType.Unlimited || t2 == DecimalType.Unlimited) {
          Some(DecimalType.Unlimited)
        } else {
          None
        }
      } else {
        None
      }
    } else {
      Some(if (valueTypes.size == 0) NullType else valueTypes.head)
    }
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
    WidenTypes ::
    PromoteStrings ::
    DecimalPrecision ::
    BooleanComparisons ::
    BooleanCasts ::
    StringToIntegralCasts ::
    FunctionArgumentConversion ::
    CaseWhenCoercion ::
    Division ::
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
    val StringNaN = Literal("NaN", StringType)

    def apply(plan: LogicalPlan): LogicalPlan = plan transform {
      case q: LogicalPlan => q transformExpressions {
        // Skip nodes who's children have not been resolved yet.
        case e if !e.childrenResolved => e

        /* Double Conversions */
        case b @ BinaryExpression(StringNaN, DoubleType(r)) =>
          b.makeCopy(Array(r, Literal(Double.NaN)))
        case b @ BinaryExpression(DoubleType(l), StringNaN) =>
          b.makeCopy(Array(Literal(Double.NaN), l))

        /* Float Conversions */
        case b @ BinaryExpression(StringNaN, FloatType(r)) =>
          b.makeCopy(Array(r, Literal(Float.NaN)))
        case b @ BinaryExpression(FloatType(l), StringNaN) =>
          b.makeCopy(Array(Literal(Float.NaN), l))

        /* Use float NaN by default to avoid unnecessary type widening */
        case b @ BinaryExpression(l @ StringNaN, StringNaN) =>
          b.makeCopy(Array(Literal(Float.NaN), l))
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
          case (StringType(l), r) if r.dataType != StringType =>
            (l, Alias(Cast(r, StringType), r.name)())
          case (l, StringType(r)) if l.dataType != StringType =>
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

        case b @ BinaryExpression(l, r) if l.dataType != r.dataType =>
          findTightestCommonType(l.dataType, r.dataType).map { widestType =>
            val newLeft =
              if (l.dataType == widestType) l else Cast(l, widestType)
            val newRight =
              if (r.dataType == widestType) r else Cast(r, widestType)
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

      case a @ BinaryArithmetic(StringType(l), r) =>
        a.makeCopy(Array(Cast(l, DoubleType), r))
      case a @ BinaryArithmetic(l, StringType(r)) =>
        a.makeCopy(Array(l, Cast(r, DoubleType)))

      // we should cast all timestamp/date/string compare into string compare
      case p @ BinaryPredicate(StringType(l), DateType(r)) =>
        p.makeCopy(Array(l, Cast(r, StringType)))
      case p @ BinaryPredicate(DateType(l), StringType(r)) =>
        p.makeCopy(Array(Cast(l, StringType), r))
      case p @ BinaryPredicate(TimestampType(l), DateType(r)) =>
        p.makeCopy(Array(Cast(l, StringType), Cast(r, StringType)))
      case p @ BinaryPredicate(DateType(l), TimestampType(r)) =>
        p.makeCopy(Array(Cast(l, StringType), Cast(r, StringType)))
      case p @ BinaryPredicate(StringType(l), TimestampType(r)) =>
        p.makeCopy(Array(Cast(l, TimestampType), r))
      case p @ BinaryPredicate(TimestampType(l), StringType(r)) =>
        p.makeCopy(Array(l, Cast(r, TimestampType)))

      case p @ BinaryPredicate(StringType(l), r) if r.dataType != StringType =>
        p.makeCopy(Array(Cast(l, DoubleType), r))
      case p @ BinaryPredicate(l, StringType(r)) if l.dataType != StringType =>
        p.makeCopy(Array(l, Cast(r, DoubleType)))

      case i @ In(DateType(a), b) if b.forall(_.dataType == StringType) =>
        i.makeCopy(Array(Cast(a, StringType), b))
      case i @ In(TimestampType(a), b) if b.forall(_.dataType == StringType) =>
        i.makeCopy(Array(Cast(a, StringType), b))
      case i @ In(DateType(a), b) if b.forall(_.dataType == TimestampType) =>
        i.makeCopy(Array(Cast(a, StringType), b.map(Cast(_, StringType))))
      case i @ In(TimestampType(a), b) if b.forall(_.dataType == DateType) =>
        i.makeCopy(Array(Cast(a, StringType), b.map(Cast(_, StringType))))
      case i @ In(DateType(a), b) if b.forall(_.dataType == StringType) =>
        i.makeCopy(Array(Cast(a, StringType), b))
      case i @ In(TimestampType(a), b) if b.forall(_.dataType == StringType) =>
        i.makeCopy(Array(a, b.map(Cast(_,TimestampType))))
      case i @ In(DateType(a), b) if b.forall(_.dataType == TimestampType) =>
        i.makeCopy(Array(Cast(a, StringType), b.map(Cast(_, StringType))))
      case i @ In(TimestampType(a), b) if b.forall(_.dataType == DateType) =>
        i.makeCopy(Array(Cast(a, StringType), b.map(Cast(_, StringType))))

      case Sum(StringType(e)) => Sum(Cast(e, DoubleType))
      case Average(StringType(e)) => Average(Cast(e, DoubleType))
      case Sqrt(StringType(e)) => Sqrt(Cast(e, DoubleType))
    }
  }

  // scalastyle:off
  /**
   * Calculates and propagates precision for fixed-precision decimals. Hive has a number of
   * rules for this based on the SQL standard and MS SQL:
   * https://cwiki.apache.org/confluence/download/attachments/27362075/Hive_Decimal_Precision_Scale_Support.pdf
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
   * - FLOAT and DOUBLE cause fixed-length decimals to turn into DOUBLE (this is the same as Hive,
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

    def apply(plan: LogicalPlan): LogicalPlan = plan transformAllExpressions {
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

  /**
   * Changes Boolean values to Bytes so that expressions like true < false can be Evaluated.
   */
  object BooleanComparisons extends Rule[LogicalPlan] {
    val trueValues = Seq(1, 1L, 1.toByte, 1.toShort, BigDecimal(1)).map(Literal(_))
    val falseValues = Seq(0, 0L, 0.toByte, 0.toShort, BigDecimal(0)).map(Literal(_))

    def apply(plan: LogicalPlan): LogicalPlan = plan transformAllExpressions {
      // Skip nodes who's children have not been resolved yet.
      case e if !e.childrenResolved => e

      // Hive treats (true = 1) as true and (false = 0) as true.
      case EqualTo(BooleanType(l), r) if trueValues.contains(r) => l
      case EqualTo(l, BooleanType(r)) if trueValues.contains(l) => r
      case EqualTo(BooleanType(l), r) if falseValues.contains(r) => Not(l)
      case EqualTo(l, BooleanType(r)) if falseValues.contains(l) => Not(r)

      // No need to change other EqualTo operators as that actually makes sense for boolean types.
      case e: EqualTo => e
      // No need to change the EqualNullSafe operators, too
      case e: EqualNullSafe => e
      // Otherwise turn them to Byte types so that there exists and ordering.
      case p @ BinaryComparison(BooleanType(l), BooleanType(r)) =>
        p.makeCopy(Array(Cast(l, ByteType), Cast(r, ByteType)))
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
      case Cast(BooleanType(e), BooleanType) => e
      // DateType should be null if be cast to boolean.
      case Cast(DateType(e), BooleanType) => Cast(e, BooleanType)
      // If the data type is not boolean and is being cast boolean, turn it into a comparison
      // with the numeric value, i.e. x != 0. This will coerce the type into numeric type.
      case Cast(e, BooleanType) if e.dataType != BooleanType => Not(EqualTo(e, Literal(0)))
      // Stringify boolean if casting to StringType.
      // TODO Ensure true/false string letter casing is consistent with Hive in all cases.
      case Cast(BooleanType(e), StringType) =>
        If(e, Literal("true"), Literal("false"))
      // Turn true into 1, and false into 0 if casting boolean into other types.
      case Cast(BooleanType(e), dataType) =>
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

      case Cast(StringType(e), t: IntegralType) =>
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
          (a,b) =>
            findTightestCommonType(a,b).getOrElse(StringType))
        CreateArray(
          children.map(c => if (c.dataType == commonType) c else Cast(c, commonType)))

      // Promote SUM, SUM DISTINCT and AVERAGE to largest types to prevent overflows.
      case s @ Sum(DecimalType(e)) => s // Decimal is already the biggest.
      case Sum(IntegralType(e)) if e.dataType != LongType => Sum(Cast(e, LongType))
      case Sum(FractionalType(e)) if e.dataType != DoubleType => Sum(Cast(e, DoubleType))

      case s @ SumDistinct(DecimalType(e)) => s // Decimal is already the biggest.
      case SumDistinct(IntegralType(e)) if e.dataType != LongType =>
        SumDistinct(Cast(e, LongType))
      case SumDistinct(FractionalType(e)) if e.dataType != DoubleType =>
        SumDistinct(Cast(e, DoubleType))

      case s @ Average(DecimalType(e)) => s // Decimal is already the biggest.
      case Average(IntegralType(e)) if e.dataType != LongType =>
        Average(Cast(e, LongType))
      case Average(FractionalType(e)) if e.dataType != DoubleType =>
        Average(Cast(e, DoubleType))

      // Hive lets you do aggregation of timestamps... for some reason
      case Sum(e @ TimestampType()) => Sum(Cast(e, DoubleType))
      case Average(e @ TimestampType()) => Average(Cast(e, DoubleType))
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

      case Divide(DecimalType(l), r) => Divide(l, Cast(r, DecimalType.Unlimited))
      case Divide(l, DecimalType(r)) => Divide(Cast(l, DecimalType.Unlimited), r)

      case Divide(l, r) => Divide(Cast(l, DoubleType), Cast(r, DoubleType))
    }
  }

  /**
   * Coerces the type of different branches of a CASE WHEN statement to a common type.
   */
  object CaseWhenCoercion extends Rule[LogicalPlan] {
    import HiveTypeCoercion._

    def apply(plan: LogicalPlan): LogicalPlan = plan transformAllExpressions {
      case cw @ CaseWhen(branches) if !cw.resolved && branches.forall(_.resolved)  =>
        val valueTypes = branches.sliding(2, 2).map {
          case Seq(_, value) => value.dataType
          case Seq(elseVal) => elseVal.dataType
        }.toSeq

        logDebug(s"Input values for null casting ${valueTypes.mkString(",")}")

        if (valueTypes.distinct.size > 1) {
          val commonType = valueTypes.reduce { (v1, v2) =>
            findTightestCommonType(v1, v2)
              .getOrElse(sys.error(
                s"Types in CASE WHEN must be the same or coercible to a common type: $v1 != $v2"))
          }
          val transformedBranches = branches.sliding(2, 2).map {
            case Seq(cond, value) if value.dataType != commonType =>
              Seq(cond, Cast(value, commonType))
            case Seq(elseVal) if elseVal.dataType != commonType =>
              Seq(Cast(elseVal, commonType))
            case s => s
          }.reduce(_ ++ _)
          CaseWhen(transformedBranches)
        } else {
          // Types match up.  Hopefully some other rule fixes whatever is wrong with resolution.
          cw
        }
    }
  }
}
