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

/**
 * A collection of [[catalyst.rules.Rule Rules]] that can be used to coerce differing types that
 * participate in operations into compatible ones.  Most of these rules are based on Hive semantics,
 * but they do not introduce any dependencies on the hive codebase.  For this reason they remain in
 * Catalyst until we have a more standard set of coercions.
 */
trait HiveTypeCoercion {

  val typeCoercionRules =
    PropagateTypes ::
    ConvertNaNs ::
    WidenTypes ::
    PromoteStrings ::
    BooleanComparisons ::
    BooleanCasts ::
    StringToIntegralCasts ::
    FunctionArgumentConversion ::
    CastNulls ::
    Nil

  /**
   * Applies any changes to [[catalyst.expressions.AttributeReference AttributeReference]] data
   * types that are made by other rules to instances higher in the query tree.
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
              logger.debug(s"Promoting $a to $newType in ${q.simpleString}}")
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
    val stringNaN = Literal("NaN", StringType)

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
   */
  object WidenTypes extends Rule[LogicalPlan] {
    // See https://cwiki.apache.org/confluence/display/Hive/LanguageManual+Types.
    // The conversion for integral and floating point types have a linear widening hierarchy:
    val numericPrecedence =
      Seq(NullType, ByteType, ShortType, IntegerType, LongType, FloatType, DoubleType, DecimalType)
    // Boolean is only wider than Void
    val booleanPrecedence = Seq(NullType, BooleanType)
    val allPromotions: Seq[Seq[DataType]] = numericPrecedence :: booleanPrecedence :: Nil

    def findTightestCommonType(t1: DataType, t2: DataType): Option[DataType] = {
      // Try and find a promotion rule that contains both types in question.
      val applicableConversion = allPromotions.find(p => p.contains(t1) && p.contains(t2))

      // If found return the widest common type, otherwise None
      applicableConversion.map(_.filter(t => t == t1 || t == t2).last)
    }

    def apply(plan: LogicalPlan): LogicalPlan = plan transform {
      case u @ Union(left, right) if u.childrenResolved && !u.resolved =>
        val castedInput = left.output.zip(right.output).map {
          // When a string is found on one side, make the other side a string too.
          case (l, r) if l.dataType == StringType && r.dataType != StringType =>
            (l, Alias(Cast(r, StringType), r.name)())
          case (l, r) if l.dataType != StringType && r.dataType == StringType =>
            (Alias(Cast(l, StringType), l.name)(), r)

          case (l, r) if l.dataType != r.dataType =>
            logger.debug(s"Resolving mismatched union input ${l.dataType}, ${r.dataType}")
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
            logger.debug(s"Widening numeric types in union $castedLeft ${left.output}")
            Project(castedLeft, left)
          } else {
            left
          }

        val newRight =
          if (castedRight.map(_.dataType) != right.output.map(_.dataType)) {
            logger.debug(s"Widening numeric types in union $castedRight ${right.output}")
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

      case p: BinaryPredicate if p.left.dataType == StringType && p.right.dataType != StringType =>
        p.makeCopy(Array(Cast(p.left, DoubleType), p.right))
      case p: BinaryPredicate if p.left.dataType != StringType && p.right.dataType == StringType =>
        p.makeCopy(Array(p.left, Cast(p.right, DoubleType)))

      case Sum(e) if e.dataType == StringType =>
        Sum(Cast(e, DoubleType))
      case Average(e) if e.dataType == StringType =>
        Average(Cast(e, DoubleType))
    }
  }

  /**
   * Changes Boolean values to Bytes so that expressions like true < false can be Evaluated.
   */
  object BooleanComparisons extends Rule[LogicalPlan] {
    def apply(plan: LogicalPlan): LogicalPlan = plan transformAllExpressions {
      // Skip nodes who's children have not been resolved yet.
      case e if !e.childrenResolved => e
      // No need to change Equals operators as that actually makes sense for boolean types.
      case e: Equals => e
      // Otherwise turn them to Byte types so that there exists and ordering.
      case p: BinaryComparison
          if p.left.dataType == BooleanType && p.right.dataType == BooleanType =>
        p.makeCopy(Array(Cast(p.left, ByteType), Cast(p.right, ByteType)))
    }
  }

  /**
   * Casts to/from [[catalyst.types.BooleanType BooleanType]] are transformed into comparisons since
   * the JVM does not consider Booleans to be numeric types.
   */
  object BooleanCasts extends Rule[LogicalPlan] {
    def apply(plan: LogicalPlan): LogicalPlan = plan transformAllExpressions {
      // Skip nodes who's children have not been resolved yet.
      case e if !e.childrenResolved => e

      case Cast(e, BooleanType) => Not(Equals(e, Literal(0)))
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
        Cast(Cast(e, DecimalType), t)
    }
  }

  /**
   * This ensure that the types for various functions are as expected.
   */
  object FunctionArgumentConversion extends Rule[LogicalPlan] {
    def apply(plan: LogicalPlan): LogicalPlan = plan transformAllExpressions {
      // Skip nodes who's children have not been resolved yet.
      case e if !e.childrenResolved => e

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
    }
  }

  /**
   * Ensures that NullType gets casted to some other types under certain circumstances.
   */
  object CastNulls extends Rule[LogicalPlan] {
    def apply(plan: LogicalPlan): LogicalPlan = plan transformAllExpressions {
      case cw @ CaseWhen(branches) =>
        val valueTypes = branches.sliding(2, 2).map {
          case Seq(_, value) if value.resolved => Some(value.dataType)
          case Seq(elseVal) if elseVal.resolved => Some(elseVal.dataType)
          case _ => None
        }.toSeq
        if (valueTypes.distinct.size == 2 && valueTypes.exists(_ == Some(NullType))) {
          val otherType = valueTypes.filterNot(_ == Some(NullType))(0).get
          val transformedBranches = branches.sliding(2, 2).map {
            case Seq(cond, value) if value.resolved && value.dataType == NullType =>
              Seq(cond, Cast(value, otherType))
            case Seq(elseVal) if elseVal.resolved && elseVal.dataType == NullType =>
              Seq(Cast(elseVal, otherType))
            case s => s
          }.reduce(_ ++ _)
          CaseWhen(transformedBranches)
        } else {
          // It is possible to have more types due to the possibility of short-circuiting.
          cw
        }
    }
  }

}
