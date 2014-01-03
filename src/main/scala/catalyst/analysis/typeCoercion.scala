package catalyst
package analysis

import expressions._
import plans.logical._
import rules._
import types._

/**
 * Converts string "NaN"s that are in binary operators with a NaN-able types (Float / Double) to the
 * appropriate numeric equivalent.
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
 *  - Any integral numeric type can be implicitly converted to a wider type.
 *  - All the integral numeric types, FLOAT, and (perhaps surprisingly) STRING can be implicitly
 *    converted to DOUBLE.
 *  - TINYINT, SMALLINT, and INT can all be converted to FLOAT.
 *  - BOOLEAN types cannot be converted to any other type.
 *
 * String conversions are handled by the PromoteStrings rule.
 */
object PromoteNumericTypes extends Rule[LogicalPlan] {
  val integralPrecedence = Seq(ByteType, ShortType, IntegerType, LongType)
  val toDouble = integralPrecedence ++ Seq(FloatType, DoubleType)
  val toFloat = Seq(ByteType, ShortType, IntegerType) :+ FloatType
  val allPromotions = integralPrecedence :: toDouble :: toFloat :: Nil

  def apply(plan: LogicalPlan): LogicalPlan = plan transform {
    case q: LogicalPlan => q transformExpressions {
      // Skip nodes who's children have not been resolved yet.
      case e if !e.childrenResolved => e

      case b: BinaryExpression if b.left.dataType != b.right.dataType =>
        // Try and find a promotion rule that contains both types in question.
        val applicableConversion =
          allPromotions.find(p => p.contains(b.left.dataType) && p.contains(b.right.dataType))

        applicableConversion match {
          case Some(promotionRule) =>
            val widestType =
              promotionRule.filter(t => t == b.left.dataType || t == b.right.dataType).last
            val newLeft =
              if (b.left.dataType == widestType) b.left else Cast(b.left, widestType)
            val newRight =
              if (b.right.dataType == widestType) b.right else Cast(b.right, widestType)
            b.makeCopy(Array(newLeft, newRight))

          // If there is no applicable conversion, leave expression unchanged.
          case None => b
        }
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
      Sum(Cast(e, DoubleType))
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
    case p: BinaryComparison if p.left.dataType == BooleanType && p.right.dataType == BooleanType =>
      p.makeCopy(Array(Cast(p.left, ByteType), Cast(p.right, ByteType)))
  }
}
