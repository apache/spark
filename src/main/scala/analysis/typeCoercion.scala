package catalyst
package analysis

import expressions._
import plans.logical._
import rules._
import types._

/**
 * Converts string "NaN"s that are in binary operators with a NaN-able types (Float / Double) to the appropriate numeric
 * equivalent.
 */
object ConvertNaNs extends Rule[LogicalPlan]{
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

object PromoteTypes extends Rule[LogicalPlan] {
  // TODO: Do this generically given some list of type precedence.
  def apply(plan: LogicalPlan): LogicalPlan = plan transform {
    case q: LogicalPlan => q transformExpressions {
      // Skip nodes who's children have not been resolved yet.
      case e if !e.childrenResolved => e

      // Int <op> String or String <op> Int => Int <op> Int
      case b: BinaryExpression if b.left.dataType == StringType && b.right.dataType == IntegerType =>
        b.makeCopy(Array(Cast(b.left, IntegerType), b.right))
      case b: BinaryExpression if b.left.dataType == IntegerType && b.right.dataType == StringType =>
        b.makeCopy(Array(b.left, Cast(b.right, IntegerType)))
    }
  }
}