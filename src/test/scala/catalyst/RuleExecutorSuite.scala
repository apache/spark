package catalyst
package trees

import org.scalatest.FunSuite

import expressions._
import rules._

class RuleExecutorSuite extends FunSuite {
  object DecrementLiterals extends Rule[Expression] {
    def apply(e: Expression): Expression = e transform {
      case IntegerLiteral(i) if i > 0 => Literal(i - 1)
    }
  }

  test("only once") {
    object ApplyOnce extends RuleExecutor[Expression] {
      val batches = Batch("once", Once, DecrementLiterals) :: Nil
    }

    assert(ApplyOnce(Literal(10)) === Literal(9))
  }

  test("to fixed point") {
    object ToFixedPoint extends RuleExecutor[Expression] {
      val batches = Batch("fixedPoint", FixedPoint(100), DecrementLiterals) :: Nil
    }

    assert(ToFixedPoint(Literal(10)) === Literal(0))
  }

  test("to maxIterations") {
    object ToFixedPoint extends RuleExecutor[Expression] {
      val batches = Batch("fixedPoint", FixedPoint(10), DecrementLiterals) :: Nil
    }

    assert(ToFixedPoint(Literal(100)) === Literal(90))
  }
}