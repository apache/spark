package catalyst
package expressions

import org.scalatest.FunSuite

import types._
import expressions._

import dsl._

class ExpressionEvaluationSuite extends FunSuite {

  test("literals") {
    assert(Evaluate(Literal(1) + Literal(1), Nil) === 2)
  }

  test("boolean logic") {
    val andTruthTable =
      (true, true, true) ::
      (true, false, false) ::
      (false, true, false) ::
      (false, false, false) :: Nil

    andTruthTable.foreach {
      case (l,r,answer) =>
        val result = Evaluate(Literal(l, BooleanType) && Literal(r, BooleanType), Nil)
        if(result != answer)
          fail(s"$l && $r != $result")
    }


  }
}