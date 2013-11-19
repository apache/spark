package catalyst

import org.scalatest.FunSuite

import types.IntegerType
import expressions._

import dsl._

class ExpressionEvaluationSuite extends FunSuite {

  test("literals") {
    assert(Evaluate(Literal(1) + Literal(1), Nil) === 2)
  }
}