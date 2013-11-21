package catalyst
package expressions

/**
 * Performs evaluation of an expression tree, given a set of input tuples.
 */
object Evaluate {
  def apply(e: Expression, input: Seq[Seq[Any]]): Any = {
    def eval(e: Expression) = Evaluate(e, input)

    e match {
      case Add(l, r) => (eval(l), eval(r)) match {
        case (l: Int, r: Int) => l + r
      }
      case Literal(v, _) => v
      case Equals(l, r) => eval(l) == eval(r)
      case BoundReference(inputTuple, ordinal, _) => input(inputTuple)(ordinal)
      case GreaterThan(l, r) => (eval(l), eval(r)) match {
        case (l: Double, r: Double) => l > r
      }
      case Rand => scala.util.Random.nextDouble
      case other => throw new NotImplementedError(s"Evaluation for:\n $e")
    }
  }
}