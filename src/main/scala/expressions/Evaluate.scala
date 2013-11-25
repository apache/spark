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
        // TODO: Use Numeric instead of writing out the cases by hand here.
        case (l: Int, r: Int) => l + r
      }
      case Literal(v, _) => v
      case Alias(c, _) => eval(c)
      case Equals(l, r) => eval(l) == eval(r)
      case BoundReference(inputTuple, ordinal, _) => input(inputTuple)(ordinal)
      case UnaryMinus(child) => eval(child) match {
        // TODO: Use Numeric instead of writing out the cases by hand here.
        case i: Int => i * -1
        case l: Long => l * -1
        case d: Double => d * -1
        case f: Float => f * -1
      }
      case GreaterThan(l, r) => (eval(l), eval(r)) match {
        // TODO: Use Numeric instead of writing out the cases by hand here.
        case (l: Double, r: Double) => l > r
        case (l: Int, r: Int) => l > r
        case (l: String, r: String) => l > r
        case (l: Float, r: Float) => l > r
        case (l, r) => throw new NotImplementedError(s"no > for ${l.getClass.getName} ${r.getClass.getName}")
      }
      case Rand => scala.util.Random.nextDouble
      case other => throw new NotImplementedError(s"Evaluation for:\n $e")
    }
  }
}