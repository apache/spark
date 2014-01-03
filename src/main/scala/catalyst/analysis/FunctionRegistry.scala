package catalyst
package analysis

import expressions._

/** A catalog for looking up user defined functions, used by an [[Analyzer]]. */
trait FunctionRegistry {
  def lookupFunction(name: String, children: Seq[Expression]): Expression
}

/**
 * A trivial catalog that returns an error when a function is requested.  Used for testing when all
 * functions are already filled in and the analyser needs only to resolve attribute references.
 */
object EmptyFunctionRegistry extends FunctionRegistry {
  def lookupFunction(name: String, children: Seq[Expression]): Expression = {
    throw new UnsupportedOperationException
  }
}
