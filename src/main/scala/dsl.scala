package catalyst

import expressions._
import plans._
import plans.logical._
import types._

/**
 * A collection of implicit conversions that create a DSL for easily constructing catalyst data structures.
 *
 * {{{
 *  scala> import catalyst.dsl._
 *
 *  // Standard operators are added to expressions.
 *  scala> Literal(1) + Literal(1)
 *  res1: catalyst.expressions.Add = (1 + 1)
 *
 *  // There is a conversion from 'symbols to unresolved attributes.
 *  scala> 'a.attr
 *  res2: catalyst.analysis.UnresolvedAttribute = 'a
 *
 *  // These unresolved attributes can be used to create more complicated expressions.
 *  scala> 'a === 'b
 *  res3: catalyst.expressions.Equals = ('a = 'b)
 *
 *  // SQL verbs can be used to construct logical query plans.
 *  scala> TestRelation('key.int, 'value.string).where('key === 1).select('value).analyze
 *  res4: catalyst.plans.logical.LogicalPlan =
 *  Project {value#1}
 *   Filter (key#0 = 1)
 *    TestRelation {key#0,value#1}
 * }}}
 */
package object dsl {
  /** A class for creating new typed attributes.  Primarily for the creation of fake "test" relations. */
  implicit class DslAttribute(s: Symbol) {
    def int = AttributeReference(s.name, IntegerType, false)()
    def string = AttributeReference(s.name, StringType, false)()
  }

  abstract protected trait ImplicitOperators {
    def expr: Expression

    def +(other: Expression) = Add(expr, other)

    def <(other: Expression) = LessThan(expr, other)
    def <=(other: Expression) = LessThanOrEqual(expr, other)
    def >(other: Expression) = GreaterThan(expr, other)
    def >=(other: Expression) = GreaterThanOrEqual(expr, other)
    def ===(other: Expression) = Equals(expr, other)
  }

  implicit class DslExpression(e: Expression) extends ImplicitOperators {
    def expr = e
  }

  implicit def intToLiteral(i: Int) = Literal(i)
  implicit def longToLiteral(l: Long) = Literal(l)
  implicit def floatToLiteral(f: Float) = Literal(f)
  implicit def doubleToLiteral(d: Double) = Literal(d)
  implicit def stringToLiteral(s: String) = Literal(s)

  implicit def symbolToUnresolvedAttribute(s: Symbol) = analysis.UnresolvedAttribute(s.name)

  implicit class DslSymbol(s: Symbol) extends ImplicitOperators {
    def expr = attr
    def attr = analysis.UnresolvedAttribute(s.name)
  }

  implicit class DslLogicalPlan(plan: LogicalPlan) {
    def select(exprs: NamedExpression*) = Project(exprs, plan)
    def where(condition: Expression) = Filter(condition, plan)
    def join(otherPlan: LogicalPlan, joinType: JoinType = Inner, condition: Option[Expression] = None) =
      Join(plan, otherPlan, joinType, condition)
    def orderBy(sortExprs: SortOrder*) = Sort(sortExprs, plan)
    def analyze = analysis.SimpleAnalyzer(plan)
  }

  implicit class toSortOrder(e: Expression) {
    def asc = SortOrder(e, Ascending)
    def desc = SortOrder(e, Descending)
  }
}