package org.apache.spark.sql
package catalyst

import scala.language.implicitConversions

import analysis.UnresolvedAttribute
import expressions._
import plans._
import plans.logical._
import types._

/**
 * A collection of implicit conversions that create a DSL for constructing catalyst data structures.
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
  protected trait ImplicitOperators {
    def expr: Expression

    def + (other: Expression) = Add(expr, other)
    def - (other: Expression) = Subtract(expr, other)
    def * (other: Expression) = Multiply(expr, other)
    def / (other: Expression) = Divide(expr, other)

    def && (other: Expression) = And(expr, other)
    def || (other: Expression) = Or(expr, other)

    def < (other: Expression) = LessThan(expr, other)
    def <= (other: Expression) = LessThanOrEqual(expr, other)
    def > (other: Expression) = GreaterThan(expr, other)
    def >= (other: Expression) = GreaterThanOrEqual(expr, other)
    def === (other: Expression) = Equals(expr, other)

    def asc = SortOrder(expr, Ascending)
    def desc = SortOrder(expr, Descending)

    def as(s: Symbol) = Alias(expr, s.name)()
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

  implicit class DslSymbol(sym: Symbol) extends ImplicitAttribute { def s = sym.name }
  implicit class DslString(val s: String) extends ImplicitAttribute

  abstract class ImplicitAttribute extends ImplicitOperators {
    def s: String
    def expr = attr
    def attr = analysis.UnresolvedAttribute(s)

    /** Creates a new typed attributes of type int */
    def int = AttributeReference(s, IntegerType, nullable = false)()

    /** Creates a new typed attributes of type string */
    def string = AttributeReference(s, StringType, nullable = false)()
  }

  implicit class DslAttribute(a: AttributeReference) {
    def notNull = a.withNullability(false)
    def nullable = a.withNullability(true)

    // Protobuf terminology
    def required = a.withNullability(false)
  }

  implicit class DslLogicalPlan(plan: LogicalPlan) {
    def select(exprs: NamedExpression*) = Project(exprs, plan)

    def where(condition: Expression) = Filter(condition, plan)

    def join(
        otherPlan: LogicalPlan,
        joinType: JoinType = Inner,
        condition: Option[Expression] = None) =
      Join(plan, otherPlan, joinType, condition)

    def orderBy(sortExprs: SortOrder*) = Sort(sortExprs, plan)

    def groupBy(groupingExprs: Expression*)(aggregateExprs: Expression*) = {
      val aliasedExprs = aggregateExprs.map {
        case ne: NamedExpression => ne
        case e => Alias(e, e.toString)()
      }
      Aggregate(groupingExprs, aliasedExprs, plan)
    }

    def subquery(alias: Symbol) = Subquery(alias.name, plan)

    def unionAll(otherPlan: LogicalPlan) = Union(plan, otherPlan)

    def sfilter[T1](arg1: Symbol)(udf: (T1) => Boolean) =
      Filter(ScalaUdf(udf, BooleanType, Seq(UnresolvedAttribute(arg1.name))), plan)

    def sfilter(dynamicUdf: (DynamicRow) => Boolean) =
      Filter(ScalaUdf(dynamicUdf, BooleanType, Seq(WrapDynamic(plan.output))), plan)

    def sample(
        fraction: Double,
        withReplacement: Boolean = true,
        seed: Int = (math.random * 1000).toInt) =
      Sample(fraction, withReplacement, seed, plan)

    def generate(
        generator: Generator,
        join: Boolean = false,
        outer: Boolean = false,
        alias: Option[String] = None) =
      Generate(generator, join, outer, None, plan)

    def insertInto(tableName: String, overwrite: Boolean = false) =
      InsertIntoTable(analysis.UnresolvedRelation(None, tableName), Map.empty, plan, overwrite)

    def analyze = analysis.SimpleAnalyzer(plan)
  }
}
