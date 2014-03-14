/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.spark.sql
package catalyst

import scala.language.implicitConversions
import scala.reflect.runtime.universe.TypeTag

import analysis.UnresolvedAttribute
import expressions._
import plans._
import plans.logical._
import types._

/**
 * Provides experimental support for generating catalyst schemas for scala objects.
 */
object ScalaReflection {
  import scala.reflect.runtime.universe._

  /** Returns a Sequence of attributes for the given case class type. */
  def attributesFor[T: TypeTag]: Seq[Attribute] = schemaFor[T] match {
    case s: StructType =>
      s.fields.map(f => AttributeReference(f.name, f.dataType, nullable = true)())
  }

  /** Returns a catalyst DataType for the given Scala Type using reflection. */
  def schemaFor[T: TypeTag]: DataType = schemaFor(typeOf[T])

  /** Returns a catalyst DataType for the given Scala Type using reflection. */
  def schemaFor(tpe: `Type`): DataType = tpe match {
    case t if t <:< typeOf[Product] =>
      val params = t.member("<init>": TermName).asMethod.paramss
      StructType(
        params.head.map(p => StructField(p.name.toString, schemaFor(p.typeSignature), true)))
    case t if t <:< typeOf[Seq[_]] =>
      val TypeRef(_, _, Seq(elementType)) = t
      ArrayType(schemaFor(elementType))
    case t if t <:< typeOf[String] => StringType
    case t if t <:< definitions.IntTpe => IntegerType
    case t if t <:< definitions.LongTpe => LongType
    case t if t <:< definitions.DoubleTpe => DoubleType
    case t if t <:< definitions.ShortTpe => ShortType
    case t if t <:< definitions.ByteTpe => ByteType
  }

  implicit class CaseClassRelation[A <: Product : TypeTag](data: Seq[A]) {

    /**
     * Implicitly added to Sequences of case class objects.  Returns a catalyst logical relation
     * for the the data in the sequence.
     */
    def asRelation: LocalRelation = {
      val output = attributesFor[A]
      LocalRelation(output, data)
    }
  }
}

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
  trait ImplicitOperators {
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

  trait ExpressionConversions {
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
  }

  object expressions extends ExpressionConversions

  abstract class LogicalPlanFunctions {
    def logicalPlan: LogicalPlan

    def select(exprs: NamedExpression*) = Project(exprs, logicalPlan)

    def where(condition: Expression) = Filter(condition, logicalPlan)

    def join(
        otherPlan: LogicalPlan,
        joinType: JoinType = Inner,
        condition: Option[Expression] = None) =
      Join(logicalPlan, otherPlan, joinType, condition)

    def orderBy(sortExprs: SortOrder*) = Sort(sortExprs, logicalPlan)

    def groupBy(groupingExprs: Expression*)(aggregateExprs: Expression*) = {
      val aliasedExprs = aggregateExprs.map {
        case ne: NamedExpression => ne
        case e => Alias(e, e.toString)()
      }
      Aggregate(groupingExprs, aliasedExprs, logicalPlan)
    }

    def subquery(alias: Symbol) = Subquery(alias.name, logicalPlan)

    def unionAll(otherPlan: LogicalPlan) = Union(logicalPlan, otherPlan)

    def sfilter[T1](arg1: Symbol)(udf: (T1) => Boolean) =
      Filter(ScalaUdf(udf, BooleanType, Seq(UnresolvedAttribute(arg1.name))), logicalPlan)

    def sfilter(dynamicUdf: (DynamicRow) => Boolean) =
      Filter(ScalaUdf(dynamicUdf, BooleanType, Seq(WrapDynamic(logicalPlan.output))), logicalPlan)

    def sample(
        fraction: Double,
        withReplacement: Boolean = true,
        seed: Int = (math.random * 1000).toInt) =
      Sample(fraction, withReplacement, seed, logicalPlan)

    def generate(
        generator: Generator,
        join: Boolean = false,
        outer: Boolean = false,
        alias: Option[String] = None) =
      Generate(generator, join, outer, None, logicalPlan)

    def insertInto(tableName: String, overwrite: Boolean = false) =
      InsertIntoTable(
        analysis.UnresolvedRelation(None, tableName), Map.empty, logicalPlan, overwrite)

    def analyze = analysis.SimpleAnalyzer(logicalPlan)
  }

  object plans {
    implicit class DslLogicalPlan(val logicalPlan: LogicalPlan) extends LogicalPlanFunctions {
      def writeToFile(path: String) = WriteToFile(path, logicalPlan)
    }
  }
}
