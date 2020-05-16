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

package org.apache.spark.sql.catalyst

import java.sql.{Date, Timestamp}

import scala.language.implicitConversions

import org.apache.spark.api.java.function.FilterFunction
import org.apache.spark.sql.Encoder
import org.apache.spark.sql.catalyst.analysis._
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.expressions.aggregate._
import org.apache.spark.sql.catalyst.expressions.objects.Invoke
import org.apache.spark.sql.catalyst.plans.{Inner, JoinType}
import org.apache.spark.sql.catalyst.plans.logical._
import org.apache.spark.sql.types._

/**
 * A collection of implicit conversions that create a DSL for constructing catalyst data structures.
 *
 * {{{
 *  scala> import org.apache.spark.sql.catalyst.dsl.expressions._
 *
 *  // Standard operators are added to expressions.
 *  scala> import org.apache.spark.sql.catalyst.expressions.Literal
 *  scala> Literal(1) + Literal(1)
 *  res0: org.apache.spark.sql.catalyst.expressions.Add = (1 + 1)
 *
 *  // There is a conversion from 'symbols to unresolved attributes.
 *  scala> 'a.attr
 *  res1: org.apache.spark.sql.catalyst.analysis.UnresolvedAttribute = 'a
 *
 *  // These unresolved attributes can be used to create more complicated expressions.
 *  scala> 'a === 'b
 *  res2: org.apache.spark.sql.catalyst.expressions.EqualTo = ('a = 'b)
 *
 *  // SQL verbs can be used to construct logical query plans.
 *  scala> import org.apache.spark.sql.catalyst.plans.logical._
 *  scala> import org.apache.spark.sql.catalyst.dsl.plans._
 *  scala> LocalRelation('key.int, 'value.string).where('key === 1).select('value).analyze
 *  res3: org.apache.spark.sql.catalyst.plans.logical.LogicalPlan =
 *  Project [value#3]
 *   Filter (key#2 = 1)
 *    LocalRelation [key#2,value#3], []
 * }}}
 */
package object dsl {
  trait ImplicitOperators {
    def expr: Expression

    def unary_- : Expression = UnaryMinus(expr)
    def unary_! : Predicate = Not(expr)
    def unary_~ : Expression = BitwiseNot(expr)

    def + (other: Expression): Expression = Add(expr, other)
    def - (other: Expression): Expression = Subtract(expr, other)
    def * (other: Expression): Expression = Multiply(expr, other)
    def / (other: Expression): Expression = Divide(expr, other)
    def % (other: Expression): Expression = Remainder(expr, other)
    def & (other: Expression): Expression = BitwiseAnd(expr, other)
    def | (other: Expression): Expression = BitwiseOr(expr, other)
    def ^ (other: Expression): Expression = BitwiseXor(expr, other)

    def && (other: Expression): Predicate = And(expr, other)
    def || (other: Expression): Predicate = Or(expr, other)

    def < (other: Expression): Predicate = LessThan(expr, other)
    def <= (other: Expression): Predicate = LessThanOrEqual(expr, other)
    def > (other: Expression): Predicate = GreaterThan(expr, other)
    def >= (other: Expression): Predicate = GreaterThanOrEqual(expr, other)
    def === (other: Expression): Predicate = EqualTo(expr, other)
    def <=> (other: Expression): Predicate = EqualNullSafe(expr, other)
    def =!= (other: Expression): Predicate = Not(EqualTo(expr, other))

    def in(list: Expression*): Expression = list match {
      case Seq(l: ListQuery) => expr match {
          case c: CreateNamedStruct => InSubquery(c.valExprs, l)
          case other => InSubquery(Seq(other), l)
        }
      case _ => In(expr, list)
    }

    def like(other: Expression): Expression = Like(expr, other)
    def rlike(other: Expression): Expression = RLike(expr, other)
    def contains(other: Expression): Expression = Contains(expr, other)
    def startsWith(other: Expression): Expression = StartsWith(expr, other)
    def endsWith(other: Expression): Expression = EndsWith(expr, other)
    def substr(pos: Expression, len: Expression = Literal(Int.MaxValue)): Expression =
      Substring(expr, pos, len)
    def substring(pos: Expression, len: Expression = Literal(Int.MaxValue)): Expression =
      Substring(expr, pos, len)

    def isNull: Predicate = IsNull(expr)
    def isNotNull: Predicate = IsNotNull(expr)

    def getItem(ordinal: Expression): UnresolvedExtractValue = UnresolvedExtractValue(expr, ordinal)
    def getField(fieldName: String): UnresolvedExtractValue =
      UnresolvedExtractValue(expr, Literal(fieldName))

    def cast(to: DataType): Expression = Cast(expr, to)

    def asc: SortOrder = SortOrder(expr, Ascending)
    def asc_nullsLast: SortOrder = SortOrder(expr, Ascending, NullsLast, Set.empty)
    def desc: SortOrder = SortOrder(expr, Descending)
    def desc_nullsFirst: SortOrder = SortOrder(expr, Descending, NullsFirst, Set.empty)
    def as(alias: String): NamedExpression = Alias(expr, alias)()
    def as(alias: Symbol): NamedExpression = Alias(expr, alias.name)()
  }

  trait ExpressionConversions {
    implicit class DslExpression(e: Expression) extends ImplicitOperators {
      def expr: Expression = e
    }

    implicit def booleanToLiteral(b: Boolean): Literal = Literal(b)
    implicit def byteToLiteral(b: Byte): Literal = Literal(b)
    implicit def shortToLiteral(s: Short): Literal = Literal(s)
    implicit def intToLiteral(i: Int): Literal = Literal(i)
    implicit def longToLiteral(l: Long): Literal = Literal(l)
    implicit def floatToLiteral(f: Float): Literal = Literal(f)
    implicit def doubleToLiteral(d: Double): Literal = Literal(d)
    implicit def stringToLiteral(s: String): Literal = Literal(s)
    implicit def dateToLiteral(d: Date): Literal = Literal(d)
    implicit def bigDecimalToLiteral(d: BigDecimal): Literal = Literal(d.underlying())
    implicit def bigDecimalToLiteral(d: java.math.BigDecimal): Literal = Literal(d)
    implicit def decimalToLiteral(d: Decimal): Literal = Literal(d)
    implicit def timestampToLiteral(t: Timestamp): Literal = Literal(t)
    implicit def binaryToLiteral(a: Array[Byte]): Literal = Literal(a)

    implicit def symbolToUnresolvedAttribute(s: Symbol): analysis.UnresolvedAttribute =
      analysis.UnresolvedAttribute(s.name)

    /** Converts $"col name" into an [[analysis.UnresolvedAttribute]]. */
    implicit class StringToAttributeConversionHelper(val sc: StringContext) {
      // Note that if we make ExpressionConversions an object rather than a trait, we can
      // then make this a value class to avoid the small penalty of runtime instantiation.
      def $(args: Any*): analysis.UnresolvedAttribute = {
        analysis.UnresolvedAttribute(sc.s(args : _*))
      }
    }

    def rand(e: Long): Expression = Rand(e)
    def sum(e: Expression): Expression = Sum(e).toAggregateExpression()
    def sumDistinct(e: Expression): Expression = Sum(e).toAggregateExpression(isDistinct = true)
    def count(e: Expression): Expression = Count(e).toAggregateExpression()
    def countDistinct(e: Expression*): Expression =
      Count(e).toAggregateExpression(isDistinct = true)
    def approxCountDistinct(e: Expression, rsd: Double = 0.05): Expression =
      HyperLogLogPlusPlus(e, rsd).toAggregateExpression()
    def avg(e: Expression): Expression = Average(e).toAggregateExpression()
    def first(e: Expression): Expression = new First(e).toAggregateExpression()
    def last(e: Expression): Expression = new Last(e).toAggregateExpression()
    def min(e: Expression): Expression = Min(e).toAggregateExpression()
    def minDistinct(e: Expression): Expression = Min(e).toAggregateExpression(isDistinct = true)
    def max(e: Expression): Expression = Max(e).toAggregateExpression()
    def maxDistinct(e: Expression): Expression = Max(e).toAggregateExpression(isDistinct = true)
    def upper(e: Expression): Expression = Upper(e)
    def lower(e: Expression): Expression = Lower(e)
    def coalesce(args: Expression*): Expression = Coalesce(args)
    def greatest(args: Expression*): Expression = Greatest(args)
    def least(args: Expression*): Expression = Least(args)
    def sqrt(e: Expression): Expression = Sqrt(e)
    def abs(e: Expression): Expression = Abs(e)
    def star(names: String*): Expression = names match {
      case Seq() => UnresolvedStar(None)
      case target => UnresolvedStar(Option(target))
    }
    def namedStruct(e: Expression*): Expression = CreateNamedStruct(e)

    def callFunction[T, U](
        func: T => U,
        returnType: DataType,
        argument: Expression): Expression = {
      val function = Literal.create(func, ObjectType(classOf[T => U]))
      Invoke(function, "apply", returnType, argument :: Nil)
    }

    def windowSpec(
        partitionSpec: Seq[Expression],
        orderSpec: Seq[SortOrder],
        frame: WindowFrame): WindowSpecDefinition =
      WindowSpecDefinition(partitionSpec, orderSpec, frame)

    def windowExpr(windowFunc: Expression, windowSpec: WindowSpecDefinition): WindowExpression =
      WindowExpression(windowFunc, windowSpec)

    implicit class DslSymbol(sym: Symbol) extends ImplicitAttribute { def s: String = sym.name }
    // TODO more implicit class for literal?
    implicit class DslString(val s: String) extends ImplicitOperators {
      override def expr: Expression = Literal(s)
      def attr: UnresolvedAttribute = analysis.UnresolvedAttribute(s)
    }

    abstract class ImplicitAttribute extends ImplicitOperators {
      def s: String
      def expr: UnresolvedAttribute = attr
      def attr: UnresolvedAttribute = analysis.UnresolvedAttribute(s)

      /** Creates a new AttributeReference of type boolean */
      def boolean: AttributeReference = AttributeReference(s, BooleanType, nullable = true)()

      /** Creates a new AttributeReference of type byte */
      def byte: AttributeReference = AttributeReference(s, ByteType, nullable = true)()

      /** Creates a new AttributeReference of type short */
      def short: AttributeReference = AttributeReference(s, ShortType, nullable = true)()

      /** Creates a new AttributeReference of type int */
      def int: AttributeReference = AttributeReference(s, IntegerType, nullable = true)()

      /** Creates a new AttributeReference of type long */
      def long: AttributeReference = AttributeReference(s, LongType, nullable = true)()

      /** Creates a new AttributeReference of type float */
      def float: AttributeReference = AttributeReference(s, FloatType, nullable = true)()

      /** Creates a new AttributeReference of type double */
      def double: AttributeReference = AttributeReference(s, DoubleType, nullable = true)()

      /** Creates a new AttributeReference of type string */
      def string: AttributeReference = AttributeReference(s, StringType, nullable = true)()

      /** Creates a new AttributeReference of type date */
      def date: AttributeReference = AttributeReference(s, DateType, nullable = true)()

      /** Creates a new AttributeReference of type decimal */
      def decimal: AttributeReference =
        AttributeReference(s, DecimalType.SYSTEM_DEFAULT, nullable = true)()

      /** Creates a new AttributeReference of type decimal */
      def decimal(precision: Int, scale: Int): AttributeReference =
        AttributeReference(s, DecimalType(precision, scale), nullable = true)()

      /** Creates a new AttributeReference of type timestamp */
      def timestamp: AttributeReference = AttributeReference(s, TimestampType, nullable = true)()

      /** Creates a new AttributeReference of type binary */
      def binary: AttributeReference = AttributeReference(s, BinaryType, nullable = true)()

      /** Creates a new AttributeReference of type array */
      def array(dataType: DataType): AttributeReference =
        AttributeReference(s, ArrayType(dataType), nullable = true)()

      def array(arrayType: ArrayType): AttributeReference =
        AttributeReference(s, arrayType)()

      /** Creates a new AttributeReference of type map */
      def map(keyType: DataType, valueType: DataType): AttributeReference =
        map(MapType(keyType, valueType))

      def map(mapType: MapType): AttributeReference =
        AttributeReference(s, mapType, nullable = true)()

      /** Creates a new AttributeReference of type struct */
      def struct(structType: StructType): AttributeReference =
        AttributeReference(s, structType, nullable = true)()
      def struct(attrs: AttributeReference*): AttributeReference =
        struct(StructType.fromAttributes(attrs))

      /** Creates a new AttributeReference of object type */
      def obj(cls: Class[_]): AttributeReference =
        AttributeReference(s, ObjectType(cls), nullable = true)()

      /** Create a function. */
      def function(exprs: Expression*): UnresolvedFunction =
        UnresolvedFunction(s, exprs, isDistinct = false)
      def distinctFunction(exprs: Expression*): UnresolvedFunction =
        UnresolvedFunction(s, exprs, isDistinct = true)
    }

    implicit class DslAttribute(a: AttributeReference) {
      def notNull: AttributeReference = a.withNullability(false)
      def canBeNull: AttributeReference = a.withNullability(true)
      def at(ordinal: Int): BoundReference = BoundReference(ordinal, a.dataType, a.nullable)
    }
  }

  object expressions extends ExpressionConversions  // scalastyle:ignore

  object plans {  // scalastyle:ignore
    def table(ref: String): LogicalPlan = UnresolvedRelation(TableIdentifier(ref))

    def table(db: String, ref: String): LogicalPlan =
      UnresolvedRelation(TableIdentifier(ref, Option(db)))

    implicit class DslLogicalPlan(val logicalPlan: LogicalPlan) {
      def select(exprs: Expression*): LogicalPlan = {
        val namedExpressions = exprs.map {
          case e: NamedExpression => e
          case e => UnresolvedAlias(e)
        }
        Project(namedExpressions, logicalPlan)
      }

      def where(condition: Expression): LogicalPlan = Filter(condition, logicalPlan)

      def filter[T : Encoder](func: T => Boolean): LogicalPlan = TypedFilter(func, logicalPlan)

      def filter[T : Encoder](func: FilterFunction[T]): LogicalPlan = TypedFilter(func, logicalPlan)

      def serialize[T : Encoder]: LogicalPlan = CatalystSerde.serialize[T](logicalPlan)

      def deserialize[T : Encoder]: LogicalPlan = CatalystSerde.deserialize[T](logicalPlan)

      def limit(limitExpr: Expression): LogicalPlan = Limit(limitExpr, logicalPlan)

      def join(
        otherPlan: LogicalPlan,
        joinType: JoinType = Inner,
        condition: Option[Expression] = None): LogicalPlan =
        Join(logicalPlan, otherPlan, joinType, condition)

      def cogroup[Key: Encoder, Left: Encoder, Right: Encoder, Result: Encoder](
          otherPlan: LogicalPlan,
          func: (Key, Iterator[Left], Iterator[Right]) => TraversableOnce[Result],
          leftGroup: Seq[Attribute],
          rightGroup: Seq[Attribute],
          leftAttr: Seq[Attribute],
          rightAttr: Seq[Attribute]
        ): LogicalPlan = {
        CoGroup.apply[Key, Left, Right, Result](
          func,
          leftGroup,
          rightGroup,
          leftAttr,
          rightAttr,
          logicalPlan,
          otherPlan)
      }

      def orderBy(sortExprs: SortOrder*): LogicalPlan = Sort(sortExprs, true, logicalPlan)

      def sortBy(sortExprs: SortOrder*): LogicalPlan = Sort(sortExprs, false, logicalPlan)

      def groupBy(groupingExprs: Expression*)(aggregateExprs: Expression*): LogicalPlan = {
        val aliasedExprs = aggregateExprs.map {
          case ne: NamedExpression => ne
          case e => Alias(e, e.toString)()
        }
        Aggregate(groupingExprs, aliasedExprs, logicalPlan)
      }

      def having(
          groupingExprs: Expression*)(
          aggregateExprs: Expression*)(
          havingCondition: Expression): LogicalPlan = {
        UnresolvedHaving(havingCondition,
          groupBy(groupingExprs: _*)(aggregateExprs: _*).asInstanceOf[Aggregate])
      }

      def window(
          windowExpressions: Seq[NamedExpression],
          partitionSpec: Seq[Expression],
          orderSpec: Seq[SortOrder]): LogicalPlan =
        Window(windowExpressions, partitionSpec, orderSpec, logicalPlan)

      def subquery(alias: Symbol): LogicalPlan = SubqueryAlias(alias.name, logicalPlan)

      def except(otherPlan: LogicalPlan, isAll: Boolean): LogicalPlan =
        Except(logicalPlan, otherPlan, isAll)

      def intersect(otherPlan: LogicalPlan, isAll: Boolean): LogicalPlan =
        Intersect(logicalPlan, otherPlan, isAll)

      def union(otherPlan: LogicalPlan): LogicalPlan = Union(logicalPlan, otherPlan)

      def generate(
        generator: Generator,
        unrequiredChildIndex: Seq[Int] = Nil,
        outer: Boolean = false,
        alias: Option[String] = None,
        outputNames: Seq[String] = Nil): LogicalPlan =
        Generate(generator, unrequiredChildIndex, outer,
          alias, outputNames.map(UnresolvedAttribute(_)), logicalPlan)

      def insertInto(tableName: String, overwrite: Boolean = false): LogicalPlan =
        InsertIntoTable(
          analysis.UnresolvedRelation(TableIdentifier(tableName)),
          Map.empty, logicalPlan, overwrite, ifPartitionNotExists = false)

      def as(alias: String): LogicalPlan = SubqueryAlias(alias, logicalPlan)

      def coalesce(num: Integer): LogicalPlan =
        Repartition(num, shuffle = false, logicalPlan)

      def repartition(num: Integer): LogicalPlan =
        Repartition(num, shuffle = true, logicalPlan)

      def distribute(exprs: Expression*)(n: Int): LogicalPlan =
        RepartitionByExpression(exprs, logicalPlan, numPartitions = n)

      def analyze: LogicalPlan =
        EliminateSubqueryAliases(analysis.SimpleAnalyzer.execute(logicalPlan))

      def hint(name: String, parameters: Any*): LogicalPlan =
        UnresolvedHint(name, parameters, logicalPlan)
    }
  }
}
