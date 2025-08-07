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
import java.time.{Duration, Instant, LocalDate, LocalDateTime, Period}

import scala.language.implicitConversions

import org.apache.spark.api.java.function.FilterFunction
import org.apache.spark.sql.Encoder
import org.apache.spark.sql.catalyst.analysis._
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.expressions.aggregate._
import org.apache.spark.sql.catalyst.expressions.objects.Invoke
import org.apache.spark.sql.catalyst.plans.{Inner, JoinType}
import org.apache.spark.sql.catalyst.plans.logical._
import org.apache.spark.sql.catalyst.trees.CurrentOrigin
import org.apache.spark.sql.catalyst.types.DataTypeUtils
import org.apache.spark.sql.catalyst.util.CollationFactory
import org.apache.spark.sql.types._
import org.apache.spark.unsafe.types.UTF8String

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
 *  scala> LocalRelation($"key".int, $"value".string).where('key === 1).select('value).analyze
 *  res3: org.apache.spark.sql.catalyst.plans.logical.LogicalPlan =
 *  Project [value#3]
 *   Filter (key#2 = 1)
 *    LocalRelation [key#2,value#3], []
 * }}}
 */
package object dsl extends SQLConfHelper {
  trait ImplicitOperators {
    def expr: Expression

    def unary_+ : Expression = UnaryPositive(expr)
    def unary_- : Expression = UnaryMinus(expr)
    def unary_! : Predicate = Not(expr)
    def unary_~ : Expression = BitwiseNot(expr)

    def + (other: Expression): Expression = Add(expr, other)
    def - (other: Expression): Expression = Subtract(expr, other)
    def * (other: Expression): Expression = Multiply(expr, other)
    def / (other: Expression): Expression = Divide(expr, other)
    def div (other: Expression): Expression = IntegralDivide(expr, other)
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

    def in(list: Expression*): Predicate = list match {
      case Seq(l: ListQuery) => expr match {
          case c: CreateNamedStruct => InSubquery(c.valExprs, l)
          case other => InSubquery(Seq(other), l)
        }
      case _ => In(expr, list)
    }

    def like(other: Expression, escapeChar: Char = '\\'): Predicate =
      Like(expr, other, escapeChar)
    def ilike(other: Expression, escapeChar: Char = '\\'): Expression =
      new ILike(expr, other, escapeChar)
    def rlike(other: Expression): Predicate = RLike(expr, other)
    def likeAll(others: Expression*): Predicate =
      LikeAll(expr, others.map(_.eval(EmptyRow).asInstanceOf[UTF8String]))
    def notLikeAll(others: Expression*): Predicate =
      NotLikeAll(expr, others.map(_.eval(EmptyRow).asInstanceOf[UTF8String]))
    def likeAny(others: Expression*): Predicate =
      LikeAny(expr, others.map(_.eval(EmptyRow).asInstanceOf[UTF8String]))
    def notLikeAny(others: Expression*): Predicate =
      NotLikeAny(expr, others.map(_.eval(EmptyRow).asInstanceOf[UTF8String]))
    def contains(other: Expression): Predicate = Contains(expr, other)
    def startsWith(other: Expression): Predicate = StartsWith(expr, other)
    def endsWith(other: Expression): Predicate = EndsWith(expr, other)
    def substr(pos: Expression, len: Expression = Literal(Int.MaxValue)): Expression =
      Substring(expr, pos, len)
    def substring(pos: Expression, len: Expression = Literal(Int.MaxValue)): Expression =
      Substring(expr, pos, len)

    def isNull: Predicate = IsNull(expr)
    def isNotNull: Predicate = IsNotNull(expr)

    def getItem(ordinal: Expression): UnresolvedExtractValue = UnresolvedExtractValue(expr, ordinal)
    def getField(fieldName: String): UnresolvedExtractValue =
      UnresolvedExtractValue(expr, Literal(fieldName))

    def cast(to: DataType): Expression = {
      if (expr.resolved && DataTypeUtils.sameType(expr.dataType, to)) {
        expr
      } else {
        val cast = Cast(expr, to)
        cast.setTagValue(Cast.USER_SPECIFIED_CAST, ())
        cast
      }
    }

    def castNullable(): Expression = {
      if (expr.resolved && expr.nullable) {
        expr
      } else {
        KnownNullable(expr)
      }
    }

    def asc: SortOrder = SortOrder(expr, Ascending)
    def asc_nullsLast: SortOrder = SortOrder(expr, Ascending, NullsLast, Seq.empty)
    def desc: SortOrder = SortOrder(expr, Descending)
    def desc_nullsFirst: SortOrder = SortOrder(expr, Descending, NullsFirst, Seq.empty)
    def as(alias: String): NamedExpression = Alias(expr, alias)()
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
    implicit def stringToLiteral(s: String): Literal = Literal.create(s, StringType)
    implicit def dateToLiteral(d: Date): Literal = Literal(d)
    implicit def localDateToLiteral(d: LocalDate): Literal = Literal(d)
    implicit def bigDecimalToLiteral(d: BigDecimal): Literal = Literal(d.underlying())
    implicit def bigDecimalToLiteral(d: java.math.BigDecimal): Literal = Literal(d)
    implicit def decimalToLiteral(d: Decimal): Literal = Literal(d)
    implicit def timestampToLiteral(t: Timestamp): Literal = Literal(t)
    implicit def timestampNTZToLiteral(l: LocalDateTime): Literal = Literal(l)
    implicit def instantToLiteral(i: Instant): Literal = Literal(i)
    implicit def binaryToLiteral(a: Array[Byte]): Literal = Literal(a)
    implicit def periodToLiteral(p: Period): Literal = Literal(p)
    implicit def durationToLiteral(d: Duration): Literal = Literal(d)

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
    def sum(e: Expression, filter: Option[Expression] = None): Expression =
      Sum(e).toAggregateExpression(isDistinct = false, filter = filter)
    def sumDistinct(e: Expression, filter: Option[Expression] = None): Expression =
      Sum(e).toAggregateExpression(isDistinct = true, filter = filter)
    def count(e: Expression, filter: Option[Expression] = None): Expression =
      Count(e).toAggregateExpression(isDistinct = false, filter = filter)
    def countDistinct(e: Expression*): Expression =
      Count(e).toAggregateExpression(isDistinct = true)
    def countDistinctWithFilter(filter: Expression, e: Expression*): Expression =
      Count(e).toAggregateExpression(isDistinct = true, filter = Some(filter))
    def approxCountDistinct(
        e: Expression,
        rsd: Double = 0.05,
        filter: Option[Expression] = None): Expression =
      HyperLogLogPlusPlus(e, rsd).toAggregateExpression(isDistinct = false, filter = filter)
    def avg(e: Expression, filter: Option[Expression] = None): Expression =
      Average(e).toAggregateExpression(isDistinct = false, filter = filter)
    def first(e: Expression, filter: Option[Expression] = None): Expression =
      new First(e).toAggregateExpression(isDistinct = false, filter = filter)
    def last(e: Expression, filter: Option[Expression] = None): Expression =
      new Last(e).toAggregateExpression(isDistinct = false, filter = filter)
    def min(e: Expression, filter: Option[Expression] = None): Expression =
      Min(e).toAggregateExpression(isDistinct = false, filter = filter)
    def minDistinct(e: Expression, filter: Option[Expression] = None): Expression =
      Min(e).toAggregateExpression(isDistinct = true, filter = filter)
    def max(e: Expression, filter: Option[Expression] = None): Expression =
      Max(e).toAggregateExpression(isDistinct = false, filter = filter)
    def maxDistinct(e: Expression, filter: Option[Expression] = None): Expression =
      Max(e).toAggregateExpression(isDistinct = true, filter = filter)
    def bitAnd(e: Expression, filter: Option[Expression] = None): Expression =
      BitAndAgg(e).toAggregateExpression(isDistinct = false, filter = filter)
    def bitOr(e: Expression, filter: Option[Expression] = None): Expression =
      BitOrAgg(e).toAggregateExpression(isDistinct = false, filter = filter)
    def bitXor(e: Expression, filter: Option[Expression] = None): Expression =
      BitXorAgg(e).toAggregateExpression(isDistinct = false, filter = filter)
    def collectList(e: Expression, filter: Option[Expression] = None): Expression =
      CollectList(e).toAggregateExpression(isDistinct = false, filter = filter)
    def collectSet(e: Expression, filter: Option[Expression] = None): Expression =
      CollectSet(e).toAggregateExpression(isDistinct = false, filter = filter)

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
    implicit class DslAttr(override val attr: UnresolvedAttribute) extends ImplicitAttribute {
      def s: String = attr.sql
    }

    abstract class ImplicitAttribute extends ImplicitOperators {
      def s: String
      def expr: UnresolvedAttribute = attr
      def attr: UnresolvedAttribute = analysis.UnresolvedAttribute(s)

      private def attrRef(dataType: DataType): AttributeReference =
        AttributeReference(attr.nameParts.last, dataType)(qualifier = attr.nameParts.init)

      /** Creates a new AttributeReference of type boolean */
      def boolean: AttributeReference = attrRef(BooleanType)

      /** Creates a new AttributeReference of type byte */
      def byte: AttributeReference = attrRef(ByteType)

      /** Creates a new AttributeReference of type short */
      def short: AttributeReference = attrRef(ShortType)

      /** Creates a new AttributeReference of type int */
      def int: AttributeReference = attrRef(IntegerType)

      /** Creates a new AttributeReference of type long */
      def long: AttributeReference = attrRef(LongType)

      /** Creates a new AttributeReference of type float */
      def float: AttributeReference = attrRef(FloatType)

      /** Creates a new AttributeReference of type double */
      def double: AttributeReference = attrRef(DoubleType)

      /** Creates a new AttributeReference of type string with default collation */
      def string: AttributeReference = attrRef(StringType)

      /** Creates a new AttributeReference of type string with specified collation */
      def string(collation: String): AttributeReference =
        attrRef(StringType(CollationFactory.collationNameToId(collation)))

      /** Creates a new AttributeReference of type date */
      def date: AttributeReference = attrRef(DateType)

      /** Creates a new AttributeReference of type decimal */
      def decimal: AttributeReference = attrRef(DecimalType.SYSTEM_DEFAULT)

      /** Creates a new AttributeReference of type decimal */
      def decimal(precision: Int, scale: Int): AttributeReference =
        attrRef(DecimalType(precision, scale))

      /** Creates a new AttributeReference of type timestamp */
      def timestamp: AttributeReference = attrRef(TimestampType)

      /** Creates a new AttributeReference of type timestamp without time zone */
      def timestampNTZ: AttributeReference = attrRef(TimestampNTZType)

      /** Creates a new AttributeReference of the day-time interval type */
      def dayTimeInterval(startField: Byte, endField: Byte): AttributeReference =
        attrRef(DayTimeIntervalType(startField, endField))

      def dayTimeInterval(): AttributeReference = attrRef(DayTimeIntervalType())

      /** Creates a new AttributeReference of the year-month interval type */
      def yearMonthInterval(startField: Byte, endField: Byte): AttributeReference =
        attrRef(YearMonthIntervalType(startField, endField))

      def yearMonthInterval(): AttributeReference = attrRef(YearMonthIntervalType())

      /** Creates a new AttributeReference of type binary */
      def binary: AttributeReference = attrRef(BinaryType)

      /** Creates a new AttributeReference of type array */
      def array(dataType: DataType): AttributeReference = attrRef(ArrayType(dataType))

      def array(arrayType: ArrayType): AttributeReference = attrRef(arrayType)

      /** Creates a new AttributeReference of type map */
      def map(keyType: DataType, valueType: DataType): AttributeReference =
        map(MapType(keyType, valueType))

      def map(mapType: MapType): AttributeReference = attrRef(mapType)

      /** Creates a new AttributeReference of type struct */
      def struct(structType: StructType): AttributeReference = attrRef(structType)

      def struct(attrs: AttributeReference*): AttributeReference =
        struct(DataTypeUtils.fromAttributes(attrs))

      /** Creates a new AttributeReference of object type */
      def obj(cls: Class[_]): AttributeReference = attrRef(ObjectType(cls))

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
    def table(parts: String*): LogicalPlan = UnresolvedRelation(parts)

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

      def localLimit(limitExpr: Expression): LogicalPlan = LocalLimit(limitExpr, logicalPlan)

      def globalLimit(limitExpr: Expression): LogicalPlan = GlobalLimit(limitExpr, logicalPlan)

      def offset(offsetExpr: Expression): LogicalPlan = Offset(offsetExpr, logicalPlan)

      def join(
        otherPlan: LogicalPlan,
        joinType: JoinType = Inner,
        condition: Option[Expression] = None): LogicalPlan =
        Join(logicalPlan, otherPlan, joinType, condition, JoinHint.NONE)

      def lateralJoin(
          otherPlan: LogicalPlan,
          joinType: JoinType = Inner,
          condition: Option[Expression] = None): LogicalPlan = {
        LateralJoin(logicalPlan, LateralSubquery(otherPlan), joinType, condition)
      }

      def cogroup[Key: Encoder, Left: Encoder, Right: Encoder, Result: Encoder](
          otherPlan: LogicalPlan,
          func: (Key, Iterator[Left], Iterator[Right]) => IterableOnce[Result],
          leftGroup: Seq[Attribute],
          rightGroup: Seq[Attribute],
          leftAttr: Seq[Attribute],
          rightAttr: Seq[Attribute],
          leftOrder: Seq[SortOrder] = Nil,
          rightOrder: Seq[SortOrder] = Nil
        ): LogicalPlan = {
        CoGroup.apply[Key, Left, Right, Result](
          func,
          leftGroup,
          rightGroup,
          leftAttr,
          rightAttr,
          leftOrder,
          rightOrder,
          logicalPlan,
          otherPlan)
      }

      def orderBy(sortExprs: SortOrder*): LogicalPlan = {
        val sortExpressionsWithOrdinals = sortExprs.map(replaceOrdinalsInSortOrder)
        Sort(sortExpressionsWithOrdinals, true, logicalPlan)
      }

      def sortBy(sortExprs: SortOrder*): LogicalPlan = {
        val sortExpressionsWithOrdinals = sortExprs.map(replaceOrdinalsInSortOrder)
        Sort(sortExpressionsWithOrdinals, false, logicalPlan)
      }

      /**
       * Replaces top-level integer literals from [[SortOrder]] with [[UnresolvedOrdinal]], if
       * `orderByOrdinal` is enabled.
       */
      private def replaceOrdinalsInSortOrder(sortOrder: SortOrder): SortOrder = sortOrder match {
        case sortOrderByOrdinal @ SortOrder(literal @ Literal(value: Int, IntegerType), _, _, _)
            if conf.orderByOrdinal =>
          val ordinal = CurrentOrigin.withOrigin(literal.origin) { UnresolvedOrdinal(value) }
          sortOrderByOrdinal
            .withNewChildren(newChildren = Seq(ordinal))
            .asInstanceOf[SortOrder]
        case other => other
      }

      def groupBy(groupingExprs: Expression*)(aggregateExprs: Expression*): LogicalPlan = {
        // Replace top-level integer literals with ordinals, if `groupByOrdinal` is enabled.
        val groupingExpressionsWithOrdinals = groupingExprs.map {
          case literal @ Literal(value: Int, IntegerType) if conf.groupByOrdinal =>
            CurrentOrigin.withOrigin(literal.origin) { UnresolvedOrdinal(value) }
          case other => other
        }
        val aliasedExprs = aggregateExprs.map {
          case ne: NamedExpression => ne
          case e => UnresolvedAlias(e)
        }
        Aggregate(groupingExpressionsWithOrdinals, aliasedExprs, logicalPlan)
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

      def windowGroupLimit(
          partitionSpec: Seq[Expression],
          orderSpec: Seq[SortOrder],
          rankLikeFunction: Expression,
          limit: Int): LogicalPlan =
        WindowGroupLimit(partitionSpec, orderSpec, rankLikeFunction, limit, logicalPlan)

      def subquery(alias: String): LogicalPlan = SubqueryAlias(alias, logicalPlan)
      def as(alias: String): LogicalPlan = SubqueryAlias(alias, logicalPlan)

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

      def insertInto(tableName: String): LogicalPlan = insertInto(table(tableName))

      def insertInto(
          table: LogicalPlan,
          partition: Map[String, Option[String]] = Map.empty,
          overwrite: Boolean = false,
          ifPartitionNotExists: Boolean = false): LogicalPlan =
        InsertIntoStatement(table, partition, Nil, logicalPlan, overwrite, ifPartitionNotExists)

      def coalesce(num: Integer): LogicalPlan =
        Repartition(num, shuffle = false, logicalPlan)

      def repartition(num: Integer): LogicalPlan =
        Repartition(num, shuffle = true, logicalPlan)

      def repartition(): LogicalPlan =
        RepartitionByExpression(Seq.empty, logicalPlan, None)

      def distribute(exprs: Expression*)(n: Int): LogicalPlan =
        RepartitionByExpression(exprs, logicalPlan, numPartitions = n)

      def rebalance(exprs: Expression*): LogicalPlan =
        RebalancePartitions(exprs, logicalPlan)

      def analyze: LogicalPlan = {
        val analyzed = analysis.SimpleAnalyzer.execute(logicalPlan)
        analysis.SimpleAnalyzer.checkAnalysis(analyzed)
        EliminateSubqueryAliases(analyzed)
      }

      def hint(name: String, parameters: Expression*): LogicalPlan = {
        UnresolvedHint(name, parameters, logicalPlan)
      }

      def sample(
          lowerBound: Double,
          upperBound: Double,
          withReplacement: Boolean,
          seed: Long): LogicalPlan = {
        Sample(lowerBound, upperBound, withReplacement, seed, logicalPlan)
      }

      def deduplicate(colNames: Attribute*): LogicalPlan = Deduplicate(colNames, logicalPlan)
    }
  }
}
