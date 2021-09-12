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

package org.apache.spark.sql.execution.datasources.orc

import java.time.{Instant, LocalDate}

import org.apache.hadoop.hive.common.`type`.HiveDecimal
import org.apache.hadoop.hive.ql.io.sarg.{PredicateLeaf, SearchArgument}
import org.apache.hadoop.hive.ql.io.sarg.SearchArgument.Builder
import org.apache.hadoop.hive.ql.io.sarg.SearchArgumentFactory.newBuilder
import org.apache.hadoop.hive.serde2.io.HiveDecimalWritable

import org.apache.spark.sql.catalyst.util.DateTimeUtils.{instantToMicros, localDateToDays, toJavaDate, toJavaTimestamp}
import org.apache.spark.sql.connector.expressions.Literal
import org.apache.spark.sql.connector.expressions.filter.{And => V2And, EqualNullSafe => V2EqualNullSafe, EqualTo => V2EqualTo, Filter => V2Filter, GreaterThan => V2GreaterThan, GreaterThanOrEqual => V2GreaterThanOrEqual, In => V2In, IsNotNull => V2IsNotNull, IsNull => V2IsNull, LessThan => V2LessThan, LessThanOrEqual => V2LessThanOrEqual, Not => V2Not, Or => V2Or}
import org.apache.spark.sql.errors.QueryExecutionErrors
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.sources.Filter
import org.apache.spark.sql.types._
import org.apache.spark.unsafe.types.UTF8String

/**
 * Helper object for building ORC `SearchArgument`s, which are used for ORC predicate push-down.
 *
 * Due to limitation of ORC `SearchArgument` builder, we had to implement separate checking and
 * conversion passes through the Filter to make sure we only convert predicates that are known
 * to be convertible.
 *
 * An ORC `SearchArgument` must be built in one pass using a single builder.  For example, you can't
 * build `a = 1` and `b = 2` first, and then combine them into `a = 1 AND b = 2`.  This is quite
 * different from the cases in Spark SQL or Parquet, where complex filters can be easily built using
 * existing simpler ones.
 *
 * The annoying part is that, `SearchArgument` builder methods like `startAnd()`, `startOr()`, and
 * `startNot()` mutate internal state of the builder instance.  This forces us to translate all
 * convertible filters with a single builder instance. However, if we try to translate a filter
 * before checking whether it can be converted or not, we may end up with a builder whose internal
 * state is inconsistent in the case of an inconvertible filter.
 *
 * For example, to convert an `And` filter with builder `b`, we call `b.startAnd()` first, and then
 * try to convert its children.  Say we convert `left` child successfully, but find that `right`
 * child is inconvertible.  Alas, `b.startAnd()` call can't be rolled back, and `b` is inconsistent
 * now.
 *
 * The workaround employed here is to trim the Spark filters before trying to convert them. This
 * way, we can only do the actual conversion on the part of the Filter that is known to be
 * convertible.
 *
 * P.S.: Hive seems to use `SearchArgument` together with `ExprNodeGenericFuncDesc` only.  Usage of
 * builder methods mentioned above can only be found in test code, where all tested filters are
 * known to be convertible.
 */
private[sql] object OrcFilters extends OrcFiltersBase {

  /**
   * Create ORC filter as a SearchArgument instance from V1 Filters.
   */
  def createFilter(schema: StructType, filters: Seq[Filter])(implicit d: DummyImplicit)
  : Option[SearchArgument] = {
    createFilter(schema, filters.map(_.toV2))
  }

  /**
   * Create ORC filter as a SearchArgument instance from V2 Filters.
   */
  def createFilter(schema: StructType, filters: Seq[V2Filter]): Option[SearchArgument] = {
    val dataTypeMap = OrcFilters.getSearchableTypeMap(schema, SQLConf.get.caseSensitiveAnalysis)
    // Combines all convertible filters using `And` to produce a single conjunction
    val conjunctionOptional = buildTree(convertibleFilters(dataTypeMap, filters))
    conjunctionOptional.map { conjunction =>
      // Then tries to build a single ORC `SearchArgument` for the conjunction predicate.
      // The input predicate is fully convertible. There should not be any empty result in the
      // following recursive method call `buildSearchArgument`.
      buildSearchArgument(dataTypeMap, conjunction, newBuilder).build()
    }
  }

  def convertibleFilters(
      dataTypeMap: Map[String, OrcPrimitiveField],
      filters: Seq[V2Filter]): Seq[V2Filter] = {

    def convertibleFiltersHelper(
        filter: V2Filter,
        canPartialPushDown: Boolean): Option[V2Filter] = filter match {
      // At here, it is not safe to just convert one side and remove the other side
      // if we do not understand what the parent filters are.
      //
      // Here is an example used to explain the reason.
      // Let's say we have NOT(a = 2 AND b in ('1')) and we do not understand how to
      // convert b in ('1'). If we only convert a = 2, we will end up with a filter
      // NOT(a = 2), which will generate wrong results.
      //
      // Pushing one side of AND down is only safe to do at the top level or in the child
      // AND before hitting NOT or OR conditions, and in this case, the unsupported predicate
      // can be safely removed.
      case f: V2And =>
        val leftResultOptional = convertibleFiltersHelper(f.left, canPartialPushDown)
        val rightResultOptional = convertibleFiltersHelper(f.right, canPartialPushDown)
        (leftResultOptional, rightResultOptional) match {
          case (Some(leftResult), Some(rightResult)) => Some(new V2And(leftResult, rightResult))
          case (Some(leftResult), None) if canPartialPushDown => Some(leftResult)
          case (None, Some(rightResult)) if canPartialPushDown => Some(rightResult)
          case _ => None
        }

      // The Or predicate is convertible when both of its children can be pushed down.
      // That is to say, if one/both of the children can be partially pushed down, the Or
      // predicate can be partially pushed down as well.
      //
      // Here is an example used to explain the reason.
      // Let's say we have
      // (a1 AND a2) OR (b1 AND b2),
      // a1 and b1 is convertible, while a2 and b2 is not.
      // The predicate can be converted as
      // (a1 OR b1) AND (a1 OR b2) AND (a2 OR b1) AND (a2 OR b2)
      // As per the logical in And predicate, we can push down (a1 OR b1).
      case f: V2Or =>
        for {
          lhs <- convertibleFiltersHelper(f.left, canPartialPushDown)
          rhs <- convertibleFiltersHelper(f.right, canPartialPushDown)
        } yield new V2Or(lhs, rhs)
      case f: V2Not =>
        val childResultOptional = convertibleFiltersHelper(f.child, canPartialPushDown = false)
        childResultOptional.map(new V2Not(_))
      case other =>
        for (_ <- buildLeafSearchArgument(dataTypeMap, other, newBuilder())) yield other
    }
    filters.flatMap { filter =>
      convertibleFiltersHelper(filter, true)
    }
  }

  /**
   * Get PredicateLeafType which is corresponding to the given DataType.
   */
  def getPredicateLeafType(dataType: DataType): PredicateLeaf.Type = dataType match {
    case BooleanType => PredicateLeaf.Type.BOOLEAN
    case ByteType | ShortType | IntegerType | LongType => PredicateLeaf.Type.LONG
    case FloatType | DoubleType => PredicateLeaf.Type.FLOAT
    case StringType => PredicateLeaf.Type.STRING
    case DateType => PredicateLeaf.Type.DATE
    case TimestampType => PredicateLeaf.Type.TIMESTAMP
    case _: DecimalType => PredicateLeaf.Type.DECIMAL
    case _ => throw QueryExecutionErrors.unsupportedOperationForDataTypeError(dataType)
  }

  /**
   * Cast literal values for filters.
   *
   * We need to cast to long because ORC raises exceptions
   * at 'checkLiteralType' of SearchArgumentImpl.java.
   */
  private def castLiteralValue(value: Any, dataType: DataType): Any = dataType match {
    case ByteType | ShortType | IntegerType | LongType =>
      value.asInstanceOf[Literal[_]].value.asInstanceOf[Number].longValue
    case FloatType | DoubleType =>
      value.asInstanceOf[Literal[_]].value.asInstanceOf[Number].doubleValue()
    case _: DecimalType
      if value.asInstanceOf[Literal[_]].value.isInstanceOf[java.math.BigDecimal] =>
      new HiveDecimalWritable(HiveDecimal.create
      (value.asInstanceOf[Literal[_]].value.asInstanceOf[java.math.BigDecimal]))
    case _: DecimalType =>
      new HiveDecimalWritable(HiveDecimal.create
        (value.asInstanceOf[Literal[_]].value.asInstanceOf[Decimal].toJavaBigDecimal))
    case _: DateType if value.asInstanceOf[Literal[_]].value.isInstanceOf[LocalDate] =>
      toJavaDate(localDateToDays(value.asInstanceOf[Literal[_]].value.asInstanceOf[LocalDate]))
    case _: DateType if value.asInstanceOf[Literal[_]].value.isInstanceOf[Integer] =>
      toJavaDate(value.asInstanceOf[Literal[_]].value.asInstanceOf[Integer])
    case _: TimestampType if value.asInstanceOf[Literal[_]].value.isInstanceOf[Instant] =>
      toJavaTimestamp(instantToMicros(value.asInstanceOf[Literal[_]].value.asInstanceOf[Instant]))
    case _: TimestampType if value.asInstanceOf[Literal[_]].value.isInstanceOf[Long] =>
      toJavaTimestamp(value.asInstanceOf[Literal[_]].value.asInstanceOf[Long])
    case StringType =>
      val str = value.asInstanceOf[Literal[_]].value
      if(str.isInstanceOf[UTF8String]) {
        str.asInstanceOf[UTF8String].toString
      } else {
        str
      }
    case _ => value.asInstanceOf[Literal[_]].value
  }

  /**
   * Build a SearchArgument and return the builder so far.
   *
   * @param dataTypeMap a map from the attribute name to its data type.
   * @param expression the input predicates, which should be fully convertible to SearchArgument.
   * @param builder the input SearchArgument.Builder.
   * @return the builder so far.
   */
  private def buildSearchArgument(
      dataTypeMap: Map[String, OrcPrimitiveField],
      expression: V2Filter,
      builder: Builder): Builder = {

    expression match {
      case f: V2And =>
        val lhs = buildSearchArgument(dataTypeMap, f.left, builder.startAnd())
        val rhs = buildSearchArgument(dataTypeMap, f.right, lhs)
        rhs.end()

      case f: V2Or =>
        val lhs = buildSearchArgument(dataTypeMap, f.left, builder.startOr())
        val rhs = buildSearchArgument(dataTypeMap, f.right, lhs)
        rhs.end()

      case f: V2Not =>
        buildSearchArgument(dataTypeMap, f.child, builder.startNot()).end()

      case other =>
        buildLeafSearchArgument(dataTypeMap, other, builder).getOrElse {
          throw QueryExecutionErrors.inputFilterNotFullyConvertibleError(
            "OrcFilters.buildSearchArgument")
        }
    }
  }

  /**
   * Build a SearchArgument for a leaf predicate and return the builder so far.
   *
   * @param dataTypeMap a map from the attribute name to its data type.
   * @param expression the input filter predicates.
   * @param builder the input SearchArgument.Builder.
   * @return the builder so far.
   */
  private def buildLeafSearchArgument(
      dataTypeMap: Map[String, OrcPrimitiveField],
      expression: V2Filter,
      builder: Builder): Option[Builder] = {
    def getType(attribute: String): PredicateLeaf.Type =
      getPredicateLeafType(dataTypeMap(attribute).fieldType)

    // NOTE: For all case branches dealing with leaf predicates below, the additional `startAnd()`
    // call is mandatory. ORC `SearchArgument` builder requires that all leaf predicates must be
    // wrapped by a "parent" predicate (`And`, `Or`, or `Not`).
    expression match {
      case f: V2EqualTo if dataTypeMap.contains(f.column.describe) =>
        val colName = f.column.describe
        val castedValue = castLiteralValue(f.value, dataTypeMap(colName).fieldType)
        Some(builder.startAnd().equals(
          dataTypeMap(colName).fieldName, getType(colName), castedValue).end())

      case f: V2EqualNullSafe
        if dataTypeMap.contains(f.column.describe) =>
        val colName = f.column.describe
        val castedValue = castLiteralValue(f.value, dataTypeMap(colName).fieldType)
        Some(builder.startAnd().nullSafeEquals(
          dataTypeMap(colName).fieldName, getType(colName), castedValue).end())

      case f: V2LessThan if dataTypeMap.contains(f.column.describe) =>
        val colName = f.column.describe
        val castedValue = castLiteralValue(f.value, dataTypeMap(colName).fieldType)
        Some(builder.startAnd().lessThan(
          dataTypeMap(colName).fieldName, getType(colName), castedValue).end())

      case f: V2LessThanOrEqual
        if dataTypeMap.contains(f.column.describe) =>
        val colName = f.column.describe
        val castedValue = castLiteralValue(f.value, dataTypeMap(colName).fieldType)
        Some(builder.startAnd().lessThanEquals(
          dataTypeMap(colName).fieldName, getType(colName), castedValue).end())

      case f: V2GreaterThan
        if dataTypeMap.contains(f.column.describe) =>
        val colName = f.column.describe
        val castedValue = castLiteralValue(f.value, dataTypeMap(colName).fieldType)
        Some(builder.startNot().lessThanEquals(
          dataTypeMap(colName).fieldName, getType(colName), castedValue).end())

      case f: V2GreaterThanOrEqual
        if dataTypeMap.contains(f.column.describe) =>
        val colName = f.column.describe
        val castedValue = castLiteralValue(f.value, dataTypeMap(colName).fieldType)
        Some(builder.startNot().lessThan(
          dataTypeMap(colName).fieldName, getType(colName), castedValue).end())

      case f: V2IsNull if dataTypeMap.contains(f.column.describe) =>
        val colName = f.column.describe
        Some(builder.startAnd().isNull(dataTypeMap(colName).fieldName, getType(colName)).end())

      case f: V2IsNotNull if dataTypeMap.contains(f.column.describe) =>
        val colName = f.column.describe
        Some(builder.startNot().isNull(dataTypeMap(colName).fieldName, getType(colName)).end())

      case f: V2In if dataTypeMap.contains(f.column.describe) =>
        val colName = f.column.describe
        val castedValues = f.values.map(v => castLiteralValue(v, dataTypeMap(colName).fieldType))
        Some(builder.startAnd().in(dataTypeMap(colName).fieldName, getType(colName),
          castedValues.map(_.asInstanceOf[AnyRef]): _*).end())

      case _ => None
    }
  }
}
