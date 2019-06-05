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

import org.apache.orc.storage.common.`type`.HiveDecimal
import org.apache.orc.storage.ql.io.sarg.{PredicateLeaf, SearchArgument}
import org.apache.orc.storage.ql.io.sarg.SearchArgument.Builder
import org.apache.orc.storage.ql.io.sarg.SearchArgumentFactory.newBuilder
import org.apache.orc.storage.serde2.io.HiveDecimalWritable

import org.apache.spark.sql.sources.Filter
import org.apache.spark.sql.types._

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
   * Create ORC filter as a SearchArgument instance.
   */
  def createFilter(schema: StructType, filters: Seq[Filter]): Option[SearchArgument] = {
    val dataTypeMap = schema.map(f => f.name -> f.dataType).toMap
    val orcFilterConverter = new OrcFilterConverter(dataTypeMap)
    for {
      // Combines all filters using `And` to produce a single conjunction
      conjunction <- buildTree(filters)
      // Then tries to build a single ORC `SearchArgument` for the conjunction predicate
      builder <- orcFilterConverter.buildSearchArgument(conjunction, newBuilder)
    } yield builder.build()
  }

  def convertibleFilters(
      schema: StructType,
      dataTypeMap: Map[String, DataType],
      filters: Seq[Filter]): Seq[Filter] = {
    val orcFilterConverter = new OrcFilterConverter(dataTypeMap)
    filters.flatMap(orcFilterConverter.trimUnconvertibleFilters)
  }

}

private class OrcFilterConverter(val dataTypeMap: Map[String, DataType]) {

  /**
   * Get PredicateLeafType which is corresponding to the given DataType.
   */
  private def getPredicateLeafType(dataType: DataType) = dataType match {
    case BooleanType => PredicateLeaf.Type.BOOLEAN
    case ByteType | ShortType | IntegerType | LongType => PredicateLeaf.Type.LONG
    case FloatType | DoubleType => PredicateLeaf.Type.FLOAT
    case StringType => PredicateLeaf.Type.STRING
    case DateType => PredicateLeaf.Type.DATE
    case TimestampType => PredicateLeaf.Type.TIMESTAMP
    case _: DecimalType => PredicateLeaf.Type.DECIMAL
    case _ => throw new UnsupportedOperationException(s"DataType: ${dataType.catalogString}")
  }

  /**
   * Cast literal values for filters.
   *
   * We need to cast to long because ORC raises exceptions
   * at 'checkLiteralType' of SearchArgumentImpl.java.
   */
  private def castLiteralValue(value: Any, dataType: DataType): Any = dataType match {
    case ByteType | ShortType | IntegerType | LongType =>
      value.asInstanceOf[Number].longValue
    case FloatType | DoubleType =>
      value.asInstanceOf[Number].doubleValue()
    case _: DecimalType =>
      new HiveDecimalWritable(HiveDecimal.create(value.asInstanceOf[java.math.BigDecimal]))
    case _ => value
  }

  import org.apache.spark.sql.sources._
  import OrcFilters._

  /**
   * Builds a SearchArgument for a Filter by first trimming the non-convertible nodes, and then
   * only building the remaining convertible nodes.
   *
   * Doing the conversion in this way avoids the computational complexity problems introduced by
   * checking whether a node is convertible while building it. The approach implemented here has
   * complexity that's linear in the size of the Filter tree - O(number of Filter nodes) - we run
   * a single pass over the tree to trim it, and then another pass on the trimmed tree to convert
   * the remaining nodes.
   *
   * The alternative approach of checking-while-building can (and did) result
   * in exponential complexity in the height of the tree, causing perf problems with Filters with
   * as few as ~35 nodes if they were skewed.
   */
  private[sql] def buildSearchArgument(
      expression: Filter,
      builder: Builder): Option[Builder] = {
    trimUnconvertibleFilters(expression).map { filter =>
      updateBuilder(filter, builder)
      builder
    }
  }

  /**
   * Removes all sub-Filters from a given Filter that are not convertible to an ORC SearchArgument.
   */
  private[sql] def trimUnconvertibleFilters(expression: Filter): Option[Filter] = {
    performAction(TrimUnconvertibleFilters(canPartialPushDownConjuncts = true), expression)
  }

  /**
   * Builds a SearchArgument for the given Filter. This method should only be called on Filters
   * that have previously been trimmed to remove unsupported sub-Filters!
   */
  private def updateBuilder(expression: Filter, builder: Builder): Unit =
    performAction(BuildSearchArgument(builder), expression)

  sealed trait ActionType[ReturnType]
  case class TrimUnconvertibleFilters(canPartialPushDownConjuncts: Boolean)
      extends ActionType[Option[Filter]]
  case class BuildSearchArgument(builder: Builder) extends ActionType[Unit]

  // The performAction method can run both the filtering and building operations for a given
  // node - we signify which one we want with the `actionType` parameter.
  //
  // There are a couple of benefits to coupling the two operations like this:
  // 1. All the logic for a given predicate is grouped logically in the same place. You don't
  //   have to scroll across the whole file to see what the filter action for an And is while
  //   you're looking at the build action.
  // 2. It's much easier to keep the implementations of the two operations up-to-date with
  //   each other. If the `filter` and `build` operations are implemented as separate case-matches
  //   in different methods, it's very easy to change one without appropriately updating the
  //   other. For example, if we add a new supported node type to `filter`, it would be very
  //   easy to forget to update `build` to support it too, thus leading to conversion errors.
  private def performAction[ReturnType](
      actionType: ActionType[ReturnType],
      expression: Filter): ReturnType = {
    def getType(attribute: String): PredicateLeaf.Type =
      getPredicateLeafType(dataTypeMap(attribute))

    expression match {
      case And(left, right) =>
        actionType match {
          case t @ TrimUnconvertibleFilters(canPartialPushDownConjuncts) =>
            // At here, it is not safe to just keep one side and remove the other side
            // if we do not understand what the parent filters are.
            //
            // Here is an example used to explain the reason.
            // Let's say we have NOT(a = 2 AND b in ('1')) and we do not understand how to
            // convert b in ('1'). If we only convert a = 2, we will end up with a filter
            // NOT(a = 2), which will generate wrong results.
            //
            // Pushing one side of AND down is only safe to do at the top level or in the child
            // AND before hitting NOT or OR conditions, and in this case, the unsupported
            // predicate can be safely removed.
            val lhs = performAction(t, left)
            val rhs = performAction(t, right)
            (lhs, rhs) match {
              case (Some(l), Some(r)) => Some(And(l, r))
              case (Some(_), None) if canPartialPushDownConjuncts => lhs
              case (None, Some(_)) if canPartialPushDownConjuncts => rhs
              case _ => None
            }
          case b @ BuildSearchArgument(builder) =>
            builder.startAnd()
            performAction(b, left)
            performAction(b, right)
            builder.end()
            ()
        }

      case Or(left, right) =>
        actionType match {
          case t: TrimUnconvertibleFilters =>
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
            for {
              lhs: Filter <- performAction(t, left)
              rhs: Filter <- performAction(t, right)
            } yield Or(lhs, rhs)
          case b @ BuildSearchArgument(builder) =>
            builder.startOr()
            performAction(b, left)
            performAction(b, right)
            builder.end()
            ()
        }

      case Not(child) =>
        actionType match {
          case t: TrimUnconvertibleFilters =>
            performAction(t.copy(canPartialPushDownConjuncts = false), child).map(Not)
          case b @ BuildSearchArgument(builder) =>
            builder.startNot()
            performAction(b, child)
            builder.end()
            ()
        }

      // NOTE: For all case branches dealing with leaf predicates below, the additional
      // `startAnd()` call is mandatory.  ORC `SearchArgument` builder requires that all leaf
      // predicates must be wrapped by a "parent" predicate (`And`, `Or`, or `Not`).

      case EqualTo(attribute, value) if isSearchableType(dataTypeMap(attribute)) =>
        actionType match {
          case _: TrimUnconvertibleFilters => Some(expression)
          case BuildSearchArgument(builder) =>
            val quotedName = quoteAttributeNameIfNeeded(attribute)
            val castedValue = castLiteralValue(value, dataTypeMap(attribute))
            builder.startAnd().equals(quotedName, getType(attribute), castedValue).end()
            ()
        }
      case EqualNullSafe(attribute, value) if isSearchableType(dataTypeMap(attribute)) =>
        actionType match {
          case _: TrimUnconvertibleFilters => Some(expression)
          case BuildSearchArgument(builder) =>
            val quotedName = quoteAttributeNameIfNeeded(attribute)
            val castedValue = castLiteralValue(value, dataTypeMap(attribute))
            builder.startAnd().nullSafeEquals(quotedName, getType(attribute), castedValue).end()
            ()
        }
      case LessThan(attribute, value) if isSearchableType(dataTypeMap(attribute)) =>
        actionType match {
          case _: TrimUnconvertibleFilters => Some(expression)
          case BuildSearchArgument(builder) =>
            val quotedName = quoteAttributeNameIfNeeded(attribute)
            val castedValue = castLiteralValue(value, dataTypeMap(attribute))
            builder.startAnd().lessThan(quotedName, getType(attribute), castedValue).end()
            ()
        }
      case LessThanOrEqual(attribute, value) if isSearchableType(dataTypeMap(attribute)) =>
        actionType match {
          case _: TrimUnconvertibleFilters => Some(expression)
          case BuildSearchArgument(builder) =>
            val quotedName = quoteAttributeNameIfNeeded(attribute)
            val castedValue = castLiteralValue(value, dataTypeMap(attribute))
            builder.startAnd().lessThanEquals(quotedName, getType(attribute), castedValue).end()
            ()
        }
      case GreaterThan(attribute, value) if isSearchableType(dataTypeMap(attribute)) =>
        actionType match {
          case _: TrimUnconvertibleFilters => Some(expression)
          case BuildSearchArgument(builder) =>
            val quotedName = quoteAttributeNameIfNeeded(attribute)
            val castedValue = castLiteralValue(value, dataTypeMap(attribute))
            builder.startNot().lessThanEquals(quotedName, getType(attribute), castedValue).end()
            ()
        }
      case GreaterThanOrEqual(attribute, value) if isSearchableType(dataTypeMap(attribute)) =>
        actionType match {
          case _: TrimUnconvertibleFilters => Some(expression)
          case BuildSearchArgument(builder) =>
            val quotedName = quoteAttributeNameIfNeeded(attribute)
            val castedValue = castLiteralValue(value, dataTypeMap(attribute))
            builder.startNot().lessThan(quotedName, getType(attribute), castedValue).end()
            ()
        }
      case IsNull(attribute) if isSearchableType(dataTypeMap(attribute)) =>
        actionType match {
          case _: TrimUnconvertibleFilters => Some(expression)
          case BuildSearchArgument(builder) =>
            val quotedName = quoteAttributeNameIfNeeded(attribute)
            builder.startAnd().isNull(quotedName, getType(attribute)).end()
            ()
        }
      case IsNotNull(attribute) if isSearchableType(dataTypeMap(attribute)) =>
        actionType match {
          case _: TrimUnconvertibleFilters => Some(expression)
          case BuildSearchArgument(builder) =>
            val quotedName = quoteAttributeNameIfNeeded(attribute)
            builder.startNot().isNull(quotedName, getType(attribute)).end()
            ()
        }
      case In(attribute, values) if isSearchableType(dataTypeMap(attribute)) =>
        actionType match {
          case _: TrimUnconvertibleFilters => Some(expression)
          case BuildSearchArgument(builder) =>
            val quotedName = quoteAttributeNameIfNeeded(attribute)
            val castedValues = values.map(v => castLiteralValue(v, dataTypeMap(attribute)))
            builder.startAnd().in(quotedName, getType(attribute),
              castedValues.map(_.asInstanceOf[AnyRef]): _*).end()
            ()
        }

      case _ =>
        actionType match {
          case _: TrimUnconvertibleFilters => None
          case BuildSearchArgument(builder) =>
            throw new IllegalArgumentException(s"Can't build unsupported filter ${expression}")
        }
    }
  }
}

