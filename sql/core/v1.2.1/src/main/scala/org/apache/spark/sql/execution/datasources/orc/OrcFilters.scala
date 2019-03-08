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

import java.util.Objects

import org.apache.orc.storage.common.`type`.HiveDecimal
import org.apache.orc.storage.ql.io.sarg.{PredicateLeaf, SearchArgument}
import org.apache.orc.storage.ql.io.sarg.SearchArgument.Builder
import org.apache.orc.storage.ql.io.sarg.SearchArgumentFactory.newBuilder
import org.apache.orc.storage.serde2.io.HiveDecimalWritable
import scala.collection.mutable

import org.apache.spark.sql.sources.Filter
import org.apache.spark.sql.types._

/**
 * Helper object for building ORC `SearchArgument`s, which are used for ORC predicate push-down.
 *
 * Due to limitation of ORC `SearchArgument` builder, we had to implement separate checking and
 * conversion code paths to make sure we only convert predicates that are known to be convertible.
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
 * The workaround employed here is that, for `And`/`Or`/`Not`, we explicitly check if the children
 * are convertible, and only do the actual conversion when the children are proven to be
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
    for {
      // Combines all convertible filters using `And` to produce a single conjunction
      conjunction <- buildTree(convertibleFilters(schema, dataTypeMap, filters))
      // Then tries to build a single ORC `SearchArgument` for the conjunction predicate
      builder <- buildSearchArgument(dataTypeMap, conjunction, newBuilder)
    } yield builder.build()
  }

  def convertibleFilters(
      schema: StructType,
      dataTypeMap: Map[String, DataType],
      filters: Seq[Filter]): Seq[Filter] = {
    import org.apache.spark.sql.sources._

    def convertibleFiltersHelper(
        filter: Filter,
        canPartialPushDown: Boolean): Option[Filter] = filter match {
      case And(left, right) =>
        val leftResultOptional = convertibleFiltersHelper(left, canPartialPushDown)
        val rightResultOptional = convertibleFiltersHelper(right, canPartialPushDown)
        (leftResultOptional, rightResultOptional) match {
          case (Some(leftResult), Some(rightResult)) => Some(And(leftResult, rightResult))
          case (Some(leftResult), None) if canPartialPushDown => Some(leftResult)
          case (None, Some(rightResult)) if canPartialPushDown => Some(rightResult)
          case _ => None
        }

      case Or(left, right) =>
        val leftResultOptional = convertibleFiltersHelper(left, canPartialPushDown)
        val rightResultOptional = convertibleFiltersHelper(right, canPartialPushDown)
        if (leftResultOptional.isEmpty || rightResultOptional.isEmpty) {
          None
        } else {
          Some(Or(leftResultOptional.get, rightResultOptional.get))
        }
      case Not(pred) =>
        val resultOptional = convertibleFiltersHelper(pred, canPartialPushDown = false)
        resultOptional.map(Not)
      case other =>
        if (buildSearchArgument(dataTypeMap, other, newBuilder()).isDefined) {
          Some(other)
        } else {
          None
        }
    }
    filters.flatMap { filter =>
      convertibleFiltersHelper(filter, true)
    }
  }

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

  /**
   * Build a SearchArgument and return the builder so far.
   */
  private def buildSearchArgument(
      dataTypeMap: Map[String, DataType],
      expression: Filter,
      builder: Builder): Option[Builder] = {
    createBuilder(
      dataTypeMap,
      new OrcConvertibilityChecker(dataTypeMap),
      expression,
      builder,
      canPartialPushDownConjuncts = true)
  }

  /**
   * @param dataTypeMap a map from the attribute name to its data type.
   * @param expression the input filter predicates.
   * @param builder the input SearchArgument.Builder.
   * @param canPartialPushDownConjuncts whether a subset of conjuncts of predicates can be pushed
   *                                    down safely. Pushing ONLY one side of AND down is safe to
   *                                    do at the top level or none of its ancestors is NOT and OR.
   * @return the builder so far.
   */
  private def createBuilder(
      dataTypeMap: Map[String, DataType],
      orcConvertibilityChecker: OrcConvertibilityChecker,
      expression: Filter,
      builder: Builder,
      canPartialPushDownConjuncts: Boolean): Option[Builder] = {
    def getType(attribute: String): PredicateLeaf.Type =
      getPredicateLeafType(dataTypeMap(attribute))

    import org.apache.spark.sql.sources._

    expression match {
      case And(left, right) =>
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
        val leftIsConvertible = orcConvertibilityChecker.isConvertible(
          left,
          canPartialPushDownConjuncts
        )
        val rightIsConvertible = orcConvertibilityChecker.isConvertible(
          right,
          canPartialPushDownConjuncts
        )
        (leftIsConvertible, rightIsConvertible) match {
          case (true, true) =>
            for {
              lhs <- createBuilder(dataTypeMap, orcConvertibilityChecker, left,
                builder.startAnd(), canPartialPushDownConjuncts)
              rhs <- createBuilder(dataTypeMap, orcConvertibilityChecker, right,
                lhs, canPartialPushDownConjuncts)
            } yield rhs.end()

          case (true, false) if canPartialPushDownConjuncts =>
            createBuilder(dataTypeMap, orcConvertibilityChecker, left,
              builder, canPartialPushDownConjuncts)

          case (false, true) if canPartialPushDownConjuncts =>
            createBuilder(dataTypeMap, orcConvertibilityChecker, right,
              builder, canPartialPushDownConjuncts)

          case _ => None
        }

      case Or(left, right) =>
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
        val leftIsConvertible = orcConvertibilityChecker.isConvertible(
          left,
          canPartialPushDownConjuncts
        )
        val rightIsConvertible = orcConvertibilityChecker.isConvertible(
          right,
          canPartialPushDownConjuncts
        )
        for {
          _ <- Option(leftIsConvertible && rightIsConvertible).filter(identity)
          lhs <- createBuilder(dataTypeMap, orcConvertibilityChecker, left,
            builder.startOr(), canPartialPushDownConjuncts)
          rhs <- createBuilder(dataTypeMap, orcConvertibilityChecker, right,
            lhs, canPartialPushDownConjuncts)
        } yield rhs.end()

      case Not(child) =>
        for {
          negate <- createBuilder(dataTypeMap, orcConvertibilityChecker,
            child, builder.startNot(), canPartialPushDownConjuncts = false)
            if orcConvertibilityChecker.isConvertible(child, canPartialPushDownConjuncts = false)
        } yield negate.end()

      // NOTE: For all case branches dealing with leaf predicates below, the additional `startAnd()`
      // call is mandatory.  ORC `SearchArgument` builder requires that all leaf predicates must be
      // wrapped by a "parent" predicate (`And`, `Or`, or `Not`).

      case EqualTo(attribute, value) if isSearchableType(dataTypeMap(attribute)) =>
        val quotedName = quoteAttributeNameIfNeeded(attribute)
        val castedValue = castLiteralValue(value, dataTypeMap(attribute))
        Some(builder.startAnd().equals(quotedName, getType(attribute), castedValue).end())

      case EqualNullSafe(attribute, value) if isSearchableType(dataTypeMap(attribute)) =>
        val quotedName = quoteAttributeNameIfNeeded(attribute)
        val castedValue = castLiteralValue(value, dataTypeMap(attribute))
        Some(builder.startAnd().nullSafeEquals(quotedName, getType(attribute), castedValue).end())

      case LessThan(attribute, value) if isSearchableType(dataTypeMap(attribute)) =>
        val quotedName = quoteAttributeNameIfNeeded(attribute)
        val castedValue = castLiteralValue(value, dataTypeMap(attribute))
        Some(builder.startAnd().lessThan(quotedName, getType(attribute), castedValue).end())

      case LessThanOrEqual(attribute, value) if isSearchableType(dataTypeMap(attribute)) =>
        val quotedName = quoteAttributeNameIfNeeded(attribute)
        val castedValue = castLiteralValue(value, dataTypeMap(attribute))
        Some(builder.startAnd().lessThanEquals(quotedName, getType(attribute), castedValue).end())

      case GreaterThan(attribute, value) if isSearchableType(dataTypeMap(attribute)) =>
        val quotedName = quoteAttributeNameIfNeeded(attribute)
        val castedValue = castLiteralValue(value, dataTypeMap(attribute))
        Some(builder.startNot().lessThanEquals(quotedName, getType(attribute), castedValue).end())

      case GreaterThanOrEqual(attribute, value) if isSearchableType(dataTypeMap(attribute)) =>
        val quotedName = quoteAttributeNameIfNeeded(attribute)
        val castedValue = castLiteralValue(value, dataTypeMap(attribute))
        Some(builder.startNot().lessThan(quotedName, getType(attribute), castedValue).end())

      case IsNull(attribute) if isSearchableType(dataTypeMap(attribute)) =>
        val quotedName = quoteAttributeNameIfNeeded(attribute)
        Some(builder.startAnd().isNull(quotedName, getType(attribute)).end())

      case IsNotNull(attribute) if isSearchableType(dataTypeMap(attribute)) =>
        val quotedName = quoteAttributeNameIfNeeded(attribute)
        Some(builder.startNot().isNull(quotedName, getType(attribute)).end())

      case In(attribute, values) if isSearchableType(dataTypeMap(attribute)) =>
        val quotedName = quoteAttributeNameIfNeeded(attribute)
        val castedValues = values.map(v => castLiteralValue(v, dataTypeMap(attribute)))
        Some(builder.startAnd().in(quotedName, getType(attribute),
          castedValues.map(_.asInstanceOf[AnyRef]): _*).end())

      case _ => None
    }
  }
}

/**
 * This case class represents a position in a `Filter` tree, paired with information about
 * whether `AND` predicates can be partially pushed down. It is used as a key in the
 * memoization map for `OrcConvertibilityChecker`.
 *
 * Because of the way this class is used, there are a few subtleties in its implementation
 * and behaviour:
 * - The default generated `hashCode` and `equals` methods have linear complexity in the size of
 *   the `expression` Filter. This is because they end up recursing over the whole `Filter` tree
 *   in order to check for equality, in order to return true for `Filter`s with different
 *   identities but the same actual contents. As a result, using this class as a key in a HashMap
 *   with the default generated methods would end up having a complexity that's linear in the
 *   size of the `expression`, instead of constnant. This means that using it as a key in the
 *   memoization map for `OrcConvertibilityChecker` would make the whole convertibility checking
 *   take time quadratic in the size of the tree instead of linear.
 * - For this reason, we override `hashCode` and `equals` to only check the identity of the
 *   `expression` and not the full expression object.
 * - Hashing based on the identity of the expression results in correct behaviour because we
 *   are indeed only interested in memoizing the results for exact locations in the `Filter`
 *   tree. If there are expressions that are exactly the same but at different locations, we
 *   would treat them as different objects and simply compute the results for them twice.
 * - Since implementing `equals` and `hashCode` in the presence of subclasses is trickier,
 *   this class is sealed to guarantee that we don't need to be concerned with subclassing.
 */
private sealed case class FilterWithConjunctPushdown(
  expression: Filter,
  canPartialPushDownConjuncts: Boolean
) {
  override def hashCode(): Int = {
    Objects.hash(
      System.identityHashCode(expression).asInstanceOf[Object],
      canPartialPushDownConjuncts.asInstanceOf[Object])
  }

  override def equals(obj: Any): Boolean = {
    obj match {
      case FilterWithConjunctPushdown(expr, canPushDown) =>
        Objects.equals(System.identityHashCode(expression), System.identityHashCode(expr)) &&
        Objects.equals(canPartialPushDownConjuncts, canPushDown)
      case _ => false
    }
  }
}

/**
 * Helper class for efficiently checking whether a `Filter` and its children can be converted to
 * ORC `SearchArgument`s. The `isConvertible` method has a constant amortized complexity for
 * checking whether a node from a Filter expression is convertible to an ORC `SearchArgument`.
 * The additional memory is O(N) in the size of the whole `Filter` being checked due to the use
 * of a hash map that possibly has an entry for each subnode in the filter.
 *
 * @param dataTypeMap a map from the attribute name to its data type.
 */
private class OrcConvertibilityChecker(dataTypeMap: Map[String, DataType]) {

  private val convertibilityCache = new mutable.HashMap[FilterWithConjunctPushdown, Boolean]

  /**
   * Checks if a given expression from a Filter is convertible.
   *
   * The result for a given (expression, canPartialPushDownConjuncts) pair within a Filter
   * will never change, so we memoize the results in the `convertibilityCache`. This means
   * that the overall complexity for checking all nodes in a `Filter` is:
   * - a total of one pass across the whole Filter tree is made when nodes are checked for
   *   convertibility for the first time.
   * - when checked for a second, etc. time, the result has already been memoized in the cache
   *   and thus the check has a constant time.
   *
   * @param expression the input filter predicates.
   * @param canPartialPushDownConjuncts whether a subset of conjuncts of predicates can be pushed
   *                                    down safely. Pushing ONLY one side of AND down is safe to
   *                                    do at the top level or none of its ancestors is NOT and OR.
   */
  def isConvertible(expression: Filter, canPartialPushDownConjuncts: Boolean): Boolean = {
    val node = FilterWithConjunctPushdown(expression, canPartialPushDownConjuncts)
    convertibilityCache.getOrElseUpdate(node, isConvertibleImpl(node))
  }

  /**
   * This method duplicates the logic from `OrcFilters.createBuilder` that is related to checking
   * if a given part of the filter is actually convertible to an ORC `SearchArgument`.
   */
  private def isConvertibleImpl(node: FilterWithConjunctPushdown): Boolean = {
    import org.apache.spark.sql.sources._
    import OrcFilters._

    node.expression match {
      case And(left, right) =>
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
        val leftIsConvertible = isConvertible(left, node.canPartialPushDownConjuncts)
        val rightIsConvertible = isConvertible(right, node.canPartialPushDownConjuncts)
        // NOTE: If we can use partial predicates here, we only need one of the children to
        // be convertible to be able to convert the parent. Otherwise, we need both to be
        // convertible.
        if (node.canPartialPushDownConjuncts) {
          leftIsConvertible || rightIsConvertible
        } else {
          leftIsConvertible && rightIsConvertible
        }

      case Or(left, right) =>
        val leftIsConvertible = isConvertible(left, node.canPartialPushDownConjuncts)
        val rightIsConvertible = isConvertible(right, node.canPartialPushDownConjuncts)
        leftIsConvertible && rightIsConvertible

      case Not(child) =>
        val childIsConvertible = isConvertible(child, canPartialPushDownConjuncts = false)
        childIsConvertible

      case EqualTo(attribute, value) if isSearchableType(dataTypeMap(attribute)) => true
      case EqualNullSafe(attribute, value) if isSearchableType(dataTypeMap(attribute)) => true
      case LessThan(attribute, value) if isSearchableType(dataTypeMap(attribute)) => true
      case LessThanOrEqual(attribute, value) if isSearchableType(dataTypeMap(attribute)) => true
      case GreaterThan(attribute, value) if isSearchableType(dataTypeMap(attribute)) => true
      case GreaterThanOrEqual(attribute, value) if isSearchableType(dataTypeMap(attribute)) => true
      case IsNull(attribute) if isSearchableType(dataTypeMap(attribute)) => true
      case IsNotNull(attribute) if isSearchableType(dataTypeMap(attribute)) => true
      case In(attribute, values) if isSearchableType(dataTypeMap(attribute)) => true

      case _ => false
    }
  }
}
