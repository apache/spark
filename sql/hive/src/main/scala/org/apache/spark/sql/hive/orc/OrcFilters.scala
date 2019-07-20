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

package org.apache.spark.sql.hive.orc

import java.lang.reflect.Method

import org.apache.hadoop.hive.ql.io.sarg.SearchArgument
import org.apache.hadoop.hive.ql.io.sarg.SearchArgument.Builder
import org.apache.hadoop.hive.ql.io.sarg.SearchArgumentFactory.newBuilder

import org.apache.spark.SparkException
import org.apache.spark.internal.Logging
import org.apache.spark.sql.execution.datasources.orc.{OrcFilters => DatasourceOrcFilters}
import org.apache.spark.sql.execution.datasources.orc.OrcFilters.buildTree
import org.apache.spark.sql.hive.HiveUtils
import org.apache.spark.sql.sources._
import org.apache.spark.sql.types._

/**
 * Helper object for building ORC `SearchArgument`s, which are used for ORC predicate push-down.
 *
 * Due to limitation of ORC `SearchArgument` builder, we had to end up with a pretty weird double-
 * checking pattern when converting `And`/`Or`/`Not` filters.
 *
 * An ORC `SearchArgument` must be built in one pass using a single builder.  For example, you can't
 * build `a = 1` and `b = 2` first, and then combine them into `a = 1 AND b = 2`.  This is quite
 * different from the cases in Spark SQL or Parquet, where complex filters can be easily built using
 * existing simpler ones.
 *
 * The annoying part is that, `SearchArgument` builder methods like `startAnd()`, `startOr()`, and
 * `startNot()` mutate internal state of the builder instance.  This forces us to translate all
 * convertible filters with a single builder instance. However, before actually converting a filter,
 * we've no idea whether it can be recognized by ORC or not. Thus, when an inconvertible filter is
 * found, we may already end up with a builder whose internal state is inconsistent.
 *
 * For example, to convert an `And` filter with builder `b`, we call `b.startAnd()` first, and then
 * try to convert its children.  Say we convert `left` child successfully, but find that `right`
 * child is inconvertible.  Alas, `b.startAnd()` call can't be rolled back, and `b` is inconsistent
 * now.
 *
 * The workaround employed here is that, for `And`/`Or`/`Not`, we first try to convert their
 * children with brand new builders, and only do the actual conversion with the right builder
 * instance when the children are proven to be convertible.
 *
 * P.S.: Hive seems to use `SearchArgument` together with `ExprNodeGenericFuncDesc` only.  Usage of
 * builder methods mentioned above can only be found in test code, where all tested filters are
 * known to be convertible.
 */
private[orc] object OrcFilters extends Logging {

  private def findMethod(klass: Class[_], name: String, args: Class[_]*): Method = {
    val method = klass.getMethod(name, args: _*)
    method.setAccessible(true)
    method
  }

  def createFilter(schema: StructType, filters: Array[Filter]): Option[SearchArgument] = {
    if (HiveUtils.isHive23) {
      DatasourceOrcFilters.createFilter(schema, filters).asInstanceOf[Option[SearchArgument]]
    } else {
      val dataTypeMap = schema.map(f => f.name -> f.dataType).toMap
      // Combines all convertible filters using `And` to produce a single conjunction
      val conjunctionOptional = buildTree(convertibleFilters(schema, dataTypeMap, filters))
      conjunctionOptional.map { conjunction =>
        // Then tries to build a single ORC `SearchArgument` for the conjunction predicate.
        // The input predicate is fully convertible. There should not be any empty result in the
        // following recursive method call `buildSearchArgument`.
        buildSearchArgument(dataTypeMap, conjunction, newBuilder).build()
      }
    }
  }

  def convertibleFilters(
      schema: StructType,
      dataTypeMap: Map[String, DataType],
      filters: Seq[Filter]): Seq[Filter] = {
    import org.apache.spark.sql.sources._

    def convertibleFiltersHelper(
        filter: Filter,
        canPartialPushDown: Boolean): Option[Filter] = filter match {
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
      case And(left, right) =>
        val leftResultOptional = convertibleFiltersHelper(left, canPartialPushDown)
        val rightResultOptional = convertibleFiltersHelper(right, canPartialPushDown)
        (leftResultOptional, rightResultOptional) match {
          case (Some(leftResult), Some(rightResult)) => Some(And(leftResult, rightResult))
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
      case Or(left, right) =>
        for {
          lhs <- convertibleFiltersHelper(left, canPartialPushDown)
          rhs <- convertibleFiltersHelper(right, canPartialPushDown)
        } yield Or(lhs, rhs)
      case Not(pred) =>
        val childResultOptional = convertibleFiltersHelper(pred, canPartialPushDown = false)
        childResultOptional.map(Not)
      case other =>
        for (_ <- buildLeafSearchArgument(dataTypeMap, other, newBuilder())) yield other
    }
    filters.flatMap { filter =>
      convertibleFiltersHelper(filter, true)
    }
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
      dataTypeMap: Map[String, DataType],
      expression: Filter,
      builder: Builder): Builder = {
    expression match {
      case And(left, right) =>
        val lhs = buildSearchArgument(dataTypeMap, left, builder.startAnd())
        val rhs = buildSearchArgument(dataTypeMap, right, lhs)
        rhs.end()

      case Or(left, right) =>
        val lhs = buildSearchArgument(dataTypeMap, left, builder.startOr())
        val rhs = buildSearchArgument(dataTypeMap, right, lhs)
        rhs.end()

      case Not(child) =>
        buildSearchArgument(dataTypeMap, child, builder.startNot()).end()

      case other =>
        buildLeafSearchArgument(dataTypeMap, other, builder).getOrElse {
          throw new SparkException(
            "The input filter of OrcFilters.buildSearchArgument should be fully convertible.")
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
      dataTypeMap: Map[String, DataType],
      expression: Filter,
      builder: Builder): Option[Builder] = {
    def isSearchableType(dataType: DataType): Boolean = dataType match {
      // Only the values in the Spark types below can be recognized by
      // the `SearchArgumentImpl.BuilderImpl.boxLiteral()` method.
      case ByteType | ShortType | FloatType | DoubleType => true
      case IntegerType | LongType | StringType | BooleanType => true
      case TimestampType | _: DecimalType => true
      case _ => false
    }

    import org.apache.spark.sql.sources._

    // NOTE: For all case branches dealing with leaf predicates below, the additional `startAnd()`
    // call is mandatory. ORC `SearchArgument` builder requires that all leaf predicates must be
    // wrapped by a "parent" predicate (`And`, `Or`, or `Not`).
    expression match {
      // NOTE: For all case branches dealing with leaf predicates below, the additional `startAnd()`
      // call is mandatory.  ORC `SearchArgument` builder requires that all leaf predicates must be
      // wrapped by a "parent" predicate (`And`, `Or`, or `Not`).

      case EqualTo(attribute, value) if isSearchableType(dataTypeMap(attribute)) =>
        val bd = builder.startAnd()
        val method = findMethod(bd.getClass, "equals", classOf[String], classOf[Object])
        Some(method.invoke(bd, attribute, value.asInstanceOf[AnyRef]).asInstanceOf[Builder].end())

      case EqualNullSafe(attribute, value) if isSearchableType(dataTypeMap(attribute)) =>
        val bd = builder.startAnd()
        val method = findMethod(bd.getClass, "nullSafeEquals", classOf[String], classOf[Object])
        Some(method.invoke(bd, attribute, value.asInstanceOf[AnyRef]).asInstanceOf[Builder].end())

      case LessThan(attribute, value) if isSearchableType(dataTypeMap(attribute)) =>
        val bd = builder.startAnd()
        val method = findMethod(bd.getClass, "lessThan", classOf[String], classOf[Object])
        Some(method.invoke(bd, attribute, value.asInstanceOf[AnyRef]).asInstanceOf[Builder].end())

      case LessThanOrEqual(attribute, value) if isSearchableType(dataTypeMap(attribute)) =>
        val bd = builder.startAnd()
        val method = findMethod(bd.getClass, "lessThanEquals", classOf[String], classOf[Object])
        Some(method.invoke(bd, attribute, value.asInstanceOf[AnyRef]).asInstanceOf[Builder].end())

      case GreaterThan(attribute, value) if isSearchableType(dataTypeMap(attribute)) =>
        val bd = builder.startNot()
        val method = findMethod(bd.getClass, "lessThanEquals", classOf[String], classOf[Object])
        Some(method.invoke(bd, attribute, value.asInstanceOf[AnyRef]).asInstanceOf[Builder].end())

      case GreaterThanOrEqual(attribute, value) if isSearchableType(dataTypeMap(attribute)) =>
        val bd = builder.startNot()
        val method = findMethod(bd.getClass, "lessThan", classOf[String], classOf[Object])
        Some(method.invoke(bd, attribute, value.asInstanceOf[AnyRef]).asInstanceOf[Builder].end())

      case IsNull(attribute) if isSearchableType(dataTypeMap(attribute)) =>
        val bd = builder.startAnd()
        val method = findMethod(bd.getClass, "isNull", classOf[String])
        Some(method.invoke(bd, attribute).asInstanceOf[Builder].end())

      case IsNotNull(attribute) if isSearchableType(dataTypeMap(attribute)) =>
        val bd = builder.startNot()
        val method = findMethod(bd.getClass, "isNull", classOf[String])
        Some(method.invoke(bd, attribute).asInstanceOf[Builder].end())

      case In(attribute, values) if isSearchableType(dataTypeMap(attribute)) =>
        val bd = builder.startAnd()
        val method = findMethod(bd.getClass, "in", classOf[String], classOf[Array[Object]])
        Some(method.invoke(bd, attribute, values.map(_.asInstanceOf[AnyRef]))
          .asInstanceOf[Builder].end())

      case _ => None
    }
  }
}
