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

import org.apache.hadoop.hive.common.`type`.{HiveChar, HiveDecimal, HiveVarchar}
import org.apache.hadoop.hive.ql.io.sarg.{SearchArgumentFactory, SearchArgument}
import org.apache.hadoop.hive.ql.io.sarg.SearchArgument.Builder
import org.apache.hadoop.hive.serde2.io.DateWritable

import org.apache.spark.Logging
import org.apache.spark.sql.sources._

/**
 * It may be optimized by push down partial filters. But we are conservative here.
 * Because if some filters fail to be parsed, the tree may be corrupted,
 * and cannot be used anymore.
 */
private[orc] object OrcFilters extends Logging {
  def createFilter(filters: Array[Filter]): Option[SearchArgument] = {
    for {
      // Combines all filters with `And`s to produce a single conjunction predicate
      conjunction <- filters.reduceOption(And)
      // Then tries to build a single ORC `SearchArgument` for the conjunction predicate
      builder <- buildSearchArgument(conjunction, SearchArgumentFactory.newBuilder())
    } yield builder.build()
  }

  private def buildSearchArgument(expression: Filter, builder: Builder): Option[Builder] = {
    def newBuilder = SearchArgumentFactory.newBuilder()

    def isSearchableLiteral(value: Any): Boolean = value match {
      // These are types recognized by the `SearchArgumentImpl.BuilderImpl.boxLiteral()` method.
      case _: String | _: Long | _: Double | _: Byte | _: Short | _: Integer | _: Float => true
      case _: DateWritable | _: HiveDecimal | _: HiveChar | _: HiveVarchar => true
      case _ => false
    }

    // lian: I probably missed something here, and had to end up with a pretty weird double-checking
    // pattern when converting `And`/`Or`/`Not` filters.
    //
    // The annoying part is that, `SearchArgument` builder methods like `startAnd()` `startOr()`,
    // and `startNot()` mutate internal state of the builder instance.  This forces us to translate
    // all convertible filters with a single builder instance. However, before actually converting a
    // filter, we've no idea whether it can be recognized by ORC or not. Thus, when an inconvertible
    // filter is found, we may already end up with a builder whose internal state is inconsistent.
    //
    // For example, to convert an `And` filter with builder `b`, we call `b.startAnd()` first, and
    // then try to convert its children.  Say we convert `left` child successfully, but find that
    // `right` child is inconvertible.  Alas, `b.startAnd()` call can't be rolled back, and `b` is
    // inconsistent now.
    //
    // The workaround employed here is that, for `And`/`Or`/`Not`, we first try to convert their
    // children with brand new builders, and only do the actual conversion with the right builder
    // instance when the children are proven to be convertible.
    //
    // P.S.: Hive seems to use `SearchArgument` together with `ExprNodeGenericFuncDesc` only.
    // Usage of builder methods mentioned above can only be found in test code, where all tested
    // filters are known to be convertible.

    expression match {
      case And(left, right) =>
        // At here, it is not safe to just convert one side if we do not understand the
        // other side. Here is an example used to explain the reason.
        // Let's say we have NOT(a = 2 AND b in ('1')) and we do not understand how to
        // convert b in ('1'). If we only convert a = 2, we will end up with a filter
        // NOT(a = 2), which will generate wrong results.
        // Pushing one side of AND down is only safe to do at the top level.
        // You can see ParquetRelation's initializeLocalJobFunc method as an example.
        for {
          _ <- buildSearchArgument(left, newBuilder)
          _ <- buildSearchArgument(right, newBuilder)
          lhs <- buildSearchArgument(left, builder.startAnd())
          rhs <- buildSearchArgument(right, lhs)
        } yield rhs.end()

      case Or(left, right) =>
        for {
          _ <- buildSearchArgument(left, newBuilder)
          _ <- buildSearchArgument(right, newBuilder)
          lhs <- buildSearchArgument(left, builder.startOr())
          rhs <- buildSearchArgument(right, lhs)
        } yield rhs.end()

      case Not(child) =>
        for {
          _ <- buildSearchArgument(child, newBuilder)
          negate <- buildSearchArgument(child, builder.startNot())
        } yield negate.end()

      case EqualTo(attribute, value) if isSearchableLiteral(value) =>
        Some(builder.startAnd().equals(attribute, value).end())

      case EqualNullSafe(attribute, value) if isSearchableLiteral(value) =>
        Some(builder.startAnd().nullSafeEquals(attribute, value).end())

      case LessThan(attribute, value) if isSearchableLiteral(value) =>
        Some(builder.startAnd().lessThan(attribute, value).end())

      case LessThanOrEqual(attribute, value) if isSearchableLiteral(value) =>
        Some(builder.startAnd().lessThanEquals(attribute, value).end())

      case GreaterThan(attribute, value) if isSearchableLiteral(value) =>
        Some(builder.startNot().lessThanEquals(attribute, value).end())

      case GreaterThanOrEqual(attribute, value) if isSearchableLiteral(value) =>
        Some(builder.startNot().lessThan(attribute, value).end())

      case IsNull(attribute) =>
        Some(builder.startAnd().isNull(attribute).end())

      case IsNotNull(attribute) =>
        Some(builder.startNot().isNull(attribute).end())

      case In(attribute, values) if values.forall(isSearchableLiteral) =>
        Some(builder.startAnd().in(attribute, values.map(_.asInstanceOf[AnyRef]): _*).end())

      case _ => None
    }
  }
}
