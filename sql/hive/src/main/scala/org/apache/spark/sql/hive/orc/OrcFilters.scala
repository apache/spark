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
  def createFilter(expr: Array[Filter]): Option[SearchArgument] = {
    expr.reduceOption(And).flatMap { conjunction =>
      val builder = SearchArgumentFactory.newBuilder()
      buildSearchArgument(conjunction, builder).map(_.build())
    }
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
        val tryLeft = buildSearchArgument(left, newBuilder)
        val tryRight = buildSearchArgument(right, newBuilder)

        val conjunction = for {
          _ <- tryLeft
          _ <- tryRight
          lhs <- buildSearchArgument(left, builder.startAnd())
          rhs <- buildSearchArgument(right, lhs)
        } yield rhs.end()

        // For filter `left AND right`, we can still push down `left` even if `right` is not
        // convertible, and vice versa.
        conjunction
          .orElse(tryLeft.flatMap(_ => buildSearchArgument(left, builder)))
          .orElse(tryRight.flatMap(_ => buildSearchArgument(right, builder)))

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

      case EqualTo(attribute, value) =>
        Option(value)
          .filter(isSearchableLiteral)
          .map(builder.equals(attribute, _))

      case LessThan(attribute, value) =>
        Option(value)
          .filter(isSearchableLiteral)
          .map(builder.lessThan(attribute, _))

      case LessThanOrEqual(attribute, value) =>
        Option(value)
          .filter(isSearchableLiteral)
          .map(builder.lessThanEquals(attribute, _))

      case GreaterThan(attribute, value) =>
        Option(value)
          .filter(isSearchableLiteral)
          .map(builder.startNot().lessThanEquals(attribute, _).end())

      case GreaterThanOrEqual(attribute, value) =>
        Option(value)
          .filter(isSearchableLiteral)
          .map(builder.startNot().lessThan(attribute, _).end())

      case IsNull(attribute) =>
        Some(builder.isNull(attribute))

      case IsNotNull(attribute) =>
        Some(builder.startNot().isNull(attribute).end())

      case In(attribute, values) =>
        Option(values)
          .filter(_.forall(isSearchableLiteral))
          .map(builder.in(attribute, _))

      case _ => None
    }
  }
}
