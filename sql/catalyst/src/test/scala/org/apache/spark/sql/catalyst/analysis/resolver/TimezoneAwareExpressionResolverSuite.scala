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

package org.apache.spark.sql.catalyst.analysis.resolver

import org.scalatestplus.mockito.MockitoSugar.mock

import org.apache.spark.SparkFunSuite
import org.apache.spark.sql.catalyst.analysis.FunctionResolution
import org.apache.spark.sql.catalyst.expressions.{
  AttributeReference,
  Cast,
  Expression,
  TimeZoneAwareExpression
}
import org.apache.spark.sql.catalyst.plans.logical.OneRowRelation
import org.apache.spark.sql.connector.catalog.CatalogManager
import org.apache.spark.sql.types.{DataType, DateType, IntegerType, StringType, TimestampLTZNanosType, TimestampNTZNanosType, TimestampNTZType, TimestampType}

class TimezoneAwareExpressionResolverSuite extends SparkFunSuite {

  class HardCodedExpressionResolver(catalogManager: CatalogManager, resolvedExpression: Expression)
      extends ExpressionResolver(
        resolver = new Resolver(catalogManager),
        functionResolution =
          new FunctionResolution(
            catalogManager,
            Resolver.createRelationResolution(catalogManager)),
        planLogger = new PlanLogger
      ) {
    override def resolve(expression: Expression): Expression = resolvedExpression
  }

  private val unresolvedChild =
    AttributeReference(name = "unresolvedChild", dataType = StringType)()
  private val resolvedChild = AttributeReference(name = "resolvedChild", dataType = IntegerType)()
  private val castExpression = Cast(child = unresolvedChild, dataType = IntegerType)
  private val nestedCasts = Cast(
    child = Cast(
      child = Cast(child = unresolvedChild, dataType = IntegerType, timeZoneId = Some("UTC")),
      dataType = IntegerType,
      timeZoneId = None
    ),
    dataType = IntegerType,
    timeZoneId = None
  )
  private val expressionResolver = new HardCodedExpressionResolver(
    catalogManager = mock[CatalogManager],
    resolvedExpression = resolvedChild
  )
  private val timezoneAwareExpressionResolver = new TimezoneAwareExpressionResolver(
    expressionResolver
  )

  test("TimeZoneAwareExpression resolution") {
    assert(castExpression.children.head == unresolvedChild)
    assert(castExpression.timeZoneId.isEmpty)
    assert(castExpression.getTagValue(Cast.USER_SPECIFIED_CAST).isEmpty)

    castExpression.setTagValue(Cast.USER_SPECIFIED_CAST, ())
    val resolvedExpression =
      expressionResolver.getExpressionTreeTraversals.withNewTraversal(OneRowRelation()) {
        timezoneAwareExpressionResolver
          .resolve(castExpression)
          .asInstanceOf[TimeZoneAwareExpression]
      }

    assert(resolvedExpression.children.head == resolvedChild)
    assert(resolvedExpression.timeZoneId.nonEmpty)
    assert(resolvedExpression.getTagValue(Cast.USER_SPECIFIED_CAST).nonEmpty)
  }

  test("Timezone is applied recursively") {
    val expressionWithTimezone =
      TimezoneAwareExpressionResolver.resolveTimezone(nestedCasts, "UTC")

    assert(expressionWithTimezone.asInstanceOf[Cast].timeZoneId.get == "UTC")
    assert(
      expressionWithTimezone.asInstanceOf[Cast].child.asInstanceOf[Cast].timeZoneId.get == "UTC"
    )
  }

  // SPARK-57323: DATE <-> nanos casts are not directly castable, so the single-pass resolver must
  // apply the same rewrite as the fixed-point `ResolveTimestampNanosCast` rule (cast through the
  // microsecond timestamp type); otherwise single-pass resolution would fail the cast's input type
  // check while fixed-point succeeds.
  private def resolveCast(resolvedChild: Expression, cast: Cast): Cast = {
    val resolver = new HardCodedExpressionResolver(
      catalogManager = mock[CatalogManager],
      resolvedExpression = resolvedChild
    )
    val tzResolver = new TimezoneAwareExpressionResolver(resolver)
    resolver.getExpressionTreeTraversals
      .withNewTraversal(OneRowRelation()) {
        tzResolver.resolve(cast)
      }
      .asInstanceOf[Cast]
  }

  private def assertRewrittenThroughMicros(
      outer: Cast,
      expectedOuterType: DataType,
      expectedMicrosType: DataType,
      expectedInnermost: Expression): Unit = {
    assert(outer.dataType == expectedOuterType)
    assert(outer.timeZoneId.nonEmpty)
    val inner = outer.child.asInstanceOf[Cast]
    assert(inner.dataType == expectedMicrosType)
    assert(inner.timeZoneId.nonEmpty)
    assert(inner.child == expectedInnermost)
  }

  test("SPARK-57323: single-pass rewrites DATE -> nanos cast through the micros timestamp type") {
    val dateChild = AttributeReference(name = "d", dataType = DateType)()
    Seq(
      TimestampNTZNanosType(TimestampNTZNanosType.MAX_PRECISION) -> TimestampNTZType,
      TimestampLTZNanosType(TimestampLTZNanosType.MAX_PRECISION) -> TimestampType
    ).foreach { case (nanos, micros) =>
      val resolved = resolveCast(dateChild, Cast(child = unresolvedChild, dataType = nanos))
      assertRewrittenThroughMicros(resolved, nanos, micros, dateChild)
    }
  }

  test("SPARK-57323: single-pass rewrites nanos -> DATE cast through the micros timestamp type") {
    Seq(
      TimestampNTZNanosType(TimestampNTZNanosType.MAX_PRECISION) -> TimestampNTZType,
      TimestampLTZNanosType(TimestampLTZNanosType.MAX_PRECISION) -> TimestampType
    ).foreach { case (nanos, micros) =>
      val nanosChild = AttributeReference(name = "n", dataType = nanos)()
      val resolved = resolveCast(nanosChild, Cast(child = unresolvedChild, dataType = DateType))
      assertRewrittenThroughMicros(resolved, DateType, micros, nanosChild)
    }
  }
}
