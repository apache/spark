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
import org.apache.spark.sql.types.{IntegerType, StringType}

class TimezoneAwareExpressionResolverSuite extends SparkFunSuite {

  class HardCodedExpressionResolver(catalogManager: CatalogManager, resolvedExpression: Expression)
      extends ExpressionResolver(
        resolver = new Resolver(catalogManager),
        functionResolution =
          new FunctionResolution(catalogManager, Resolver.createRelationResolution(catalogManager)),
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
}
