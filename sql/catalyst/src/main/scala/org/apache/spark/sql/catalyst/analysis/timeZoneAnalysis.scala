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
package org.apache.spark.sql.catalyst.analysis

import org.apache.spark.sql.catalyst.expressions.{Cast, Expression, ListQuery, TimeZoneAwareExpression}
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.types.DataType

/**
 * Replace [[TimeZoneAwareExpression]] without timezone id by its copy with session local
 * time zone.
 */
case class ResolveTimeZone(conf: SQLConf) extends Rule[LogicalPlan] {
  private val transformTimeZoneExprs: PartialFunction[Expression, Expression] = {
    case e: TimeZoneAwareExpression if e.timeZoneId.isEmpty =>
      e.withTimeZone(conf.sessionLocalTimeZone)
    // Casts could be added in the subquery plan through the rule TypeCoercion while coercing
    // the types between the value expression and list query expression of IN expression.
    // We need to subject the subquery plan through ResolveTimeZone again to setup timezone
    // information for time zone aware expressions.
    case e: ListQuery => e.withNewPlan(apply(e.plan))
  }

  override def apply(plan: LogicalPlan): LogicalPlan =
    plan.resolveExpressions(transformTimeZoneExprs)

  def resolveTimeZones(e: Expression): Expression = e.transform(transformTimeZoneExprs)
}

/**
 * Mix-in trait for constructing valid [[Cast]] expressions.
 */
trait CastSupport {
  /**
   * Configuration used to create a valid cast expression.
   */
  def conf: SQLConf

  /**
   * Create a Cast expression with the session local time zone.
   */
  def cast(child: Expression, dataType: DataType): Cast = {
    Cast(child, dataType, Option(conf.sessionLocalTimeZone))
  }
}
