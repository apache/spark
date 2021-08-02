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

package org.apache.spark.sql.catalyst.optimizer

import scala.collection.mutable

import org.apache.spark.sql.catalyst.CurrentUserContext.CURRENT_USER
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.expressions.aggregate._
import org.apache.spark.sql.catalyst.plans.logical._
import org.apache.spark.sql.catalyst.rules._
import org.apache.spark.sql.catalyst.trees.TreePattern._
import org.apache.spark.sql.connector.catalog.CatalogManager
import org.apache.spark.sql.types._
import org.apache.spark.util.Utils


/**
 * Finds all the expressions that are unevaluable and replace/rewrite them with semantically
 * equivalent expressions that can be evaluated. Currently we replace two kinds of expressions:
 * 1) [[RuntimeReplaceable]] expressions
 * 2) [[UnevaluableAggregate]] expressions such as Every, Some, Any, CountIf
 * This is mainly used to provide compatibility with other databases.
 * Few examples are:
 *   we use this to support "nvl" by replacing it with "coalesce".
 *   we use this to replace Every and Any with Min and Max respectively.
 *
 * TODO: In future, explore an option to replace aggregate functions similar to
 * how RuntimeReplaceable does.
 */
object ReplaceExpressions extends Rule[LogicalPlan] {
  def apply(plan: LogicalPlan): LogicalPlan = plan.transformAllExpressionsWithPruning(
    _.containsAnyPattern(RUNTIME_REPLACEABLE, COUNT_IF, BOOL_AGG)) {
    case e: RuntimeReplaceable => e.child
    case CountIf(predicate) => Count(new NullIf(predicate, Literal.FalseLiteral))
    case BoolOr(arg) => Max(arg)
    case BoolAnd(arg) => Min(arg)
  }
}

/**
 * Rewrite non correlated exists subquery to use ScalarSubquery
 *   WHERE EXISTS (SELECT A FROM TABLE B WHERE COL1 > 10)
 * will be rewritten to
 *   WHERE (SELECT 1 FROM (SELECT A FROM TABLE B WHERE COL1 > 10) LIMIT 1) IS NOT NULL
 */
object RewriteNonCorrelatedExists extends Rule[LogicalPlan] {
  override def apply(plan: LogicalPlan): LogicalPlan = plan.transformAllExpressionsWithPruning(
    _.containsPattern(EXISTS_SUBQUERY)) {
    case exists: Exists if exists.children.isEmpty =>
      IsNotNull(
        ScalarSubquery(
          plan = Limit(Literal(1), Project(Seq(Alias(Literal(1), "col")()), exists.plan)),
          exprId = exists.exprId))
  }
}

/**
 * Computes the current date and time to make sure we return the same result in a single query.
 */
object ComputeCurrentTime extends Rule[LogicalPlan] {
  def apply(plan: LogicalPlan): LogicalPlan = {
    val currentDates = mutable.Map.empty[String, Literal]
    val timeExpr = CurrentTimestamp()
    val timestamp = timeExpr.eval(EmptyRow).asInstanceOf[Long]
    val currentTime = Literal.create(timestamp, timeExpr.dataType)
    val timezone = Literal.create(conf.sessionLocalTimeZone, StringType)
    val localTimestamps = mutable.Map.empty[String, Literal]

    plan.transformAllExpressionsWithPruning(_.containsPattern(CURRENT_LIKE)) {
      case currentDate @ CurrentDate(Some(timeZoneId)) =>
        currentDates.getOrElseUpdate(timeZoneId, {
          Literal.create(currentDate.eval().asInstanceOf[Int], DateType)
        })
      case CurrentTimestamp() | Now() => currentTime
      case CurrentTimeZone() => timezone
      case localTimestamp @ LocalTimestamp(Some(timeZoneId)) =>
        localTimestamps.getOrElseUpdate(timeZoneId, {
          Literal.create(localTimestamp.eval().asInstanceOf[Long], TimestampNTZType)
        })
    }
  }
}


/**
 * Replaces the expression of CurrentDatabase with the current database name.
 * Replaces the expression of CurrentCatalog with the current catalog name.
 */
case class ReplaceCurrentLike(catalogManager: CatalogManager) extends Rule[LogicalPlan] {
  def apply(plan: LogicalPlan): LogicalPlan = {
    import org.apache.spark.sql.connector.catalog.CatalogV2Implicits._
    val currentNamespace = catalogManager.currentNamespace.quoted
    val currentCatalog = catalogManager.currentCatalog.name()
    val currentUser = Option(CURRENT_USER.get()).getOrElse(Utils.getCurrentUserName())

    plan.transformAllExpressionsWithPruning(_.containsPattern(CURRENT_LIKE)) {
      case CurrentDatabase() =>
        Literal.create(currentNamespace, StringType)
      case CurrentCatalog() =>
        Literal.create(currentCatalog, StringType)
      case CurrentUser() =>
        Literal.create(currentUser, StringType)
    }
  }
}
