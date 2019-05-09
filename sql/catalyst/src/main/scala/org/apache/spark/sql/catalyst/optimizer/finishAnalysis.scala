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

import java.time.LocalDate

import scala.collection.mutable

import org.apache.spark.sql.catalyst.catalog.SessionCatalog
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.expressions.aggregate._
import org.apache.spark.sql.catalyst.plans.logical._
import org.apache.spark.sql.catalyst.rules._
import org.apache.spark.sql.catalyst.util.DateTimeUtils
import org.apache.spark.sql.types._


/**
 * Finds all the expressions that are unevaluable and replace/rewrite them with semantically
 * equivalent expressions that can be evaluated. Currently we replace two kinds of expressions:
 * 1) [[RuntimeReplaceable]] expressions
 * 2) [[UnevaluableAggregate]] expressions such as Every, Some, Any
 * This is mainly used to provide compatibility with other databases.
 * Few examples are:
 *   we use this to support "nvl" by replacing it with "coalesce".
 *   we use this to replace Every and Any with Min and Max respectively.
 *
 * TODO: In future, explore an option to replace aggregate functions similar to
 * how RruntimeReplaceable does.
 */
object ReplaceExpressions extends Rule[LogicalPlan] {
  def apply(plan: LogicalPlan): LogicalPlan = plan transformAllExpressions {
    case e: RuntimeReplaceable => e.child
    case SomeAgg(arg) => Max(arg)
    case AnyAgg(arg) => Max(arg)
    case EveryAgg(arg) => Min(arg)
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

    plan transformAllExpressions {
      case CurrentDate(Some(timeZoneId)) =>
        currentDates.getOrElseUpdate(timeZoneId, {
          Literal.create(
            LocalDate.now(DateTimeUtils.getZoneId(timeZoneId)),
            DateType)
        })
      case CurrentTimestamp() => currentTime
    }
  }
}


/** Replaces the expression of CurrentDatabase with the current database name. */
case class GetCurrentDatabase(sessionCatalog: SessionCatalog) extends Rule[LogicalPlan] {
  def apply(plan: LogicalPlan): LogicalPlan = {
    plan transformAllExpressions {
      case CurrentDatabase() =>
        Literal.create(sessionCatalog.getCurrentDatabase, StringType)
    }
  }
}
