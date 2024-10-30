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

import org.apache.spark.sql.AnalysisException
import org.apache.spark.sql.catalyst.expressions.{Alias, Cast, Expression, Literal, SubqueryExpression, Unevaluable}
import org.apache.spark.sql.catalyst.optimizer.{ComputeCurrentTime, ReplaceExpressions}
import org.apache.spark.sql.catalyst.plans.logical.{OneRowRelation, Project}
import org.apache.spark.sql.errors.QueryCompilationErrors
import org.apache.spark.sql.types.TimestampType
import org.apache.spark.sql.util.CaseInsensitiveStringMap

sealed trait TimeTravelSpec

case class AsOfTimestamp(timestamp: Long) extends TimeTravelSpec
case class AsOfVersion(version: String) extends TimeTravelSpec

object TimeTravelSpec {
  def create(
      timestamp: Option[Expression],
      version: Option[String],
      sessionLocalTimeZone: String) : Option[TimeTravelSpec] = {
    if (timestamp.nonEmpty && version.nonEmpty) {
      throw QueryCompilationErrors.invalidTimeTravelSpecError()
    } else if (timestamp.nonEmpty) {
      val ts = timestamp.get
      assert(ts.resolved && ts.references.isEmpty && !SubqueryExpression.hasSubquery(ts))
      if (!Cast.canAnsiCast(ts.dataType, TimestampType)) {
        throw QueryCompilationErrors.invalidTimestampExprForTimeTravel(
          "INVALID_TIME_TRAVEL_TIMESTAMP_EXPR.INPUT", ts)
      }
      val tsToEval = {
        val fakeProject = Project(Seq(Alias(ts, "ts")()), OneRowRelation())
        ComputeCurrentTime(ReplaceExpressions(fakeProject)).asInstanceOf[Project]
          .expressions.head.asInstanceOf[Alias].child
      }
      tsToEval.foreach {
        case _: Unevaluable =>
          throw QueryCompilationErrors.invalidTimestampExprForTimeTravel(
            "INVALID_TIME_TRAVEL_TIMESTAMP_EXPR.UNEVALUABLE", ts)
        case e if !e.deterministic =>
          throw QueryCompilationErrors.invalidTimestampExprForTimeTravel(
            "INVALID_TIME_TRAVEL_TIMESTAMP_EXPR.NON_DETERMINISTIC", ts)
        case _ =>
      }
      val tz = Some(sessionLocalTimeZone)
      // Set `ansiEnabled` to false, so that it can return null for invalid input and we can provide
      // better error message.
      val value = Cast(tsToEval, TimestampType, tz, ansiEnabled = false).eval()
      if (value == null) {
        throw QueryCompilationErrors.invalidTimestampExprForTimeTravel(
          "INVALID_TIME_TRAVEL_TIMESTAMP_EXPR.INPUT", ts)
      }
      Some(AsOfTimestamp(value.asInstanceOf[Long]))
    } else if (version.nonEmpty) {
      Some(AsOfVersion(version.get))
    } else {
      None
    }
  }

  def fromOptions(
      options: CaseInsensitiveStringMap,
      timestampKey: String,
      versionKey: String,
      sessionLocalTimeZone: String): Option[TimeTravelSpec] = {
    (Option(options.get(timestampKey)), Option(options.get(versionKey))) match {
      case (Some(_), Some(_)) =>
        throw QueryCompilationErrors.invalidTimeTravelSpecError()

      case (Some(timestampStr), None) =>
        val timestampValue = Cast(
          Literal(timestampStr),
          TimestampType,
          Some(sessionLocalTimeZone),
          ansiEnabled = false
        ).eval()
        if (timestampValue == null) {
          throw new AnalysisException(
            "INVALID_TIME_TRAVEL_TIMESTAMP_EXPR.OPTION",
            Map("expr" -> s"'$timestampStr'")
          )
        }
        Some(AsOfTimestamp(timestampValue.asInstanceOf[Long]))

      case (None, Some(versionStr)) =>
        Some(AsOfVersion(versionStr))

      case _ => None
    }
  }
}
