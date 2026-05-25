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

case class AsOfTimestamp(timestamp: Long) extends TimeTravelSpec {
  override def toString: String = s"TIMESTAMP AS OF $timestamp"
}

case class AsOfVersion(version: String) extends TimeTravelSpec {
  override def toString: String = s"VERSION AS OF '$version'"
}

case class AsOfBranch(branch: String, isExplicit: Boolean = true) extends TimeTravelSpec {
  override def toString: String = s"BRANCH '$branch'"
}

object TimeTravelSpec {

  /**
   * Evaluate a resolved timestamp expression to microseconds since epoch.
   * Shared by time travel and CDC timestamp resolution.
   */
  def resolveTimestampExpression(ts: Expression, sessionLocalTimeZone: String): Long = {
    assert(ts.resolved && ts.references.isEmpty)
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
    value.asInstanceOf[Long]
  }

  def create(
      timestamp: Option[Expression],
      version: Option[String],
      branch: Option[String],
      sessionLocalTimeZone: String) : Option[TimeTravelSpec] = {
    val nonEmptyCount = Seq(timestamp.nonEmpty, version.nonEmpty, branch.nonEmpty).count(identity)
    if (nonEmptyCount > 1) {
      throw QueryCompilationErrors.invalidTimeTravelSpecError()
    } else if (timestamp.nonEmpty) {
      val ts = timestamp.get
      assert(!SubqueryExpression.hasSubquery(ts))
      Some(AsOfTimestamp(resolveTimestampExpression(ts, sessionLocalTimeZone)))
    } else if (version.nonEmpty) {
      Some(AsOfVersion(version.get))
    } else if (branch.nonEmpty) {
      Some(AsOfBranch(branch.get))
    } else {
      None
    }
  }

  // Kept for binary compatibility with callers that only know about timestamp/version.
  def create(
      timestamp: Option[Expression],
      version: Option[String],
      sessionLocalTimeZone: String) : Option[TimeTravelSpec] = {
    create(timestamp, version, None, sessionLocalTimeZone)
  }

  def fromOptions(
      options: CaseInsensitiveStringMap,
      timestampKey: String,
      versionKey: String,
      sessionLocalTimeZone: String): Option[TimeTravelSpec] = {
    fromOptions(options, timestampKey, versionKey, branchKey = null, sessionLocalTimeZone)
  }

  def fromOptions(
      options: CaseInsensitiveStringMap,
      timestampKey: String,
      versionKey: String,
      branchKey: String,
      sessionLocalTimeZone: String): Option[TimeTravelSpec] = {
    val timestampOpt = Option(options.get(timestampKey))
    val versionOpt = Option(options.get(versionKey))
    val branchOpt = Option(branchKey).flatMap(k => Option(options.get(k)))

    val provided = Seq(timestampOpt.nonEmpty, versionOpt.nonEmpty, branchOpt.nonEmpty)
      .count(identity)
    if (provided > 1) {
      throw QueryCompilationErrors.invalidTimeTravelSpecError()
    }

    if (timestampOpt.isDefined) {
      val timestampStr = timestampOpt.get
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
    } else if (versionOpt.isDefined) {
      Some(AsOfVersion(versionOpt.get))
    } else if (branchOpt.isDefined) {
      Some(AsOfBranch(branchOpt.get))
    } else {
      None
    }
  }
}
