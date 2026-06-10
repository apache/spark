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

import org.apache.spark.sql.catalyst.expressions.Cast
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.catalyst.trees.TreePattern.CAST
import org.apache.spark.sql.types.{DataType, DateType, TimestampLTZNanosType, TimestampNTZNanosType, TimestampNTZType, TimestampType}

/**
 * Rewrites casts between [[DateType]] and the nanosecond-precision timestamp types
 * ([[TimestampLTZNanosType]] / [[TimestampNTZNanosType]]) into a two-step cast that goes through
 * the corresponding microsecond-precision timestamp type:
 *
 *   - `nanos(p) -> DATE`  ==>  `nanos(p) -> micros -> DATE`
 *   - `DATE -> nanos(p)`  ==>  `DATE -> micros -> nanos(p)`
 *
 * where the microsecond counterpart is `TIMESTAMP` for `*_LTZ` and `TIMESTAMP_NTZ` for `*_NTZ`.
 * Both component casts already exist (`nanos(p) <-> micros` and `micros <-> DATE`), so no dedicated
 * `DATE <-> nanos` conversion is needed in [[Cast]] and the semantics match the microsecond
 * `DATE <-> TIMESTAMP` casts: the LTZ directions are resolved in the session time zone and the NTZ
 * directions on the UTC wall-clock grid; sub-microsecond digits and the time-of-day are dropped
 * when narrowing to `DATE`.
 *
 * `Cast.canCast` intentionally does not allow `DATE <-> nanos` directly, so such a cast stays
 * unresolved until this rule rewrites it into the resolvable nested form. The new casts inherit the
 * original `timeZoneId` and `evalMode`; the only zone-sensitive part (the `micros <-> DATE` cast)
 * gets its session time zone from [[ResolveTimeZone]] within the same fixed-point batch.
 *
 * The per-cast rewrite is exposed via [[rewriteDateNanosCast]] so that the single-pass resolver
 * (see `TimezoneAwareExpressionResolver`) can apply the same transformation and produce an
 * identical plan; otherwise single-pass resolution would fail the cast's input type check while
 * fixed-point succeeds.
 */
object ResolveTimestampNanosCast extends Rule[LogicalPlan] {

  /** The microsecond-precision timestamp counterpart of a nanosecond-precision timestamp type. */
  private def microTimestampType(dt: DataType): Option[DataType] = dt match {
    case _: TimestampLTZNanosType => Some(TimestampType)
    case _: TimestampNTZNanosType => Some(TimestampNTZType)
    case _ => None
  }

  /**
   * If `cast` converts between [[DateType]] and a nanosecond-precision timestamp type, returns the
   * equivalent two-step cast through the corresponding microsecond-precision timestamp type;
   * returns `None` for any other cast. The nested casts inherit the original `timeZoneId` and
   * `evalMode`.
   */
  def rewriteDateNanosCast(cast: Cast): Option[Cast] = cast match {
    // nanos(p) -> DATE  ==>  nanos(p) -> micros -> DATE
    case Cast(child, DateType, tz, mode) if child.resolved =>
      microTimestampType(child.dataType).map { micros =>
        Cast(Cast(child, micros, tz, mode), DateType, tz, mode)
      }
    // DATE -> nanos(p)  ==>  DATE -> micros -> nanos(p)
    case Cast(child, dt, tz, mode) if child.resolved && child.dataType == DateType =>
      microTimestampType(dt).map { micros =>
        Cast(Cast(child, micros, tz, mode), dt, tz, mode)
      }
    case _ => None
  }

  override def apply(plan: LogicalPlan): LogicalPlan =
    plan.resolveExpressionsWithPruning(_.containsPattern(CAST), ruleId) {
      case cast: Cast => rewriteDateNanosCast(cast).getOrElse(cast)
    }
}
