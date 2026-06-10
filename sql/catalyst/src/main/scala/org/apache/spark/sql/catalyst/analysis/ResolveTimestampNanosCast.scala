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
import org.apache.spark.sql.types.{ArrayType, DataType, DateType, MapType, StructType, TimestampLTZNanosType, TimestampNTZNanosType, TimestampNTZType, TimestampType}

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
 * The same rewrite applies when the `DATE <-> nanos` pair is nested inside complex types
 * ([[ArrayType]] / [[MapType]] / [[StructType]]) at any depth, e.g.
 * `ARRAY<nanos(p)> -> ARRAY<DATE>` becomes `ARRAY<nanos(p)> -> ARRAY<micros> -> ARRAY<DATE>`. The
 * intermediate type mirrors the structure (and nullability) of the target type, with every
 * `DATE <-> nanos` leaf swapped for the corresponding microsecond-precision timestamp type, so that
 * both component casts pass `Cast.canCast`.
 *
 * `Cast.canCast` intentionally does not allow `DATE <-> nanos` directly (it recurses into complex
 * types, so nested pairs are rejected too), so such a cast stays unresolved until this rule
 * rewrites it into the resolvable nested form. The new casts inherit the original `timeZoneId` and
 * `evalMode`; the only zone-sensitive part (the `micros <-> DATE` cast) gets its session time zone
 * from [[ResolveTimeZone]] within the same fixed-point batch.
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
   * Computes the intermediate ("bridge") type to route a `from -> to` cast through when the
   * conversion involves a `DATE <-> nanos(p)` pair at any nesting depth. The bridge mirrors the
   * structure (and nullability) of `to`, with every `DATE <-> nanos` leaf replaced by the
   * corresponding microsecond-precision timestamp type. Returns `None` when no `DATE <-> nanos`
   * pair is present, in which case [[Cast]] already handles the conversion directly.
   */
  private def bridgeType(from: DataType, to: DataType): Option[DataType] = (from, to) match {
    // Scalar DATE <-> nanos(p): route through the microsecond counterpart of the nanos side.
    case (DateType, _) => microTimestampType(to)
    case (_, DateType) => microTimestampType(from)

    case (ArrayType(fromEl, _), ArrayType(toEl, toContainsNull)) =>
      bridgeType(fromEl, toEl).map(ArrayType(_, toContainsNull))

    case (MapType(fromKey, fromVal, _), MapType(toKey, toVal, toValContainsNull)) =>
      val keyBridge = bridgeType(fromKey, toKey)
      val valBridge = bridgeType(fromVal, toVal)
      if (keyBridge.isEmpty && valBridge.isEmpty) {
        None
      } else {
        Some(MapType(keyBridge.getOrElse(toKey), valBridge.getOrElse(toVal), toValContainsNull))
      }

    case (StructType(fromFields), StructType(toFields))
        if fromFields.length == toFields.length =>
      val fieldBridges = fromFields.zip(toFields).map {
        case (fromField, toField) => bridgeType(fromField.dataType, toField.dataType)
      }
      if (fieldBridges.forall(_.isEmpty)) {
        None
      } else {
        Some(StructType(toFields.zip(fieldBridges).map {
          case (toField, bridge) => bridge.map(bt => toField.copy(dataType = bt)).getOrElse(toField)
        }))
      }

    case _ => None
  }

  /**
   * If `cast` converts (recursively, through complex types) between [[DateType]] and a
   * nanosecond-precision timestamp type, returns the equivalent two-step cast routed through the
   * corresponding microsecond-precision timestamp type; returns `None` for any other cast. The
   * nested casts inherit the original `timeZoneId` and `evalMode`.
   */
  def rewriteDateNanosCast(cast: Cast): Option[Cast] = cast match {
    case Cast(child, to, tz, mode) if child.resolved =>
      bridgeType(child.dataType, to).map { micros =>
        Cast(Cast(child, micros, tz, mode), to, tz, mode)
      }
    case _ => None
  }

  override def apply(plan: LogicalPlan): LogicalPlan =
    plan.resolveExpressionsWithPruning(_.containsPattern(CAST), ruleId) {
      case cast: Cast => rewriteDateNanosCast(cast).getOrElse(cast)
    }
}
