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

import scala.collection.mutable

import org.apache.spark.sql.catalyst.expressions.Attribute
import org.apache.spark.sql.catalyst.plans.logical.{Deduplicate, DeduplicateAllColumnsAsKey, DeduplicateKeyColumns, DeduplicateSpec, DeduplicateWithinWatermark, LogicalPlan}
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.catalyst.trees.TreePattern.UNRESOLVED_DEDUPLICATE
import org.apache.spark.sql.errors.QueryCompilationErrors
import org.apache.spark.sql.internal.SQLConf

/**
 * Resolves [[UnresolvedDeduplicate]] (built by `Dataset.dropDuplicates*` in Spark Classic and by
 * the Deduplicate relation in the Spark Connect planner) into a [[Deduplicate]] /
 * [[DeduplicateWithinWatermark]] with resolved key attributes, shared by both engines.
 *
 * The key attributes are computed with [[SQLConf.DROP_DUPLICATES_DETERMINISTIC_KEY_ORDER]] read
 * from the current session: true (the default) produces a stable, first-occurrence ordering; false
 * reproduces each engine's legacy resolution. The resolved node also carries the original recipe
 * (`subset`, `allColumnsAsKeys`, `viaSparkClassic`) so that streaming queries can recompute the
 * keys at query start with the value pinned in the offset log (see
 * `StreamingQueryManager.createQuery` and [[computeKeys]]). See SPARK-57489.
 */
object ResolveDeduplicate extends Rule[LogicalPlan] {

  override def apply(plan: LogicalPlan): LogicalPlan = plan.resolveOperatorsUpWithPruning(
    _.containsPattern(UNRESOLVED_DEDUPLICATE)) {
    case d: UnresolvedDeduplicate if d.child.resolved =>
      val orderDeterministically = conf.getConf(SQLConf.DROP_DUPLICATES_DETERMINISTIC_KEY_ORDER)
      val keySpec =
        if (d.allColumnsAsKeys) DeduplicateAllColumnsAsKey
        else DeduplicateKeyColumns(d.columnNames)
      val spec = DeduplicateSpec(keySpec, d.viaSparkClassic)
      val keys = computeKeys(d.child, spec, orderDeterministically, conf.resolver)
      if (d.withinWatermark) {
        DeduplicateWithinWatermark(keys, d.child, Some(spec))
      } else {
        Deduplicate(keys, d.child, Some(spec))
      }
  }

  /**
   * Computes the deduplication key attributes from the requested recipe against `child`'s output.
   * Shared by the analyzer rule (with the session config) and by the streaming bootstrap (with the
   * value pinned in the offset log), so the ordering rules stay in one place.
   *
   * @param orderDeterministically when true, a stable first-occurrence order; when false, the
   *   legacy (engine-specific) order selected by `spec.viaSparkClassic`.
   */
  def computeKeys(
      child: LogicalPlan,
      spec: DeduplicateSpec,
      orderDeterministically: Boolean,
      resolver: Resolver): Seq[Attribute] = {
    spec.keySpec match {
      case DeduplicateAllColumnsAsKey =>
        // All child columns are keys. The deterministic order and legacy Spark Connect both key on
        // the child output directly (in output order, no name resolution); only legacy Spark
        // Classic reorders the names through a Set (see legacyClassicColumnNames).
        if (!orderDeterministically && spec.viaSparkClassic) {
          resolveColumnNames(child, legacyClassicColumnNames(child.output.map(_.name)), resolver)
        } else {
          child.output
        }
      case DeduplicateKeyColumns(colNames) =>
        val orderedNames = if (orderDeterministically) {
          dedupKeepingOrder(colNames)
        } else if (spec.viaSparkClassic) {
          legacyClassicColumnNames(colNames)
        } else {
          // Legacy Spark Connect resolution: no dedup, caller-provided input order.
          colNames
        }
        resolveColumnNames(child, orderedNames, resolver)
    }
  }

  /**
   * Reproduces Spark Classic's legacy dedup-key resolution (SPARK-31990), which collapsed the
   * requested column names through a `Set`. The resulting (Set iteration) order is preserved for
   * existing queries whose checkpoints predate the deterministic order. Shared by the all-columns
   * and subset forms so both stay faithful to Spark Classic.
   */
  private def legacyClassicColumnNames(names: Seq[String]): Seq[String] = names.toSet.toSeq

  private def resolveColumnNames(
      child: LogicalPlan, names: Seq[String], resolver: Resolver): Seq[Attribute] = {
    names.flatMap { colName =>
      // It is possible there is more than one column with the same name, so we call filter
      // instead of find.
      val cols = child.output.filter(col => resolver(col.name, colName))
      if (cols.isEmpty) {
        throw QueryCompilationErrors.cannotResolveColumnNameAmongAttributesError(
          colName, child.output.map(_.name).mkString(", "))
      }
      cols
    }
  }

  /**
   * Deduplicate `names` preserving first-occurrence order. The stable ordering is an invariant for
   * stateful operators, so we rely only on the documented contract of `Seq` traversal and explicit
   * append, not on `Seq.distinct`/`Set` (whose ordering is not contractually guaranteed).
   * See SPARK-57489.
   */
  private def dedupKeepingOrder(names: Seq[String]): Seq[String] = {
    val seen = mutable.HashSet.empty[String]
    val out = mutable.ArrayBuffer.empty[String]
    names.foreach(name => if (seen.add(name)) out += name)
    out.toSeq
  }
}
