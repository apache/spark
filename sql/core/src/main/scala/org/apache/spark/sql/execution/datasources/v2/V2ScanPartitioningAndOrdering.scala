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
package org.apache.spark.sql.execution.datasources.v2

import org.apache.spark.internal.Logging
import org.apache.spark.internal.LogKeys.{CLASS_NAME, COLUMN_NAMES}
import org.apache.spark.sql.catalyst.expressions.{NamedExpression, V2ExpressionUtils}
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.catalyst.trees.TreePattern.DATA_SOURCE_V2_SCAN_RELATION
import org.apache.spark.sql.connector.read.{SupportsReportOrdering, SupportsReportPartitioning}
import org.apache.spark.sql.connector.read.partitioning.{KeyGroupedPartitioning, UnknownPartitioning}
import org.apache.spark.util.ArrayImplicits._
import org.apache.spark.util.collection.Utils.sequenceToOption

/**
 * Extracts [[DataSourceV2ScanRelation]] from the input logical plan, converts any V2 partitioning
 * and ordering reported by data sources to their catalyst counterparts. Then, annotates the plan
 * with the partitioning and ordering result.
 */
object V2ScanPartitioningAndOrdering extends Rule[LogicalPlan] with Logging {
  override def apply(plan: LogicalPlan): LogicalPlan = {
    val scanRules = Seq[LogicalPlan => LogicalPlan] (partitioning, ordering)

    scanRules.foldLeft(plan) { (newPlan, scanRule) =>
      scanRule(newPlan)
    }
  }

  private def partitioning(plan: LogicalPlan) = plan.transformDownWithPruning(
      _.containsPattern(DATA_SOURCE_V2_SCAN_RELATION)) {
    case d @ ExtractV2ScanInfo(relation, scan: SupportsReportPartitioning, _)
        if d.keyGroupedPartitioning.isEmpty =>
      val catalystPartitioning = scan.outputPartitioning() match {
        case kgp: KeyGroupedPartitioning =>
          val partitioning = sequenceToOption(
            kgp.keys().map(V2ExpressionUtils.toCatalystOpt(_, relation, relation.funCatalog))
              .toImmutableArraySeq)
          if (partitioning.isEmpty) {
            val unresolvedRefs = kgp.keys().flatMap(_.references()).filter { ref =>
              V2ExpressionUtils.resolveRefOpt[NamedExpression](ref, relation).isEmpty
            }
            if (unresolvedRefs.nonEmpty) {
              logWarning(
                log"Spark ignores the reported ${MDC(CLASS_NAME, kgp.getClass.getSimpleName)} " +
                  log"because the partition key columns cannot be resolved: " +
                  log"${MDC(COLUMN_NAMES, unresolvedRefs.map(_.describe()).mkString(", "))}. " +
                  log"Storage-partitioned join will not be applied for this scan.")
            }
          }
          // Keep the partitioning when at least one of its keys is still in the scan output: the
          // scan projects the pruned key positions away when reporting its physical output
          // partitioning (see DataSourceV2ScanExecBase.outputPartitioning). When no key survives,
          // and likewise when the source reported no key at all, there is nothing to report.
          // Grouping a projection that collapsed distinct keys onto the same key stays gated on
          // allowKeysSubsetOfPartitionKeys one layer down, in KeyedPartitioning.mayGroupToSatisfy.
          //
          // A kept pruned key leaves a dangling attribute on the relation. What keeps that off
          // `missingInput`, and so past the optimizer's plan-change validation, is the
          // `DataSourceV2ScanRelation.references` override; see the comment there.
          partitioning.filter(_.exists(_.references.subsetOf(d.outputSet)))
        case _: UnknownPartitioning => None
        case p =>
          logWarning(
            log"Spark ignores the partitioning ${MDC(CLASS_NAME, p.getClass.getSimpleName)}. " +
              log"Please use KeyGroupedPartitioning for better performance")
          None
      }

      d.copy(keyGroupedPartitioning = catalystPartitioning)
  }

  private def ordering(plan: LogicalPlan) = plan.transformDownWithPruning(
      _.containsPattern(DATA_SOURCE_V2_SCAN_RELATION)) {
    case d @ ExtractV2ScanInfo(relation, scan: SupportsReportOrdering, _) =>
      // The ordering is kept as reported, even where it references columns pruned out of the scan
      // output: truncating it here would also drop the sort orders on a partition key past a
      // pruned column, which still hold. `DataSourceV2ScanRelation.doCanonicalize` and
      // `DataSourceV2ScanExecBase.outputOrdering` restrict it to the scan output instead.
      val ordering =
        V2ExpressionUtils.toCatalystOrdering(scan.outputOrdering(), relation, relation.funCatalog)
      d.copy(ordering = Some(ordering))
  }
}
