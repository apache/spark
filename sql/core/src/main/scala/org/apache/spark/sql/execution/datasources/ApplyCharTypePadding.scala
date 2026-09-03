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

package org.apache.spark.sql.execution.datasources

import java.util.concurrent.atomic.AtomicBoolean

import org.apache.spark.internal.LogKeys
import org.apache.spark.sql.catalyst.analysis.ApplyCharTypePaddingHelper
import org.apache.spark.sql.catalyst.catalog.HiveTableRelation
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.catalyst.util.{CharVarcharScanMode, CharVarcharUtils}
import org.apache.spark.sql.execution.datasources.v2.DataSourceV2Relation
import org.apache.spark.sql.internal.SQLConf

/**
 * This rule performs string padding for char type.
 *
 * When reading values from column/field of type CHAR(N), right-pad the values to length N, if the
 * read-side padding config is turned on.
 *
 * When comparing char type column/field with string literal or char type column/field,
 * right-pad the shorter one to the longer length.
 */
object ApplyCharTypePadding extends Rule[LogicalPlan] {

  private val readSidePaddingOverrideWarned = new AtomicBoolean(false)

  private def warnReadSidePaddingOverride(): Unit = {
    if (readSidePaddingOverrideWarned.compareAndSet(false, true)) {
      logWarning(log"${MDC(LogKeys.CONFIG, SQLConf.READ_SIDE_CHAR_PADDING.key)} is disabled but " +
        log"${MDC(LogKeys.CONFIG2, SQLConf.CHAR_VARCHAR_STANDARD_SEMANTICS.key)} is enabled; " +
        log"read-side CHAR/VARCHAR checks are still applied because standard semantics require " +
        log"a read to observe the value a write would have produced.")
    }
  }

  override def apply(plan: LogicalPlan): LogicalPlan = {
    val standardSemantics = conf.charVarcharStandardSemantics
    val scanMode = CharVarcharScanMode(standardSemantics)

    // Bind into case-class state, not a TreeNodeTag: `TreeNode.makeCopy` calls `copyTagsFrom`,
    // so a tag survives canonicalization, but it does not participate in structural plan
    // equality / sameResult, so cache lookup and scan reuse would treat preserve-only and
    // standard scans as the same plan. A case-class field does participate. Keep an
    // already-bound value (views, catalog-cached relations) unchanged.
    def bindStandardSemantics(p: LogicalPlan): LogicalPlan = p match {
      case relation: LogicalRelation if relation.charVarcharScanMode.isEmpty =>
        val bound = relation.copy(charVarcharScanMode = Some(scanMode))
        bound.copyTagsFrom(relation)
        bound
      case relation: DataSourceV2Relation if relation.charVarcharScanMode.isEmpty =>
        val bound = relation.copy(charVarcharScanMode = Some(scanMode))
        bound.copyTagsFrom(relation)
        bound
      case relation: HiveTableRelation if relation.charVarcharScanMode.isEmpty =>
        val bound = relation.copy(charVarcharScanMode = Some(scanMode))
        bound.copyTagsFrom(relation)
        bound
      case _ => p
    }

    val boundPlan = if (conf.charVarcharFirstClassTypes) {
      plan.resolveOperatorsUp {
        case relation: LogicalRelation => bindStandardSemantics(relation)
        case relation: DataSourceV2Relation => bindStandardSemantics(relation)
        case relation: HiveTableRelation => bindStandardSemantics(relation)
      }
    } else {
      plan
    }

    // standardSemantics takes precedence over legacy charVarcharAsString.
    if (conf.charVarcharAsString && !standardSemantics) {
      return boundPlan
    }

    if (standardSemantics && !conf.readSideCharPadding) {
      warnReadSidePaddingOverride()
    }

    if (conf.readSideCharPadding || standardSemantics) {
      val newPlan = boundPlan.resolveOperatorsUpWithNewOutput {
        case r: LogicalRelation =>
          bindStandardSemantics(r)
          ApplyCharTypePaddingHelper.readSidePadding(r, () =>
            bindStandardSemantics(
              r.copy(output = r.output.map(CharVarcharUtils.cleanAttrMetadata))))
        case r: DataSourceV2Relation =>
          bindStandardSemantics(r)
          ApplyCharTypePaddingHelper.readSidePadding(r, () =>
            bindStandardSemantics(
              r.copy(output = r.output.map(CharVarcharUtils.cleanAttrMetadata))))
        case r: HiveTableRelation =>
          bindStandardSemantics(r)
          ApplyCharTypePaddingHelper.readSidePadding(r, () => {
            val cleanedDataCols = r.dataCols.map(CharVarcharUtils.cleanAttrMetadata)
            val cleanedPartCols = r.partitionCols.map(CharVarcharUtils.cleanAttrMetadata)
            bindStandardSemantics(
              r.copy(dataCols = cleanedDataCols, partitionCols = cleanedPartCols))
          })
      }
      ApplyCharTypePaddingHelper.paddingForStringComparison(newPlan, padCharCol = false)
    } else {
      ApplyCharTypePaddingHelper.paddingForStringComparison(
        boundPlan, padCharCol = !conf.getConf(SQLConf.LEGACY_NO_CHAR_PADDING_IN_PREDICATE))
    }
  }
}
