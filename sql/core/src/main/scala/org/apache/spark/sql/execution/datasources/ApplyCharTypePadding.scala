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
import org.apache.spark.sql.catalyst.expressions.AttributeReference
import org.apache.spark.sql.catalyst.plans.logical.{DeleteFromTable, LogicalPlan}
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.catalyst.trees.TreePattern.DELETE_FROM_TABLE
import org.apache.spark.sql.catalyst.util.CharVarcharUtils
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
    // standardSemantics takes precedence over legacy charVarcharAsString.
    if (conf.charVarcharAsString && !conf.charVarcharStandardSemantics) {
      return plan
    }

    if (conf.charVarcharStandardSemantics && !conf.readSideCharPadding) {
      warnReadSidePaddingOverride()
    }

    if (conf.readSideCharPadding || conf.charVarcharStandardSemantics) {
      // A DELETE that is not rewritten as a row-level operation does not read its table, so the
      // table is not padded, and the condition gets the padding instead, see padDeleteConditions.
      val newPlan = plan.transformUpWithNewOutput({
        case r: LogicalRelation =>
          ApplyCharTypePaddingHelper.readSidePadding(r, () =>
            r.copy(output = r.output.map(CharVarcharUtils.cleanAttrMetadata)))
        case r: DataSourceV2Relation =>
          ApplyCharTypePaddingHelper.readSidePadding(r, () =>
            r.copy(output = r.output.map(CharVarcharUtils.cleanAttrMetadata)))
        case r: HiveTableRelation =>
          ApplyCharTypePaddingHelper.readSidePadding(r, () => {
            val cleanedDataCols = r.dataCols.map(CharVarcharUtils.cleanAttrMetadata)
            val cleanedPartCols = r.partitionCols.map(CharVarcharUtils.cleanAttrMetadata)
            r.copy(dataCols = cleanedDataCols, partitionCols = cleanedPartCols)
          })
      }, skipCond = p => p.analyzed || p.isInstanceOf[DeleteFromTable], canGetOutput = _.resolved)
      padDeleteConditions(
        ApplyCharTypePaddingHelper.paddingForStringComparison(newPlan, padCharCol = false))
    } else {
      ApplyCharTypePaddingHelper.paddingForStringComparison(
        plan, padCharCol = !conf.getConf(SQLConf.LEGACY_NO_CHAR_PADDING_IN_PREDICATE))
    }
  }

  /**
   * A DELETE that is not rewritten as a row-level operation deletes with filters translated from
   * its condition, which the data source compares with the stored values. To match what a read of
   * the table returns, the condition evaluates the read-side padding of the columns itself, after
   * its comparisons are padded. The data source can only delete with such a filter if it supports
   * the padding, e.g. `CHAR_READ_SIDE_PADDING`.
   */
  private def padDeleteConditions(plan: LogicalPlan): LogicalPlan = {
    plan.resolveOperatorsWithPruning(_.containsPattern(DELETE_FROM_TABLE)) {
      case d: DeleteFromTable =>
        d.copy(condition = d.condition.transform {
          // the padded value refers to the attribute without the raw type and is not padded again
          case attr: AttributeReference if CharVarcharUtils.getRawType(attr.metadata).isDefined =>
            CharVarcharUtils.addPaddingForScan(attr).transform {
              case a: AttributeReference => CharVarcharUtils.cleanAttrMetadata(a)
            }
        })
    }
  }
}
