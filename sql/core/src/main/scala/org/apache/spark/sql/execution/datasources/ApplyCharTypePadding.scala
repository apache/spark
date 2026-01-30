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

import org.apache.spark.sql.catalyst.analysis.ApplyCharTypePaddingHelper
import org.apache.spark.sql.catalyst.catalog.HiveTableRelation
import org.apache.spark.sql.catalyst.plans.logical.{LogicalPlan, SubqueryAlias}
import org.apache.spark.sql.catalyst.rules.Rule
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
  private def readSideCharPaddingAfterSubqueryAlias =
    conf.getConf(SQLConf.READ_SIDE_CHAR_PADDING_AFTER_SUBQUERY_ALIAS)

  override def apply(plan: LogicalPlan): LogicalPlan = {
    if (conf.charVarcharAsString) {
      return plan
    }

    if (conf.readSideCharPadding) {
      // Two-pass approach when readSideCharPaddingAfterSubqueryAlias is enabled:
      // - First pass: match SubqueryAlias with data source scan, add padding Project AFTER
      //   SubqueryAlias to preserve metadata column access
      // - Second pass: match data source scan alone (for cases like spark.read.format(...).load())
      // For idempotence, readSidePadding clears char type metadata in output attributes,
      // so the second pass does nothing if the first pass already matched.
      val afterFirstPass = if (readSideCharPaddingAfterSubqueryAlias) {
        // First pass: match SubqueryAlias with data source scan
        plan.resolveOperatorsUpWithNewOutput {
          case s @ SubqueryAlias(identifier, r: LogicalRelation) =>
            ApplyCharTypePaddingHelper.readSidePadding(s, () =>
              SubqueryAlias(identifier,
                r.copy(output = r.output.map(CharVarcharUtils.cleanAttrMetadata))))
          case s @ SubqueryAlias(identifier, r: DataSourceV2Relation) =>
            ApplyCharTypePaddingHelper.readSidePadding(s, () =>
              SubqueryAlias(identifier,
                r.copy(output = r.output.map(CharVarcharUtils.cleanAttrMetadata))))
          case s @ SubqueryAlias(identifier, r: HiveTableRelation) =>
            val cleanedDataCols = r.dataCols.map(CharVarcharUtils.cleanAttrMetadata)
            val cleanedPartCols = r.partitionCols.map(CharVarcharUtils.cleanAttrMetadata)
            ApplyCharTypePaddingHelper.readSidePadding(s, () =>
              SubqueryAlias(identifier,
                r.copy(dataCols = cleanedDataCols, partitionCols = cleanedPartCols)))
        }
      } else {
        plan
      }
      // Second pass: match data source scan alone (char type already cleared if first pass
      // matched, so this does nothing for those cases)
      val newPlan = afterFirstPass.resolveOperatorsUpWithNewOutput {
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
      }
      ApplyCharTypePaddingHelper.paddingForStringComparison(newPlan, padCharCol = false)
    } else {
      ApplyCharTypePaddingHelper.paddingForStringComparison(
        plan, padCharCol = !conf.getConf(SQLConf.LEGACY_NO_CHAR_PADDING_IN_PREDICATE))
    }
  }
}
