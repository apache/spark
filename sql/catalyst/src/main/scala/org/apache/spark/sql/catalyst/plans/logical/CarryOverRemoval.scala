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

package org.apache.spark.sql.catalyst.plans.logical

import org.apache.spark.sql.catalyst.expressions.{Attribute, AttributeSet}

/**
 * Logical plan node for CDC carry-over removal.
 *
 * Expects its child to be pre-sorted by (rowId, rowVersion, _change_type ASC) within each
 * partition, with data repartitioned by (rowId, rowVersion).
 *
 * Column references are stored as names (not ordinals) because the optimizer may insert
 * projections that change column positions between analysis and execution.
 *
 * @param child the pre-sorted, repartitioned child plan
 * @param rowIdColumnNames top-level names of row identity columns, e.g. Seq("id") or
 *                         Seq("pk1", "pk2") for composite keys. Nested rowId paths are
 *                         rejected at analysis time; see the TODO in
 *                         [[org.apache.spark.sql.catalyst.analysis.ResolveChangelogTable]].
 * @param rowVersionColumnName name of the row version column (e.g. "_commit_version")
 * @param dataColumnNames names of data columns for field-by-field comparison
 * @param requiredAttributes attributes the optimizer must not project away
 */
case class CarryOverRemoval(
    child: LogicalPlan,
    rowIdColumnNames: Seq[String],
    rowVersionColumnName: String,
    dataColumnNames: Seq[String],
    requiredAttributes: Seq[Attribute] = Seq.empty) extends UnaryNode {

  override def output: Seq[Attribute] = child.output

  // Tell the optimizer we still need every data column at execution time so ColumnPruning
  // doesn't drop columns that CarryOverRemovalExec compares field-by-field.
  override def references: AttributeSet =
    AttributeSet(requiredAttributes) ++ super.references

  override def maxRows: Option[Long] = child.maxRows

  override protected def withNewChildInternal(newChild: LogicalPlan): CarryOverRemoval =
    copy(child = newChild)
}
