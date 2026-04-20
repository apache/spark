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

package org.apache.spark.sql.execution

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.Attribute

/**
 * Physical execution node for CDC carry-over removal.
 *
 * Expects input pre-sorted by (rowId, rowVersion, _change_type ASC) within each partition.
 * Uses [[CarryOverIterator]] to compare consecutive delete+insert pairs and drop identical
 * pairs (CoW carry-over artifacts).
 *
 * All ordinals are resolved dynamically from the actual child output at execution time,
 * because the optimizer may insert projections that change column positions.
 *
 * rowId columns are restricted to top-level columns in the child output. Nested rowId paths
 * (e.g. `payload.id`) are rejected at analysis time by
 * [[org.apache.spark.sql.catalyst.analysis.ResolveChangelogTable]]. If a future connector
 * needs nested row identity, see the TODO there for the strategy and prior-commit pointer.
 *
 * @param child the pre-sorted child plan
 * @param rowIdColumnNames top-level names of row identity columns (supports composite keys)
 * @param rowVersionColumnName name of the row version column
 * @param dataColumnNames names of data columns for comparison
 */
case class CarryOverRemovalExec(
    child: SparkPlan,
    rowIdColumnNames: Seq[String],
    rowVersionColumnName: String,
    dataColumnNames: Seq[String]) extends UnaryExecNode {

  override def output: Seq[Attribute] = child.output

  override protected def doExecute(): RDD[InternalRow] = {
    val outputSchema = child.schema
    val outputNames = child.output.map(_.name)

    val changeTypeOrdinal = resolveOrdinal(outputNames, "_change_type")
    val rowVersionOrdinal = resolveOrdinal(outputNames, rowVersionColumnName)
    val rowIdOrdinals = rowIdColumnNames.map(resolveOrdinal(outputNames, _)).toArray
    val dataOrdinals = dataColumnNames.map(resolveOrdinal(outputNames, _)).toArray

    val rdd = child.execute()
    rdd.mapPartitionsInternal { iter =>
      new CarryOverIterator(
        iter,
        rowIdOrdinals,
        rowVersionOrdinal,
        changeTypeOrdinal,
        dataOrdinals,
        outputSchema
      )
    }
  }

  /** Resolves a column name to its ordinal index, throwing a clear error if not found. */
  private def resolveOrdinal(outputNames: Seq[String], name: String): Int = {
    val idx = outputNames.indexOf(name)
    require(idx >= 0,
      s"Column '$name' not found in CarryOverRemovalExec child output: " +
        s"${outputNames.mkString(", ")}")
    idx
  }

  override protected def withNewChildInternal(newChild: SparkPlan): CarryOverRemovalExec =
    copy(child = newChild)
}
