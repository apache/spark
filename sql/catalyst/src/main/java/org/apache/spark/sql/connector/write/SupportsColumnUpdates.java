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

package org.apache.spark.sql.connector.write;

import org.apache.spark.annotation.Experimental;
import org.apache.spark.sql.connector.expressions.NamedReference;

/**
 * A mix-in interface for {@link RowLevelOperation}. Data sources can implement this interface to
 * receive a narrow row containing only the columns declared via {@link #requiredDataAttributes()}
 * for updated, copied, and reinserted records, instead of the full table row.
 * <p>
 * When {@link LogicalWriteInfo#columnUpdateSchema()} is present, updated, copied, and reinserted
 * rows follow it: group-based operations receive them through
 * {@link DataWriter#writeColumnUpdate(Object, Object)} or
 * {@link DataWriter#writeColumnUpdate(Object)}, and operations that mix in {@link SupportsDelta}
 * through {@link DeltaWriter#update} and {@link DeltaWriter#reinsert}. Rows the command inserts
 * always follow {@link LogicalWriteInfo#schema()} and arrive through
 * {@link DataWriter#write(Object)} and {@link DeltaWriter#insert}. When
 * {@link LogicalWriteInfo#columnUpdateSchema()} is absent, every row follows
 * {@link LogicalWriteInfo#schema()}, as for any other operation. Connectors must decide which
 * layout to expect by whether {@link LogicalWriteInfo#columnUpdateSchema()} is present, not by
 * the command. A builder that wants full-width rows for some command can check
 * {@link RowLevelOperationInfo#command()} and build an operation without this mix-in. Currently
 * Spark narrows only UPDATE.
 * <p>
 * The scan builder returned by {@link #newScanBuilder} should implement
 * {@link org.apache.spark.sql.connector.read.SupportsPushDownRequiredColumns}, so that the scan
 * does not read columns the command neither references nor declares. Otherwise the scan reads
 * every column. For operations that do not mix in {@link SupportsDelta}, only top-level columns
 * are pruned: a struct column the command references through a nested field is read in full.
 *
 * @since 4.4.0
 */
@Experimental
public interface SupportsColumnUpdates extends RowLevelOperation {
  /**
   * Returns the data column references required to perform this row-level operation.
   * <p>
   * When Spark narrows the write, the returned columns become
   * {@link LogicalWriteInfo#columnUpdateSchema()}, in declared order. Spark matches each declared
   * column with the session's resolver, and {@link LogicalWriteInfo#columnUpdateSchema()} uses the
   * table's spelling of each column, which can differ in case from the declared reference.
   * Implementations must include every column they want to receive: every column reported by
   * {@link RowLevelOperationInfo#updatedColumns()}, plus any columns needed for row lookup or
   * routing. Columns that are not declared are absent from the rows the connector receives, so
   * the connector must preserve their values itself. Spark may call this method more than once
   * for the same operation, and every call must return the same columns.
   * <p>
   * If the returned array is empty, Spark does not narrow the write and the operation receives
   * full-width rows. An UPDATE whose assignments are all identity assignments reports no
   * {@link RowLevelOperationInfo#updatedColumns()}, so an operation that declares exactly those
   * columns takes this path for such an UPDATE. An empty array is not an opt-out: an UPDATE that
   * sets a column is still rejected, because the array misses that column.
   * <p>
   * An operation that does not mix in {@link SupportsDelta} should locate original rows through
   * metadata columns returned by {@link #requiredMetadataAttributes()} that are preserved on
   * update ({@link org.apache.spark.sql.connector.catalog.MetadataColumn#PRESERVE_ON_UPDATE}),
   * such as a file and a row position. If it locates rows through a data column, its builder
   * should build an operation without this mix-in when
   * {@link RowLevelOperationInfo#updatedColumns()} contains that column.
   * <p>
   * Each entry must name a top-level data column of the table. For updates on nested fields such
   * as {@code SET s.c1 = -1}, the connector must declare the root struct column {@code s}. Spark
   * rejects with an analysis exception a column that does not exist, a column declared more than
   * once, a nested field, a metadata column (declare those through
   * {@link #requiredMetadataAttributes()} instead), and an array that misses a column reported by
   * {@link RowLevelOperationInfo#updatedColumns()}.
   * <p>
   * If this operation also mixes in {@link SupportsDelta} and represents updates as deletes and
   * inserts ({@link SupportsDelta#representUpdateAsDeleteAndInsert()} returns {@code true}),
   * every row-ID column ({@link SupportsDelta#rowId()}) must reach the reinserted row: a data
   * row-ID column must be declared here, and a metadata row-ID column must be returned by
   * {@link #requiredMetadataAttributes()} with
   * {@link org.apache.spark.sql.connector.catalog.MetadataColumn#PRESERVE_ON_REINSERT} set. In
   * this mode, an UPDATE that assigns a new value to a row-ID column is also rejected, unless
   * every data column is declared here. With the default CHAR and VARCHAR handling, this includes
   * a self-assignment on a CHAR or VARCHAR row-ID column. Both cases are rejected with an analysis
   * exception.
   * <p>
   * Data columns the write needs must be declared here too, even if the command does not
   * reference them, such as the source columns of partition transforms and any columns used by
   * {@link RequiresDistributionAndOrdering#requiredDistribution()} or
   * {@link RequiresDistributionAndOrdering#requiredOrdering()}. Such columns appear in the rows
   * the connector receives; a connector that does not want to persist them should project them
   * away inside the connector before writing. A write whose distribution or ordering references
   * any other column is rejected with an analysis exception, except for metadata columns returned
   * by {@link #requiredMetadataAttributes()} and, for operations that mix in
   * {@link SupportsDelta}, row-ID columns.
   * <p>
   * Spark does not read an undeclared column that neither the command nor a table constraint
   * references, so such a column is not available for runtime filtering. A scan that reports
   * runtime filter attributes based on the columns it reads loses runtime group filtering on it,
   * and a scan that still reports it as a runtime filter attribute fails with
   * {@code DATA_SOURCE_INVALID_RUNTIME_FILTER_ATTRIBUTE}. Declare partition source columns used
   * for runtime filtering to keep it.
   */
  NamedReference[] requiredDataAttributes();
}
