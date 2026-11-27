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

package org.apache.spark.sql.connector.catalog;

import java.util.Optional;

import org.apache.spark.annotation.Experimental;
import org.apache.spark.sql.connector.write.LogicalWriteInfo;
import org.apache.spark.sql.connector.write.RowLevelOperation;
import org.apache.spark.sql.connector.write.RowLevelOperationBuilder;
import org.apache.spark.sql.connector.write.RowLevelOperationInfo;
import org.apache.spark.sql.util.CaseInsensitiveStringMap;

/**
 * A mix-in interface for {@link Table} row-level operations support. Data sources can implement
 * this interface to indicate they support rewriting data for DELETE, UPDATE, MERGE operations.
 *
 * @since 3.3.0
 */
@Experimental
public interface SupportsRowLevelOperations extends Table {
  /**
   * Returns a {@link RowLevelOperationBuilder} to build a {@link RowLevelOperation}.
   * Spark will call this method while planning DELETE, UPDATE and MERGE operations
   * that require rewriting data.
   *
   * @param info the row-level operation info such as command (e.g. DELETE) and options
   * @return the row-level operation builder
   */
  RowLevelOperationBuilder newRowLevelOperationBuilder(RowLevelOperationInfo info);

  /**
   * Returns an optional {@link RowLevelOperationBuilder} to coordinate an insert-only MERGE
   * operation (a MERGE statement with only {@code WHEN NOT MATCHED THEN INSERT} actions) on
   * tables that do not use {@link TableCapability#V1_BATCH_WRITE}.
   * <p>
   * Unlike {@link #newRowLevelOperationBuilder(RowLevelOperationInfo)}, Spark plans insert-only
   * MERGE using a LeftAnti join and an append-only write rather than rewriting existing rows or
   * groups. When this method returns a builder:
   * <ul>
   *   <li>Spark uses {@link RowLevelOperation#newScanBuilder(CaseInsensitiveStringMap)} for the
   *       target scan of the LeftAnti join. Because untouched rows in matched groups are not
   *       rewritten, the scan does not need to preserve whole groups (e.g. intra-file residual
   *       filtering and runtime filtering can remain enabled).</li>
   *   <li>Spark configures the scan via
   *       {@link RowLevelOperation#newScanBuilder(CaseInsensitiveStringMap)} before calling
   *       {@link RowLevelOperation#newWriteBuilder(LogicalWriteInfo)}, allowing implementations
   *       to pass scan state (such as starting snapshot ID or pushed filters) into the write for
   *       conflict detection and isolation-level enforcement.</li>
   *   <li>{@link RowLevelOperation#newWriteBuilder(LogicalWriteInfo)} must construct an append
   *       write that accepts only newly inserted data rows matching
   *       {@link LogicalWriteInfo#schema()}.</li>
   *   <li>If the optimizer eliminates the target scan before scan planning (for example, when
   *       the source is statically empty or the merge condition folds to {@code false}), Spark
   *       skips calling {@link RowLevelOperation#newWriteBuilder(LogicalWriteInfo)} and falls
   *       back directly to {@link SupportsWrite#newWriteBuilder(LogicalWriteInfo)} on the
   *       underlying table.</li>
   * </ul>
   * <p>
   * When this method returns {@link Optional#empty()} (the default), Spark plans insert-only
   * MERGE directly against the underlying table's {@link SupportsRead} and {@link SupportsWrite}
   * implementations (requiring {@link TableCapability#BATCH_WRITE} or
   * {@link TableCapability#V1_BATCH_WRITE}).
   *
   * @param info the row-level operation info with {@link RowLevelOperation.Command#MERGE} and
   *             user-specified options
   * @return an optional row-level operation builder for coordinating the insert-only MERGE
   * @since 4.4.0
   */
  default Optional<RowLevelOperationBuilder> newInsertOnlyMergeOperationBuilder(
      RowLevelOperationInfo info) {
    return Optional.empty();
  }
}
