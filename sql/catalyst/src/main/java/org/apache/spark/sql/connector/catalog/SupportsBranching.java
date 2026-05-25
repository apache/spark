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

import java.util.OptionalLong;

import org.apache.spark.annotation.Evolving;
import org.apache.spark.sql.catalyst.analysis.BranchAlreadyExistsException;
import org.apache.spark.sql.catalyst.analysis.BranchNotFoundException;
import org.apache.spark.sql.catalyst.analysis.InvalidFastForwardException;

/**
 * A mix-in interface for {@link Table} branching support. Data sources can implement this
 * interface to expose multi-branch capabilities (such as Iceberg branches) through standard
 * Spark SQL DDL:
 *
 * <pre>
 *   ALTER TABLE t CREATE [OR REPLACE] BRANCH [IF NOT EXISTS] name [VERSION AS OF snapshotId]
 *   ALTER TABLE t DROP BRANCH [IF EXISTS] name
 *   ALTER TABLE t FASTFORWARD BRANCH branch TO target
 *   SHOW BRANCHES IN t
 * </pre>
 *
 * <p>The meaning of {@code snapshotId} is left to the data source. When a snapshot id is not
 * supplied the implementation should branch from the current snapshot of the table.
 *
 * @since 4.3.0
 */
@Evolving
public interface SupportsBranching extends Table {

  /**
   * Create a new branch on this table.
   *
   * @param name the branch name; must not be {@code null}
   * @param sourceSnapshotId an optional snapshot id to branch from. If empty, the branch is
   *                         created from the table's current snapshot.
   * @return a {@link TableBranch} describing the new branch
   * @throws BranchAlreadyExistsException if a branch with the given name already exists
   */
  TableBranch createBranch(String name, OptionalLong sourceSnapshotId)
      throws BranchAlreadyExistsException;

  /**
   * Replace an existing branch (or create it if missing).
   *
   * <p>Implementations must perform the replacement atomically with respect to concurrent
   * {@link #createBranch}, {@link #dropBranch}, and other {@code replaceBranch} calls on the
   * same branch name. A naive drop-then-create implementation is not sufficient because it
   * allows a concurrent caller to observe the branch as missing and create it with a different
   * snapshot in the window between the two operations.
   *
   * @return a {@link TableBranch} describing the (possibly new) branch
   */
  TableBranch replaceBranch(String name, OptionalLong sourceSnapshotId);

  /**
   * Drop the named branch.
   *
   * @return {@code true} if the branch was removed; {@code false} if it did not exist.
   */
  boolean dropBranch(String name);

  /**
   * Fast-forward {@code branch} to the head of {@code targetBranch}.
   *
   * <p>The operation succeeds only when the branch's current snapshot is an ancestor of the
   * target branch's current snapshot. Implementations should throw
   * {@link InvalidFastForwardException} otherwise.
   *
   * @return the updated {@link TableBranch} after fast-forwarding
   * @throws BranchNotFoundException if either branch is missing
   * @throws InvalidFastForwardException if the operation would not be a fast-forward
   */
  TableBranch fastForwardBranch(String branch, String targetBranch)
      throws BranchNotFoundException, InvalidFastForwardException;

  /**
   * List all branches of this table. The default implementation returns an empty array.
   */
  default TableBranch[] listBranches() {
    return new TableBranch[0];
  }
}
