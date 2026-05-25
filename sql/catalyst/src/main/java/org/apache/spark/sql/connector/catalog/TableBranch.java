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

import java.util.Objects;
import java.util.OptionalLong;

import org.apache.spark.annotation.Evolving;

/**
 * Describes a named branch of a {@link Table} that supports branching.
 * <p>
 * A branch is a named reference to a particular snapshot of a table. The interpretation of
 * {@code snapshotId} is left to the data source.
 *
 * @since 4.3.0
 */
@Evolving
public final class TableBranch {
  private final String name;
  private final OptionalLong snapshotId;
  private final long creationTimeMs;

  public TableBranch(String name, OptionalLong snapshotId, long creationTimeMs) {
    this.name = Objects.requireNonNull(name, "name");
    this.snapshotId = Objects.requireNonNull(snapshotId, "snapshotId");
    this.creationTimeMs = creationTimeMs;
  }

  public TableBranch(String name, long snapshotId, long creationTimeMs) {
    this(name, OptionalLong.of(snapshotId), creationTimeMs);
  }

  /** The branch name. */
  public String name() {
    return name;
  }

  /**
   * The snapshot the branch points at, if known. Some implementations may not expose snapshot
   * identifiers (for example, an empty table that has no snapshots yet).
   */
  public OptionalLong snapshotId() {
    return snapshotId;
  }

  /** Milliseconds since the epoch at which the branch was created. */
  public long creationTimeMs() {
    return creationTimeMs;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;
    TableBranch that = (TableBranch) o;
    return creationTimeMs == that.creationTimeMs
        && name.equals(that.name)
        && snapshotId.equals(that.snapshotId);
  }

  @Override
  public int hashCode() {
    return Objects.hash(name, snapshotId, creationTimeMs);
  }

  @Override
  public String toString() {
    return "TableBranch{name=" + name
        + ", snapshotId=" + snapshotId
        + ", creationTimeMs=" + creationTimeMs + "}";
  }
}
