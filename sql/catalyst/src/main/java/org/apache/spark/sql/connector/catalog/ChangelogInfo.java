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

import org.apache.spark.annotation.Evolving;

/**
 * Encapsulates the parameters of a Change Data Capture (CDC) query, passed from the
 * parser / DataFrame API to the catalog's
 * {@link TableCatalog#loadChangelog(Identifier, ChangelogInfo)} method.
 *
 * @since 4.2.0
 */
@Evolving
public class ChangelogInfo {

  /**
   * Deduplication modes controlling how Spark post-processes raw change data.
   */
  public enum DeduplicationMode {
    /** Raw change rows as-is from the connector — no post-processing. */
    NONE,
    /** Remove identical insert/delete pairs from copy-on-write file rewrites (default). */
    DROP_CARRYOVERS,
    /** Collapse to one net change per row identity across the entire changelog range. */
    NET_CHANGES
  }

  private final ChangelogRange range;
  private final DeduplicationMode deduplicationMode;
  private final boolean computeUpdates;

  public ChangelogInfo(
      ChangelogRange range,
      DeduplicationMode deduplicationMode,
      boolean computeUpdates) {
    this.range = range;
    this.deduplicationMode = deduplicationMode;
    this.computeUpdates = computeUpdates;
  }

  /** Returns the version/timestamp range for this CDC query. */
  public ChangelogRange range() { return range; }

  /** Returns the deduplication mode for this CDC query. */
  public DeduplicationMode deduplicationMode() { return deduplicationMode; }

  /** Whether to derive update_preimage/update_postimage from insert/delete pairs. */
  public boolean computeUpdates() { return computeUpdates; }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (!(o instanceof ChangelogInfo that)) return false;
    return computeUpdates == that.computeUpdates
        && Objects.equals(range, that.range)
        && deduplicationMode == that.deduplicationMode;
  }

  @Override
  public int hashCode() {
    return Objects.hash(range, deduplicationMode, computeUpdates);
  }

  @Override
  public String toString() {
    return "ChangelogInfo{range=" + range +
        ", deduplicationMode=" + deduplicationMode +
        ", computeUpdates=" + computeUpdates + "}";
  }
}
