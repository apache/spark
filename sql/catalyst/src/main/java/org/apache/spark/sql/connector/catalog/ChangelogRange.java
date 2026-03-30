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

import org.apache.spark.annotation.Evolving;

/**
 * Represents the version or timestamp range for a Change Data Capture (CDC) query.
 * <p>
 * This sealed interface has three implementations:
 * <ul>
 *   <li>{@link VersionRange} — range defined by version identifiers</li>
 *   <li>{@link TimestampRange} — range defined by timestamps</li>
 *   <li>{@link UnboundedRange} — no boundaries (used by streaming queries)</li>
 * </ul>
 *
 * @since 4.2.0
 */
@Evolving
public sealed interface ChangelogRange
    permits ChangelogRange.VersionRange, ChangelogRange.TimestampRange,
        ChangelogRange.UnboundedRange {

  /** Whether the starting bound is inclusive. */
  boolean startingBoundInclusive();

  /** Whether the ending bound is inclusive. */
  boolean endingBoundInclusive();

  /**
   * A changelog range defined by version identifiers.
   *
   * @param startingVersion the starting version (always present)
   * @param endingVersion the ending version (empty means latest)
   * @param startingBoundInclusive whether the starting bound is inclusive
   * @param endingBoundInclusive whether the ending bound is inclusive
   */
  record VersionRange(
      String startingVersion,
      Optional<String> endingVersion,
      boolean startingBoundInclusive,
      boolean endingBoundInclusive) implements ChangelogRange {}

  /**
   * A changelog range defined by timestamps.
   *
   * @param startingTimestamp the starting timestamp in microseconds since epoch
   * @param endingTimestamp the ending timestamp in microseconds since epoch (empty means latest)
   * @param startingBoundInclusive whether the starting bound is inclusive
   * @param endingBoundInclusive whether the ending bound is inclusive
   */
  record TimestampRange(
      long startingTimestamp,
      Optional<Long> endingTimestamp,
      boolean startingBoundInclusive,
      boolean endingBoundInclusive) implements ChangelogRange {}

  /**
   * An unbounded changelog range with no starting or ending boundaries.
   * Used by streaming queries where the connector determines the starting point.
   */
  record UnboundedRange() implements ChangelogRange {
    @Override public boolean startingBoundInclusive() {
      throw new UnsupportedOperationException("Unbounded range has no starting bound.");
    }
    @Override public boolean endingBoundInclusive() {
      throw new UnsupportedOperationException("Unbounded range has no ending bound.");
    }
  }
}
