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

import org.apache.spark.annotation.Evolving;
import org.apache.spark.sql.connector.expressions.NamedReference;
import org.apache.spark.sql.connector.read.ScanBuilder;
import org.apache.spark.sql.util.CaseInsensitiveStringMap;

/**
 * The central connector interface for Change Data Capture (CDC).
 * <p>
 * Connectors implement this minimal interface to expose change data. Spark handles
 * post-processing (carry-over removal, update detection, net change computation) based on
 * the properties declared by the connector.
 * <p>
 * The columns returned by {@link #columns()} must include the following metadata columns:
 * <ul>
 *   <li>{@code _change_type} (STRING) — the kind of change: {@code insert}, {@code delete},
 *       {@code update_preimage}, or {@code update_postimage}</li>
 *   <li>{@code _commit_version} — the commit version containing this change. Must be
 *       either {@code LongType} or {@code StringType}; all other types are rejected.
 *       The column's natural ordering (numeric for {@code LongType}, lexicographic for
 *       {@code StringType}) must match commit order, because the netChanges
 *       post-processing path sorts rows of a given row identity by this column to
 *       determine the first and last events.</li>
 *   <li>{@code _commit_timestamp} (TIMESTAMP) -- the timestamp of the commit. All rows
 *       belonging to a single {@code _commit_version} must share the same
 *       {@code _commit_timestamp}. For streaming reads with post-processing enabled,
 *       two additional requirements apply:
 *       <ol>
 *         <li>All rows of a single commit must appear in the same micro-batch (i.e.
 *             micro-batch boundaries align with commit boundaries).</li>
 *         <li>Each micro-batch's rows must have {@code _commit_timestamp} strictly
 *             greater than the maximum {@code _commit_timestamp} of any prior
 *             micro-batch.</li>
 *       </ol>
 *       Streaming post-processing uses {@code _commit_timestamp} as event time with a
 *       zero-delay watermark, so once a micro-batch observes max event time T the
 *       global watermark advances to T. Both Spark's late-event filter and its
 *       state-eviction predicate then use {@code eventTime <= T} -- so any later row
 *       at {@code _commit_timestamp <= T} (whether from the same commit split across
 *       batches, a different commit emitted later, or simply an out-of-order commit)
 *       is silently dropped as late. Requirement 1 keeps a single commit's rows
 *       together; requirement 2 keeps distinct commits in strictly increasing
 *       event-time order across batches. Multiple distinct commits with equal
 *       {@code _commit_timestamp} are allowed within a single micro-batch -- only
 *       <em>across</em> batches does timestamp progression need to be strictly
 *       increasing. Atomic-commit CDC connectors (e.g. Delta versions, Iceberg
 *       snapshots) that derive {@code _commit_timestamp} from wall-clock time at
 *       commit time naturally satisfy both requirements.
 *       {@code _commit_timestamp} must be non-{@code NULL} on every row of a streaming
 *       read engaging post-processing; both the row-level Aggregate path and the
 *       netChanges {@code transformWithState} path raise
 *       {@code CHANGELOG_CONTRACT_VIOLATION.NULL_COMMIT_TIMESTAMP} on a violation</li>
 * </ul>
 * <p>
 * Streaming reads support carry-over removal, update detection, and net change
 * computation. Two streaming-specific behaviors to be aware of:
 * <ul>
 *   <li><b>Output is buffered until the watermark advances past the commit.</b>
 *       When a micro-batch ingests a commit, that commit's output rows are
 *       buffered in state and not emitted in the same batch. They are emitted
 *       by a later micro-batch -- whichever one advances the watermark past
 *       the commit's {@code _commit_timestamp}. The last commit's output is
 *       emitted when the source terminates.</li>
 *   <li><b>netChanges only merges changes that are buffered together.</b>
 *       When each row identity appears in at most one commit within any
 *       buffered window, the streaming output is the same as
 *       {@code computeUpdates}. Cross-commit merging only happens when
 *       several commits touch the same row before the earliest one's output
 *       has been released. For full-range collapse, use a batch read.</li>
 * </ul>
 * <p>
 * <b>Pushdown contract.</b> When any post-processing pass applies (carry-over
 * removal, update detection, or netChanges), Spark only pushes predicates
 * that reference {@code _commit_version}, {@code _commit_timestamp}, or
 * columns named by {@link #rowId()} to the connector's
 * {@link org.apache.spark.sql.connector.read.SupportsPushDownFilters} /
 * {@link org.apache.spark.sql.connector.read.SupportsPushDownV2Filters}.
 * Predicates on {@code _change_type}, the {@link #rowVersion()} column, or
 * non-rowId data columns are kept above the scan: pushing them would drop
 * one half of a delete/insert pair within a row-identity group and silently
 * break post-processing. Catalyst's pushdown rules enforce this via the
 * rewrite operators, so connectors do not need to code the restriction
 * themselves -- but must not bypass it via connector-specific options. When
 * no post-processing pass applies, Spark does not impose any CDC-specific
 * predicate-pushdown restriction.
 * {@link org.apache.spark.sql.connector.read.SupportsPushDownRequiredColumns}
 * (column pruning) is unrestricted in either case: Spark's pruning already
 * respects what the rewrite operators reference.
 *
 * @since 4.2.0
 */
@Evolving
public interface Changelog {

  /** Constant for the {@code _change_type} value of a row inserted into the table. */
  String CHANGE_TYPE_INSERT = "insert";
  /** Constant for the {@code _change_type} value of a row deleted from the table. */
  String CHANGE_TYPE_DELETE = "delete";
  /** Constant for the {@code _change_type} value of an update's pre-image row. */
  String CHANGE_TYPE_UPDATE_PREIMAGE = "update_preimage";
  /** Constant for the {@code _change_type} value of an update's post-image row. */
  String CHANGE_TYPE_UPDATE_POSTIMAGE = "update_postimage";

  /** A name to identify this changelog. */
  String name();

  /**
   * Returns the columns of this changelog, including data columns and the required
   * metadata columns ({@code _change_type}, {@code _commit_version},
   * {@code _commit_timestamp}).
   */
  Column[] columns();

  /**
   * Whether the raw change data may contain identical insert/delete carry-over pairs
   * produced by copy-on-write file rewrites.
   * <p>
   * When {@code true} and the CDC query's {@code deduplicationMode} is not {@code none},
   * Spark will remove carry-over pairs from the raw change data.
   * If {@code false}, the connector guarantees that no carry-over pairs are present in the
   * raw change data and Spark will skip carry-over removal entirely.
   */
  boolean containsCarryoverRows();

  /**
   * Whether the raw change data may contain multiple intermediate states per row identity
   * within the requested changelog range (across all commit versions in the range).
   * <p>
   * When {@code true} and the CDC query's {@code deduplicationMode} is {@code netChanges},
   * Spark will collapse multiple changes per row identity into the net effect.
   * If {@code false}, the connector guarantees at most one change per row identity across
   * the entire changelog range, and Spark will skip net change computation.
   * <p>
   * Note this flag is range-scoped (across all commits in the request), not
   * micro-batch-scoped.
   */
  boolean containsIntermediateChanges();

  /**
   * Whether updates in the raw change data are represented as delete+insert pairs rather
   * than fully materialized {@code update_preimage} and {@code update_postimage} entries.
   * <p>
   * When {@code true} and the CDC query's {@code computeUpdates} option is enabled,
   * Spark will derive {@code update_preimage}/{@code update_postimage} from insert/delete
   * pairs in the raw change data.
   * If {@code false}, the connector guarantees that update pre/post-images are already
   * present in the raw change data.
   */
  boolean representsUpdateAsDeleteAndInsert();

  /**
   * Returns a new {@link ScanBuilder} for reading the change data.
   *
   * @param options read options (case-insensitive string map)
   */
  ScanBuilder newScanBuilder(CaseInsensitiveStringMap options);

  /**
   * Returns the columns that uniquely identify a row, used for carry-over removal, update
   * detection, and net change computation.
   * <p>
   * The default implementation throws {@link UnsupportedOperationException}. Connectors must
   * override this method when any of {@link #containsCarryoverRows()},
   * {@link #representsUpdateAsDeleteAndInsert()}, or {@link #containsIntermediateChanges()}
   * returns {@code true}. Each referenced column must be non-nullable.
   */
  default NamedReference[] rowId() {
    throw new UnsupportedOperationException("rowId is not supported.");
  }

  /**
   * Returns the column that holds the row version — the commit version at which the row's
   * content was last modified. The row version has these properties:
   * <ul>
   *   <li>Assigned the current commit version when the row is initially inserted.</li>
   *   <li>Bumped to the current commit version when the row's content is updated.</li>
   *   <li><b>Preserved</b> when the row is rewritten by a copy-on-write operation without a
   *       content change — it is NOT bumped to the current commit version.</li>
   * </ul>
   * The row version is distinct from {@code _commit_version}. {@code _commit_version}
   * identifies the commit that emitted this change row; the row version identifies the commit
   * that last wrote the row's content. For a delete+insert pair produced within a single
   * commit, both halves share the same row version if the pair is a copy-on-write
   * carry-over, and have different row versions (old on the delete, new on the insert) if
   * the pair is a true update.
   * <p>
   * Spark uses the row version to distinguish copy-on-write carry-over from update without
   * scanning data columns, for both carry-over removal and update detection.
   * <p>
   * The default implementation throws {@link UnsupportedOperationException}. Connectors must
   * override this method when {@link #containsCarryoverRows()} or
   * {@link #representsUpdateAsDeleteAndInsert()} returns {@code true}. The referenced
   * column must be non-nullable.
   */
  default NamedReference rowVersion() {
    throw new UnsupportedOperationException("rowVersion is not supported.");
  }
}
