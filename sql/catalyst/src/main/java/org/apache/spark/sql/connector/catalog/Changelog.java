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
 *   <li>{@code _commit_version} (connector-defined type, e.g. LONG) — the version containing
 *       this change</li>
 *   <li>{@code _commit_timestamp} (TIMESTAMP) — the timestamp of the commit</li>
 * </ul>
 *
 * @since 4.2.0
 */
@Evolving
public interface Changelog {

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
   * Returns the columns that uniquely identify a row, used for update detection and
   * net change computation.
   * <p>
   * The default implementation throws {@link UnsupportedOperationException}. Connectors
   * that support update detection or net change computation must override this method.
   */
  default NamedReference[] rowId() {
    throw new UnsupportedOperationException("rowId is not supported.");
  }

  /**
   * Returns the column used for ordering changes within the same row identity, used for
   * update detection.
   * <p>
   * The default implementation throws {@link UnsupportedOperationException}. Connectors
   * that support update detection must override this method.
   */
  default NamedReference rowVersion() {
    throw new UnsupportedOperationException("rowVersion is not supported.");
  }
}
