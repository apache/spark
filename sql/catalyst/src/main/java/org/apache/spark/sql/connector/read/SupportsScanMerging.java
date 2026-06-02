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

package org.apache.spark.sql.connector.read;

import java.util.Optional;

import org.apache.spark.annotation.Evolving;
import org.apache.spark.sql.connector.catalog.SupportsRead;

/**
 * A mix-in interface for {@link Scan}. Data sources can implement this interface to allow
 * Spark's optimizer to fuse two {@link Scan}s of the same {@link SupportsRead} table into a
 * single {@link Scan} covering both: it reads the union of their projected columns and a
 * superset of their rows, from which each original scan's result can be recovered by a
 * projection and filter applied above the merged scan.
 * <p>
 * Spark calls {@link #mergeWith(SupportsScanMerging, SupportsRead)} when it detects that two
 * scans target the same table and read structurally compatible data (e.g. same file
 * index, schema, partitioning) but differ in pushed predicates, projections, or other
 * scan-level state. Implementations decide whether the two scans can be safely combined
 * and, if so, return the merged scan via {@link Optional#of}. Returning {@link Optional#empty}
 * declines the merge.
 * <p>
 * <b>Contract for implementations:</b>
 * <ul>
 *   <li>{@link #mergeWith} must be commutative: {@code a.mergeWith(b, t)} and
 *       {@code b.mergeWith(a, t)} must produce semantically equivalent merged scans (or both
 *       decline).</li>
 *   <li>The merged scan must produce a superset of the rows produced by either input scan.
 *       Read-side filtering can then be applied above the scan to recover the original
 *       per-scan rows, e.g. via {@code FILTER (WHERE ...)} clauses on aggregates.</li>
 *   <li>If the two scans carry state that cannot be safely combined -- for example a pushed
 *       aggregate, or incompatible partitioning -- the implementation must decline by
 *       returning {@link Optional#empty}.</li>
 *   <li>The {@code table} argument is the {@link SupportsRead} that owns both scans;
 *       implementations typically use it to obtain a fresh
 *       {@link ScanBuilder} via {@link SupportsRead#newScanBuilder} for constructing the
 *       merged scan.</li>
 * </ul>
 *
 * @since 4.3.0
 */
@Evolving
public interface SupportsScanMerging extends Scan {

  /**
   * Attempts to merge this scan with {@code other} into a single equivalent scan.
   *
   * @param other the other scan to merge with; guaranteed to also implement
   *              {@link SupportsScanMerging} and to be a scan of the same {@code table}
   * @param table the {@link SupportsRead} table that owns both scans, used to obtain
   *              a {@link ScanBuilder} for constructing the merged scan
   * @return {@link Optional#of} the merged scan if merging is supported, or
   *         {@link Optional#empty} to decline
   */
  Optional<SupportsScanMerging> mergeWith(SupportsScanMerging other, SupportsRead table);
}
