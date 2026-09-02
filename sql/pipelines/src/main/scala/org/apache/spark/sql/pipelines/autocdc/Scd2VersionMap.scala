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

package org.apache.spark.sql.pipelines.autocdc

import org.apache.spark.sql.types.{BooleanType, MapType, StringType}

/**
 * Per-row column authorship tracker for SCD2 ignore-null semantics.
 *
 * Recall in SCD2, every materialized row traces back to an upsert event that created it (and
 * if the row is closed then also a delete/succession event that closed it, but that's not
 * relevant here). Every data column in the row is at least partially derived by the
 * corresponding data column in the upsert event that spawned the row.
 *
 * For columns where ignore-null was not applied, the data column in the row is fully derived
 * (authored) by the corresponding data column in the upsert event. For columns where
 * ignore-null was applied however, if the data column in the upsert event was null
 * (unauthored), then we need to look backwards to deduce the corresponding inherited data
 * column for the row.
 *
 * Non-null values in an event are always considered authored, regardless of whether the column
 * in the event was included in the ignore-null configuration or not. Null values however, as
 * mentioned above, may or may not be considered authored -- it depends on whether they are
 * specified for a column that was included in the ignore-null configuration.
 *
 * In SCD2 the version map helps us answer per row: for all the columns that received a null
 * value in the upsert event that created this row, which nulls are considered authored vs
 * unauthored?
 *
 * Concretely, the contract of the version map is as follows.
 * 1. Every column that received a null in the event but is considered authored (i.e. not part
 *    of ignore-null selection at ingestion), receives an entry of (column name, true) in the
 *    version map.
 * 2. Every column that received a null in the event but is considered unauthored (i.e. part
 *    of the ignore-null selection at ingestion), receives an entry of (column name, false) in
 *    the version map.
 * 3. Every column that was not present in the event, but schema evolved in later with a null
 *    value, will be treated as unauthored BUT does not yet have any entry in the version map.
 *    An entry will be added as per (2).
 *
 * In a single sentence: if a null column in the SCD2 row is either absent from the version
 * map or has a false value in the version map, the null is considered unauthored by the
 * upsert event that spawned this row. Otherwise the null value was explicitly authored by
 * the row.
 *
 * As mentioned above, authorship is dependent on the configured ignore-null selection, which
 * is free to change between pipeline runs for the same AutoCDC flow. As such, we choose that
 * the version map strictly reflects authorship as of the ignore-null selection that was active
 * when the upsert event that produced this row was ingested. This means the authorship
 * information the version map encoded at creation time is invariant/frozen -- even if the
 * ignore-null selection changes on a future run, the version map is not rewritten (unless the
 * table is full refreshed).
 *
 * It's worth noting that while contract case (3) materializes new entries in the version map
 * after creation, it does not change the set of columns whose null values are considered
 * authored/unauthored. Therefore authorship information encoded by the mutated version map is
 * still invariant, and independent of a changing ignore-null configuration. New rows materialized
 * in the map are still compliant to whatever the ignore-null selection was at ingestion time.
 */
private[pipelines] object Scd2VersionMap {

  /**
   * Schema of the version map: `Map(String, Boolean)`.
   *
   * Keys are dot-delimited paths to *leaf* columns that received a null value in their
   * corresponding upsert event (e.g. `"address.city"`, `` "`has space`.city" ``). Paths
   * must be formatted by [[org.apache.spark.sql.catalyst.util.QuotingUtils.quoted]] to
   * ensure segments that need quoting are back-tick escaped.
   *
   * Values indicate authorship. I.e, `true` => authored-null, `false` => unauthored-null.
   *
   * Lack of entry in the map for a null-valued leaf column implies the column was
   * schema-evolved with an unauthored-null.
   */
  def mapType: MapType = MapType(StringType, BooleanType, valueContainsNull = false)
}
