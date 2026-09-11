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
package org.apache.spark.sql.execution.datasources.v2.python

import org.apache.spark.sql.connector.read.{Scan, ScanBuilder, SupportsPushDownFilters, SupportsPushDownLimit}
import org.apache.spark.sql.errors.QueryCompilationErrors
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.sources.Filter
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.util.CaseInsensitiveStringMap


class PythonScanBuilder(
    ds: PythonDataSourceV2,
    shortName: String,
    outputSchema: StructType,
    options: CaseInsensitiveStringMap)
    extends ScanBuilder
    with SupportsPushDownFilters
    with SupportsPushDownLimit {
  private var supportedFilters: Array[Filter] = Array.empty
  // All filters handed to `pushFilters`, kept so that `pushLimit` can replay them and bring the
  // Python reader back to the same state before calling `pushLimit` on it.
  private var allFilters: Array[Filter] = Array.empty
  private var pushedLimit: Option[Int] = None
  // Read info (partitions + read function) produced as a side effect of filter/limit pushdown,
  // carried into the `PythonScan` so it stays scoped to this scan. It must NOT be stored on the
  // provider-scoped `PythonDataSourceV2`: a single data source instance is shared by every scan
  // built from it -- e.g. a base DataFrame and its `.limit(n)` reuse the same relation -- so a
  // pushdown-specific read function stored there would leak into an unrelated scan and make it
  // read too few rows.
  private var readInfo: Option[PythonDataSourceReadInfo] = None
  // True when a pushdown pass ran but produced no read info, so build() must plan the read
  // itself. Two paths set it: (1) the filter-pushdown pass defers planning while limit pushdown
  // is enabled and a limit pass might follow; (2) a pushed limit was rejected, so planning falls
  // back to a fresh reader. In both, build() plans the read with the filters only. This is
  // distinct from `readInfo.isEmpty` when both pushdowns are disabled -- in that case, no
  // pushdown pass ran and `PythonScan` plans the read later instead, so build() must not plan.
  private var deferredPlanning: Boolean = false

  override def build(): Scan = {
    if (deferredPlanning && readInfo.isEmpty) {
      // Reached when a pushdown pass ran but planned no read info: either the filter pass
      // deferred planning and no limit followed, or a pushed limit was rejected (including a
      // limit-only scan with no filters). Plan now, with the filters only and no limit, so that
      // partitions()/read() run exactly once and only after all pushdowns are known -- never in
      // the filter pass before a possible pushLimit.
      val dataSource = ds.getOrCreateDataSourceInPython(shortName, options, Some(outputSchema))
      val result = ds.source.pushdownLimitInPython(dataSource, outputSchema, allFilters, None)
      checkFiltersReplayedDeterministically(result)
      readInfo = result.readInfo
    }
    new PythonScan(ds, shortName, outputSchema, options, supportedFilters, pushedLimit, readInfo)
  }

  // Optionally called by DSv2 once to push down filters before the scan is built.
  override def pushFilters(filters: Array[Filter]): Array[Filter] = {
    if (!SQLConf.get.pythonFilterPushDown) {
      return filters
    }
    allFilters = filters

    val dataSource = ds.getOrCreateDataSourceInPython(shortName, options, Some(outputSchema))
    ds.source.pushdownFiltersInPython(dataSource, outputSchema, filters) match {
      case None => filters // No filters are supported.
      case Some(result) =>
        // Filter pushdown may also return the read function and partitions. It defers that
        // planning when limit pushdown is enabled -- so partitions()/read() are not planned
        // before a possible pushLimit -- in which case the limit pass or build() plans instead.
        readInfo = result.readInfo
        deferredPlanning = result.readInfo.isEmpty

        // Partition the filters into supported and unsupported ones.
        val isPushed = result.isFilterPushed.zip(filters)
        supportedFilters = isPushed.collect { case (true, filter) => filter }.toArray
        val unsupported = isPushed.collect { case (false, filter) => filter }.toArray
        unsupported
    }
  }

  override def pushedFilters(): Array[Filter] = supportedFilters

  // Optionally called by DSv2 once to push down a LIMIT before the scan is built. DSv2 calls this
  // after `pushFilters`, so the filters that were pushed there (none, for a query without
  // pushable filters) are replayed here to rebuild the same reader state before `pushLimit` is
  // invoked on it in Python.
  override def pushLimit(limit: Int): Boolean = {
    if (!SQLConf.get.pythonLimitPushDown) {
      return false
    }

    val dataSource = ds.getOrCreateDataSourceInPython(shortName, options, Some(outputSchema))
    val result = ds.source.pushdownLimitInPython(dataSource, outputSchema, allFilters, Some(limit))
    checkFiltersReplayedDeterministically(result)

    // Adopt the read info the worker planned. It is empty when the reader rejected the limit --
    // the worker plans nothing then, to keep the rejected (possibly mutated) reader state out of
    // the scan -- in which case build() plans the filters-only read from a fresh reader and
    // re-validates the replayed filters. Record the pushed limit only when the reader accepted it.
    readInfo = result.readInfo
    deferredPlanning = result.readInfo.isEmpty
    if (result.isLimitPushed) {
      pushedLimit = Some(limit)
    }
    result.isLimitPushed
  }

  // The read is replayed on a fresh reader (to push a limit, or to plan after filter pushdown
  // deferred). That replay runs `pushFilters` again, which must reach the same decision as the
  // first pass. Spark has already committed to that first decision -- `pushedFilters()` was read
  // by the optimizer and the filters it reported were dropped from the plan -- so a reader whose
  // `pushFilters` is not deterministic would leave Spark applying the first pass's filters while
  // reading with the replayed reader, silently returning wrong rows. Fail fast instead.
  private def checkFiltersReplayedDeterministically(result: PythonFilterPushdownResult): Unit = {
    val replayedFilters = result.isFilterPushed.zip(allFilters).collect {
      case (true, filter) => filter
    }.toArray
    if (!replayedFilters.sameElements(supportedFilters)) {
      throw QueryCompilationErrors.pythonDataSourceError(
        action = "plan",
        tpe = "read",
        msg = "pushFilters() returned a different set of supported filters when it was replayed " +
          s"during planning: [${supportedFilters.mkString(", ")}] then " +
          s"[${replayedFilters.mkString(", ")}]. pushFilters() must be deterministic.")
    }
  }

  // Spark always applies the LIMIT again after the scan: a Python data source is not trusted to
  // return at most `limit` rows, and it is free to over-deliver.
  override def isPartiallyPushed(): Boolean = true
}
