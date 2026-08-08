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

  override def build(): Scan =
    new PythonScan(ds, shortName, outputSchema, options, supportedFilters, pushedLimit)

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
        // Filter pushdown also returns partitions and the read function.
        // This helps reduce the number of Python worker calls.
        ds.setReadInfo(result.readInfo)

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
    val result = ds.source.pushdownLimitInPython(dataSource, outputSchema, allFilters, limit)

    // The replay runs `pushFilters` on a fresh reader, which must reach the same decision as the
    // first pass. Spark has already committed to that first decision -- `pushedFilters()` was
    // read by the optimizer and the filters it reported were dropped from the plan -- so a
    // reader whose `pushFilters` is not deterministic would leave Spark applying the first
    // pass's filters while reading with the second pass's reader, silently returning wrong rows.
    // Fail fast instead.
    val replayedFilters = result.isFilterPushed.zip(allFilters).collect {
      case (true, filter) => filter
    }.toArray
    if (!replayedFilters.sameElements(supportedFilters)) {
      throw QueryCompilationErrors.pythonDataSourceError(
        action = "plan",
        tpe = "limit",
        msg = "pushFilters() returned a different set of supported filters when it was called " +
          "again to push down a limit: " +
          s"[${supportedFilters.mkString(", ")}] then [${replayedFilters.mkString(", ")}]. " +
          "pushFilters() must be deterministic.")
    }

    // Limit pushdown also returns partitions and the read function, which reflect the pushed
    // limit. They are valid whether or not the limit was pushed, because the filters were
    // replayed identically, so always replace the read info computed by filter pushdown.
    ds.setReadInfo(result.readInfo)
    if (result.isLimitPushed) {
      pushedLimit = Some(limit)
    }
    result.isLimitPushed
  }

  // Spark always applies the LIMIT again after the scan: a Python data source is not trusted to
  // return at most `limit` rows, and it is free to over-deliver.
  override def isPartiallyPushed(): Boolean = true
}
