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

import org.apache.spark.sql.connector.read.{Scan, ScanBuilder, SupportsPushDownFilters}
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
    with SupportsPushDownFilters {
  private var supportedFilters: Array[Filter] = Array.empty

  override def build(): Scan =
    new PythonScan(ds, shortName, outputSchema, options, supportedFilters)

  // Optionally called by DSv2 once to push down filters before the scan is built.
  override def pushFilters(filters: Array[Filter]): Array[Filter] = {
    if (!SQLConf.get.pythonFilterPushDown) {
      return filters
    }

    val dataSource = ds.getOrCreateDataSourceInPython(shortName, options, Some(outputSchema))
    val result = ds.source.pushdownFiltersInPython(dataSource, outputSchema, filters)

    // The Data Source instance state changes after pushdown to remember the reader instance
    // created and the filters pushed down. So pushdownFiltersInPython returns a new pickled
    // Data Source instance. We need to use that new instance for further operations.
    ds.setDataSourceInPython(dataSource.copy(dataSource = result.dataSource))

    // Partition the filters into supported and unsupported ones.
    val isPushed = result.isFilterPushed.zip(filters)
    supportedFilters = isPushed.collect { case (true, filter) => filter }.toArray
    val unsupported = isPushed.collect { case (false, filter) => filter }.toArray
    unsupported
  }

  override def pushedFilters(): Array[Filter] = supportedFilters
}
