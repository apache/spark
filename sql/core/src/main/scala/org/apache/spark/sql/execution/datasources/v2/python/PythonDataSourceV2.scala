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

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.connector.catalog._
import org.apache.spark.sql.connector.expressions.Transform
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.util.CaseInsensitiveStringMap

/**
 * Data Source V2 wrapper for Python Data Source.
 */
class PythonDataSourceV2 extends TableProvider {
  private var name: String = _

  def setShortName(str: String): Unit = {
    assert(name == null)
    name = str
  }

  private def shortName: String = {
    assert(name != null)
    name
  }

  private var dataSourceInPython: PythonDataSourceCreationResult = _
  private[python] lazy val source: UserDefinedPythonDataSource =
    SparkSession.active.sessionState.dataSourceManager.lookupDataSource(shortName)

  def getOrCreateDataSourceInPython(
      shortName: String,
      options: CaseInsensitiveStringMap,
      userSpecifiedSchema: Option[StructType]): PythonDataSourceCreationResult = {
    if (dataSourceInPython == null) {
      dataSourceInPython = source.createDataSourceInPython(shortName, options, userSpecifiedSchema)
    }
    dataSourceInPython
  }

  private var readInfo: PythonDataSourceReadInfo = _
  // The pushdown flags in effect when `readInfo` was cached, as (filter, limit). The cached read
  // info is identical regardless of these flags, but planning it also validates -- via
  // DATA_SOURCE_PUSHDOWN_DISABLED -- that the reader does not implement a pushdown method whose
  // config is off. That validation runs only when the planning worker runs, so reusing the cache
  // across a change in these flags (e.g. a reused relation scanned first with pushdown on, then
  // off) would skip it. Track the flags and recompute when they differ so the check always runs
  // for the flags currently in effect.
  private var readInfoPushdownFlags: Option[(Boolean, Boolean)] = None

  // Caches the pushdown-free read info for reads that push nothing down. This is safe to keep on
  // the (provider-scoped) data source because every such scan produces the same full read info.
  // Pushdown-specific read info is never stored here; it is carried by the `PythonScan` instead,
  // so that it cannot leak across scans that share this data source (e.g. a base DataFrame and
  // its `.limit(n)`).
  def getOrCreateReadInfo(
      shortName: String,
      options: CaseInsensitiveStringMap,
      outputSchema: StructType,
      isStreaming: Boolean
  ): PythonDataSourceReadInfo = {
    val flags = (SQLConf.get.pythonFilterPushDown, SQLConf.get.pythonLimitPushDown)
    if (readInfo == null || !readInfoPushdownFlags.contains(flags)) {
      val creationResult = getOrCreateDataSourceInPython(shortName, options, Some(outputSchema))
      readInfo = source.createReadInfoInPython(creationResult, outputSchema, isStreaming)
      readInfoPushdownFlags = Some(flags)
    }
    readInfo
  }

  override def inferSchema(options: CaseInsensitiveStringMap): StructType = {
    getOrCreateDataSourceInPython(shortName, options, None).schema
  }

  override def getTable(
      schema: StructType,
      partitioning: Array[Transform],
      properties: java.util.Map[String, String]): Table = {
    new PythonTable(this, shortName, schema)
  }

  override def supportsExternalMetadata(): Boolean = true
}
