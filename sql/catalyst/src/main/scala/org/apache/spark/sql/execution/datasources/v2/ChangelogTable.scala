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

package org.apache.spark.sql.execution.datasources.v2

import java.util.{EnumSet => JEnumSet, Set => JSet}

import org.apache.spark.sql.connector.catalog.{Changelog, ChangelogInfo, Column, SupportsRead, Table, TableCapability}
import org.apache.spark.sql.connector.catalog.TableCapability.{BATCH_READ, MICRO_BATCH_READ}
import org.apache.spark.sql.connector.read.ScanBuilder
import org.apache.spark.sql.util.CaseInsensitiveStringMap

/**
 * An internal wrapper that adapts a connector's [[Changelog]] into a DSv2 [[Table]] with
 * [[SupportsRead]], enabling reuse of [[DataSourceV2Relation]] without logical plan changes.
 *
 * This class is NOT part of the connector API. Connectors implement [[Changelog]]; Spark
 * wraps it in [[ChangelogTable]] during analysis.
 */
case class ChangelogTable(
    changelog: Changelog,
    changelogInfo: ChangelogInfo) extends Table with SupportsRead {

  override def name: String = changelog.name

  override def columns: Array[Column] = changelog.columns

  override def newScanBuilder(options: CaseInsensitiveStringMap): ScanBuilder = {
    changelog.newScanBuilder(options)
  }

  override def capabilities: JSet[TableCapability] = JEnumSet.of(BATCH_READ, MICRO_BATCH_READ)
}
