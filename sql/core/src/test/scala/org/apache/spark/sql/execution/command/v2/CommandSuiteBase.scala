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

package org.apache.spark.sql.execution.command.v2

import org.apache.spark.SparkConf
import org.apache.spark.sql.catalyst.analysis.ResolvePartitionSpec
import org.apache.spark.sql.catalyst.catalog.CatalogTypes.TablePartitionSpec
import org.apache.spark.sql.connector.catalog.{CatalogV2Implicits, Identifier, InMemoryPartitionTable, InMemoryPartitionTableCatalog, InMemoryTableCatalog}
import org.apache.spark.sql.test.SharedSparkSession

/**
 * The trait contains settings and utility functions. It can be mixed to the test suites for
 * datasource v2 catalogs (in-memory test catalogs). This trait complements the trait
 * `org.apache.spark.sql.execution.command.DDLCommandTestUtils` with common utility functions
 * for all unified datasource V1 and V2 test suites.
 */
trait CommandSuiteBase extends SharedSparkSession {
  def catalogVersion: String = "V2" // The catalog version is added to test names
  def commandVersion: String = "V2" // The command version is added to test names
  def catalog: String = "test_catalog" // The default V2 catalog for testing
  def defaultUsing: String = "USING _" // The clause is used in creating v2 tables under testing

  // V2 catalogs created and used especially for testing
  override def sparkConf: SparkConf = super.sparkConf
    .set(s"spark.sql.catalog.$catalog", classOf[InMemoryPartitionTableCatalog].getName)
    .set(s"spark.sql.catalog.non_part_$catalog", classOf[InMemoryTableCatalog].getName)

  def checkLocation(
      t: String,
      spec: TablePartitionSpec,
      expected: String): Unit = {
    import CatalogV2Implicits._

    val tablePath = t.split('.')
    val catalogName = tablePath.head
    val namespaceWithTable = tablePath.tail
    val namespaces = namespaceWithTable.init
    val tableName = namespaceWithTable.last
    val catalogPlugin = spark.sessionState.catalogManager.catalog(catalogName)
    val partTable = catalogPlugin.asTableCatalog
      .loadTable(Identifier.of(namespaces, tableName))
      .asInstanceOf[InMemoryPartitionTable]
    val ident = ResolvePartitionSpec.convertToPartIdent(spec, partTable.partitionSchema.fields)
    val partMetadata = partTable.loadPartitionMetadata(ident)

    assert(partMetadata.containsKey("location"))
    assert(partMetadata.get("location") === expected)
  }
}
