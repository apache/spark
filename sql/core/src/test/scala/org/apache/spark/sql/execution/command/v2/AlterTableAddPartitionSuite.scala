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

import org.apache.spark.sql.AnalysisException
import org.apache.spark.sql.catalyst.analysis.ResolvePartitionSpec
import org.apache.spark.sql.catalyst.catalog.CatalogTypes.TablePartitionSpec
import org.apache.spark.sql.connector.InMemoryPartitionTable
import org.apache.spark.sql.connector.catalog.{CatalogV2Implicits, Identifier}
import org.apache.spark.sql.execution.command

class AlterTableAddPartitionSuite
  extends command.AlterTableAddPartitionSuiteBase
  with CommandSuiteBase {

  import CatalogV2Implicits._

  override protected def checkLocation(
      t: String,
      spec: TablePartitionSpec,
      expected: String): Unit = {
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

  test("SPARK-33650: add partition into a table which doesn't support partition management") {
    withNamespaceAndTable("ns", "tbl", s"non_part_$catalog") { t =>
      sql(s"CREATE TABLE $t (id bigint, data string) $defaultUsing")
      val errMsg = intercept[AnalysisException] {
        sql(s"ALTER TABLE $t ADD PARTITION (id=1)")
      }.getMessage
      assert(errMsg.contains(s"Table $t can not alter partitions"))
    }
  }

  test("empty string as partition value") {
    withNamespaceAndTable("ns", "tbl") { t =>
      sql(s"CREATE TABLE $t (col1 INT, p1 STRING) $defaultUsing PARTITIONED BY (p1)")
      sql(s"ALTER TABLE $t ADD PARTITION (p1 = '')")
      checkPartitions(t, Map("p1" -> ""))
    }
  }
}
