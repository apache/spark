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
import org.apache.spark.sql.catalyst.TableIdentifier
import org.apache.spark.sql.connector.catalog.{Identifier, InMemoryPartitionTable}
import org.apache.spark.sql.connector.catalog.CatalogV2Implicits.CatalogHelper
import org.apache.spark.sql.connector.expressions.{ClusterByTransform, FieldReference}
import org.apache.spark.sql.execution.command

/**
 * The class contains tests for the `CREATE TABLE ... CLUSTER BY` command to check V2 table
 * catalogs.
 */
class CreateTableClusterBySuite extends command.CreateTableClusterBySuiteBase
  with CommandSuiteBase {
  override def validateClusterBy(
      tableIdent: TableIdentifier, clusteringColumns: Seq[String]): Unit = {
    val catalogPlugin = spark.sessionState.catalogManager.catalog(tableIdent.catalog.get)
    val partTable = catalogPlugin.asTableCatalog
      .loadTable(Identifier.of(Array(tableIdent.database.get), tableIdent.table))
      .asInstanceOf[InMemoryPartitionTable]
    assert(partTable.partitioning ===
      Array(ClusterByTransform(clusteringColumns.map(FieldReference(_)))))
  }

  test("test basic CREATE/REPLACE TABLE with clustering columns") {
    withNamespaceAndTable("ns", "table") { tbl =>
      spark.sql(s"CREATE TABLE $tbl (id INT) $defaultUsing CLUSTER BY (id)")
      validateClusterBy(toTableIdentifier(tbl), Seq("id"))

      spark.sql(s"REPLACE TABLE $tbl (id2 INT) $defaultUsing CLUSTER BY (id2)")
      validateClusterBy(toTableIdentifier(tbl), Seq("id2"))
    }
  }

  test("clustering columns not defined in schema") {
    withNamespaceAndTable("ns", "table") { tbl =>
      val err = intercept[AnalysisException] {
        sql(s"CREATE TABLE $tbl (id bigint, data string) $defaultUsing CLUSTER BY (unknown)")
      }
      assert(err.message.contains("Couldn't find column unknown in:"))
    }
  }
}
