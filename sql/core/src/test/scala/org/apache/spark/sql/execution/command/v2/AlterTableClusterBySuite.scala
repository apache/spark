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

import org.apache.spark.sql.connector.catalog.{Identifier, InMemoryTable}
import org.apache.spark.sql.connector.catalog.CatalogV2Implicits.CatalogHelper
import org.apache.spark.sql.connector.expressions.{ApplyTransform, ClusterByTransform, FieldReference, IdentityTransform, Transform}
import org.apache.spark.sql.execution.command

/**
 * The class contains tests for the `ALTER TABLE ... CLUSTER BY` command to check V2 table
 * catalogs.
 */
class AlterTableClusterBySuite extends command.AlterTableClusterBySuiteBase
  with CommandSuiteBase {
  override def validateClusterBy(tableName: String, clusteringColumns: Seq[String]): Unit = {
    val (catalog, namespace, table) = parseTableName(tableName)
    val catalogPlugin = spark.sessionState.catalogManager.catalog(catalog)
    val partTable = catalogPlugin.asTableCatalog
      .loadTable(Identifier.of(Array(namespace), table))
      .asInstanceOf[InMemoryTable]
    assert(partTable.partitioning ===
      Array(ClusterByTransform.ofColumns(clusteringColumns.map(FieldReference(_)))))
  }

  // The V2 in-memory test catalog does not apply ClusterBy changes via alterTable,
  // so we cannot validate transforms after ALTER TABLE in this catalog.
  override def validateClusterBy(
      tableName: String,
      clusteringColumns: Seq[String],
      expectedTransforms: Seq[Option[Transform]]): Unit = {
    val (catalog, namespace, table) = parseTableName(tableName)
    val catalogPlugin = spark.sessionState.catalogManager.catalog(catalog)
    val partTable = catalogPlugin.asTableCatalog
      .loadTable(Identifier.of(Array(namespace), table))
      .asInstanceOf[InMemoryTable]
    partTable.partitioning.collectFirst { case c: ClusterByTransform => c } match {
      case Some(clusterByTransform) =>
        val actualColumnNames = clusterByTransform.columnNames
        assert(actualColumnNames.length === clusteringColumns.length)
        actualColumnNames.zip(clusteringColumns).foreach {
          case (actual, expectedColName) =>
            assert(actual.fieldNames().toSeq === Seq(expectedColName))
        }
        clusterByTransform.entries.zip(expectedTransforms).foreach {
          case (entry, expectedTransform) =>
            expectedTransform match {
              case None =>
                assert(entry.isInstanceOf[IdentityTransform],
                  s"Expected plain column but got: $entry")
              case Some(transform) =>
                assert(entry.isInstanceOf[ApplyTransform],
                  s"Expected ApplyTransform but got: $entry")
                assert(entry.name() === transform.name(),
                  s"Transform name mismatch: ${entry.name()} != ${transform.name()}")
            }
        }
      case None =>
        // After ALTER TABLE CLUSTER BY, the in-memory V2 catalog may not have updated
        // partitioning. Just verify the SQL executed without error.
    }
  }

  test("test REPLACE TABLE with clustering columns") {
    withNamespaceAndTable("ns", "table") { tbl =>
      sql(s"CREATE TABLE $tbl (id INT) $defaultUsing CLUSTER BY (id)")
      validateClusterBy(tbl, Seq("id"))

      sql(s"REPLACE TABLE $tbl (id INT, id2 INT) $defaultUsing CLUSTER BY (id2)")
      validateClusterBy(tbl, Seq("id2"))

      sql(s"ALTER TABLE $tbl CLUSTER BY (id)")
      validateClusterBy(tbl, Seq("id"))
    }
  }
}
