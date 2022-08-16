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

package org.apache.spark.sql.execution.command.v1

import java.net.URI

import org.apache.hadoop.fs.Path

import org.apache.spark.sql.AnalysisException
import org.apache.spark.sql.catalyst.TableIdentifier
import org.apache.spark.sql.catalyst.catalog.CatalogTypes.TablePartitionSpec
import org.apache.spark.sql.catalyst.catalog.CatalogUtils
import org.apache.spark.sql.execution.command
import org.apache.spark.sql.internal.SQLConf

/**
 * This base suite contains unified tests for the `ALTER TABLE .. SET LOCATION`
 * command that check V1 table catalogs. The tests that cannot run for all V1 catalogs
 * are located in more specific test suites:
 *
 *   - V1 In-Memory catalog: `org.apache.spark.sql.execution.command.v1.AlterTableSetLocationSuite`
 *   - V1 Hive External catalog:
 *     `org.apache.spark.sql.hive.execution.command.AlterTableSetLocationSuite`
 */
trait AlterTableSetLocationSuiteBase extends command.AlterTableSetLocationSuiteBase {

  private lazy val sessionCatalog = spark.sessionState.catalog

  protected def buildCreateTableSQL(t: String): String

  private def normalizeSerdeProp(props: Map[String, String]): Map[String, String] = {
    props.filterNot(p => Seq("serialization.format", "path").contains(p._1))
  }

  // Verify that the location is set to the expected string
  private def checkLocation(
      tableIdent: TableIdentifier,
      expected: URI,
      spec: Option[TablePartitionSpec] = None): Unit = {
    val storageFormat = spec
      .map { s => sessionCatalog.getPartition(tableIdent, s).storage }
      .getOrElse { sessionCatalog.getTableMetadata(tableIdent).storage }
    assert(storageFormat.locationUri ===
      Some(makeQualifiedPath(CatalogUtils.URIToString(expected))))
  }

  test("alter table set location") {
    withNamespaceAndTable("ns", "tbl") { t =>
      sql(buildCreateTableSQL(t))
      sql(s"INSERT INTO $t PARTITION (a = '1', b = '2') SELECT 1, 'abc'")

      val tableIdent = TableIdentifier("tbl", Some("ns"))
      val partSpec = Map("a" -> "1", "b" -> "2")

      val catalogTable = sessionCatalog.getTableMetadata(tableIdent)
      assert(catalogTable.storage.locationUri.isDefined)
      assert(normalizeSerdeProp(catalogTable.storage.properties).isEmpty)

      val catalogTablePartition = sessionCatalog.getPartition(tableIdent, partSpec)
      assert(catalogTablePartition.storage.locationUri.isDefined)
      assert(normalizeSerdeProp(catalogTablePartition.storage.properties).isEmpty)

      // set table location
      sql(s"ALTER TABLE $t SET LOCATION '/path/to/your/lovely/heart'")
      checkLocation(tableIdent, new URI("/path/to/your/lovely/heart"))

      // set table partition location
      sql(s"ALTER TABLE $t PARTITION (a='1', b='2') SET LOCATION '/path/to/part/ways'")
      checkLocation(tableIdent, new URI("/path/to/part/ways"), Some(partSpec))

      // set location for partition spec in the upper case
      withSQLConf(SQLConf.CASE_SENSITIVE.key -> "false") {
        sql(s"ALTER TABLE $t PARTITION (A='1', B='2') SET LOCATION '/path/to/part/ways2'")
        checkLocation(tableIdent, new URI("/path/to/part/ways2"), Some(partSpec))
      }
      withSQLConf(SQLConf.CASE_SENSITIVE.key -> "true") {
        val e = intercept[AnalysisException] {
          sql(s"ALTER TABLE $t PARTITION (A='1', B='2') SET LOCATION '/path/to/part/ways3'")
        }.getMessage
        assert(e.contains("not a valid partition column"))
      }

      sessionCatalog.setCurrentDatabase("ns")
      // set table location without explicitly specifying database
      sql("ALTER TABLE tbl SET LOCATION '/swanky/steak/place'")
      checkLocation(tableIdent, new URI("/swanky/steak/place"))
      // set table partition location without explicitly specifying database
      sql("ALTER TABLE tbl PARTITION (a='1', b='2') SET LOCATION 'vienna'")
      val table = sessionCatalog.getTableMetadata(TableIdentifier("tbl"))
      val viennaPartPath = new Path(new Path(table.location), "vienna")
      checkLocation(tableIdent, CatalogUtils.stringToURI(viennaPartPath.toString), Some(partSpec))
    }
  }

  test("table to alter set location does not exist") {
    val e = intercept[AnalysisException] {
      sql("ALTER TABLE ns.does_not_exist SET LOCATION '/mister/spark'")
    }
    assert(e.getMessage.contains("Table not found: ns.does_not_exist"))
  }

  test("partition to alter set location does not exist") {
    withNamespaceAndTable("ns", "tbl") { t =>
      sql(buildCreateTableSQL(t))

      sql(s"INSERT INTO $t PARTITION (a = '1', b = '2') SELECT 1, 'abc'")
      val e = intercept[AnalysisException] {
        sql(s"ALTER TABLE $t PARTITION (b='2') SET LOCATION '/mister/spark'")
      }
      assert(e.getMessage == "Partition spec is invalid. The spec (b) must match the partition " +
        "spec (a, b) defined in table '`spark_catalog`.`ns`.`tbl`'")
    }
  }
}

class AlterTableSetLocationSuite extends AlterTableSetLocationSuiteBase with CommandSuiteBase {

  override def buildCreateTableSQL(t: String): String =
    s"CREATE TABLE $t (col1 int, col2 string, a int, b int) $defaultUsing PARTITIONED BY (a, b)"
}
