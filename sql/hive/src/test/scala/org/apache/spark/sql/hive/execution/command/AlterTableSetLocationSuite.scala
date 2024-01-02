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

package org.apache.spark.sql.hive.execution.command

import org.apache.spark.sql.catalyst.TableIdentifier
import org.apache.spark.sql.catalyst.catalog.CatalogTypes.TablePartitionSpec
import org.apache.spark.sql.execution.command.v1
import org.apache.spark.sql.internal.SQLConf

/**
 * The class contains tests for the `ALTER TABLE .. SET LOCATION` command to check
 * V1 Hive external table catalog.
 */
class AlterTableSetLocationSuite extends v1.AlterTableSetLocationSuiteBase with CommandSuiteBase {

  override def buildCreateTableSQL(t: String): String = {
    s"""CREATE TABLE $t (col1 int, col2 string, a int, b int)
       |PARTITIONED BY (a, b)
       |ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe'
       |STORED AS INPUTFORMAT 'org.apache.hadoop.mapred.SequenceFileInputFormat'
       |OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.HiveSequenceFileOutputFormat'""".stripMargin
  }

  private def buildBuckeTableCreateTableSQL(t: String): String = {
    s"""CREATE TABLE $t (col1 int, col2 string, a int, b int)
       |PARTITIONED BY (a, b)
       |CLUSTERED BY (col1) INTO 4 BUCKET
       |ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe'
       |STORED AS INPUTFORMAT 'org.apache.hadoop.mapred.SequenceFileInputFormat'
       |OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.HiveSequenceFileOutputFormat'""".stripMargin
  }

  // Verify table and partition bucketSpec
  private def checkBucketSpec(
                               tableIdent: TableIdentifier,
                               spec: TablePartitionSpec): Unit = {
    val ht = sessionCatalog.getTableMetadata(tableIdent)
    val part = sessionCatalog.getPartition(tableIdent, spec)
    assert(ht.bucketSpec == part.bucketSpec)
  }

  test("SPARK-46477: Alter table table_name " +
    "partition part_spec set location for bucketed hive Table") {
    withNamespaceAndTable("ns", "tbl") { t =>
      sql(buildBuckeTableCreateTableSQL(t))
      sql(s"INSERT INTO $t PARTITION (a = '1', b = '2') SELECT 1, 'abc'")

      val tableIdent = TableIdentifier("tbl", Some("ns"))
      val partSpec = Map("a" -> "1", "b" -> "2")

      val catalogTable = sessionCatalog.getTableMetadata(tableIdent)
      assert(catalogTable.storage.locationUri.isDefined)
      assert(normalizeSerdeProp(catalogTable.storage.properties).isEmpty)

      val catalogTablePartition = sessionCatalog.getPartition(tableIdent, partSpec)
      assert(catalogTablePartition.storage.locationUri.isDefined)
      assert(normalizeSerdeProp(catalogTablePartition.storage.properties).isEmpty)

      // set table partition location
      sql(s"ALTER TABLE $t PARTITION (a='1', b='2') SET LOCATION '/path/to/part/ways'")
      checkLocation(tableIdent, new URI("/path/to/part/ways"), Some(partSpec))
      checkBucketSpec(tableIdent, partSpec)

      // set location for partition spec in the upper case
      withSQLConf(SQLConf.CASE_SENSITIVE.key -> "false") {
        sql(s"ALTER TABLE $t PARTITION (A='1', B='2') SET LOCATION '/path/to/part/ways2'")
        checkLocation(tableIdent, new URI("/path/to/part/ways2"), Some(partSpec))
        checkBucketSpec(tableIdent, partSpec)
      }
    }
  }

}
