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

package org.apache.spark.sql.connector

import org.apache.spark.sql.AnalysisException
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.analysis.NoSuchPartitionsException
import org.apache.spark.sql.connector.catalog.{CatalogV2Implicits, Identifier}
import org.apache.spark.sql.execution.datasources.v2.DataSourceV2Implicits
import org.apache.spark.sql.internal.SQLConf

class AlterTablePartitionV2SQLSuite extends DatasourceV2SQLBase {

  import CatalogV2Implicits._
  import DataSourceV2Implicits._


  test("ALTER TABLE RECOVER PARTITIONS") {
    val t = "testcat.ns1.ns2.tbl"
    withTable(t) {
      spark.sql(s"CREATE TABLE $t (id bigint, data string) USING foo")
      val e = intercept[AnalysisException] {
        sql(s"ALTER TABLE $t RECOVER PARTITIONS")
      }
      assert(e.message.contains("ALTER TABLE RECOVER PARTITIONS is only supported with v1 tables"))
    }
  }

  test("ALTER TABLE RENAME PARTITION") {
    val t = "testcat.ns1.ns2.tbl"
    withTable(t) {
      spark.sql(s"CREATE TABLE $t (id bigint, data string) USING foo PARTITIONED BY (id)")
      val e = intercept[AnalysisException] {
        sql(s"ALTER TABLE $t PARTITION (id=1) RENAME TO PARTITION (id=2)")
      }
      assert(e.message.contains("ALTER TABLE RENAME PARTITION is only supported with v1 tables"))
    }
  }

  test("ALTER TABLE DROP PARTITION") {
    val t = "testpart.ns1.ns2.tbl"
    withTable(t) {
      spark.sql(s"CREATE TABLE $t (id bigint, data string) USING foo PARTITIONED BY (id)")
      spark.sql(s"ALTER TABLE $t ADD PARTITION (id=1) LOCATION 'loc'")
      spark.sql(s"ALTER TABLE $t DROP PARTITION (id=1)")

      val partTable =
        catalog("testpart").asTableCatalog.loadTable(Identifier.of(Array("ns1", "ns2"), "tbl"))
      assert(!partTable.asPartitionable.partitionExists(InternalRow.fromSeq(Seq(1))))
    }
  }

  test("ALTER TABLE DROP PARTITIONS") {
    val t = "testpart.ns1.ns2.tbl"
    withTable(t) {
      spark.sql(s"CREATE TABLE $t (id bigint, data string) USING foo PARTITIONED BY (id)")
      spark.sql(s"ALTER TABLE $t ADD IF NOT EXISTS PARTITION (id=1) LOCATION 'loc'" +
        " PARTITION (id=2) LOCATION 'loc1'")
      spark.sql(s"ALTER TABLE $t DROP PARTITION (id=1), PARTITION (id=2)")

      val partTable =
        catalog("testpart").asTableCatalog.loadTable(Identifier.of(Array("ns1", "ns2"), "tbl"))
      assert(!partTable.asPartitionable.partitionExists(InternalRow.fromSeq(Seq(1))))
      assert(!partTable.asPartitionable.partitionExists(InternalRow.fromSeq(Seq(2))))
      assert(
        partTable.asPartitionable.listPartitionIdentifiers(Array.empty, InternalRow.empty).isEmpty)
    }
  }

  test("ALTER TABLE DROP PARTITIONS: partition not exists") {
    val t = "testpart.ns1.ns2.tbl"
    withTable(t) {
      spark.sql(s"CREATE TABLE $t (id bigint, data string) USING foo PARTITIONED BY (id)")
      spark.sql(s"ALTER TABLE $t ADD PARTITION (id=1) LOCATION 'loc'")

      assertThrows[NoSuchPartitionsException](
        spark.sql(s"ALTER TABLE $t DROP PARTITION (id=1), PARTITION (id=2)"))

      val partTable =
        catalog("testpart").asTableCatalog.loadTable(Identifier.of(Array("ns1", "ns2"), "tbl"))
      assert(partTable.asPartitionable.partitionExists(InternalRow.fromSeq(Seq(1))))

      spark.sql(s"ALTER TABLE $t DROP IF EXISTS PARTITION (id=1), PARTITION (id=2)")
      assert(!partTable.asPartitionable.partitionExists(InternalRow.fromSeq(Seq(1))))
      assert(!partTable.asPartitionable.partitionExists(InternalRow.fromSeq(Seq(2))))
      assert(
        partTable.asPartitionable.listPartitionIdentifiers(Array.empty, InternalRow.empty).isEmpty)
    }
  }

  test("case sensitivity in resolving partition specs") {
    val t = "testpart.ns1.ns2.tbl"
    withTable(t) {
      spark.sql(s"CREATE TABLE $t (id bigint, data string) USING foo PARTITIONED BY (id)")
      withSQLConf(SQLConf.CASE_SENSITIVE.key -> "true") {
        val errMsg = intercept[AnalysisException] {
          spark.sql(s"ALTER TABLE $t DROP PARTITION (ID=1)")
        }.getMessage
        assert(errMsg.contains(s"ID is not a valid partition column in table $t"))
      }

      val partTable = catalog("testpart").asTableCatalog
        .loadTable(Identifier.of(Array("ns1", "ns2"), "tbl"))
        .asPartitionable
      assert(!partTable.partitionExists(InternalRow.fromSeq(Seq(1))))

      withSQLConf(SQLConf.CASE_SENSITIVE.key -> "false") {
        spark.sql(s"ALTER TABLE $t ADD PARTITION (ID=1) LOCATION 'loc1'")
        assert(partTable.partitionExists(InternalRow.fromSeq(Seq(1))))
        spark.sql(s"ALTER TABLE $t DROP PARTITION (Id=1)")
        assert(!partTable.partitionExists(InternalRow.fromSeq(Seq(1))))
      }
    }
  }

  test("SPARK-33650: drop partition into a table which doesn't support partition management") {
    val t = "testcat.ns1.ns2.tbl"
    withTable(t) {
      spark.sql(s"CREATE TABLE $t (id bigint, data string) USING _")
      val errMsg = intercept[AnalysisException] {
        spark.sql(s"ALTER TABLE $t DROP PARTITION (id=1)")
      }.getMessage
      assert(errMsg.contains(s"Table $t can not alter partitions"))
    }
  }

  test("SPARK-33676: not fully specified partition spec") {
    val t = "testpart.ns1.ns2.tbl"
    withTable(t) {
      sql(s"""
        |CREATE TABLE $t (id bigint, part0 int, part1 string)
        |USING foo
        |PARTITIONED BY (part0, part1)""".stripMargin)
      val errMsg = intercept[AnalysisException] {
        sql(s"ALTER TABLE $t DROP PARTITION (part0 = 1)")
      }.getMessage
      assert(errMsg.contains("Partition spec is invalid. " +
        "The spec (part0) must match the partition spec (part0, part1)"))
    }
  }
}
