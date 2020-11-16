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
import org.apache.spark.sql.catalyst.analysis.{NoSuchPartitionsException, PartitionsAlreadyExistException}
import org.apache.spark.sql.connector.catalog.{CatalogV2Implicits, Identifier}
import org.apache.spark.sql.execution.datasources.v2.DataSourceV2Implicits

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

  test("ALTER TABLE ADD PARTITION") {
    val t = "testpart.ns1.ns2.tbl"
    withTable(t) {
      spark.sql(s"CREATE TABLE $t (id bigint, data string) USING foo PARTITIONED BY (id)")
      spark.sql(s"ALTER TABLE $t ADD PARTITION (id=1) LOCATION 'loc'")

      val partTable = catalog("testpart").asTableCatalog
        .loadTable(Identifier.of(Array("ns1", "ns2"), "tbl")).asInstanceOf[InMemoryPartitionTable]
      assert(partTable.partitionExists(InternalRow.fromSeq(Seq(1))))

      val partMetadata = partTable.loadPartitionMetadata(InternalRow.fromSeq(Seq(1)))
      assert(partMetadata.containsKey("location"))
      assert(partMetadata.get("location") == "loc")
    }
  }

  test("ALTER TABLE ADD PARTITIONS") {
    val t = "testpart.ns1.ns2.tbl"
    withTable(t) {
      spark.sql(s"CREATE TABLE $t (id bigint, data string) USING foo PARTITIONED BY (id)")
      spark.sql(
        s"ALTER TABLE $t ADD PARTITION (id=1) LOCATION 'loc' PARTITION (id=2) LOCATION 'loc1'")

      val partTable = catalog("testpart").asTableCatalog
        .loadTable(Identifier.of(Array("ns1", "ns2"), "tbl")).asInstanceOf[InMemoryPartitionTable]
      assert(partTable.partitionExists(InternalRow.fromSeq(Seq(1))))
      assert(partTable.partitionExists(InternalRow.fromSeq(Seq(2))))

      val partMetadata = partTable.loadPartitionMetadata(InternalRow.fromSeq(Seq(1)))
      assert(partMetadata.containsKey("location"))
      assert(partMetadata.get("location") == "loc")

      val partMetadata1 = partTable.loadPartitionMetadata(InternalRow.fromSeq(Seq(2)))
      assert(partMetadata1.containsKey("location"))
      assert(partMetadata1.get("location") == "loc1")
    }
  }

  test("ALTER TABLE ADD PARTITIONS: partition already exists") {
    val t = "testpart.ns1.ns2.tbl"
    withTable(t) {
      spark.sql(s"CREATE TABLE $t (id bigint, data string) USING foo PARTITIONED BY (id)")
      spark.sql(
        s"ALTER TABLE $t ADD PARTITION (id=2) LOCATION 'loc1'")

      assertThrows[PartitionsAlreadyExistException](
        spark.sql(s"ALTER TABLE $t ADD PARTITION (id=1) LOCATION 'loc'" +
          " PARTITION (id=2) LOCATION 'loc1'"))

      val partTable = catalog("testpart").asTableCatalog
        .loadTable(Identifier.of(Array("ns1", "ns2"), "tbl")).asInstanceOf[InMemoryPartitionTable]
      assert(!partTable.partitionExists(InternalRow.fromSeq(Seq(1))))

      spark.sql(s"ALTER TABLE $t ADD IF NOT EXISTS PARTITION (id=1) LOCATION 'loc'" +
        " PARTITION (id=2) LOCATION 'loc1'")
      assert(partTable.partitionExists(InternalRow.fromSeq(Seq(1))))
      assert(partTable.partitionExists(InternalRow.fromSeq(Seq(2))))
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
      assert(partTable.asPartitionable.listPartitionIdentifiers(InternalRow.empty).isEmpty)
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
      assert(partTable.asPartitionable.listPartitionIdentifiers(InternalRow.empty).isEmpty)
    }
  }
}
