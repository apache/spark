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

import java.time.{LocalDate, LocalDateTime}

import org.apache.spark.sql.AnalysisException
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.analysis.{NoSuchPartitionsException, PartitionsAlreadyExistException}
import org.apache.spark.sql.catalyst.util.{DateTimeTestUtils, DateTimeUtils}
import org.apache.spark.sql.connector.catalog.{CatalogV2Implicits, Identifier}
import org.apache.spark.sql.execution.datasources.v2.DataSourceV2Implicits
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.unsafe.types.UTF8String

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

  test("case sensitivity in resolving partition specs") {
    val t = "testpart.ns1.ns2.tbl"
    withTable(t) {
      spark.sql(s"CREATE TABLE $t (id bigint, data string) USING foo PARTITIONED BY (id)")
      withSQLConf(SQLConf.CASE_SENSITIVE.key -> "true") {
        val errMsg = intercept[AnalysisException] {
          spark.sql(s"ALTER TABLE $t ADD PARTITION (ID=1) LOCATION 'loc1'")
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

  test("SPARK-33521: universal type conversions of partition values") {
    val t = "testpart.ns1.ns2.tbl"
    withTable(t) {
      sql(s"""
        |CREATE TABLE $t (
        |  part0 tinyint,
        |  part1 smallint,
        |  part2 int,
        |  part3 bigint,
        |  part4 float,
        |  part5 double,
        |  part6 string,
        |  part7 boolean,
        |  part8 date,
        |  part9 timestamp
        |) USING foo
        |PARTITIONED BY (part0, part1, part2, part3, part4, part5, part6, part7, part8, part9)
        |""".stripMargin)
      val partTable = catalog("testpart").asTableCatalog
        .loadTable(Identifier.of(Array("ns1", "ns2"), "tbl"))
        .asPartitionable
      val expectedPartition = InternalRow.fromSeq(Seq[Any](
        -1,    // tinyint
        0,     // smallint
        1,     // int
        2,     // bigint
        3.14F, // float
        3.14D, // double
        UTF8String.fromString("abc"), // string
        true, // boolean
        LocalDate.parse("2020-11-23").toEpochDay,
        DateTimeUtils.instantToMicros(
          LocalDateTime.parse("2020-11-23T22:13:10.123456").atZone(DateTimeTestUtils.LA).toInstant)
      ))
      assert(!partTable.partitionExists(expectedPartition))
      val partSpec = """
        |  part0 = -1,
        |  part1 = 0,
        |  part2 = 1,
        |  part3 = 2,
        |  part4 = 3.14,
        |  part5 = 3.14,
        |  part6 = 'abc',
        |  part7 = true,
        |  part8 = '2020-11-23',
        |  part9 = '2020-11-23T22:13:10.123456'
        |""".stripMargin
      sql(s"ALTER TABLE $t ADD PARTITION ($partSpec) LOCATION 'loc1'")
      assert(partTable.partitionExists(expectedPartition))
      sql(s" ALTER TABLE $t DROP PARTITION ($partSpec)")
      assert(!partTable.partitionExists(expectedPartition))
    }
  }
}
