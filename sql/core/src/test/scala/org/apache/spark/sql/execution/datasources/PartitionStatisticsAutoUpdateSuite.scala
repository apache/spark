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

package org.apache.spark.sql.execution.datasources

import org.apache.spark.sql.catalyst.TableIdentifier
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.internal.SQLConf.PartitionOverwriteMode
import org.apache.spark.sql.sources.DataSourceTest
import org.apache.spark.sql.test.SharedSparkSession

class PartitionStatisticsAutoUpdateSuite extends DataSourceTest with SharedSparkSession {
  private def catalog = spark.sessionState.catalog

  test("Auto update Partition Statistics while writing a partitioned table") {
    withSQLConf(SQLConf.AUTO_PARTITION_STATISTICS_UPDATE_ENABLED.key -> "true") {
      withTempPath { file =>
        sql(s"create table test(a string, b string, c string) using parquet " +
          s"partitioned by (b, c) location '${file.toString}'")
        withTable("test") {
          sql("insert into test partition(b,c) values(1,2,1),(2,2,1)")
          val partition =
            catalog.getPartition(TableIdentifier("test"), Map("b" -> "2", "c" -> "1"))
          assert(partition.stats.nonEmpty)
          val partStats = partition.stats.get
          assert(partStats.rowCount.contains(2))
          assert(partStats.sizeInBytes == 862)
          val table = catalog.getTableMetadata(new TableIdentifier("test"))
          assert(table.stats.nonEmpty)
          val tableStats = table.stats.get
          assert(tableStats.rowCount.contains(2))
          assert(tableStats.sizeInBytes == 862)

          sql("insert into test partition(b=2,c) values(1,2),(2,2)")
          val partition1 =
            catalog.getPartition(TableIdentifier("test"), Map("b" -> "2", "c" -> "2"))
          assert(partition1.stats.nonEmpty)
          val partStats1 = partition1.stats.get
          assert(partStats1.rowCount.contains(2))
          assert(partStats1.sizeInBytes == 862)
          val table1 = catalog.getTableMetadata(new TableIdentifier("test"))
          assert(table1.stats.nonEmpty)
          val tableStats1 = table1.stats.get
          assert(tableStats1.rowCount.contains(4))
          assert(tableStats1.sizeInBytes == 1724)

          sql("insert into test partition(b=3,c=3) values(1),(2)")
          val partition2 =
            catalog.getPartition(TableIdentifier("test"), Map("b" -> "3", "c" -> "3"))
          assert(partition2.stats.nonEmpty)
          val partStats2 = partition2.stats.get
          assert(partStats2.rowCount.contains(2))
          assert(partStats2.sizeInBytes == 862)
          val table2 = catalog.getTableMetadata(new TableIdentifier("test"))
          assert(table2.stats.nonEmpty)
          val tableStats2 = table2.stats.get
          assert(tableStats2.rowCount.contains(6))
          assert(tableStats2.sizeInBytes == 2586)

          withSQLConf(
            SQLConf.PARTITION_OVERWRITE_MODE.key -> PartitionOverwriteMode.DYNAMIC.toString) {
            sql("insert overwrite test partition(b=2,c) values(1,2),(2,2),(1,3),(2,3)")
            val partition =
              catalog.getPartition(TableIdentifier("test"), Map("b" -> "2", "c" -> "2"))
            assert(partition.stats.nonEmpty)
            val partStats = partition.stats.get
            assert(partStats.rowCount.contains(2))
            assert(partStats.sizeInBytes == 430)
            val partition1 =
              catalog.getPartition(TableIdentifier("test"), Map("b" -> "2", "c" -> "3"))
            assert(partition1.stats.nonEmpty)
            val partStats1 = partition1.stats.get
            assert(partStats1.rowCount.contains(2))
            assert(partStats1.sizeInBytes == 430)
            val table = catalog.getTableMetadata(new TableIdentifier("test"))
            assert(table.stats.nonEmpty)
            val tableStats = table.stats.get
            assert(tableStats.rowCount.contains(8))
            assert(tableStats.sizeInBytes == 2584)
          }

          withSQLConf(
            SQLConf.PARTITION_OVERWRITE_MODE.key -> PartitionOverwriteMode.STATIC.toString) {
            sql("insert overwrite test partition(b=2,c) values(1,2),(2,2),(1,3),(2,3)")
            val partition =
              catalog.getPartition(TableIdentifier("test"), Map("b" -> "2", "c" -> "2"))
            assert(partition.stats.nonEmpty)
            val partStats = partition.stats.get
            assert(partStats.rowCount.contains(2))
            assert(partStats.sizeInBytes == 430)
            val partition1 =
              catalog.getPartition(TableIdentifier("test"), Map("b" -> "2", "c" -> "3"))
            assert(partition1.stats.nonEmpty)
            val partStats1 = partition1.stats.get
            assert(partStats1.rowCount.contains(2))
            assert(partStats1.sizeInBytes == 430)
            val table = catalog.getTableMetadata(new TableIdentifier("test"))
            assert(table.stats.nonEmpty)
            val tableStats = table.stats.get
            assert(tableStats.rowCount.contains(6))
            assert(tableStats.sizeInBytes == 1722)
          }
        }
      }
    }
  }

  test("Auto update Partition Statistics while writing a non-partitioned table") {
    withSQLConf(SQLConf.AUTO_PARTITION_STATISTICS_UPDATE_ENABLED.key -> "true") {
      withTempPath { file =>
        sql(s"create table test(a string, b string) using parquet " +
          s"location '${file.toString}'")
        withTable("test") {
          sql("insert into test values(1,2),(2,2)")
          val tableMetadata =
            catalog.getTableMetadata(TableIdentifier("test"))
          assert(tableMetadata.stats.nonEmpty)
          val tableStats = tableMetadata.stats.get
          assert(tableStats.rowCount.contains(2))
          assert(tableStats.sizeInBytes == 1256)

          sql("insert overwrite test values(3,3)")
          val tableMetadata1 =
            catalog.getTableMetadata(TableIdentifier("test"))
          assert(tableMetadata1.stats.nonEmpty)
          val tableStats1 = tableMetadata1.stats.get
          assert(tableStats1.rowCount.contains(1))
          assert(tableStats1.sizeInBytes == 628)
        }
      }
    }
  }
}
