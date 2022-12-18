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
import org.apache.spark.sql.sources.DataSourceTest
import org.apache.spark.sql.test.SharedSparkSession

class PartitionStatisticsAutoUpdateSuite extends DataSourceTest with SharedSparkSession {
  test("Auto update Partition Statistics while writing with dynamicPartition") {
    withSQLConf(SQLConf.AUTO_PARTITION_STATISTICS_UPDATE_ENABLED.key -> "true") {
      withTempPath { file =>
        sql(s"create table test(a string, b string) using parquet " +
          s"partitioned by (b) location '${file.toString}'")
        withTable("test") {
          sql("insert into test partition(b) values(1,2),(2,2)")
          val partition =
            spark.sessionState.catalog.getPartition(TableIdentifier("test"), Map("b" -> "2"))
          assert(partition.stats.nonEmpty)
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
            spark.sessionState.catalog.getTableMetadata(TableIdentifier("test"))
          assert(tableMetadata.stats.nonEmpty)
        }
      }
    }
  }

  test("Auto update Partition Statistics while writing with static partition") {
    withSQLConf(SQLConf.AUTO_PARTITION_STATISTICS_UPDATE_ENABLED.key -> "true") {
      withTempPath { file =>
        sql(s"create table test(a string, b string) using parquet " +
          s"partitioned by (b) location '${file.toString}'")
        withTable("test") {
          sql("insert into test partition(b = '2') values(1),(2)")
          val partition =
            spark.sessionState.catalog.getPartition(TableIdentifier("test"), Map("b" -> "2"))
          assert(partition.stats.nonEmpty)
        }
      }
    }
  }
}
