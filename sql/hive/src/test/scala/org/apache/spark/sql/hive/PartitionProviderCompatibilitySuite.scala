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

package org.apache.spark.sql.hive

import java.io.File

import org.apache.spark.metrics.source.HiveCatalogMetrics
import org.apache.spark.sql.{AnalysisException, QueryTest}
import org.apache.spark.sql.hive.test.TestHiveSingleton
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.test.SQLTestUtils

class PartitionProviderCompatibilitySuite
  extends QueryTest with TestHiveSingleton with SQLTestUtils {

  private def setupPartitionedDatasourceTable(tableName: String, dir: File): Unit = {
    // TODO(ekl) make these mixed-case fields once support for that is fixed
    spark.range(5).selectExpr("id as fieldone", "id as partcol1", "id as partcol2").write
      .partitionBy("partcol1", "partcol2")
      .mode("overwrite")
      .parquet(dir.getAbsolutePath)

    spark.sql(s"""
      |create table $tableName (fieldone long, partcol1 int, partcol2 int)
      |using parquet
      |options (path "${dir.getAbsolutePath}")
      |partitioned by (partcol1, partcol2)""".stripMargin)
  }

  private def verifyIsLegacyTable(tableName: String): Unit = {
    val unsupportedCommands = Seq(
      s"DESCRIBE $tableName PARTITION (partcol1=1)",
      s"SHOW PARTITIONS $tableName")

    for (cmd <- unsupportedCommands) {
      val e = intercept[AnalysisException] {
        spark.sql(s"show partitions $tableName")
      }
      assert(e.getMessage.contains("partition metadata is not stored in the Hive metastore"), e)
    }
  }

  private def verifyIsNewTable(tableName: String): Unit = {
    withSQLConf(SQLConf.HIVE_FILESOURCE_PARTITION_MANAGEMENT.key -> "true") {
      assert(spark.sql(s"show partitions $tableName").count() == 5)
      HiveCatalogMetrics.reset()

      // sanity check table performance
      assert(spark.sql(s"select * from $tableName where partcol1 < 2").count() == 2)
      assert(HiveCatalogMetrics.METRIC_PARTITIONS_FETCHED.getCount() == 2)
      assert(HiveCatalogMetrics.METRIC_FILES_DISCOVERED.getCount() == 2)
    }
  }

  test("convert partition provider to hive with repair table") {
    withTable("test") {
      withTempDir { dir =>
        withSQLConf(SQLConf.HIVE_FILESOURCE_PARTITION_MANAGEMENT.key -> "false") {
          setupPartitionedDatasourceTable("test", dir)
        }
        withSQLConf(SQLConf.HIVE_FILESOURCE_PARTITION_MANAGEMENT.key -> "true") {
          verifyIsLegacyTable("test")
          spark.sql("msck repair table test")
          verifyIsNewTable("test")
        }
      }
    }
  }

  test("when partition management is enabled, new tables have partition provider hive") {
    withTable("test") {
      withTempDir { dir =>
        withSQLConf(SQLConf.HIVE_FILESOURCE_PARTITION_MANAGEMENT.key -> "true") {
          setupPartitionedDatasourceTable("test", dir)
          verifyIsNewTable("test")
        }
      }
    }
  }

  test("when partition management is disabled, new tables have no partition provider") {
    withTable("test") {
      withTempDir { dir =>
        withSQLConf(SQLConf.HIVE_FILESOURCE_PARTITION_MANAGEMENT.key -> "false") {
          setupPartitionedDatasourceTable("test", dir)
          verifyIsLegacyTable("test")
        }
      }
    }
  }
}
