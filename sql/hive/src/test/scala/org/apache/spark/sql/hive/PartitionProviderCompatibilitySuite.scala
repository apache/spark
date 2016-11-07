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
    spark.range(5).selectExpr("id as fieldOne", "id as partCol").write
      .partitionBy("partCol")
      .mode("overwrite")
      .parquet(dir.getAbsolutePath)

    spark.sql(s"""
      |create table $tableName (fieldOne long, partCol int)
      |using parquet
      |options (path "${dir.getAbsolutePath}")
      |partitioned by (partCol)""".stripMargin)
  }

  private def verifyIsLegacyTable(tableName: String): Unit = {
    val unsupportedCommands = Seq(
      s"ALTER TABLE $tableName ADD PARTITION (partCol=1) LOCATION '/foo'",
      s"ALTER TABLE $tableName PARTITION (partCol=1) RENAME TO PARTITION (partCol=2)",
      s"ALTER TABLE $tableName PARTITION (partCol=1) SET LOCATION '/foo'",
      s"ALTER TABLE $tableName DROP PARTITION (partCol=1)",
      s"DESCRIBE $tableName PARTITION (partCol=1)",
      s"SHOW PARTITIONS $tableName")

    withSQLConf(SQLConf.HIVE_MANAGE_FILESOURCE_PARTITIONS.key -> "true") {
      for (cmd <- unsupportedCommands) {
        val e = intercept[AnalysisException] {
          spark.sql(cmd)
        }
        assert(e.getMessage.contains("partition metadata is not stored in the Hive metastore"), e)
      }
    }
  }

  test("convert partition provider to hive with repair table") {
    withTable("test") {
      withTempDir { dir =>
        withSQLConf(SQLConf.HIVE_MANAGE_FILESOURCE_PARTITIONS.key -> "false") {
          setupPartitionedDatasourceTable("test", dir)
          assert(spark.sql("select * from test").count() == 5)
        }
        withSQLConf(SQLConf.HIVE_MANAGE_FILESOURCE_PARTITIONS.key -> "true") {
          verifyIsLegacyTable("test")
          spark.sql("msck repair table test")
          spark.sql("show partitions test").count()  // check we are a new table

          // sanity check table performance
          HiveCatalogMetrics.reset()
          assert(spark.sql("select * from test where partCol < 2").count() == 2)
          assert(HiveCatalogMetrics.METRIC_PARTITIONS_FETCHED.getCount() == 2)
          assert(HiveCatalogMetrics.METRIC_FILES_DISCOVERED.getCount() == 2)
        }
      }
    }
  }

  test("when partition management is enabled, new tables have partition provider hive") {
    withTable("test") {
      withTempDir { dir =>
        withSQLConf(SQLConf.HIVE_MANAGE_FILESOURCE_PARTITIONS.key -> "true") {
          setupPartitionedDatasourceTable("test", dir)
          spark.sql("show partitions test").count()  // check we are a new table
          assert(spark.sql("select * from test").count() == 0)  // needs repair
          spark.sql("msck repair table test")
          assert(spark.sql("select * from test").count() == 5)
        }
      }
    }
  }

  test("when partition management is disabled, new tables have no partition provider") {
    withTable("test") {
      withTempDir { dir =>
        withSQLConf(SQLConf.HIVE_MANAGE_FILESOURCE_PARTITIONS.key -> "false") {
          setupPartitionedDatasourceTable("test", dir)
          verifyIsLegacyTable("test")
          assert(spark.sql("select * from test").count() == 5)
        }
      }
    }
  }

  test("when partition management is disabled, we preserve the old behavior even for new tables") {
    withTable("test") {
      withTempDir { dir =>
        withSQLConf(SQLConf.HIVE_MANAGE_FILESOURCE_PARTITIONS.key -> "true") {
          setupPartitionedDatasourceTable("test", dir)
          spark.sql("show partitions test").count()  // check we are a new table
          spark.sql("refresh table test")
          assert(spark.sql("select * from test").count() == 0)
        }
        // disabled
        withSQLConf(SQLConf.HIVE_MANAGE_FILESOURCE_PARTITIONS.key -> "false") {
          val e = intercept[AnalysisException] {
            spark.sql(s"show partitions test")
          }
          assert(e.getMessage.contains("filesource partition management is disabled"))
          spark.sql("refresh table test")
          assert(spark.sql("select * from test").count() == 5)
        }
        // then enabled again
        withSQLConf(SQLConf.HIVE_MANAGE_FILESOURCE_PARTITIONS.key -> "true") {
          spark.sql("refresh table test")
          assert(spark.sql("select * from test").count() == 0)
        }
      }
    }
  }

  test("insert overwrite partition of legacy datasource table overwrites entire table") {
    withSQLConf(SQLConf.HIVE_MANAGE_FILESOURCE_PARTITIONS.key -> "false") {
      withTable("test") {
        withTempDir { dir =>
          setupPartitionedDatasourceTable("test", dir)
          spark.sql(
            """insert overwrite table test
              |partition (partCol=1)
              |select * from range(100)""".stripMargin)
          assert(spark.sql("select * from test").count() == 100)

          // Dynamic partitions case
          spark.sql("insert overwrite table test select id, id from range(10)".stripMargin)
          assert(spark.sql("select * from test").count() == 10)
        }
      }
    }
  }

  test("insert overwrite partition of new datasource table overwrites just partition") {
    withSQLConf(SQLConf.HIVE_MANAGE_FILESOURCE_PARTITIONS.key -> "true") {
      withTable("test") {
        withTempDir { dir =>
          setupPartitionedDatasourceTable("test", dir)
          sql("msck repair table test")
          spark.sql(
            """insert overwrite table test
              |partition (partCol=1)
              |select * from range(100)""".stripMargin)
          assert(spark.sql("select * from test").count() == 104)

          // Test overwriting a partition that has a custom location
          withTempDir { dir2 =>
            sql(
              s"""alter table test partition (partCol=1)
                |set location '${dir2.getAbsolutePath}'""".stripMargin)
            assert(sql("select * from test").count() == 4)
            sql(
              """insert overwrite table test
                |partition (partCol=1)
                |select * from range(30)""".stripMargin)
            sql(
              """insert overwrite table test
                |partition (partCol=1)
                |select * from range(20)""".stripMargin)
            assert(sql("select * from test").count() == 24)
          }
        }
      }
    }
  }
}
