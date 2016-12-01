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
import org.apache.spark.util.Utils

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

  test("insert overwrite partition of legacy datasource table") {
    withSQLConf(SQLConf.HIVE_MANAGE_FILESOURCE_PARTITIONS.key -> "false") {
      withTable("test") {
        withTempDir { dir =>
          setupPartitionedDatasourceTable("test", dir)
          spark.sql(
            """insert overwrite table test
              |partition (partCol=1)
              |select * from range(100)""".stripMargin)
          assert(spark.sql("select * from test").count() == 104)

          // Overwriting entire table
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

  for (enabled <- Seq(true, false)) {
    test(s"SPARK-18544 append with saveAsTable - partition management $enabled") {
      withSQLConf(SQLConf.HIVE_MANAGE_FILESOURCE_PARTITIONS.key -> enabled.toString) {
        withTable("test") {
          withTempDir { dir =>
            setupPartitionedDatasourceTable("test", dir)
            if (enabled) {
              spark.sql("msck repair table test")
            }
            assert(spark.sql("select * from test").count() == 5)
            spark.range(10).selectExpr("id as fieldOne", "id as partCol")
              .write.partitionBy("partCol").mode("append").saveAsTable("test")
            assert(spark.sql("select * from test").count() == 15)
          }
        }
      }
    }

    test(s"SPARK-18635 special chars in partition values - partition management $enabled") {
      withTable("test") {
        spark.range(10)
          .selectExpr("id", "id as A", "'%' as B")
          .write.partitionBy("A", "B").mode("overwrite")
          .saveAsTable("test")
        assert(spark.sql("select * from test").count() == 10)
        assert(spark.sql("select * from test where B = '%'").count() == 10)
        assert(spark.sql("select * from test where B = '$'").count() == 0)
        spark.range(10)
          .selectExpr("id", "id as A", "'=' as B")
          .write.mode("append").insertInto("test")
        spark.sql("insert into test partition (A, B) select id, id, '%=' from range(10)")
        assert(spark.sql("select * from test").count() == 30)
        assert(spark.sql("select * from test where B = '%'").count() == 10)
        assert(spark.sql("select * from test where B = '='").count() == 10)
        assert(spark.sql("select * from test where B = '%='").count() == 10)

        // show partitions sanity check
        val parts = spark.sql("show partitions test").collect().map(_.get(0)).toSeq
        assert(parts.length == 30)
        assert(parts.contains("A=0/B=%25"))
        assert(parts.contains("A=0/B=%3D"))
        assert(parts.contains("A=0/B=%25%3D"))

        // drop partition sanity check
        spark.sql("alter table test drop partition (A=1, B='%')")
        assert(spark.sql("select * from test").count() == 29)  // 1 file in dropped partition

        withTempDir { dir =>
          // custom locations sanity check
          spark.sql(s"""
            |alter table test partition (A=0, B='%')
            |set location '${dir.getAbsolutePath}'""".stripMargin)
          assert(spark.sql("select * from test").count() == 28)  // moved to empty dir

          // rename partition sanity check
          spark.sql(s"""
            |alter table test partition (A=5, B='%')
            |rename to partition (A=100, B='%')""".stripMargin)
          assert(spark.sql("select * from test where a = 5 and b = '%'").count() == 0)
          assert(spark.sql("select * from test where a = 100 and b = '%'").count() == 1)

          // try with A=0 which has a custom location
          spark.sql("insert into test partition (A=0, B='%') select 1")
          spark.sql(s"""
            |alter table test partition (A=0, B='%')
            |rename to partition (A=101, B='%')""".stripMargin)
          assert(spark.sql("select * from test where a = 0 and b = '%'").count() == 0)
          assert(spark.sql("select * from test where a = 101 and b = '%'").count() == 1)
        }
      }
    }

    test(s"SPARK-18659 insert overwrite table files - partition management $enabled") {
      withSQLConf(SQLConf.HIVE_MANAGE_FILESOURCE_PARTITIONS.key -> enabled.toString) {
        withTable("test") {
          spark.range(10)
            .selectExpr("id", "id as A", "'x' as B")
            .write.partitionBy("A", "B").mode("overwrite")
            .saveAsTable("test")
          spark.sql("insert overwrite table test select id, id, 'x' from range(1)")
          assert(spark.sql("select * from test").count() == 1)

          spark.range(10)
            .selectExpr("id", "id as A", "'x' as B")
            .write.partitionBy("A", "B").mode("overwrite")
            .saveAsTable("test")
          spark.sql(
            "insert overwrite table test partition (A, B) select id, id, 'x' from range(1)")
          assert(spark.sql("select * from test").count() == 1)
        }
      }
    }

    test(s"SPARK-18659 insert overwrite table with lowercase - partition management $enabled") {
      withSQLConf(SQLConf.HIVE_MANAGE_FILESOURCE_PARTITIONS.key -> enabled.toString) {
        withTable("test") {
          spark.range(10)
            .selectExpr("id", "id as A", "'x' as B")
            .write.partitionBy("A", "B").mode("overwrite")
            .saveAsTable("test")
          // note that 'A', 'B' are lowercase instead of their original case here
          spark.sql("insert overwrite table test partition (a=1, b) select id, 'x' from range(1)")
          assert(spark.sql("select * from test").count() == 10)
        }
      }
    }
  }

  /**
   * Runs a test against a multi-level partitioned table, then validates that the custom locations
   * were respected by the output writer.
   *
   * The initial partitioning structure is:
   *   /P1=0/P2=0  -- custom location a
   *   /P1=0/P2=1  -- custom location b
   *   /P1=1/P2=0  -- custom location c
   *   /P1=1/P2=1  -- default location
   */
  private def testCustomLocations(testFn: => Unit): Unit = {
    val base = Utils.createTempDir(namePrefix = "base")
    val a = Utils.createTempDir(namePrefix = "a")
    val b = Utils.createTempDir(namePrefix = "b")
    val c = Utils.createTempDir(namePrefix = "c")
    try {
      spark.sql(s"""
        |create table test (id long, P1 int, P2 int)
        |using parquet
        |options (path "${base.getAbsolutePath}")
        |partitioned by (P1, P2)""".stripMargin)
      spark.sql(s"alter table test add partition (P1=0, P2=0) location '${a.getAbsolutePath}'")
      spark.sql(s"alter table test add partition (P1=0, P2=1) location '${b.getAbsolutePath}'")
      spark.sql(s"alter table test add partition (P1=1, P2=0) location '${c.getAbsolutePath}'")
      spark.sql(s"alter table test add partition (P1=1, P2=1)")

      testFn

      // Now validate the partition custom locations were respected
      val initialCount = spark.sql("select * from test").count()
      val numA = spark.sql("select * from test where P1=0 and P2=0").count()
      val numB = spark.sql("select * from test where P1=0 and P2=1").count()
      val numC = spark.sql("select * from test where P1=1 and P2=0").count()
      Utils.deleteRecursively(a)
      spark.sql("refresh table test")
      assert(spark.sql("select * from test where P1=0 and P2=0").count() == 0)
      assert(spark.sql("select * from test").count() == initialCount - numA)
      Utils.deleteRecursively(b)
      spark.sql("refresh table test")
      assert(spark.sql("select * from test where P1=0 and P2=1").count() == 0)
      assert(spark.sql("select * from test").count() == initialCount - numA - numB)
      Utils.deleteRecursively(c)
      spark.sql("refresh table test")
      assert(spark.sql("select * from test where P1=1 and P2=0").count() == 0)
      assert(spark.sql("select * from test").count() == initialCount - numA - numB - numC)
    } finally {
      Utils.deleteRecursively(base)
      Utils.deleteRecursively(a)
      Utils.deleteRecursively(b)
      Utils.deleteRecursively(c)
      spark.sql("drop table test")
    }
  }

  test("sanity check table setup") {
    testCustomLocations {
      assert(spark.sql("select * from test").count() == 0)
      assert(spark.sql("show partitions test").count() == 4)
    }
  }

  test("insert into partial dynamic partitions") {
    testCustomLocations {
      spark.sql("insert into test partition (P1=0, P2) select id, id from range(10)")
      assert(spark.sql("select * from test").count() == 10)
      assert(spark.sql("show partitions test").count() == 12)
      spark.sql("insert into test partition (P1=0, P2) select id, id from range(10)")
      assert(spark.sql("select * from test").count() == 20)
      assert(spark.sql("show partitions test").count() == 12)
      spark.sql("insert into test partition (P1=1, P2) select id, id from range(10)")
      assert(spark.sql("select * from test").count() == 30)
      assert(spark.sql("show partitions test").count() == 20)
      spark.sql("insert into test partition (P1=2, P2) select id, id from range(10)")
      assert(spark.sql("select * from test").count() == 40)
      assert(spark.sql("show partitions test").count() == 30)
    }
  }

  test("insert into fully dynamic partitions") {
    testCustomLocations {
      spark.sql("insert into test partition (P1, P2) select id, id, id from range(10)")
      assert(spark.sql("select * from test").count() == 10)
      assert(spark.sql("show partitions test").count() == 12)
      spark.sql("insert into test partition (P1, P2) select id, id, id from range(10)")
      assert(spark.sql("select * from test").count() == 20)
      assert(spark.sql("show partitions test").count() == 12)
    }
  }

  test("insert into static partition") {
    testCustomLocations {
      spark.sql("insert into test partition (P1=0, P2=0) select id from range(10)")
      assert(spark.sql("select * from test").count() == 10)
      assert(spark.sql("show partitions test").count() == 4)
      spark.sql("insert into test partition (P1=0, P2=0) select id from range(10)")
      assert(spark.sql("select * from test").count() == 20)
      assert(spark.sql("show partitions test").count() == 4)
      spark.sql("insert into test partition (P1=1, P2=1) select id from range(10)")
      assert(spark.sql("select * from test").count() == 30)
      assert(spark.sql("show partitions test").count() == 4)
    }
  }

  test("overwrite partial dynamic partitions") {
    testCustomLocations {
      spark.sql("insert overwrite table test partition (P1=0, P2) select id, id from range(10)")
      assert(spark.sql("select * from test").count() == 10)
      assert(spark.sql("show partitions test").count() == 12)
      spark.sql("insert overwrite table test partition (P1=0, P2) select id, id from range(5)")
      assert(spark.sql("select * from test").count() == 5)
      assert(spark.sql("show partitions test").count() == 7)
      spark.sql("insert overwrite table test partition (P1=0, P2) select id, id from range(1)")
      assert(spark.sql("select * from test").count() == 1)
      assert(spark.sql("show partitions test").count() == 3)
      spark.sql("insert overwrite table test partition (P1=1, P2) select id, id from range(10)")
      assert(spark.sql("select * from test").count() == 11)
      assert(spark.sql("show partitions test").count() == 11)
      spark.sql("insert overwrite table test partition (P1=1, P2) select id, id from range(1)")
      assert(spark.sql("select * from test").count() == 2)
      assert(spark.sql("show partitions test").count() == 2)
      spark.sql("insert overwrite table test partition (P1=3, P2) select id, id from range(100)")
      assert(spark.sql("select * from test").count() == 102)
      assert(spark.sql("show partitions test").count() == 102)
    }
  }

  test("overwrite fully dynamic partitions") {
    testCustomLocations {
      spark.sql("insert overwrite table test partition (P1, P2) select id, id, id from range(10)")
      assert(spark.sql("select * from test").count() == 10)
      assert(spark.sql("show partitions test").count() == 10)
      spark.sql("insert overwrite table test partition (P1, P2) select id, id, id from range(5)")
      assert(spark.sql("select * from test").count() == 5)
      assert(spark.sql("show partitions test").count() == 5)
    }
  }

  test("overwrite static partition") {
    testCustomLocations {
      spark.sql("insert overwrite table test partition (P1=0, P2=0) select id from range(10)")
      assert(spark.sql("select * from test").count() == 10)
      assert(spark.sql("show partitions test").count() == 4)
      spark.sql("insert overwrite table test partition (P1=0, P2=0) select id from range(5)")
      assert(spark.sql("select * from test").count() == 5)
      assert(spark.sql("show partitions test").count() == 4)
      spark.sql("insert overwrite table test partition (P1=1, P2=1) select id from range(5)")
      assert(spark.sql("select * from test").count() == 10)
      assert(spark.sql("show partitions test").count() == 4)
      spark.sql("insert overwrite table test partition (P1=1, P2=2) select id from range(5)")
      assert(spark.sql("select * from test").count() == 15)
      assert(spark.sql("show partitions test").count() == 5)
    }
  }
}
