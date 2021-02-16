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

import java.io.File

import org.apache.hadoop.fs.Path
import org.apache.hadoop.fs.permission.{AclEntry, AclEntryScope, AclEntryType, FsAction, FsPermission}

import org.apache.spark.sql.{AnalysisException, Row}
import org.apache.spark.sql.catalyst.{QualifiedTableName, TableIdentifier}
import org.apache.spark.sql.catalyst.analysis.NoSuchPartitionException
import org.apache.spark.sql.execution.command
import org.apache.spark.sql.execution.command.FakeLocalFsFileSystem
import org.apache.spark.sql.internal.SQLConf

/**
 * This base suite contains unified tests for the `TRUNCATE TABLE` command that check V1
 * table catalogs. The tests that cannot run for all V1 catalogs are located in more
 * specific test suites:
 *
 *   - V1 In-Memory catalog: `org.apache.spark.sql.execution.command.v1.TruncateTableSuite`
 *   - V1 Hive External catalog: `org.apache.spark.sql.hive.execution.command.TruncateTableSuite`
 */
trait TruncateTableSuiteBase extends command.TruncateTableSuiteBase {

  test("table does not exist") {
    withNamespaceAndTable("ns", "does_not_exist") { t =>
      val errMsg = intercept[AnalysisException] {
        sql(s"TRUNCATE TABLE $t")
      }.getMessage
      assert(errMsg.contains("Table not found"))
    }
  }

  test("truncate non-partitioned table") {
    withNamespaceAndTable("ns", "tbl") { t =>
      sql(s"CREATE TABLE $t (c0 INT, c1 INT) $defaultUsing")
      sql(s"INSERT INTO $t SELECT 0, 1")

      sql(s"TRUNCATE TABLE $t")
      checkAnswer(sql(s"SELECT * FROM $t"), Nil)

      // not supported since the table is not partitioned
      val errMsg = intercept[AnalysisException] {
        sql(s"TRUNCATE TABLE $t PARTITION (width=1)")
      }.getMessage
      assert(errMsg.contains(
        "TRUNCATE TABLE ... PARTITION is not supported for tables that are not partitioned"))
    }
  }

  private def createPartTable(t: String): Unit = {
    sql(s"""
      |CREATE TABLE $t (width INT, length INT, height INT)
      |$defaultUsing
      |PARTITIONED BY (width, length)""".stripMargin)
    sql(s"INSERT INTO $t PARTITION (width = 0, length = 0) SELECT 0")
    sql(s"INSERT INTO $t PARTITION (width = 1, length = 1) SELECT 1")
    sql(s"INSERT INTO $t PARTITION (width = 1, length = 2) SELECT 3")
  }

  test("truncate partitioned tables") {
    withNamespaceAndTable("ns", "partTable") { t =>
      createPartTable(t)
      sql(s"TRUNCATE TABLE $t PARTITION (width = 1, length = 1)")
      checkAnswer(sql(s"SELECT width, length, height FROM $t"), Seq(Row(0, 0, 0), Row(1, 2, 3)))
    }

    withNamespaceAndTable("ns", "partTable") { t =>
      createPartTable(t)
      // support partial partition spec
      sql(s"TRUNCATE TABLE $t PARTITION (width = 1)")
      checkAnswer(sql(s"SELECT * FROM $t"), Row(0, 0, 0))
    }

    withNamespaceAndTable("ns", "partTable") { t =>
      createPartTable(t)
      // do nothing if no partition is matched for the given partial partition spec
      sql(s"TRUNCATE TABLE $t PARTITION (width = 100)")
      checkAnswer(
        sql(s"SELECT width, length, height FROM $t"),
        Seq(Row(0, 0, 0), Row(1, 1, 1), Row(1, 2, 3)))

      // throw exception if no partition is matched for the given non-partial partition spec.
      intercept[NoSuchPartitionException] {
        sql(s"TRUNCATE TABLE $t PARTITION (width = 100, length = 100)")
      }

      // throw exception if the column in partition spec is not a partition column.
      val errMsg = intercept[AnalysisException] {
        sql(s"TRUNCATE TABLE $t PARTITION (unknown = 1)")
      }.getMessage
      assert(errMsg.contains("unknown is not a valid partition column"))
    }
  }

  test("SPARK-30312: truncate table - keep acl/permission") {
    Seq(true, false).foreach { ignore =>
      withSQLConf(
        "fs.file.impl" -> classOf[FakeLocalFsFileSystem].getName,
        "fs.file.impl.disable.cache" -> "true",
        SQLConf.TRUNCATE_TABLE_IGNORE_PERMISSION_ACL.key -> ignore.toString) {
        withNamespaceAndTable("ns", "tbl") { t =>
          sql(s"CREATE TABLE $t (col INT) $defaultUsing")
          sql(s"INSERT INTO $t SELECT 1")
          checkAnswer(spark.table(t), Row(1))

          val tablePath = new Path(spark.sessionState.catalog
            .getTableMetadata(TableIdentifier("tbl", Some("ns")))
            .storage.locationUri.get)

          val hadoopConf = spark.sessionState.newHadoopConf()
          val fs = tablePath.getFileSystem(hadoopConf)
          val fileStatus = fs.getFileStatus(tablePath);

          fs.setPermission(tablePath, new FsPermission("777"))
          assert(fileStatus.getPermission().toString() == "rwxrwxrwx")

          // Set ACL to table path.
          val customAcl = new java.util.ArrayList[AclEntry]()
          customAcl.add(new AclEntry.Builder()
            .setName("test")
            .setType(AclEntryType.USER)
            .setScope(AclEntryScope.ACCESS)
            .setPermission(FsAction.READ).build())
          fs.setAcl(tablePath, customAcl)
          assert(fs.getAclStatus(tablePath).getEntries().get(0) == customAcl.get(0))

          sql(s"TRUNCATE TABLE $t")
          assert(spark.table(t).collect().isEmpty)

          val fileStatus2 = fs.getFileStatus(tablePath)
          if (ignore) {
            assert(fileStatus2.getPermission().toString() != "rwxrwxrwx")
          } else {
            assert(fileStatus2.getPermission().toString() == "rwxrwxrwx")
          }
          val aclEntries = fs.getAclStatus(tablePath).getEntries()
          if (ignore) {
            assert(aclEntries.size() == 0)
          } else {
            assert(aclEntries.size() == 4)
            assert(aclEntries.get(0) == customAcl.get(0))

            // Setting ACLs will also set user/group/other permissions
            // as ACL entries.
            val user = new AclEntry.Builder()
              .setType(AclEntryType.USER)
              .setScope(AclEntryScope.ACCESS)
              .setPermission(FsAction.ALL).build()
            val group = new AclEntry.Builder()
              .setType(AclEntryType.GROUP)
              .setScope(AclEntryScope.ACCESS)
              .setPermission(FsAction.ALL).build()
            val other = new AclEntry.Builder()
              .setType(AclEntryType.OTHER)
              .setScope(AclEntryScope.ACCESS)
              .setPermission(FsAction.ALL).build()
            assert(aclEntries.get(1) == user)
            assert(aclEntries.get(2) == group)
            assert(aclEntries.get(3) == other)
          }
        }
      }
    }
  }

  test("SPARK-31163: acl/permission should handle non-existed path when truncating table") {
    withSQLConf(SQLConf.TRUNCATE_TABLE_IGNORE_PERMISSION_ACL.key -> "false") {
      withNamespaceAndTable("ns", "tbl") { t =>
        sql(s"CREATE TABLE $t (col1 STRING, col2 INT) $defaultUsing PARTITIONED BY (col2)")
        sql(s"INSERT INTO $t PARTITION (col2 = 1) SELECT 'one'")
        checkAnswer(spark.table(t), Row("one", 1))
        val part = spark.sessionState.catalog
          .listPartitions(TableIdentifier("tbl", Some("ns")))
          .head
        val path = new File(part.location.getPath)
        sql(s"TRUNCATE TABLE $t")
        // simulate incomplete/unsuccessful truncate
        assert(path.exists())
        path.delete()
        assert(!path.exists())
        // execute without java.io.FileNotFoundException
        sql(s"TRUNCATE TABLE $t")
        // partition path should be re-created
        assert(path.exists())
      }
    }
  }

  test("invalidation of tableRelationCache after table truncation") {
    Seq(false, true).foreach { autoUpdate =>
      withSQLConf(SQLConf.AUTO_SIZE_UPDATE_ENABLED.key -> autoUpdate.toString) {
        withNamespaceAndTable("ns", "tbl") { t =>
          spark.range(100).write.saveAsTable(t)
          sql(s"ANALYZE TABLE $t COMPUTE STATISTICS")
          spark.table(t)
          sql(s"TRUNCATE TABLE $t")
          spark.table(t)

          val catalog = spark.sessionState.catalog
          val qualifiedTableName = QualifiedTableName("ns", "tbl")
          val cachedPlan = catalog.getCachedTable(qualifiedTableName)
          assert(cachedPlan.stats.sizeInBytes == 0)
        }
      }
    }
  }

  test("case sensitivity in resolving partition specs") {
    withNamespaceAndTable("ns", "tbl") { t =>
      sql(s"CREATE TABLE $t (id bigint, data string) $defaultUsing PARTITIONED BY (id)")
      sql(s"INSERT INTO $t PARTITION (id=0) SELECT 'abc'")
      sql(s"INSERT INTO $t PARTITION (id=1) SELECT 'def'")
      withSQLConf(SQLConf.CASE_SENSITIVE.key -> "true") {
        val errMsg = intercept[AnalysisException] {
          sql(s"TRUNCATE TABLE $t PARTITION (ID=1)")
        }.getMessage
        assert(errMsg.contains("ID is not a valid partition column"))
      }
      withSQLConf(SQLConf.CASE_SENSITIVE.key -> "false") {
        sql(s"TRUNCATE TABLE $t PARTITION (ID=1)")
        checkAnswer(sql(s"SELECT id, data FROM $t"), Row(0, "abc"))
      }
    }
  }

  test("change stats after truncate command") {
    withNamespaceAndTable("ns", "tbl") { t =>
      sql(s"CREATE TABLE $t (id INT, value INT) $defaultUsing")
      sql(s"INSERT INTO $t SELECT 0, 100")
      // analyze to get initial stats
      sql(s"ANALYZE TABLE $t COMPUTE STATISTICS FOR COLUMNS id, value")
      assert(getTableSize(t) > 0)

      withSQLConf(SQLConf.AUTO_SIZE_UPDATE_ENABLED.key -> "true") {
        sql(s"TRUNCATE TABLE $t")
        assert(getTableSize(t) == 0)
      }
    }
  }

  test("SPARK-34251: stats in truncated non-empty table") {
    withSQLConf(SQLConf.AUTO_SIZE_UPDATE_ENABLED.key -> "true") {
      withNamespaceAndTable("ns", "tbl") { t =>
        sql(s"CREATE TABLE $t (c0 int, part int) $defaultUsing PARTITIONED BY (part)")
        sql(s"INSERT INTO $t PARTITION (part=0) SELECT 0")
        sql(s"INSERT INTO $t PARTITION (part=1) SELECT 1")
        val sizeOfTwoParts = getTableSize(t)
        assert(sizeOfTwoParts > 0)
        sql(s"TRUNCATE TABLE $t PARTITION (part=1)")
        val sizeOfOnePart = getTableSize(t)
        assert(0 < sizeOfOnePart && sizeOfOnePart < sizeOfTwoParts)
      }
    }
  }

  test("SPARK-34215: keep table cached after truncation") {
    withNamespaceAndTable("ns", "tbl") { t =>
      sql(s"CREATE TABLE $t (c0 int) $defaultUsing")
      sql(s"INSERT INTO $t SELECT 0")
      sql(s"CACHE TABLE $t")
      assert(spark.catalog.isCached(t))
      checkAnswer(sql(s"SELECT * FROM $t"), Row(0))
      sql(s"TRUNCATE TABLE $t")
      assert(spark.catalog.isCached(t))
      checkAnswer(sql(s"SELECT * FROM $t"), Seq.empty)
    }
  }

  test("keep dependents as cached after table truncation") {
    withNamespaceAndTable("ns", "tbl") { t =>
      createPartTable(t)
      cacheRelation(t)
      checkCachedRelation(t, Seq(Row(0, 0, 0), Row(1, 1, 1), Row(3, 1, 2)))

      withView("v0") {
        sql(s"CREATE VIEW v0 AS SELECT * FROM $t")
        cacheRelation("v0")
        sql(s"TRUNCATE TABLE $t PARTITION (width = 1, length = 2)")
        checkCachedRelation("v0", Seq(Row(0, 0, 0), Row(1, 1, 1)))
      }

      withTempView("v1") {
        sql(s"CREATE TEMP VIEW v1 AS SELECT * FROM $t")
        cacheRelation("v1")
        sql(s"TRUNCATE TABLE $t PARTITION (width = 1, length = 1)")
        checkCachedRelation("v1", Seq(Row(0, 0, 0)))
      }

      val v2 = s"${spark.sharedState.globalTempViewManager.database}.v2"
      withGlobalTempView("v2") {
        sql(s"INSERT INTO $t PARTITION (width = 10, length = 10) SELECT 10")
        sql(s"CREATE GLOBAL TEMP VIEW v2 AS SELECT * FROM $t")
        cacheRelation(v2)
        sql(s"TRUNCATE TABLE $t PARTITION (width = 10, length = 10)")
        checkCachedRelation(v2, Seq(Row(0, 0, 0)))
      }
    }
  }

  test("truncation of views is not allowed") {
    withNamespaceAndTable("ns", "tbl") { t =>
      sql(s"CREATE TABLE $t (id int, part int) $defaultUsing PARTITIONED BY (part)")
      sql(s"INSERT INTO $t PARTITION (part=0) SELECT 0")

      withView("v0") {
        sql(s"CREATE VIEW v0 AS SELECT * FROM $t")
        val errMsg = intercept[AnalysisException] {
          sql("TRUNCATE TABLE v0")
        }.getMessage
        assert(errMsg.contains("'TRUNCATE TABLE' expects a table"))
      }

      withTempView("v1") {
        sql(s"CREATE TEMP VIEW v1 AS SELECT * FROM $t")
        val errMsg = intercept[AnalysisException] {
          sql("TRUNCATE TABLE v1")
        }.getMessage
        assert(errMsg.contains("'TRUNCATE TABLE' expects a table"))
      }

      val v2 = s"${spark.sharedState.globalTempViewManager.database}.v2"
      withGlobalTempView("v2") {
        sql(s"CREATE GLOBAL TEMP VIEW v2 AS SELECT * FROM $t")
        val errMsg = intercept[AnalysisException] {
          sql(s"TRUNCATE TABLE $v2")
        }.getMessage
        assert(errMsg.contains("'TRUNCATE TABLE' expects a table"))
      }
    }
  }
}

/**
 * The class contains tests for the `TRUNCATE TABLE` command to check V1 In-Memory table catalog.
 */
class TruncateTableSuite extends TruncateTableSuiteBase with CommandSuiteBase {
  test("truncation of external tables is not allowed") {
    import testImplicits._
    withTempPath { tempDir =>
      withNamespaceAndTable("ns", "tbl") { t =>
        (("a", "b") :: Nil).toDF().write.parquet(tempDir.getCanonicalPath)
        sql(s"CREATE TABLE $t $defaultUsing LOCATION '${tempDir.toURI}'")
        val errMsg = intercept[AnalysisException] {
          sql(s"TRUNCATE TABLE $t")
        }.getMessage
        assert(errMsg.contains("Operation not allowed: TRUNCATE TABLE on external tables"))
      }
    }
  }
}
