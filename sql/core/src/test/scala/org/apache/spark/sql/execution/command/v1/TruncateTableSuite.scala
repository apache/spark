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
}

/**
 * The class contains tests for the `TRUNCATE TABLE` command to check V1 In-Memory table catalog.
 */
class TruncateTableSuite extends TruncateTableSuiteBase with CommandSuiteBase {
  import testImplicits._

  test("truncate datasource table") {
    val data = (1 to 10).map { i => (i, i) }.toDF("width", "length")
    // Test both a Hive compatible and incompatible code path.
    Seq("json", "parquet").foreach { format =>
      withNamespaceAndTable("ns", "tbl") { t =>
        data.write.format(format).saveAsTable(t)
        assert(spark.table(t).collect().nonEmpty, "bad test; table was empty to begin with")

        sql(s"TRUNCATE TABLE $t")
        assert(spark.table(t).collect().isEmpty)

        // not supported since the table is not partitioned
        val errMsg = intercept[AnalysisException] {
          sql(s"TRUNCATE TABLE $t PARTITION (width=1)")
        }.getMessage
        assert(errMsg.contains(
          "TRUNCATE TABLE ... PARTITION is not supported for tables that are not partitioned"))
      }
    }
  }

  test("truncate partitioned table - datasource table") {
    val data = (1 to 10).map { i => (i % 3, i % 5, i) }.toDF("width", "length", "height")

    withNamespaceAndTable("ns", "partTable") { t =>
      data.write.partitionBy("width", "length").saveAsTable(t)
      // supported since partitions are stored in the metastore
      sql(s"TRUNCATE TABLE $t PARTITION (width=1, length=1)")
      assert(spark.table(t).filter($"width" === 1).collect().nonEmpty)
      assert(spark.table(t).filter($"width" === 1 && $"length" === 1).collect().isEmpty)
    }

    withNamespaceAndTable("ns", "partTable") { t =>
      data.write.partitionBy("width", "length").saveAsTable(t)
      // support partial partition spec
      sql(s"TRUNCATE TABLE $t PARTITION (width=1)")
      assert(spark.table(t).collect().nonEmpty)
      assert(spark.table(t).filter($"width" === 1).collect().isEmpty)
    }

    withNamespaceAndTable("ns", "partTable") { t =>
      data.write.partitionBy("width", "length").saveAsTable(t)
      // do nothing if no partition is matched for the given partial partition spec
      sql(s"TRUNCATE TABLE $t PARTITION (width=100)")
      assert(spark.table(t).count() == data.count())

      // throw exception if no partition is matched for the given non-partial partition spec.
      intercept[NoSuchPartitionException] {
        sql(s"TRUNCATE TABLE $t PARTITION (width=100, length=100)")
      }

      // throw exception if the column in partition spec is not a partition column.
      val errMsg = intercept[AnalysisException] {
        sql(s"TRUNCATE TABLE $t PARTITION (unknown=1)")
      }.getMessage
      assert(errMsg.contains("unknown is not a valid partition column"))
    }
  }

  test("SPARK-30312: truncate table - keep acl/permission") {
    val ignorePermissionAcl = Seq(true, false)

    ignorePermissionAcl.foreach { ignore =>
      withSQLConf(
        "fs.file.impl" -> classOf[FakeLocalFsFileSystem].getName,
        "fs.file.impl.disable.cache" -> "true",
        SQLConf.TRUNCATE_TABLE_IGNORE_PERMISSION_ACL.key -> ignore.toString) {
        withTable("tab1") {
          sql("CREATE TABLE tab1 (col INT) USING parquet")
          sql("INSERT INTO tab1 SELECT 1")
          checkAnswer(spark.table("tab1"), Row(1))

          val tablePath = new Path(spark.sessionState.catalog
            .getTableMetadata(TableIdentifier("tab1")).storage.locationUri.get)

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

          sql("TRUNCATE TABLE tab1")
          assert(spark.table("tab1").collect().isEmpty)

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
      withTable("tab1") {
        sql("CREATE TABLE tab1 (col1 STRING, col2 INT) USING parquet PARTITIONED BY (col2)")
        sql("INSERT INTO tab1 SELECT 'one', 1")
        checkAnswer(spark.table("tab1"), Row("one", 1))
        val part = spark.sessionState.catalog.listPartitions(TableIdentifier("tab1")).head
        val path = new File(part.location.getPath)
        sql("TRUNCATE TABLE tab1")
        // simulate incomplete/unsuccessful truncate
        assert(path.exists())
        path.delete()
        assert(!path.exists())
        // execute without java.io.FileNotFoundException
        sql("TRUNCATE TABLE tab1")
        // partition path should be re-created
        assert(path.exists())
      }
    }
  }
}
