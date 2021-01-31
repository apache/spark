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

import org.apache.hadoop.fs.{FileSystem, Path}

import org.apache.spark.internal.config.RDD_PARALLEL_LISTING_THRESHOLD
import org.apache.spark.sql.AnalysisException
import org.apache.spark.sql.execution.command

/**
 * This base suite contains unified tests for the `ALTER TABLE .. RECOVER PARTITIONS` command that
 * check V1 table catalogs. The tests that cannot run for all V1 catalogs are located in more
 * specific test suites:
 *
 *   - V1 In-Memory catalog (sequential):
 *     `org.apache.spark.sql.execution.command.v1.AlterTableRecoverPartitionsSuite`
 *   - V1 In-Memory catalog (parallel):
 *     `org.apache.spark.sql.execution.command.v1.AlterTableRecoverPartitionsParallelSuite`
 *   - V1 Hive External catalog:
 *     `org.apache.spark.sql.hive.execution.command.AlterTableRecoverPartitionsSuite`
 */
trait AlterTableRecoverPartitionsSuiteBase extends command.AlterTableRecoverPartitionsSuiteBase {
  test("table does not exist") {
    val errMsg = intercept[AnalysisException] {
      sql("ALTER TABLE does_not_exist RECOVER PARTITIONS")
    }.getMessage
    assert(errMsg.contains("Table not found"))
  }

  def withTableDir(tableName: String)(f: (FileSystem, Path) => Unit): Unit = {
    val location = sql(s"DESCRIBE TABLE EXTENDED $tableName")
      .where("col_name = 'Location'")
      .select("data_type")
      .first()
      .getString(0)
    val root = new Path(location)
    val fs = root.getFileSystem(spark.sessionState.newHadoopConf())
    f(fs, root)
  }

  test("valid locations") {
    withNamespaceAndTable("ns", "tbl") { t =>
      sql(s"CREATE TABLE $t (a INT, b INT, c INT, d INT) $defaultUsing PARTITIONED BY (a, b, c)")
      checkPartitions(t)

      withTableDir(t) { case (fs, root) =>
        fs.mkdirs(new Path(new Path(new Path(root, "a=1"), "b=5"), "c=19"))
        fs.createNewFile(new Path(new Path(root, "a=1/b=5/c=19"), "a.csv"))  // file
        fs.createNewFile(new Path(new Path(root, "a=1/b=5/c=19"), "_SUCCESS"))  // file

        fs.mkdirs(new Path(new Path(new Path(root, "A=2"), "B=6"), "C=31"))
        fs.createNewFile(new Path(new Path(root, "A=2/B=6/C=31"), "b.csv"))  // file
        fs.createNewFile(new Path(new Path(root, "A=2/B=6/C=31"), "c.csv"))  // file
        fs.createNewFile(new Path(new Path(root, "A=2/B=6/C=31"), ".hiddenFile"))  // file
        fs.mkdirs(new Path(new Path(root, "A=2/B=6/C=31"), "_temporary"))
      }

      sql(s"ALTER TABLE $t RECOVER PARTITIONS")
      checkPartitions(t,
        Map("a" -> "1", "b" -> "5", "c" -> "19"),
        Map("a" -> "2", "b" -> "6", "c" -> "31"))
    }
  }

  test("invalid locations") {
    withNamespaceAndTable("ns", "tbl") { t =>
      sql(s"CREATE TABLE $t (a INT, b INT, c INT, d INT) $defaultUsing PARTITIONED BY (a, b, c)")
      sql(s"INSERT INTO $t PARTITION (a=0, b=1, c=2) SELECT 3")
      checkPartitions(t, Map("a" -> "0", "b" -> "1", "c" -> "2"))

      withTableDir(t) { case (fs, root) =>
        fs.mkdirs(new Path(new Path(root, "a"), "b"))  // bad name
        fs.mkdirs(new Path(new Path(root, "b=1"), "a=1"))  // wrong order
        fs.mkdirs(new Path(root, "a=4")) // not enough columns
        fs.createNewFile(new Path(new Path(root, "a=1"), "b=4"))  // file
        fs.createNewFile(new Path(new Path(root, "a=1"), "_SUCCESS"))  // _SUCCESS
        fs.mkdirs(new Path(new Path(root, "a=1"), "_temporary"))  // _temporary
        fs.mkdirs(new Path(new Path(root, "a=1"), ".b=4"))  // start with .
      }

      sql(s"ALTER TABLE $t RECOVER PARTITIONS")
      checkPartitions(t, Map("a" -> "0", "b" -> "1", "c" -> "2"))
    }
  }

  test("multiple locations") {
    withNamespaceAndTable("ns", "tbl") { t =>
      sql(s"CREATE TABLE $t (a INT, b INT, c INT, d INT) $defaultUsing PARTITIONED BY (a, b, c)")
      sql(s"INSERT INTO $t PARTITION (a=0, b=1, c=2) SELECT 3")
      val initPart = Map("a" -> "0", "b" -> "1", "c" -> "2")
      checkPartitions(t, initPart)

      withTableDir(t) { case (fs, root) =>
        (0 to 100).foreach { a =>
          val part = Map("a" -> a.toString, "b" -> "5", "c" -> "42")
          fs.mkdirs(new Path(new Path(new Path(root, s"a=$a"), "b=5"), "c=42"))
          val loc = s"a=$a/b=5/c=42"
          fs.createNewFile(new Path(new Path(root, loc), "a.csv"))  // file
          if (a >= 10) {
            sql(s"ALTER TABLE $t ADD ${partSpecToString(part)} LOCATION '$loc'")
          }
        }
      }

      sql(s"ALTER TABLE $t RECOVER PARTITIONS")
      val expected = (0 to 100)
        .map(a => Map("a" -> a.toString, "b" -> "5", "c" -> "42")) :+ initPart
      checkPartitions(t, expected: _*)
    }
  }

  test("SPARK-XXXXX: recover partitions in views is not allowed") {
    withNamespaceAndTable("ns", "tbl") { t =>
      sql(s"CREATE TABLE $t (id INT, part INT) $defaultUsing PARTITIONED BY (part)")
      def checkViewAltering(createViewCmd: String, alterCmd: String): Unit = {
        sql(createViewCmd)
        val errMsg = intercept[AnalysisException] {
          sql(alterCmd)
        }.getMessage
        assert(errMsg.contains("'ALTER TABLE ... RECOVER PARTITIONS' expects a table"))
        checkPartitions(t) // no partitions
      }

      withView("v0") {
        checkViewAltering(
          s"CREATE VIEW v0 AS SELECT * FROM $t",
          "ALTER TABLE v0 RECOVER PARTITIONS")
      }

      withTempView("v1") {
        checkViewAltering(
          s"CREATE TEMP VIEW v1 AS SELECT * FROM $t",
          "ALTER TABLE v1 RECOVER PARTITIONS")
      }

      withGlobalTempView("v2") {
        val v2 = s"${spark.sharedState.globalTempViewManager.database}.v2"
        checkViewAltering(
          s"CREATE GLOBAL TEMP VIEW v2 AS SELECT * FROM $t",
          s"ALTER TABLE $v2 RECOVER PARTITIONS")
      }
    }
  }
}

/**
 * The class contains tests for the `ALTER TABLE .. RECOVER PARTITIONS` command to check
 * V1 In-Memory table catalog (sequential).
 */
class AlterTableRecoverPartitionsSuite
  extends AlterTableRecoverPartitionsSuiteBase
  with CommandSuiteBase {

  override protected def sparkConf = super.sparkConf
    .set(RDD_PARALLEL_LISTING_THRESHOLD, 0)
}

/**
 * The class contains tests for the `ALTER TABLE .. RECOVER PARTITIONS` command to check
 * V1 In-Memory table catalog (parallel).
 */
class AlterTableRecoverPartitionsParallelSuite
  extends AlterTableRecoverPartitionsSuiteBase
  with CommandSuiteBase {

  override protected def sparkConf = super.sparkConf
    .set(RDD_PARALLEL_LISTING_THRESHOLD, 10)
}
