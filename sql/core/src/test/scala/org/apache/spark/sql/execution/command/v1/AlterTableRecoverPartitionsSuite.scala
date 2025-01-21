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

import org.apache.hadoop.fs.Path

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
    val e = intercept[AnalysisException] {
      sql("ALTER TABLE does_not_exist RECOVER PARTITIONS")
    }
    checkErrorTableNotFound(e, "`does_not_exist`",
      ExpectedContext("does_not_exist", 12, 11 + "does_not_exist".length))
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

  test("ALTER TABLE .. RECOVER PARTITIONS is not allowed for non-partitioned table") {
    withTable("tbl") {
      sql("CREATE TABLE tbl(col1 int, col2 string) USING parquet")
      val exception = intercept[AnalysisException] {
        sql("ALTER TABLE tbl RECOVER PARTITIONS")
      }
      checkError(
        exception = exception,
        condition = "NOT_A_PARTITIONED_TABLE",
        parameters = Map(
          "operation" -> "ALTER TABLE RECOVER PARTITIONS",
          "tableIdentWithDB" -> "`spark_catalog`.`default`.`tbl`")
      )
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
