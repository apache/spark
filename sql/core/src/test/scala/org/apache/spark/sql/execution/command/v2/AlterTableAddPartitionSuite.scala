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

package org.apache.spark.sql.execution.command.v2

import org.apache.spark.sql.{AnalysisException, Row}
import org.apache.spark.sql.catalyst.analysis.PartitionsAlreadyExistException
import org.apache.spark.sql.execution.command
import org.apache.spark.sql.internal.SQLConf

/**
 * The class contains tests for the `ALTER TABLE .. ADD PARTITION` command
 * to check V2 table catalogs.
 */
class AlterTableAddPartitionSuite
  extends command.AlterTableAddPartitionSuiteBase
  with CommandSuiteBase {
  override def defaultPartitionName: String = "null"

  test("SPARK-33650: add partition into a table which doesn't support partition management") {
    withNamespaceAndTable("ns", "tbl", s"non_part_$catalog") { t =>
      sql(s"CREATE TABLE $t (id bigint, data string) $defaultUsing")
      val errMsg = intercept[AnalysisException] {
        sql(s"ALTER TABLE $t ADD PARTITION (id=1)")
      }.getMessage
      assert(errMsg.contains(s"Table $t does not support partition management"))
    }
  }

  test("empty string as partition value") {
    withNamespaceAndTable("ns", "tbl") { t =>
      sql(s"CREATE TABLE $t (col1 INT, p1 STRING) $defaultUsing PARTITIONED BY (p1)")
      sql(s"ALTER TABLE $t ADD PARTITION (p1 = '')")
      checkPartitions(t, Map("p1" -> ""))
    }
  }

  test("SPARK-34143: add a partition to fully partitioned table") {
    withNamespaceAndTable("ns", "tbl") { t =>
      sql(s"CREATE TABLE $t (p0 INT, p1 STRING) $defaultUsing PARTITIONED BY (p0, p1)")
      sql(s"ALTER TABLE $t ADD PARTITION (p0 = 0, p1 = 'abc')")
      checkPartitions(t, Map("p0" -> "0", "p1" -> "abc"))
      checkAnswer(sql(s"SELECT * FROM $t"), Row(0, "abc"))
    }
  }

  test("SPARK-34149: refresh cache in partition adding") {
    withNamespaceAndTable("ns", "tbl") { t =>
      sql(s"CREATE TABLE $t (part int) $defaultUsing PARTITIONED BY (part)")
      sql(s"ALTER TABLE $t ADD PARTITION (part=0)")
      assert(!spark.catalog.isCached(t))
      sql(s"CACHE TABLE $t")
      assert(spark.catalog.isCached(t))
      checkAnswer(sql(s"SELECT * FROM $t"), Row(0))

      sql(s"ALTER TABLE $t ADD PARTITION (part=1)")
      assert(spark.catalog.isCached(t))
      checkAnswer(sql(s"SELECT * FROM $t"), Seq(Row(0), Row(1)))
    }
  }

  test("SPARK-34099, SPARK-34161: keep dependents cached after table altering") {
    withNamespaceAndTable("ns", "tbl") { t =>
      sql(s"CREATE TABLE $t (id int, part int) $defaultUsing PARTITIONED BY (id, part)")
      sql(s"INSERT INTO $t PARTITION (part=0) SELECT 0")
      cacheRelation(t)
      checkCachedRelation(t, Seq(Row(0, 0)))

      withView("v0") {
        sql(s"CREATE VIEW v0 AS SELECT * FROM $t")
        cacheRelation("v0")
        sql(s"ALTER TABLE $t ADD PARTITION (id=0, part=1)")
        checkCachedRelation("v0", Seq(Row(0, 0), Row(0, 1)))
      }

      withTempView("v1") {
        sql(s"CREATE TEMP VIEW v1 AS SELECT * FROM $t")
        cacheRelation("v1")
        sql(s"ALTER TABLE $t ADD PARTITION (id=1, part=2)")
        checkCachedRelation("v1", Seq(Row(0, 0), Row(0, 1), Row(1, 2)))
      }

      val v2 = s"${spark.sharedState.globalTempViewManager.database}.v2"
      withGlobalTempView(v2) {
        sql(s"CREATE GLOBAL TEMP VIEW v2 AS SELECT * FROM $t")
        cacheRelation(v2)
        sql(s"ALTER TABLE $t ADD PARTITION (id=2, part=3)")
        checkCachedRelation(v2, Seq(Row(0, 0), Row(0, 1), Row(1, 2), Row(2, 3)))
      }
    }
  }

  // TODO: Move this test to the common trait as soon as it is migrated on checkError()
  test("partition already exists") {
    withNamespaceAndTable("ns", "tbl") { t =>
      sql(s"CREATE TABLE $t (id bigint, data string) $defaultUsing PARTITIONED BY (id)")
      sql(s"ALTER TABLE $t ADD PARTITION (id=2) LOCATION 'loc1'")

      val e = intercept[PartitionsAlreadyExistException] {
        sql(s"ALTER TABLE $t ADD PARTITION (id=1) LOCATION 'loc'" +
          " PARTITION (id=2) LOCATION 'loc1'")
      }
      checkError(e,
        errorClass = "PARTITIONS_ALREADY_EXIST",
        parameters = Map("partitionList" -> "PARTITION (`id` = 2)",
        "tableName" -> "`test_catalog`.`ns`.`tbl`"))

      sql(s"ALTER TABLE $t ADD IF NOT EXISTS PARTITION (id=1) LOCATION 'loc'" +
        " PARTITION (id=2) LOCATION 'loc1'")
      checkPartitions(t, Map("id" -> "1"), Map("id" -> "2"))
    }
  }

  test("SPARK-40798: Alter partition should verify partition value - legacy") {
    withNamespaceAndTable("ns", "tbl") { t =>
      sql(s"CREATE TABLE $t (c int) $defaultUsing PARTITIONED BY (p int)")

      withSQLConf(
          SQLConf.SKIP_TYPE_VALIDATION_ON_ALTER_PARTITION.key -> "true",
          SQLConf.ANSI_ENABLED.key -> "false") {
        sql(s"ALTER TABLE $t ADD PARTITION (p='aaa')")
        checkPartitions(t, Map("p" -> defaultPartitionName))
        sql(s"ALTER TABLE $t DROP PARTITION (p=null)")
      }
    }
  }
}
