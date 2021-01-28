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

import org.apache.spark.sql.{AnalysisException, Row}
import org.apache.spark.sql.catalyst.QualifiedTableName
import org.apache.spark.sql.catalyst.analysis.NoSuchPartitionException
import org.apache.spark.sql.execution.command
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
}
