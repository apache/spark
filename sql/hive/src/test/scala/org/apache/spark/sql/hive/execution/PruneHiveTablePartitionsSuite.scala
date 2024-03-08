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

package org.apache.spark.sql.hive.execution

import org.apache.spark.metrics.source.HiveCatalogMetrics
import org.apache.spark.sql.{QueryTest, Row}
import org.apache.spark.sql.catalyst.analysis.EliminateSubqueryAliases
import org.apache.spark.sql.catalyst.catalog.CatalogTablePartition
import org.apache.spark.sql.catalyst.expressions.Expression
import org.apache.spark.sql.catalyst.plans.logical.{ColumnStat, LogicalPlan}
import org.apache.spark.sql.catalyst.rules.RuleExecutor
import org.apache.spark.sql.execution.SparkPlan
import org.apache.spark.sql.execution.datasources.PrunePartitionSuiteBase
import org.apache.spark.sql.hive.test.TestHiveSingleton
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.types.LongType
import org.apache.spark.tags.SlowHiveTest

@SlowHiveTest
class PruneHiveTablePartitionsSuite extends PrunePartitionSuiteBase with TestHiveSingleton {

  override def format: String = "hive"

  object Optimize extends RuleExecutor[LogicalPlan] {
    val batches =
      Batch("PruneHiveTablePartitions", Once,
        EliminateSubqueryAliases, new PruneHiveTablePartitions(spark)) :: Nil
  }

  test("SPARK-15616: statistics pruned after going through PruneHiveTablePartitions") {
    withTable("test", "temp") {
      sql(
        s"""
          |CREATE TABLE test(i int)
          |PARTITIONED BY (p int)
          |STORED AS textfile""".stripMargin)
      spark.range(0, 1000, 1).selectExpr("id as col")
        .createOrReplaceTempView("temp")

      for (part <- Seq(1, 2, 3, 4)) {
        sql(
          s"""
            |INSERT OVERWRITE TABLE test PARTITION (p='$part')
            |select col from temp""".stripMargin)
      }
      val analyzed1 = sql("select i from test where p > 0").queryExecution.analyzed
      val analyzed2 = sql("select i from test where p = 1").queryExecution.analyzed
      assert(Optimize.execute(analyzed1).stats.sizeInBytes / 4 ===
        Optimize.execute(analyzed2).stats.sizeInBytes)
    }
  }

  test("Avoid generating too many predicates in partition pruning") {
    withTempView("temp") {
      withTable("t") {
        sql(
          s"""
             |CREATE TABLE t(i INT, p0 INT, p1 INT)
             |USING $format
             |PARTITIONED BY (p0, p1)""".stripMargin)

        spark.range(0, 10, 1).selectExpr("id as col")
          .createOrReplaceTempView("temp")

        for (part <- (0 to 25)) {
          sql(
            s"""
               |INSERT OVERWRITE TABLE t PARTITION (p0='$part', p1='$part')
               |SELECT col FROM temp""".stripMargin)
        }
        val scale = 20
        val predicate = (1 to scale).map(i => s"(p0 = '$i' AND p1 = '$i')").mkString(" OR ")
        val expectedStr = {
          // left
          "(((((((p0 = 1) && (p1 = 1)) || ((p0 = 2) && (p1 = 2))) ||" +
          " ((p0 = 3) && (p1 = 3))) || (((p0 = 4) && (p1 = 4)) ||" +
          " ((p0 = 5) && (p1 = 5)))) || (((((p0 = 6) && (p1 = 6)) ||" +
          " ((p0 = 7) && (p1 = 7))) || ((p0 = 8) && (p1 = 8))) ||" +
          " (((p0 = 9) && (p1 = 9)) || ((p0 = 10) && (p1 = 10))))) ||" +
          // right
          " ((((((p0 = 11) && (p1 = 11)) || ((p0 = 12) && (p1 = 12))) ||" +
          " ((p0 = 13) && (p1 = 13))) || (((p0 = 14) && (p1 = 14)) ||" +
          " ((p0 = 15) && (p1 = 15)))) || (((((p0 = 16) && (p1 = 16)) ||" +
          " ((p0 = 17) && (p1 = 17))) || ((p0 = 18) && (p1 = 18))) ||" +
          " (((p0 = 19) && (p1 = 19)) || ((p0 = 20) && (p1 = 20))))))"
        }
        assertPrunedPartitions(s"SELECT * FROM t WHERE $predicate", scale,
          expectedStr)
      }
    }
  }

  test("SPARK-34119: Keep necessary stats after PruneHiveTablePartitions") {
    withTable("SPARK_34119") {
      withSQLConf(
        SQLConf.CBO_ENABLED.key -> "true",
        "hive.exec.dynamic.partition.mode" -> "nonstrict") {
        sql(s"CREATE TABLE SPARK_34119 PARTITIONED BY (p) STORED AS textfile AS " +
          "(SELECT id, CAST(id % 5 AS STRING) AS p FROM range(20))")
        sql(s"ANALYZE TABLE SPARK_34119 COMPUTE STATISTICS FOR ALL COLUMNS")

        checkOptimizedPlanStats(sql(s"SELECT id FROM SPARK_34119"),
          320L,
          Some(20),
          Seq(ColumnStat(
            distinctCount = Some(20),
            min = Some(0),
            max = Some(19),
            nullCount = Some(0),
            avgLen = Some(LongType.defaultSize),
            maxLen = Some(LongType.defaultSize))))

        checkOptimizedPlanStats(sql("SELECT id FROM SPARK_34119 WHERE p = '2'"),
          64L,
          Some(4),
          Seq(ColumnStat(
            distinctCount = Some(4),
            min = Some(0),
            max = Some(19),
            nullCount = Some(0),
            avgLen = Some(LongType.defaultSize),
            maxLen = Some(LongType.defaultSize))))
      }
    }
  }

  test("SPARK-39073: Keep rowCount after PruneHiveTablePartitions " +
    "if table only has hive statistics") {
    withTable("SPARK_39073") {
      withSQLConf(
        SQLConf.CBO_ENABLED.key -> "true",
        "hive.exec.dynamic.partition.mode" -> "nonstrict") {
        sql(s"CREATE TABLE SPARK_39073 PARTITIONED BY (p) STORED AS textfile AS " +
          "(SELECT id, CAST(id % 5 AS STRING) AS p FROM range(20))")
        val newPartitions = hiveClient.getPartitions("default", "SPARK_39073", None).map(p => {
          val map = Map[String, String](
            "numRows" -> "4", "rawDataSize" -> "6", "totalSize" -> "10")
          CatalogTablePartition(
            p.spec, p.storage, p.parameters ++ map, p.createTime, p.lastAccessTime, p.stats)
        })
        hiveClient.alterPartitions("default", "SPARK_39073", newPartitions)
        checkOptimizedPlanStats(sql("SELECT id FROM SPARK_39073 WHERE p = '2'"),
          64L,
          Some(4),
          Seq.empty)
      }
    }
  }

  test("SPARK-36128: spark.sql.hive.metastorePartitionPruning should work for file data sources") {
    Seq(true, false).foreach { enablePruning =>
      withTable("tbl") {
        withSQLConf(SQLConf.HIVE_METASTORE_PARTITION_PRUNING.key -> enablePruning.toString) {
          spark.range(10).selectExpr("id", "id % 3 as p").write.partitionBy("p").saveAsTable("tbl")
          HiveCatalogMetrics.reset()
          QueryTest.checkAnswer(sql("SELECT id FROM tbl WHERE p = 1"),
            Seq(1, 4, 7).map(Row.apply(_)), checkToRDD = false) // avoid analyzing the query twice
          val expectedCount = if (enablePruning) 1 else 3
          assert(HiveCatalogMetrics.METRIC_PARTITIONS_FETCHED.getCount == expectedCount)
        }
      }
    }
  }

  protected def collectPartitionFiltersFn(): PartialFunction[SparkPlan, Seq[Expression]] = {
    case scan: HiveTableScanExec => scan.partitionPruningPred
  }

  override def getScanExecPartitionSize(plan: SparkPlan): Long = {
    plan.collectFirst {
      case p: HiveTableScanExec => p
    }.get.prunedPartitions.size
  }
}
