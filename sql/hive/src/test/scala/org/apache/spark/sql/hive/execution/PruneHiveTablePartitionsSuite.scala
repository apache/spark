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

import org.apache.spark.sql.QueryTest
import org.apache.spark.sql.catalyst.TableIdentifier
import org.apache.spark.sql.catalyst.analysis.EliminateSubqueryAliases
import org.apache.spark.sql.catalyst.catalog.{CatalogStatistics, HiveTableRelation}
import org.apache.spark.sql.catalyst.dsl.expressions._
import org.apache.spark.sql.catalyst.dsl.plans._
import org.apache.spark.sql.catalyst.plans.logical.{Filter, LogicalPlan, Project}
import org.apache.spark.sql.catalyst.rules.RuleExecutor
import org.apache.spark.sql.hive.test.TestHiveSingleton
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.test.SQLTestUtils

class PruneHiveTablePartitionsSuite extends QueryTest with SQLTestUtils with TestHiveSingleton {

  object Optimize extends RuleExecutor[LogicalPlan] {
    val batches =
      Batch("PruneHiveTablePartitions", Once,
        EliminateSubqueryAliases, new PruneHiveTablePartitions(spark)) :: Nil
  }

  test("SPARK-15616 statistics pruned after going through PruneHiveTablePartitions") {
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

  test("SPARK-30427 spark.sql.statistics.fallBackToFs.maxPartitionNumber can not control " +
    "the action of rule PruneHiveTablePartitions when sizeInBytes of all partitions are " +
    "available in meta data.") {
    withTable("test", "temp") {
      sql(
        s"""
          |CREATE TABLE test(i int)
          |PARTITIONED BY (p int)
          |STORED AS textfile""".stripMargin)

      spark.range(0, 1000, 1).selectExpr("id as col")
        .createOrReplaceTempView("temp")
      for (part <- Seq(1, 2, 3, 4)) {
        sql(s"""
              |INSERT OVERWRITE TABLE test PARTITION (p='$part')
              |select col from temp""".stripMargin)
      }

      val tableMeta = spark.sharedState.externalCatalog.getTable("default", "test")
      val relation =
        HiveTableRelation(tableMeta,
          tableMeta.dataSchema.asNullable.toAttributes,
          tableMeta.partitionSchema.asNullable.toAttributes)
      val query = Project(Seq(Symbol("i"), Symbol("p")),
        Filter(Symbol("p") === 1, relation)).analyze
      var plan1: LogicalPlan = null
      var plan2: LogicalPlan = null
      var plan3: LogicalPlan = null
      var plan4: LogicalPlan = null
      withSQLConf(
        SQLConf.MAX_PARTITION_NUMBER_FOR_STATS_CALCULATION_VIA_FS.key -> s"-1") {
        plan1 = Optimize.execute(query)
      }
      withSQLConf(
        SQLConf.MAX_PARTITION_NUMBER_FOR_STATS_CALCULATION_VIA_FS.key -> s"1") {
        plan2 = Optimize.execute(query)
      }
      withSQLConf(
        SQLConf.MAX_PARTITION_NUMBER_FOR_STATS_CALCULATION_VIA_FS.key -> s"2") {
        plan3 = Optimize.execute(query)
      }
      withSQLConf(
        SQLConf.MAX_PARTITION_NUMBER_FOR_STATS_CALCULATION_VIA_FS.key -> s"3") {
        plan4 = Optimize.execute(query)
      }
      assert(plan1.stats.sizeInBytes === plan2.stats.sizeInBytes)
      assert(plan2.stats.sizeInBytes === plan3.stats.sizeInBytes)
      assert(plan3.stats.sizeInBytes === plan4.stats.sizeInBytes)
    }
  }
}
