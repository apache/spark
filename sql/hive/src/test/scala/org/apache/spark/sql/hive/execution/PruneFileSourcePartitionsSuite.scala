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

import org.scalatest.Matchers._

import org.apache.spark.sql.QueryTest
import org.apache.spark.sql.catalyst.TableIdentifier
import org.apache.spark.sql.catalyst.dsl.expressions._
import org.apache.spark.sql.catalyst.dsl.plans._
import org.apache.spark.sql.catalyst.plans.logical.{Filter, LogicalPlan, Project}
import org.apache.spark.sql.catalyst.rules.RuleExecutor
import org.apache.spark.sql.execution.datasources.{CatalogFileIndex, HadoopFsRelation, LogicalRelation, PruneFileSourcePartitions}
import org.apache.spark.sql.execution.datasources.parquet.ParquetFileFormat
import org.apache.spark.sql.execution.joins.BroadcastHashJoinExec
import org.apache.spark.sql.functions.broadcast
import org.apache.spark.sql.hive.test.TestHiveSingleton
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.test.SQLTestUtils
import org.apache.spark.sql.types.StructType

class PruneFileSourcePartitionsSuite extends QueryTest with SQLTestUtils with TestHiveSingleton {

  object Optimize extends RuleExecutor[LogicalPlan] {
    val batches = Batch("PruneFileSourcePartitions", Once, PruneFileSourcePartitions) :: Nil
  }

  test("PruneFileSourcePartitions should not change the output of LogicalRelation") {
    withTable("test") {
      withTempDir { dir =>
        sql(
          s"""
            |CREATE EXTERNAL TABLE test(i int)
            |PARTITIONED BY (p int)
            |STORED AS parquet
            |LOCATION '${dir.toURI}'""".stripMargin)

        val tableMeta = spark.sharedState.externalCatalog.getTable("default", "test")
        val catalogFileIndex = new CatalogFileIndex(spark, tableMeta, 0)

        val dataSchema = StructType(tableMeta.schema.filterNot { f =>
          tableMeta.partitionColumnNames.contains(f.name)
        })
        val relation = HadoopFsRelation(
          location = catalogFileIndex,
          partitionSchema = tableMeta.partitionSchema,
          dataSchema = dataSchema,
          bucketSpec = None,
          fileFormat = new ParquetFileFormat(),
          options = Map.empty)(sparkSession = spark)

        val logicalRelation = LogicalRelation(relation, tableMeta)
        val query = Project(Seq('i, 'p), Filter('p === 1, logicalRelation)).analyze

        val optimized = Optimize.execute(query)
        assert(optimized.missingInput.isEmpty)
      }
    }
  }

  test("SPARK-20986 Reset table's statistics after PruneFileSourcePartitions rule") {
    withTable("tbl") {
      spark.range(10).selectExpr("id", "id % 3 as p").write.partitionBy("p").saveAsTable("tbl")
      sql(s"ANALYZE TABLE tbl COMPUTE STATISTICS")
      val tableStats = spark.sessionState.catalog.getTableMetadata(TableIdentifier("tbl")).stats
      assert(tableStats.isDefined && tableStats.get.sizeInBytes > 0, "tableStats is lost")

      val df = sql("SELECT * FROM tbl WHERE p = 1")
      val sizes1 = df.queryExecution.analyzed.collect {
        case relation: LogicalRelation => relation.catalogTable.get.stats.get.sizeInBytes
      }
      assert(sizes1.size === 1, s"Size wrong for:\n ${df.queryExecution}")
      assert(sizes1(0) == tableStats.get.sizeInBytes)

      val relations = df.queryExecution.optimizedPlan.collect {
        case relation: LogicalRelation => relation
      }
      assert(relations.size === 1, s"Size wrong for:\n ${df.queryExecution}")
      val size2 = relations(0).stats.sizeInBytes
      assert(size2 == relations(0).catalogTable.get.stats.get.sizeInBytes)
      assert(size2 < tableStats.get.sizeInBytes)
    }
  }

  test("SPARK-26576 Broadcast hint not applied to partitioned table") {
    withTable("tbl") {
      withSQLConf(SQLConf.AUTO_BROADCASTJOIN_THRESHOLD.key -> "-1") {
        spark.range(10).selectExpr("id", "id % 3 as p").write.partitionBy("p").saveAsTable("tbl")
        val df = spark.table("tbl")
        val qe = df.join(broadcast(df), "p").queryExecution
        qe.sparkPlan.collect { case j: BroadcastHashJoinExec => j } should have size 1
      }
    }
  }
}
