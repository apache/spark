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

import org.apache.spark.sql.execution.metric.SQLMetricsTestUtils
import org.apache.spark.sql.hive.test.TestHiveSingleton
import org.apache.spark.sql.internal.SQLConf

class SQLMetricsSuite extends SQLMetricsTestUtils with TestHiveSingleton {

  test("writing data out metrics: hive") {
    testMetricsNonDynamicPartition("hive", "t1")
  }

  test("writing data out metrics dynamic partition: hive") {
    withSQLConf(("hive.exec.dynamic.partition.mode", "nonstrict")) {
      testMetricsDynamicPartition("hive", "hive", "t1")
    }
  }

  test("metastore operations of initialize rawPartitions in HiveTableScanExec") {
    val df = sql("SELECT key FROM srcpart WHERE ds = '2008-04-08' LIMIT 3")
    df.collect()
    val scanNode =
      df.queryExecution.executedPlan.collectLeaves().head.asInstanceOf[HiveTableScanExec]
    assert(scanNode.metrics("metastoreOpsTime").value > 0)
    val phaseSummary = scanNode.relation.tableMeta.metastoreOpsPhaseSummaries
    assert(phaseSummary.nonEmpty)
    assert(phaseSummary(0).name == "LookUpRelation")
    assert(phaseSummary(1).name == "GetAllPartitions")
    assert(df.queryExecution.executedPlan.collectLeaves().head.simpleString
      .contains("GetAllPartitions"))
  }

  test("metastore operations of RelationConversions rule") {
    withTable("parquetHiveTable") {
      sql(
        s"""
         |CREATE TABLE parquetHiveTable
         |(
         |  key INT,
         |  value STRING
         |)
         |PARTITIONED BY (ds STRING, hr STRING)
         |ROW FORMAT SERDE 'org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe'
         | STORED AS
         | INPUTFORMAT 'org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat'
         | OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat'
        """.stripMargin)
      sql("SET hive.exec.dynamic.partition.mode=nonstrict")
      sql("INSERT INTO TABLE parquetHiveTable SELECT * FROM srcpart")

      withSQLConf(SQLConf.HIVE_MANAGE_FILESOURCE_PARTITIONS.key -> "true") {
        val df = sql("SELECT key FROM parquetHiveTable WHERE ds = '2008-04-08' LIMIT 3")
        df.collect()
        val (metrics, phaseSummary) = getFileScanNodeMetricsAndPhaseSummary(df)
        assert(metrics("metadataTime").value > 0)
        assert(phaseSummary.size == 2)
        assert(phaseSummary(0).name == "LookUpRelation")
        assert(phaseSummary(1).name == "PartitionPruningInCatalogFileIndex")
      }

      withSQLConf(SQLConf.HIVE_MANAGE_FILESOURCE_PARTITIONS.key -> "false") {
        val df = sql("SELECT key FROM parquetHiveTable WHERE ds = '2008-04-08' LIMIT 3")
        df.collect()
        val (metrics, phaseSummary) = getFileScanNodeMetricsAndPhaseSummary(df)
        assert(metrics("metadataTime").value > 0)
        assert(phaseSummary.size == 3)
        assert(phaseSummary(0).name == "LookUpRelation")
        assert(phaseSummary(1).name == "PartitionPruningInRelationConversions")
        assert(phaseSummary(2).name == "PartitionPruningInCatalogFileIndex")
      }
    }
  }
}
