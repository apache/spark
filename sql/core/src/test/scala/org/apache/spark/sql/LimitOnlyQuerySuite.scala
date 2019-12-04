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

package org.apache.spark.sql

import org.apache.spark.SparkFunSuite
import org.apache.spark.sql.execution.{DataSourceScanExec, FileSourceScanExec}
import org.apache.spark.sql.execution.datasources.{FileScanRDD, SinglePartitionFileScanRDD}
import org.apache.spark.sql.execution.metric.{SQLMetric, SQLMetricsTestUtils}
import org.apache.spark.sql.test.SharedSparkSession

class LimitOnlyQuerySuite extends SparkFunSuite with SQLMetricsTestUtils with SharedSparkSession {

  private def getScanNodeMetrics(df: DataFrame): Map[String, SQLMetric] = {
    val scanNode =
      df.queryExecution.executedPlan.collectLeaves().head.asInstanceOf[FileSourceScanExec]
    scanNode.metrics
  }

  test("Limit only query on normal table") {
    withTable("testDataForScan") {
      spark.range(0, 1000, 1, 10).selectExpr("id")
        .write.saveAsTable("testDataForScan")
      val df = spark.sql("SELECT * FROM testDataForScan limit 600")
      df.collect()
      val fileScanRDD = df.queryExecution.executedPlan.collect {
        case scan: DataSourceScanExec
          if scan.inputRDDs().head.isInstanceOf[SinglePartitionFileScanRDD] =>
          scan.inputRDDs().head.asInstanceOf[SinglePartitionFileScanRDD]
      }.headOption.getOrElse {
        fail(s"No SinglePartitionFileScanRDD in query\n${df.queryExecution}")
      }
      assert(fileScanRDD.partitions.length == 1)

      val metrics = getScanNodeMetrics(df)
      // Check deterministic metrics.
      assert(metrics("numFiles").value == 5)
      assert(metrics("numOutputRows").value == 600)
    }
  }

  test("Limit with filter on normal table") {
    withTable("testDataForScan") {
      spark.range(0, 1000, 1, 10).selectExpr("id")
        .write.saveAsTable("testDataForScan")
      val df = spark.sql(
        "SELECT * FROM testDataForScan where id > 0 limit 600")
      df.collect()
      val fileScanRDD = df.queryExecution.executedPlan.collect {
        case scan: DataSourceScanExec
          if scan.inputRDDs().head.isInstanceOf[FileScanRDD] =>
          scan.inputRDDs().head.asInstanceOf[FileScanRDD]
      }.headOption.getOrElse {
        fail(s"No FileScanRDD in query\n${df.queryExecution}")
      }
      assert(fileScanRDD.partitions.length == 2)
      val metrics = getScanNodeMetrics(df)
      // Check deterministic metrics.
      assert(metrics("numFiles").value == 10)
      assert(metrics("numOutputRows").value == 1000)
    }
  }

  test("Limit with order by on normal table") {
    withTable("testDataForScan") {
      spark.range(0, 1000, 1, 10).selectExpr("id")
        .write.saveAsTable("testDataForScan")
      val df = spark.sql(
        "SELECT * FROM testDataForScan order by id limit 600")
      df.collect()
      val fileScanRDD = df.queryExecution.executedPlan.collect {
        case scan: DataSourceScanExec
          if scan.inputRDDs().head.isInstanceOf[FileScanRDD] =>
          scan.inputRDDs().head.asInstanceOf[FileScanRDD]
      }.headOption.getOrElse {
        fail(s"No FileScanRDD in query\n${df.queryExecution}")
      }
      assert(fileScanRDD.partitions.length == 2)
      val metrics = getScanNodeMetrics(df)
      // Check deterministic metrics.
      assert(metrics("numFiles").value == 10)
      assert(metrics("numOutputRows").value == 1000)
    }
  }

  test("Limit only query on partition table") {
    withTable("testDataForScan") {
      spark.range(1000).selectExpr("id", "id % 5 as p")
        .write.partitionBy("p").saveAsTable("testDataForScan")
      val df = spark.sql("SELECT * FROM testDataForScan limit 600")
      df.collect()
      val fileScanRDD = df.queryExecution.executedPlan.collect {
        case scan: DataSourceScanExec
          if scan.inputRDDs().head.isInstanceOf[SinglePartitionFileScanRDD] =>
          scan.inputRDDs().head.asInstanceOf[SinglePartitionFileScanRDD]
      }.headOption.getOrElse {
        fail(s"No SinglePartitionFileScanRDD in query\n${df.queryExecution}")
      }
      assert(fileScanRDD.partitions.length == 1)
      val metrics = getScanNodeMetrics(df)
      // Check deterministic metrics.
      assert(metrics("numFiles").value == 5)
      assert(metrics("numOutputRows").value == 600)
    }
  }

  test("Limit with filter on partition table") {
    withTable("testDataForScan") {
      spark.range(1000).selectExpr("id", "id % 5 as p")
        .write.partitionBy("p").saveAsTable("testDataForScan")
      val df = spark.sql(
        "SELECT * FROM testDataForScan where id > 0 limit 600")
      df.collect()
      val fileScanRDD = df.queryExecution.executedPlan.collect {
        case scan: DataSourceScanExec
          if scan.inputRDDs().head.isInstanceOf[FileScanRDD] =>
          scan.inputRDDs().head.asInstanceOf[FileScanRDD]
      }.headOption.getOrElse {
        fail(s"No FileScanRDD in query\n${df.queryExecution}")
      }
      assert(fileScanRDD.partitions.length == 2)
      val metrics = getScanNodeMetrics(df)
      // Check deterministic metrics.
      assert(metrics("numFiles").value == 10)
      assert(metrics("numOutputRows").value == 1000)
    }
  }

  test("Limit with partition filter on partition table") {
    withTable("testDataForScan") {
      spark.range(1000).selectExpr("id", "id % 5 as p")
        .write.partitionBy("p").saveAsTable("testDataForScan")
      val df = spark.sql(
        "SELECT * FROM testDataForScan where p = 0 limit 600")
      df.collect()
      val fileScanRDD = df.queryExecution.executedPlan.collect {
        case scan: DataSourceScanExec
          if scan.inputRDDs().head.isInstanceOf[FileScanRDD] =>
          scan.inputRDDs().head.asInstanceOf[FileScanRDD]
      }.headOption.getOrElse {
        fail(s"No FileScanRDD in query\n${df.queryExecution}")
      }
      assert(fileScanRDD.partitions.length == 2)
      val metrics = getScanNodeMetrics(df)
      // Check deterministic metrics.
      assert(metrics("numFiles").value == 2)
      assert(metrics("numOutputRows").value == 200)
    }
  }

  test("Limit only query on bucketed table") {
    withTable("testDataForScan") {
      spark.range(1000).selectExpr("id", "id % 5 as p")
        .write.bucketBy(20, "id").saveAsTable("testDataForScan")
      val df = spark.sql(
        "SELECT * FROM testDataForScan limit 600")
      df.collect()
      val fileScanRDD = df.queryExecution.executedPlan.collect {
        case scan: DataSourceScanExec
          if scan.inputRDDs().head.isInstanceOf[SinglePartitionFileScanRDD] =>
          scan.inputRDDs().head.asInstanceOf[SinglePartitionFileScanRDD]
      }.headOption.getOrElse {
        fail(s"No SinglePartitionFileScanRDD in query\n${df.queryExecution}")
      }
      assert(fileScanRDD.partitions.length == 1)
      val metrics = getScanNodeMetrics(df)
      // Check deterministic metrics.
      assert(metrics("numFiles").value == 5)
    }
  }

  test("Limit with filter on bucketed table") {
    withTable("testDataForScan") {
      spark.range(1000).selectExpr("id", "id % 5 as p")
        .write.bucketBy(20, "id").saveAsTable("testDataForScan")
      val df = spark.sql(
        "SELECT * FROM testDataForScan where p >= 0 limit 600")
      df.collect()
      val fileScanRDD = df.queryExecution.executedPlan.collect {
        case scan: DataSourceScanExec
          if scan.inputRDDs().head.isInstanceOf[FileScanRDD] =>
          scan.inputRDDs().head.asInstanceOf[FileScanRDD]
      }.headOption.getOrElse {
        fail(s"No FileScanRDD in query\n${df.queryExecution}")
      }
      assert(fileScanRDD.partitions.length == 20)
      val metrics = getScanNodeMetrics(df)
      // Check deterministic metrics.
      assert(metrics("numFiles").value == 40)
    }
  }

  test("Limit only query on partition + bucketed table") {
    withTable("testDataForScan") {
      spark.range(1000).selectExpr("id", "id % 5 as p")
        .write.partitionBy("p").bucketBy(3, "id").saveAsTable("testDataForScan")
      val df = spark.sql(
        "SELECT id FROM testDataForScan limit 600")
      df.collect()
      val fileScanRDD = df.queryExecution.executedPlan.collect {
        case scan: DataSourceScanExec
          if scan.inputRDDs().head.isInstanceOf[SinglePartitionFileScanRDD] =>
          scan.inputRDDs().head.asInstanceOf[SinglePartitionFileScanRDD]
      }.headOption.getOrElse {
        fail(s"No SinglePartitionFileScanRDD in query\n${df.queryExecution}")
      }
      assert(fileScanRDD.partitions.length == 1)
      val metrics = getScanNodeMetrics(df)
      // Check deterministic metrics.
      assert(metrics("numFiles").value == 5)
    }
  }

  test("Join with limit only scan on partition + bucketed table") {
    withTable("testDataForScan") {
      spark.range(1000).selectExpr("id", "id % 5 as p")
        .write.partitionBy("p").bucketBy(3, "id").saveAsTable("testDataForScan")
      // The execution plan has 2 FileScan nodes.
      val df = spark.sql(
        " SELECT * FROM testDataForScan JOIN" +
          " (SELECT id as id2 FROM testDataForScan limit 600)" +
          " ON id = id2")
      assert(df.count() == 600)

      val fileScanRDDs = df.queryExecution.executedPlan.collect {
        case scan: DataSourceScanExec
          if scan.inputRDDs().head.isInstanceOf[FileScanRDD] =>
          scan.inputRDDs().head.asInstanceOf[FileScanRDD]
        case scan: DataSourceScanExec
          if scan.inputRDDs().head.isInstanceOf[SinglePartitionFileScanRDD] =>
          scan.inputRDDs().head.asInstanceOf[SinglePartitionFileScanRDD]
      }
      assert(fileScanRDDs.length == 2)

      val firstScanRDD = fileScanRDDs.head
      val secondScanRDD = fileScanRDDs.tail.head.asInstanceOf[SinglePartitionFileScanRDD]

      assert(firstScanRDD.partitions.length == 3)
      assert(secondScanRDD.partitions.length == 1)
    }
  }
}
