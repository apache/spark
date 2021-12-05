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

package org.apache.spark.sql.execution.datasources

import org.apache.spark.sql._
import org.apache.spark.sql.execution.FileSourceScanExec
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.test.SharedSparkSession

class FileSourceParallelSuite extends QueryTest with SharedSparkSession {

  test("Specify parallel with read option") {
    withSQLConf(SQLConf.FILES_OPEN_COST_IN_BYTES.key -> "10") {
      withTempPath { path =>
        spark.range(1, 1000, 1, 1)
          .selectExpr("id as key", "id * 3 as s1", "id * 5 as s2")
          .write.format("parquet").save(path.getAbsolutePath)

        Seq(3, 7, 10).foreach { parallelism =>
          val plan = spark.read.option("Parallel", parallelism)
            .parquet(path.getAbsolutePath)
            .queryExecution
            .sparkPlan
          val scan = plan.collect { case scan: FileSourceScanExec => scan }
          assert(scan.size === 1)
          assert(scan.head.inputRDD.partitions.length === parallelism)
        }
      }
    }
  }

  test("Specify parallel with table option") {
    withSQLConf(SQLConf.FILES_OPEN_COST_IN_BYTES.key -> "10") {
      withTable("t1") {
        spark.range(1, 1000, 1, 1)
          .selectExpr("id as key", "id * 3 as s1", "id * 5 as s2")
          .write.format("parquet").option("Parallel", 5).saveAsTable("t1")

        val plan = spark.table("t1").queryExecution.sparkPlan
        val scan = plan.collect { case scan: FileSourceScanExec => scan }
        assert(scan.size === 1)
        assert(scan.head.inputRDD.partitions.length === 5)
      }
    }
  }
}
