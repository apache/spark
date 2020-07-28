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

import org.apache.spark.sql.execution.PartitionRecombinationExec
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.test.SharedSparkSession

class PartitionRecombinationSuite extends QueryTest with SharedSparkSession {
  import testImplicits._
  test("test operator PartitionDistributionExec") {
    withTable("tbl1") {
      withSQLConf(
        SQLConf.ADAPTIVE_EXECUTION_ENABLED.key -> "true",
        SQLConf.AUTO_BROADCASTJOIN_THRESHOLD.key -> "-1",
        SQLConf.SKEW_JOIN_ENABLED.key -> "true",
        SQLConf.SKEW_JOIN_SKEWED_PARTITION_THRESHOLD.key -> "600",
        SQLConf.ADVISORY_PARTITION_SIZE_IN_BYTES.key -> "600") {

        spark
          .range(0, 10, 1, 2)
          .selectExpr("id % 1 as key1", "id as value1")
          .write.bucketBy(5, "key1")
          .format("parquet").saveAsTable("tbl1")
        val p = spark.table("tbl1").queryExecution.executedPlan
        assert(PartitionRecombinationExec(_ => Seq(3, 3, 3, 3), 4, p).executeCollect().length == 0)
        assert(PartitionRecombinationExec(_ => Seq(0, 0, 0), 3, p).executeCollect().length == 30)
        assert(PartitionRecombinationExec(_ => Seq(0, 0, 0, 0, 0, 0), 6, p).
          executeCollect().length == 60)
      }
    }
  }
}