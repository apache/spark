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

package org.apache.spark.sql.execution

import org.apache.spark.sql.{QueryTest, SaveMode}
import org.apache.spark.sql.execution.split.SplitExchangeExec
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.test.SharedSparkSession

class SplitSourcePartitionSuite extends QueryTest with SharedSparkSession {

  private val TABLE_FORMAT: String = "parquet"

  test("split source partition") {
    withSQLConf(
      SQLConf.ADAPTIVE_EXECUTION_ENABLED.key -> "false",
      SQLConf.FILES_MIN_PARTITION_NUM.key -> "1",
      SQLConf.SPLIT_SOURCE_PARTITION_ENABLED.key -> "true",
      SQLConf.SPLIT_SOURCE_PARTITION_THRESHOLD.key -> "1B") {
      withTable("ssp_t1", "ssp_t2") {
        spark
          .range(10)
          .select(col("id"), col("id").as("k"))
          .write
          .mode(SaveMode.Overwrite)
          .format(TABLE_FORMAT)
          .saveAsTable("ssp_t1")

        spark
          .range(5)
          .select(col("id"), col("id").as("k"))
          .write
          .mode(SaveMode.Overwrite)
          .format(TABLE_FORMAT)
          .saveAsTable("ssp_t2")

        val df = sql("""
                       |SELECT ssp_t1.id, ssp_t2.k
                       |FROM ssp_t1 INNER JOIN ssp_t2 ON ssp_t1.k = ssp_t2.k
                       |WHERE ssp_t2.id < 2
                       |""".stripMargin)

        val plan = df.queryExecution.executedPlan
        assertResult(1, "SplitExchangeExec applied.")(plan.collectWithSubqueries {
          case e: SplitExchangeExec =>
            e
        }.size)

        assertResult(spark.sparkContext.defaultParallelism, "split partitions.")(
          df.rdd.partitions.length)
      }

    }
  }

}
