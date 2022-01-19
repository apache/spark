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

package org.apache.spark.sql.execution.datasources.parquet

import scala.util.Random

import org.apache.spark.sql.QueryTest
import org.apache.spark.sql.execution.CollectLimitExec
import org.apache.spark.sql.execution.datasources.v2.BatchScanExec
import org.apache.spark.sql.execution.datasources.v2.parquet.ParquetScan
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.test.SharedSparkSession

/**
 * A test suite that tests Parquet based limit pushdown optimization.
 */
class ParquetLimitPushDownSuite extends QueryTest with ParquetTest with SharedSparkSession {
  test("[SPARK-37933] test limit pushdown for vectorized parquet reader") {
    import testImplicits._
    withSQLConf(
      SQLConf.PARQUET_LIMIT_PUSHDOWN_ENABLED.key -> "true",
      SQLConf.PARQUET_VECTORIZED_READER_ENABLED.key -> "true",
      SQLConf.USE_V1_SOURCE_LIST.key -> "") {
      withTempPath { path =>
        (1 to 1024).map(i => (101, i)).toDF("a", "b").coalesce(1).write.parquet(path.getPath)
        val pushedLimit = Random.nextInt(100)
        val df = spark.read.parquet(path.getPath).limit(pushedLimit)
        val sparkPlan = df.queryExecution.sparkPlan
        sparkPlan foreachUp  {
          case r@ BatchScanExec(_, f: ParquetScan, _) =>
            assert(f.pushedLimit.contains(pushedLimit))
            assert(r.executeColumnar().map(_.numRows()).sum() == pushedLimit)
          case CollectLimitExec(limit, _) =>
            assert(limit == pushedLimit)
        }
        assert(df.count() == pushedLimit)
      }
    }
  }
}
