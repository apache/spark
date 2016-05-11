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

package org.apache.spark.sql.execution.adaptive

import org.apache.spark.sql.QueryTest
import org.apache.spark.sql.functions._
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.test.{SharedSQLContext, SQLTestUtils}

class QueryFragmentSuite extends QueryTest with SQLTestUtils with SharedSQLContext {
  import testImplicits._

  setupTestData()

  test("adaptive optimization: transform sort merge join to broadcast join for inner join") {
    withSQLConf(SQLConf.ADAPTIVE_EXECUTION2_ENABLED.key -> "true",
      SQLConf.AUTO_BROADCASTJOIN_THRESHOLD.key -> "100000") {
      val numInputPartitions: Int = 2
      val df1 = sqlContext.range(0, 100000, 1, numInputPartitions)
        .selectExpr("id % 50 as key1", "id as value1")
        .groupBy("key1")
        .agg($"key1", count("value1") as "cnt1")
      val df2 = sqlContext.range(0, 100000, 1, numInputPartitions)
        .selectExpr("id % 50 as key2", "id as value2")
        .groupBy("key2")
        .agg($"key2", count("value2") as "cnt2")
      val join1 = df1.join(df2, col("key1") === col("key2"))
          .select(col("key1"), col("cnt1"), col("cnt2"))
      checkAnswer(join1,
        sqlContext.range(0, 50).selectExpr("id as key", "2000 as cnt1", "2000 as cnt2").collect())

      val df3 = sqlContext.range(0, 100000, 1, numInputPartitions)
        .selectExpr("id as key3", "id as value3")
        .groupBy("key3")
        .agg($"key3", count("value3") as "cnt3")
      val join2 = df3.join(df1, col("key3") === col("key1"))
          .select(col("key1"), col("cnt1"), col("cnt3"))
      checkAnswer(join2,
        sqlContext.range(0, 50).selectExpr("id as key", "2000 as cnt1", "1 as cnt3").collect())
    }
  }

  test("adaptive optimization: transform sort merge join to broadcast join for outer join") {
    withSQLConf(SQLConf.ADAPTIVE_EXECUTION2_ENABLED.key -> "true",
      SQLConf.AUTO_BROADCASTJOIN_THRESHOLD.key -> "100000") {
      val numInputPartitions: Int = 2
      val df1 = sqlContext.range(0, 100000, 1, numInputPartitions)
        .selectExpr("id % 50 as key1", "id as value1")
        .groupBy("key1")
        .agg($"key1", count("value1") as "cnt1")
      val df2 = sqlContext.range(0, 100000, 1, numInputPartitions)
        .selectExpr("id % 50 as key2", "id as value2")
        .groupBy("key2")
        .agg($"key2", count("value2") as "cnt2")
      val join1 = df1.join(df2, col("key1") === col("key2"), "left_outer")
        .select(col("key1"), col("cnt1"), col("cnt2"))
      checkAnswer(join1,
        sqlContext.range(0, 50).selectExpr("id as key", "2000 as cnt1", "2000 as cnt2").collect())

      val join2 = df1.join(df2, col("key1") === col("key2"), "right_outer")
        .select(col("key1"), col("cnt1"), col("cnt2"))
      checkAnswer(join2,
        sqlContext.range(0, 50).selectExpr("id as key", "2000 as cnt1", "2000 as cnt2").collect())

      val df3 = sqlContext.range(0, 100000, 1, numInputPartitions)
        .selectExpr("id as key3", "id as value3")
        .groupBy("key3")
        .agg($"key3", count("value3") as "cnt3")
      val join3 = df3.join(df1, col("key3") === col("key1"), "left_outer")
        .select(col("key1"), col("cnt1"), col("cnt3"))
      checkAnswer(join3,
        sqlContext.range(0, 50).selectExpr("id as key", "2000 as cnt1", "1 as cnt3")
          .union(sqlContext.range(0, 99950).selectExpr("null as key", "null as cnt1", "1 as cnt3"))
          .collect())

      val join4 = df3.join(df1, col("key3") === col("key1"), "right_outer")
        .select(col("key1"), col("cnt1"), col("cnt3"))
      checkAnswer(join4,
        sqlContext.range(0, 50).selectExpr("id as key", "2000 as cnt1", "1 as cnt3").collect())
    }
  }

  test("adaptive optimization: transform sort merge join to broadcast join for left semi join") {
    withSQLConf(SQLConf.ADAPTIVE_EXECUTION2_ENABLED.key -> "true",
      SQLConf.AUTO_BROADCASTJOIN_THRESHOLD.key -> "100000") {
      val numInputPartitions: Int = 2
      val df1 = sqlContext.range(0, 100000, 1, numInputPartitions)
        .selectExpr("id % 50 as key1", "id as value1")
        .groupBy("key1")
        .agg($"key1", count("value1") as "cnt1")
      val df2 = sqlContext.range(0, 100000, 1, numInputPartitions)
        .selectExpr("id % 50 as key2", "id as value2")
        .groupBy("key2")
        .agg($"key2", count("value2") as "cnt2")
      val join1 = df1.join(df2, col("key1") === col("key2"), "leftsemi")
        .select(col("key1"), col("cnt1"))

      checkAnswer(join1,
        sqlContext.range(0, 50).selectExpr("id as key", "2000 as cnt1").collect())

      val df3 = sqlContext.range(0, 100000, 1, numInputPartitions)
        .selectExpr("id as key3", "id as value3")
        .groupBy("key3")
        .agg($"key3", count("value3") as "cnt3")
      val join2 = df3.join(df1, col("key3") === col("key1"), "leftsemi")
        .select(col("key3"), col("cnt3"))

      checkAnswer(join2,
        sqlContext.range(0, 50).selectExpr("id as key3", "1 as cnt3").collect())
    }
  }

  test("adaptive optimization: transform sort merge join to broadcast join for left anti join") {
    withSQLConf(SQLConf.ADAPTIVE_EXECUTION2_ENABLED.key -> "true",
      SQLConf.AUTO_BROADCASTJOIN_THRESHOLD.key -> "100000") {
      val numInputPartitions: Int = 2
      val df1 = sqlContext.range(0, 100000, 1, numInputPartitions)
        .selectExpr("id % 100 as key1", "id as value1")
        .groupBy("key1")
        .agg($"key1", count("value1") as "cnt1")
      val df2 = sqlContext.range(0, 100000, 1, numInputPartitions)
        .selectExpr("id % 50 as key2", "id as value2")
        .groupBy("key2")
        .agg($"key2", count("value2") as "cnt2")
      val join1 = df1.join(df2, col("key1") === col("key2"), "leftanti")
        .select(col("key1"), col("cnt1"))
      checkAnswer(join1,
        sqlContext.range(50, 100).selectExpr("id as key", "1000 as cnt1").collect())

      val df3 = sqlContext.range(0, 100000, 1, numInputPartitions)
        .selectExpr("id as key3", "id as value3")
        .groupBy("key3")
        .agg($"key3", count("value3") as "cnt3")
      val join2 = df3.join(df1, col("key3") === col("key1"), "leftanti")
        .select(col("key3"), col("cnt3"))

      checkAnswer(join2,
        sqlContext.range(100, 100000).selectExpr("id as key3", "1 as cnt3").collect())
    }
  }

  test("adaptive optimization: transform sort merge join to broadcast join for existence join") {
    withSQLConf(SQLConf.ADAPTIVE_EXECUTION2_ENABLED.key -> "true",
      SQLConf.AUTO_BROADCASTJOIN_THRESHOLD.key -> "100000") {
      val numInputPartitions: Int = 2
      sqlContext.range(0, 100000, 1, numInputPartitions)
        .selectExpr("id % 50 as key1", "id as value1")
        .registerTempTable("testData")
      sqlContext.range(0, 100000, 1, numInputPartitions)
        .selectExpr("id % 50 as key2", "id as value2")
        .registerTempTable("testData2")
      val join1 = sqlContext.sql("select key1, cnt1 from " +
        "(select key1, count(value1) as cnt1 from testData group by key1) t1 " +
        "where key1 in (select distinct key2 from testData2)")
      checkAnswer(join1,
        sqlContext.range(0, 50).selectExpr("id as key1", "2000 as cnt1").collect())
      sqlContext.range(0, 100000, 1, numInputPartitions)
        .selectExpr("id as key3", "id as value3")
        .registerTempTable("testData3")
      val join2 = sqlContext.sql("select key3, value3 from testData3 " +
        "where key3 in (select distinct key2 from testData2)")
      checkAnswer(join2,
        sqlContext.range(0, 50).selectExpr("id as key3", "id as value3").collect())
    }
  }
}
