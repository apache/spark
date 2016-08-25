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

import org.apache.spark.sql.catalyst.plans.logical.{GlobalLimit, Join, LocalLimit}
import org.apache.spark.sql.execution.datasources.LogicalRelation
import org.apache.spark.sql.test.SharedSQLContext
import org.apache.spark.sql.types._

class StatisticsSuite extends QueryTest with SharedSQLContext {
  import testImplicits._

  test("SPARK-15392: DataFrame created from RDD should not be broadcasted") {
    val rdd = sparkContext.range(1, 100).map(i => Row(i, i))
    val df = spark.createDataFrame(rdd, new StructType().add("a", LongType).add("b", LongType))
    assert(df.queryExecution.analyzed.statistics.sizeInBytes >
      spark.sessionState.conf.autoBroadcastJoinThreshold)
    assert(df.selectExpr("a").queryExecution.analyzed.statistics.sizeInBytes >
      spark.sessionState.conf.autoBroadcastJoinThreshold)
  }

  test("estimates the size of limit") {
    withTempView("test") {
      Seq(("one", 1), ("two", 2), ("three", 3), ("four", 4)).toDF("k", "v")
        .createOrReplaceTempView("test")
      Seq((0, 1), (1, 24), (2, 48)).foreach { case (limit, expected) =>
        val df = sql(s"""SELECT * FROM test limit $limit""")

        val sizesGlobalLimit = df.queryExecution.analyzed.collect { case g: GlobalLimit =>
          g.statistics.sizeInBytes
        }
        assert(sizesGlobalLimit.size === 1, s"Size wrong for:\n ${df.queryExecution}")
        assert(sizesGlobalLimit.head === BigInt(expected),
          s"expected exact size $expected for table 'test', got: ${sizesGlobalLimit.head}")

        val sizesLocalLimit = df.queryExecution.analyzed.collect { case l: LocalLimit =>
          l.statistics.sizeInBytes
        }
        assert(sizesLocalLimit.size === 1, s"Size wrong for:\n ${df.queryExecution}")
        assert(sizesLocalLimit.head === BigInt(expected),
          s"expected exact size $expected for table 'test', got: ${sizesLocalLimit.head}")
      }
    }
  }

  test("estimates the size of a limit 0 on outer join") {
    withTempView("test") {
      Seq(("one", 1), ("two", 2), ("three", 3), ("four", 4)).toDF("k", "v")
        .createOrReplaceTempView("test")
      val df1 = spark.table("test")
      val df2 = spark.table("test").limit(0)
      val df = df1.join(df2, Seq("k"), "left")

      val sizes = df.queryExecution.analyzed.collect { case g: Join =>
        g.statistics.sizeInBytes
      }

      assert(sizes.size === 1, s"number of Join nodes is wrong:\n ${df.queryExecution}")
      assert(sizes.head === BigInt(96),
        s"expected exact size 96 for table 'test', got: ${sizes.head}")
    }
  }

  test("test table-level statistics for data source table created in InMemoryCatalog") {
    def checkTableStats(tableName: String, rowCount: Option[BigInt]): Unit = {
      val df = sql(s"SELECT * FROM $tableName")
      val statsSeq = df.queryExecution.analyzed.collect { case rel: LogicalRelation =>
        rel.statistics
      }
      assert(statsSeq.size === 1)
      assert(statsSeq.head.rowCount === rowCount)
    }

    val tableName = "tbl"
    withTable(tableName) {
      sql(s"CREATE TABLE $tableName(i INT, j STRING) USING parquet")
      Seq(1 -> "a", 2 -> "b").toDF("i", "j").write.mode("overwrite").insertInto("tbl")

      // noscan won't count the number of rows
      sql(s"ANALYZE TABLE $tableName COMPUTE STATISTICS noscan")
      checkTableStats(tableName, None)

      // without noscan, we count the number of rows
      sql(s"ANALYZE TABLE $tableName COMPUTE STATISTICS")
      checkTableStats(tableName, Some(2))
    }
  }
}
