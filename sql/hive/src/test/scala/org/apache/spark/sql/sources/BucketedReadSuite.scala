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

package org.apache.spark.sql.sources

import org.apache.spark.sql.{DataFrame, QueryTest}
import org.apache.spark.sql.catalyst.expressions.UnsafeProjection
import org.apache.spark.sql.execution.{Sort, Exchange}
import org.apache.spark.sql.execution.datasources.BucketingUtils
import org.apache.spark.sql.execution.joins.SortMergeJoin
import org.apache.spark.sql.hive.test.TestHiveSingleton
import org.apache.spark.sql.test.SQLTestUtils

class BucketedReadSuite extends QueryTest with SQLTestUtils with TestHiveSingleton {
  import testImplicits._

  test("read bucketed data") {
    val df = (0 until 50).map(i => (i % 5, i % 13, i.toString)).toDF("i", "j", "k")
    withTable("bucketed_table") {
      df.write
        .format("parquet")
        .partitionBy("i")
        .bucketBy(8, "j", "k")
        .saveAsTable("bucketed_table")

      for (i <- 0 until 5) {
        val rdd = hiveContext.table("bucketed_table").filter($"i" === i).queryExecution.toRdd
        assert(rdd.partitions.length == 8)

        val attrs = df.select("j", "k").schema.toAttributes
        val checkBucketId = rdd.mapPartitionsWithIndex((index, rows) => {
          val bucketIdExpression = BucketingUtils.bucketIdExpression(attrs, 8)
          val projection = UnsafeProjection.create(bucketIdExpression :: Nil, attrs)
          rows.map(row => projection(row).getInt(0) == index)
        })

        assert(checkBucketId.collect().reduce(_ && _))
      }
    }
  }

  test("avoid shuffle and sort when join 2 bucketed tables") {
    def createTable(df: DataFrame, tableName: String): Unit = {
      df.write.format("parquet").bucketBy(8, "i", "j").sortBy("i", "j").saveAsTable(tableName)
    }

    withTable("bucketed_table1", "bucketed_table2") {
      val df1 = (0 until 50).map(i => (i % 5, i % 13, i.toString)).toDF("i", "j", "k")
      val df2 = (0 until 50).map(i => (i % 7, i % 11, i.toString)).toDF("i", "j", "k")

      createTable(df1, "bucketed_table1")
      createTable(df2, "bucketed_table2")

      withSQLConf("spark.sql.autoBroadcastJoinThreshold" -> "0") {
        val t1 = hiveContext.table("bucketed_table1")
        val t2 = hiveContext.table("bucketed_table2")

        val joined = t1.join(t2, t1("i") === t2("i") && t1("j") === t2("j"))
        assert(joined.queryExecution.executedPlan.isInstanceOf[SortMergeJoin])
        assert(joined.queryExecution.executedPlan.find(_.isInstanceOf[Exchange]).isEmpty)
        assert(joined.queryExecution.executedPlan.find(_.isInstanceOf[Sort]).isEmpty)
        checkAnswer(
          joined.sort(t1("k")),
          df1.join(df2, df1("i") === df2("i") && df1("j") === df2("j")).sort(df1("k")))
      }
    }
  }

  test("shuffle when join 2 bucketed tables with bucketing disabled") {
    withSQLConf("spark.sql.sources.bucketing.enabled" -> "false") {
      def createTable(df: DataFrame, tableName: String): Unit = {
        df.write.format("parquet").bucketBy(8, "i", "j").saveAsTable(tableName)
      }

      withTable("bucketed_table1", "bucketed_table2") {
        val df1 = (0 until 50).map(i => (i % 5, i % 13, i.toString)).toDF("i", "j", "k")
        val df2 = (0 until 50).map(i => (i % 7, i % 11, i.toString)).toDF("i", "j", "k")

        createTable(df1, "bucketed_table1")
        createTable(df2, "bucketed_table2")

        withSQLConf("spark.sql.autoBroadcastJoinThreshold" -> "0") {
          val t1 = hiveContext.table("bucketed_table1")
          val t2 = hiveContext.table("bucketed_table2")

          val joined = t1.join(t2, t1("i") === t2("i") && t1("j") === t2("j"))
          assert(joined.queryExecution.executedPlan.isInstanceOf[SortMergeJoin])
          assert(joined.queryExecution.executedPlan.find(_.isInstanceOf[Exchange]).isDefined)
          checkAnswer(
            joined.sort(t1("k")),
            df1.join(df2, df1("i") === df2("i") && df1("j") === df2("j")).sort(df1("k")))
        }
      }
    }
  }
}
