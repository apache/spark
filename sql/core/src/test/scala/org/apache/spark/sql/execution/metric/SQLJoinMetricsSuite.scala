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

package org.apache.spark.sql.execution.metric

import org.apache.spark.sql.execution.adaptive.DisableAdaptiveExecutionSuite
import org.apache.spark.sql.execution.joins.ShuffledHashJoinExec
import org.apache.spark.sql.functions.broadcast
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.test.SharedSparkSession

class SQLJoinMetricsSuite  extends SharedSparkSession with SQLMetricsTestUtils
  with DisableAdaptiveExecutionSuite {
  import testImplicits._

  test("SortMergeJoin metrics") {
    // Because SortMergeJoin may skip different rows if the number of partitions is different, this
    // test should use the deterministic number of partitions.
    val testDataForJoin = testData2.filter('a < 2) // TestData2(1, 1) :: TestData2(1, 2)
    testDataForJoin.createOrReplaceTempView("testDataForJoin")
    withTempView("testDataForJoin") {
      // Assume the execution plan is
      // ... -> SortMergeJoin(nodeId = 1) -> TungstenProject(nodeId = 0)
      val query1 = "SELECT * FROM testData2 JOIN testDataForJoin ON testData2.a = testDataForJoin.a"
      Seq((0L, 2L, false), (1L, 4L, true)).foreach { case (nodeId1, nodeId2, enableWholeStage) =>
        val df = spark.sql(query1)
        testSparkPlanMetrics(df, 1, Map(
          nodeId1 -> (("SortMergeJoin", Map(
            // It's 4 because we only read 3 rows in the first partition and 1 row in the second one
            "number of output rows" -> 4L,
            "number of matched rows" -> 4L))),
          nodeId2 -> (("Exchange", Map(
            "records read" -> 4L,
            "local blocks read" -> 2L,
            "remote blocks read" -> 0L,
            "shuffle records written" -> 2L)))),
          enableWholeStage
        )
      }

      val query2 = "SELECT * FROM testData2 JOIN testDataForJoin ON " +
        "testData2.a = testDataForJoin.a AND testData2.b <= testDataForJoin.b"
      val df2 = spark.sql(query2)
      Seq(false, true).foreach { case  enableWholeStage =>
        testSparkPlanMetrics(df2, 1, Map(
          0L -> ("SortMergeJoin", Map(
            "number of output rows" -> 3L,
            "number of matched rows" -> 4L))),
          enableWholeStage
        )
      }
    }
  }

  test("SortMergeJoin(outer) metrics") {
    // Because SortMergeJoin may skip different rows if the number of partitions is different,
    // this test should use the deterministic number of partitions.
    val testDataForJoin = testData2.filter('a < 2) // TestData2(1, 1) :: TestData2(1, 2)
    testDataForJoin.createOrReplaceTempView("testDataForJoin")
    withTempView("testDataForJoin") {
      // Assume the execution plan is
      // ... -> SortMergeJoin(nodeId = 1) -> TungstenProject(nodeId = 0)
      val query1 = "SELECT * FROM testData2 LEFT JOIN testDataForJoin ON " +
        "testData2.a = testDataForJoin.a"
      val query2 = "SELECT * FROM testDataForJoin RIGHT JOIN testData2 ON " +
        "testData2.a = testDataForJoin.a"
      val query3 = "SELECT * FROM testData2 RIGHT JOIN testDataForJoin ON " +
        "testData2.a = testDataForJoin.a"
      val query4 = "SELECT * FROM testData2 FULL OUTER JOIN testDataForJoin ON " +
        "testData2.a = testDataForJoin.a"
      val boundCondition1 = " AND testData2.b >= testDataForJoin.b"
      val boundCondition2 = " AND testData2.a >= testDataForJoin.b"

      Seq((query1, 8L, false),
        (query1 + boundCondition1, 7L, false),
        (query1 + boundCondition1, 7L, true),
        (query3 + boundCondition2, 3L, false),
        (query3 + boundCondition2, 3L, true),
        (query4, 8L, false),
        (query4, 8L, true),
        (query4 + boundCondition1, 7L, false),
        (query4 + boundCondition1, 7L, true),
        (query1, 8L, true),
        (query2, 8L, false),
        (query2, 8L, true)).foreach { case (query, rows, enableWholeStage) =>
        val df = spark.sql(query)
        testSparkPlanMetrics(df, 1, Map(
          0L -> (("SortMergeJoin", Map(
            "number of output rows" -> rows,
            "number of matched rows" -> 4L)))),
          enableWholeStage
        )
      }
    }
  }

  test("BroadcastHashJoin metrics") {
    val df1 = Seq((1, "1"), (2, "2")).toDF("key", "value")
    val df2 = Seq((1, "1"), (2, "2"), (3, "3"), (4, "4")).toDF("key", "value")
    val df3 = Seq((1, 1), (2, 3), (2, 4), (2, 4)).toDF("key", "value1")
    val df4 = Seq((1, 1), (2, 2), (3, 3), (4, 4)).toDF("key", "value2")

    Seq((false, df1, df2, 1L, 2L, 2L, false),
      (false, df1, df2, 2L, 2L, 2L, true),
      (true, df3, df4, 2L, 3L, 4L, true),
      (true, df3, df4, 1L, 3L, 4L, false)
    ).foreach {
      case (boundCondition, dfLeft, dfRight, nodeId, rows, matchedRows, enableWholeStage) =>
        var df = dfLeft.join(broadcast(dfRight), "key")
        if (boundCondition) {
          df = df.filter("value1 > value2")
        }
        testSparkPlanMetrics(df, 2, Map(
          nodeId -> (("BroadcastHashJoin", Map(
            "number of output rows" -> rows,
            "number of matched rows" -> matchedRows)))),
          enableWholeStage
        )
    }
  }

  test("ShuffledHashJoin metrics") {
    withSQLConf(SQLConf.AUTO_BROADCASTJOIN_THRESHOLD.key -> "40",
      SQLConf.SHUFFLE_PARTITIONS.key -> "2",
      SQLConf.PREFER_SORTMERGEJOIN.key -> "false") {
      val df1 = Seq((1, "1"), (2, "2")).toDF("key", "value")
      val df2 = (1 to 10).map(i => (i, i.toString)).toSeq.toDF("key", "value")
      val df3 = Seq((1, "1", 1), (2, "2", 4)).toDF("key", "val0", "val1")
      val df4 = (1 to 10).map(i => (i, i.toString, i)).toSeq.toDF("key", "val2", "val3")
      Seq((df1, df2, false, 1L, 2L, 5L, 2L, false),
        (df1, df2, false, 2L, 3L, 7L, 2L, true),
        (df3, df4, true, 1L, 2L, 5L, 1L, false),
        (df3, df4, true, 2L, 3L, 7L, 1L, true)).foreach {
        case (dfA, dfB, boundCondition, nodeId1, nodeId2, nodeId3, rows, enableWholeStage) =>
          var df = dfA.join(dfB, "key")
          if (boundCondition) {
            df = df.filter("val1 > val3")
          }
          testSparkPlanMetrics(df, 1, Map(
            nodeId1 -> (("ShuffledHashJoin", Map(
              "number of output rows" -> rows,
              "number of matched rows" -> 2L))),
            nodeId2 -> (("Exchange", Map(
              "shuffle records written" -> 2L,
              "records read" -> 2L))),
            nodeId3 -> (("Exchange", Map(
              "shuffle records written" -> 10L,
              "records read" -> 10L)))),
            enableWholeStage
          )
      }
    }
  }

  test("SPARK-34369: ShuffledHashJoin(left/right/outer/semi/anti, outer) metrics") {
    val leftDf = Seq((1, 2), (2, 1)).toDF("key", "value1")
    val rightDf = (1 to 10).map(i => (i, i)).toSeq.toDF("key2", "value2")
    Seq((false, 0L, "right_outer", leftDf, rightDf, 10L, 2L, false),
      (false, 0L, "left_outer", rightDf, leftDf, 10L, 2L, false),
      (false, 1L, "right_outer", leftDf, rightDf, 10L, 2L, true),
      (false, 1L, "left_outer", rightDf, leftDf, 10L, 2L, true),
      (false, 2L, "left_anti", rightDf, leftDf, 8L, 2L, true),
      (false, 2L, "left_semi", rightDf, leftDf, 2L, 2L, true),
      (false, 1L, "left_anti", rightDf, leftDf, 8L, 0L, false),
      (false, 1L, "left_semi", rightDf, leftDf, 2L, 0L, false),
      (true, 0L, "right_outer", leftDf, rightDf, 10L, 2L, false),
      (true, 0L, "left_outer", rightDf, leftDf, 10L, 2L, false),
      (true, 1L, "right_outer", leftDf, rightDf, 10L, 2L, true),
      (true, 1L, "left_outer", rightDf, leftDf, 10L, 2L, true),
      (true, 2L, "left_anti", rightDf, leftDf, 9L, 2L, true),
      (true, 2L, "left_semi", rightDf, leftDf, 1L, 2L, true),
      (true, 1L, "left_anti", rightDf, leftDf, 9L, 2L, false),
      (true, 1L, "left_semi", rightDf, leftDf, 1L, 2L, false)).foreach {
      case (boundCondition, nodeId, joinType, leftDf,
      rightDf, rows, numMatched, enableWholeStage) =>
        val joinExprs = if (!boundCondition) {
          $"key" === $"key2"
        } else {
          $"key" === $"key2" and $"value1" > $"value2"
        }
        val df = leftDf.hint("shuffle_hash").join(
          rightDf.hint("shuffle_hash"), joinExprs, joinType)
        testSparkPlanMetrics(df, 1, Map(
          nodeId -> (("ShuffledHashJoin", Map(
            "number of output rows" -> rows,
            "number of matched rows" -> numMatched)))),
          enableWholeStage
        )
    }
  }

  test("SPARK-32629: ShuffledHashJoin(full outer) metrics") {
    val uniqueLeftDf = Seq(("1", "1"), ("11", "11")).toDF("key", "value")
    val nonUniqueLeftDf = Seq(("1", "1"), ("1", "2"), ("11", "11")).toDF("key", "value")
    val rightDf = (1 to 10).map(i => (i.toString, i.toString)).toDF("key2", "value")
    Seq(
      // Test unique key on build side
      (uniqueLeftDf, rightDf, 11, 134228048, 10, 134221824),
      // Test non-unique key on build side
      (nonUniqueLeftDf, rightDf, 12, 134228552, 11, 134221824)
    ).foreach { case (leftDf, rightDf, fojRows, fojBuildSize, rojRows, rojBuildSize) =>
      val fojDf = leftDf.hint("shuffle_hash").join(
        rightDf, $"key" === $"key2", "full_outer")
      fojDf.collect()
      val fojPlan = fojDf.queryExecution.executedPlan.collectFirst {
        case s: ShuffledHashJoinExec => s
      }
      assert(fojPlan.isDefined, "The query plan should have shuffled hash join")
      testMetricsInSparkPlanOperator(fojPlan.get,
        Map("numOutputRows" -> fojRows, "buildDataSize" -> fojBuildSize))

      // Test right outer join as well to verify build data size to be different
      // from full outer join. This makes sure we take extra BitSet/OpenHashSet
      // for full outer join into account.
      val rojDf = leftDf.hint("shuffle_hash").join(
        rightDf, $"key" === $"key2", "right_outer")
      rojDf.collect()
      val rojPlan = rojDf.queryExecution.executedPlan.collectFirst {
        case s: ShuffledHashJoinExec => s
      }
      assert(rojPlan.isDefined, "The query plan should have shuffled hash join")
      testMetricsInSparkPlanOperator(rojPlan.get,
        Map("numOutputRows" -> rojRows, "buildDataSize" -> rojBuildSize))
    }
  }

  test("BroadcastHashJoin(outer) metrics") {
    val df1 = Seq((1, "a"), (1, "b"), (2, "c"), (4, "d")).toDF("key", "value")
    val df2 = Seq((1, "a"), (1, "b"), (2, "c"), (3, "d")).toDF("key2", "value")
    // Testing the case where all keys are unique
    val df3 = Seq((1, "a"), (2, "c"), (4, "d")).toDF("key", "value")
    val df4 = Seq((1, "a"), (2, "c"), (3, "d")).toDF("key2", "value")
    // Testing with bound condition
    val df5 = Seq((1, 12), (1, 3), (4, 4), (4, 4)).toDF("key1", "value1")
    val df6 = Seq((1, 5), (1, 4), (2, 3), (3, 2), (3, 2)).toDF("key2", "value2")
    // Testing with bound condition with unique keys
    val df7 = Seq((1, 12), (2, 2), (4, 4), (4, 4)).toDF("key1", "value1")
    val df8 = Seq((1, 5), (2, 3), (3, 2)).toDF("key2", "value2")
    Seq((df1, df2, false, "left_outer", 0L, 6L, 5L, false),
      (df1, df2, false, "left_outer", 1L, 6L, 5L, true),
      (df1, df2, false, "right_outer", 0L, 6L, 5L, false),
      (df1, df2, false, "right_outer", 1L, 6L, 5L, true),
      (df3, df4, false, "left_outer", 0L, 3L, 2L, false),
      (df3, df4, false, "left_outer", 1L, 3L, 2L, true),
      (df3, df4, false, "right_outer", 0L, 3L, 2L, false),
      (df3, df4, false, "right_outer", 1L, 3L, 2L, true),
      (df5, df6, true, "right_outer", 0L, 5L, 4L, false),
      (df5, df6, true, "right_outer", 1L, 5L, 4L, true),
      (df5, df6, true, "left_outer", 0L, 5L, 4L, false),
      (df5, df6, true, "left_outer", 1L, 5L, 4L, true),
      (df7, df8, true, "left_outer", 0L, 4L, 2L, false),
      (df7, df8, true, "left_outer", 1L, 4L, 2L, true),
      (df7, df8, true, "right_outer", 0L, 3L, 2L, false),
      (df7, df8, true, "right_outer", 1L, 3L, 2L, true)
    ).foreach {
      case (dfA, dfB, boundCondition, joinType, nodeId, numRows, numMatched, enableWholeStage) =>
        val joinExprs = if (!boundCondition) {
          $"key" === $"key2"
        } else {
          $"key1" === $"key2" and $"value1" < $"value2"
        }
        val df = dfA.join(broadcast(dfB), joinExprs, joinType)
        testSparkPlanMetrics(df, 2, Map(
          nodeId -> (("BroadcastHashJoin", Map(
            "number of output rows" -> numRows,
            "number of matched rows" -> numMatched)))),
          enableWholeStage
        )
    }
  }

  test("BroadcastNestedLoopJoin metrics") {
    val testDataForJoin = testData2.filter('a < 2) // TestData2(1, 1) :: TestData2(1, 2)
    testDataForJoin.createOrReplaceTempView("testDataForJoin")
    withSQLConf(SQLConf.CROSS_JOINS_ENABLED.key -> "true") {
      withTempView("testDataForJoin") {
        // Assume the execution plan is
        // ... -> BroadcastNestedLoopJoin(nodeId = 1) -> TungstenProject(nodeId = 0)
        val leftQuery = "SELECT * FROM testData2 LEFT JOIN testDataForJoin ON " +
          "testData2.a * testDataForJoin.a != testData2.a + testDataForJoin.a"
        val rightQuery = "SELECT * FROM testData2 RIGHT JOIN testDataForJoin ON " +
          "testData2.a * testDataForJoin.a != testData2.a + testDataForJoin.a"
        val boundCondition = " and testData2.b <= testDataForJoin.b"
        val leftBound = leftQuery + boundCondition
        val rightBound = leftQuery + boundCondition

        Seq((leftQuery, 12L, false), (rightQuery, 12L, false), (leftQuery, 12L, true),
          (rightQuery, 12L, true), (leftBound, 9L, false), (leftBound, 9L, true),
          (rightBound, 9L, false), (rightBound, 9L, true))
          .foreach { case (query, rows, enableWholeStage) =>
            val df = spark.sql(query)
            testSparkPlanMetrics(df, 2, Map(
              0L -> (("BroadcastNestedLoopJoin", Map(
                "number of output rows" -> rows,
                "number of matched rows" -> 12L)))),
              enableWholeStage
            )
          }
      }
    }
  }

  test("BroadcastLeftSemiJoinHash metrics") {
    val df1 = Seq((1, "1"), (2, "2")).toDF("key", "value")
    val df2 = Seq((1, "1"), (2, "2"), (3, "3"), (4, "4")).toDF("key2", "value")

    val df3 = Seq((1, 1), (2, 4), (6, 7)).toDF("key", "value1")
    val df4 = Seq((1, 1), (2, 2), (3, 3), (4, 4)).toDF("key2", "value2")
    Seq((df1, df2, false, 1L, 2L, 0L, false),
      (df1, df2, false, 2L, 2L, 2L, true),
      (df3, df4, true, 2L, 0L, 2L, true),
      (df3, df4, true, 1L, 0L, 2L, false)
    ).foreach {
      case (dfA, dfB, boundCondition, nodeId, rows, matched, enableWholeStage) =>
        val joinExprs = if (!boundCondition) {
          $"key" === $"key2"
        } else {
          $"key" === $"key2" and $"value1" < $"value2"
        }
        val df = dfA.join(broadcast(dfB), joinExprs, "leftsemi")
        testSparkPlanMetrics(df, 2, Map(
          nodeId -> (("BroadcastHashJoin", Map(
            "number of output rows" -> rows,
            "number of matched rows" -> matched)))),
          enableWholeStage
        )
    }
  }

  test("BroadcastLeftAntiJoinHash metrics") {
    val df1 = Seq((1, "1"), (2, "2")).toDF("key", "value")
    val df2 = Seq((1, "1"), (2, "2"), (3, "3"), (4, "4")).toDF("key2", "value")
    Seq((1L, false), (2L, true)).foreach { case (nodeId, enableWholeStage) =>
      val df = df2.join(broadcast(df1), $"key" === $"key2", "left_anti")
      testSparkPlanMetrics(df, 2, Map(
        nodeId -> (("BroadcastHashJoin", Map(
          "number of output rows" -> 2L)))),
        enableWholeStage
      )
    }
  }

  test("CartesianProduct metrics") {
    withSQLConf(SQLConf.CROSS_JOINS_ENABLED.key -> "true") {
      val testDataForJoin = testData2.filter('a < 2) // TestData2(1, 1) :: TestData2(1, 2)
      testDataForJoin.createOrReplaceTempView("testDataForJoin")
      withTempView("testDataForJoin") {
        // Assume the execution plan is
        // ... -> CartesianProduct(nodeId = 1) -> TungstenProject(nodeId = 0)
        val query = "SELECT * FROM testData2 JOIN testDataForJoin"
        val boundedQuery = query + " where  testData2.a > testDataForJoin.b"
        Seq((12L, query, true), (12L, query, false), (6L, boundedQuery, true),
          (6L, boundedQuery, false))
          .foreach { case (rows, query, enableWholeStage) =>
            val df = spark.sql(query)
            testSparkPlanMetrics(df, 1, Map(
              0L -> (("CartesianProduct",
                Map("number of output rows" -> rows,
                  "number of matched rows" -> 12L)))),
              enableWholeStage
            )
          }
      }
    }
  }

  test("SortMergeJoin(left-anti) metrics") {
    val anti = testData2.filter("a > 2")
    withTempView("antiData") {
      anti.createOrReplaceTempView("antiData")
      val query = "SELECT * FROM testData2 ANTI JOIN antiData ON testData2.a = antiData.a"
      Seq(false, true).foreach { enableWholeStage =>
        val df = spark.sql(query)
        testSparkPlanMetrics(df, 1, Map(
          0L -> (("SortMergeJoin",
            Map("number of output rows" -> 4L,
              "number of matched rows" -> 2L)))),
          enableWholeStage
        )
      }
    }
  }
}
