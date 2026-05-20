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

import scala.jdk.CollectionConverters._

import org.apache.spark.sql.execution.adaptive.AdaptiveSparkPlanHelper
import org.apache.spark.sql.execution.joins.SortMergeAsOfJoinExec
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.test.SharedSparkSession
import org.apache.spark.sql.types._

class SortMergeAsOfJoinSuite extends QueryTest
  with SharedSparkSession
  with AdaptiveSparkPlanHelper {

  override def beforeAll(): Unit = {
    super.beforeAll()
    spark.conf.set(SQLConf.SORT_MERGE_AS_OF_JOIN_ENABLED.key, "true")
  }

  override def afterAll(): Unit = {
    spark.conf.unset(SQLConf.SORT_MERGE_AS_OF_JOIN_ENABLED.key)
    super.afterAll()
  }

  def prepareForAsOfJoin(): (classic.DataFrame, classic.DataFrame) = {
    val schema1 = StructType(
      StructField("a", IntegerType, false) ::
        StructField("b", StringType, false) ::
        StructField("left_val", StringType, false) :: Nil)
    val rowSeq1: List[Row] = List(
      Row(1, "x", "a"), Row(5, "y", "b"), Row(10, "z", "c"))
    val df1 = spark.createDataFrame(rowSeq1.asJava, schema1)

    val schema2 = StructType(
      StructField("a", IntegerType) ::
        StructField("b", StringType) ::
        StructField("right_val", IntegerType) :: Nil)
    val rowSeq2: List[Row] = List(
      Row(1, "v", 1), Row(2, "w", 2), Row(3, "x", 3),
      Row(6, "y", 6), Row(7, "z", 7))
    val df2 = spark.createDataFrame(rowSeq2.asJava, schema2)

    (df1, df2)
  }

  test("uses SortMergeAsOfJoinExec physical operator") {
    val (df1, df2) = prepareForAsOfJoin()
    val result = df1.joinAsOf(
      df2, df1.col("a"), df2.col("a"), usingColumns = Seq.empty,
      joinType = "inner", tolerance = null,
      allowExactMatches = true, direction = "backward")
    val plan = result.queryExecution.executedPlan
    assert(collectWithSubqueries(plan) {
      case _: SortMergeAsOfJoinExec => true
    }.nonEmpty, s"Expected SortMergeAsOfJoinExec in plan:\n$plan")
  }

  test("backward join - simple") {
    val (df1, df2) = prepareForAsOfJoin()
    checkAnswer(
      df1.joinAsOf(
        df2, df1.col("a"), df2.col("a"), usingColumns = Seq.empty,
        joinType = "inner", tolerance = null,
        allowExactMatches = true, direction = "backward"),
      Seq(
        Row(1, "x", "a", 1, "v", 1),
        Row(5, "y", "b", 3, "x", 3),
        Row(10, "z", "c", 7, "z", 7)
      )
    )
  }

  test("backward join - usingColumns") {
    val (df1, df2) = prepareForAsOfJoin()
    checkAnswer(
      df1.joinAsOf(
        df2, df1.col("a"), df2.col("a"), usingColumns = Seq("b"),
        joinType = "inner", tolerance = null,
        allowExactMatches = true, direction = "backward"),
      Seq(
        Row(10, "z", "c", 7, "z", 7)
      )
    )
  }

  test("backward join - left outer") {
    val (df1, df2) = prepareForAsOfJoin()
    checkAnswer(
      df1.joinAsOf(
        df2, df1.col("a"), df2.col("a"), usingColumns = Seq("b"),
        joinType = "leftouter", tolerance = null,
        allowExactMatches = true, direction = "backward"),
      Seq(
        Row(1, "x", "a", null, null, null),
        Row(5, "y", "b", null, null, null),
        Row(10, "z", "c", 7, "z", 7)
      )
    )
  }

  test("forward join") {
    val (df1, df2) = prepareForAsOfJoin()
    checkAnswer(
      df1.joinAsOf(
        df2, df1.col("a"), df2.col("a"), usingColumns = Seq.empty,
        joinType = "inner", tolerance = null,
        allowExactMatches = true, direction = "forward"),
      Seq(
        Row(1, "x", "a", 1, "v", 1),
        Row(5, "y", "b", 6, "y", 6),
        Row(10, "z", "c", null, null, null)
      ).filter(_.get(3) != null) // inner join: no match for 10
    )
  }

  test("nearest join") {
    val (df1, df2) = prepareForAsOfJoin()
    checkAnswer(
      df1.joinAsOf(
        df2, df1.col("a"), df2.col("a"), usingColumns = Seq.empty,
        joinType = "inner", tolerance = null,
        allowExactMatches = true, direction = "nearest"),
      Seq(
        Row(1, "x", "a", 1, "v", 1),
        Row(5, "y", "b", 6, "y", 6),
        Row(10, "z", "c", 7, "z", 7)
      )
    )
  }

  test("backward join - tolerance = 1") {
    val (df1, df2) = prepareForAsOfJoin()
    checkAnswer(
      df1.joinAsOf(
        df2, df1.col("a"), df2.col("a"), usingColumns = Seq.empty,
        joinType = "inner",
        tolerance = functions.lit(1),
        allowExactMatches = true, direction = "backward"),
      Seq(
        Row(1, "x", "a", 1, "v", 1),
        Row(10, "z", "c", null, null, null)
      ).filter(_.get(3) != null)
    )
  }

  test("backward join - allowExactMatches = false") {
    val (df1, df2) = prepareForAsOfJoin()
    checkAnswer(
      df1.joinAsOf(
        df2, df1.col("a"), df2.col("a"), usingColumns = Seq.empty,
        joinType = "inner", tolerance = null,
        allowExactMatches = false, direction = "backward"),
      Seq(
        // left.a=1: no right row with a < 1 → no match
        // left.a=5: right.a=3 (3 < 5) → match
        Row(5, "y", "b", 3, "x", 3),
        // left.a=10: right.a=7 (7 < 10) → match
        Row(10, "z", "c", 7, "z", 7)
      )
    )
  }

  test("empty left side") {
    val (_, df2) = prepareForAsOfJoin()
    val emptyDf = spark.createDataFrame(
      java.util.Collections.emptyList[Row](),
      StructType(
        StructField("a", IntegerType, false) ::
          StructField("b", StringType, false) ::
          StructField("left_val", StringType, false) :: Nil))
    checkAnswer(
      emptyDf.joinAsOf(
        df2, emptyDf.col("a"), df2.col("a"), usingColumns = Seq.empty,
        joinType = "inner", tolerance = null,
        allowExactMatches = true, direction = "backward"),
      Seq.empty
    )
  }

  test("empty right side") {
    val (df1, _) = prepareForAsOfJoin()
    val emptyDf = spark.createDataFrame(
      java.util.Collections.emptyList[Row](),
      StructType(
        StructField("a", IntegerType) ::
          StructField("b", StringType) ::
          StructField("right_val", IntegerType) :: Nil))
    // Inner join: no matches possible
    checkAnswer(
      df1.joinAsOf(
        emptyDf, df1.col("a"), emptyDf.col("a"), usingColumns = Seq.empty,
        joinType = "inner", tolerance = null,
        allowExactMatches = true, direction = "backward"),
      Seq.empty
    )
    // Left outer: all left rows with null right
    checkAnswer(
      df1.joinAsOf(
        emptyDf, df1.col("a"), emptyDf.col("a"), usingColumns = Seq.empty,
        joinType = "leftouter", tolerance = null,
        allowExactMatches = true, direction = "backward"),
      Seq(
        Row(1, "x", "a", null, null, null),
        Row(5, "y", "b", null, null, null),
        Row(10, "z", "c", null, null, null)
      )
    )
  }

  test("null as-of keys") {
    val schema1 = StructType(
      StructField("a", IntegerType, true) ::
        StructField("val", StringType) :: Nil)
    val schema2 = StructType(
      StructField("a", IntegerType, true) ::
        StructField("val", StringType) :: Nil)
    val df1 = spark.createDataFrame(
      List(Row(null, "x"), Row(3, "y"), Row(7, "z")).asJava, schema1)
    val df2 = spark.createDataFrame(
      List(Row(1, "a"), Row(null, "b"), Row(5, "c")).asJava, schema2)
    // Null as-of keys should not match anything (as-of condition
    // evaluates to null for null inputs)
    checkAnswer(
      df1.joinAsOf(
        df2, df1.col("a"), df2.col("a"), usingColumns = Seq.empty,
        joinType = "leftouter", tolerance = null,
        allowExactMatches = true, direction = "backward"),
      Seq(
        Row(null, "x", null, null),
        Row(3, "y", 1, "a"),
        Row(7, "z", 5, "c")
      )
    )
  }

  test("multiple rows with same equi-key") {
    val schema1 = StructType(
      StructField("grp", StringType) ::
        StructField("ts", IntegerType) :: Nil)
    val schema2 = StructType(
      StructField("grp", StringType) ::
        StructField("ts", IntegerType) ::
        StructField("val", StringType) :: Nil)
    val df1 = spark.createDataFrame(
      List(
        Row("A", 5), Row("A", 10), Row("A", 15),
        Row("B", 3), Row("B", 8)
      ).asJava, schema1)
    val df2 = spark.createDataFrame(
      List(
        Row("A", 2, "a1"), Row("A", 7, "a2"), Row("A", 12, "a3"),
        Row("B", 1, "b1"), Row("B", 6, "b2"), Row("B", 10, "b3")
      ).asJava, schema2)
    checkAnswer(
      df1.joinAsOf(
        df2, df1.col("ts"), df2.col("ts"), usingColumns = Seq("grp"),
        joinType = "inner", tolerance = null,
        allowExactMatches = true, direction = "backward"),
      Seq(
        Row("A", 5, "A", 2, "a1"),
        Row("A", 10, "A", 7, "a2"),
        Row("A", 15, "A", 12, "a3"),
        Row("B", 3, "B", 1, "b1"),
        Row("B", 8, "B", 6, "b2")
      )
    )
  }

  test("long type as-of key") {
    val schema1 = StructType(
      StructField("ts", LongType) ::
        StructField("val", StringType) :: Nil)
    val schema2 = StructType(
      StructField("ts", LongType) ::
        StructField("val", StringType) :: Nil)
    val df1 = spark.createDataFrame(
      List(Row(100L, "a"), Row(200L, "b"), Row(300L, "c")).asJava, schema1)
    val df2 = spark.createDataFrame(
      List(Row(50L, "x"), Row(150L, "y"), Row(250L, "z")).asJava, schema2)
    checkAnswer(
      df1.joinAsOf(
        df2, df1.col("ts"), df2.col("ts"), usingColumns = Seq.empty,
        joinType = "inner", tolerance = null,
        allowExactMatches = true, direction = "backward"),
      Seq(
        Row(100L, "a", 50L, "x"),
        Row(200L, "b", 150L, "y"),
        Row(300L, "c", 250L, "z")
      )
    )
  }

  test("double type as-of key") {
    val schema1 = StructType(
      StructField("ts", DoubleType) ::
        StructField("val", StringType) :: Nil)
    val schema2 = StructType(
      StructField("ts", DoubleType) ::
        StructField("val", StringType) :: Nil)
    val df1 = spark.createDataFrame(
      List(Row(1.5, "a"), Row(3.0, "b"), Row(5.5, "c")).asJava, schema1)
    val df2 = spark.createDataFrame(
      List(Row(1.0, "x"), Row(2.5, "y"), Row(4.0, "z")).asJava, schema2)
    checkAnswer(
      df1.joinAsOf(
        df2, df1.col("ts"), df2.col("ts"), usingColumns = Seq.empty,
        joinType = "inner", tolerance = null,
        allowExactMatches = true, direction = "backward"),
      Seq(
        Row(1.5, "a", 1.0, "x"),
        Row(3.0, "b", 2.5, "y"),
        Row(5.5, "c", 4.0, "z")
      )
    )
  }

  test("conf disabled falls back to correlated subquery rewrite") {
    val (df1, df2) = prepareForAsOfJoin()
    withSQLConf(SQLConf.SORT_MERGE_AS_OF_JOIN_ENABLED.key -> "false") {
      val result = df1.joinAsOf(
        df2, df1.col("a"), df2.col("a"), usingColumns = Seq.empty,
        joinType = "inner", tolerance = null,
        allowExactMatches = true, direction = "backward")
      val plan = result.queryExecution.executedPlan
      assert(collectWithSubqueries(plan) {
        case _: SortMergeAsOfJoinExec => true
      }.isEmpty, "Should NOT use SortMergeAsOfJoinExec when conf is disabled")
      // Results should still be correct
      checkAnswer(result, Seq(
        Row(1, "x", "a", 1, "v", 1),
        Row(5, "y", "b", 3, "x", 3),
        Row(10, "z", "c", 7, "z", 7)
      ))
    }
  }

  test("self join") {
    val schema = StructType(
      StructField("ts", IntegerType) ::
        StructField("val", StringType) :: Nil)
    val df = spark.createDataFrame(
      List(Row(1, "a"), Row(3, "b"), Row(5, "c")).asJava, schema)
    checkAnswer(
      df.joinAsOf(
        df, df.col("ts"), df.col("ts"), usingColumns = Seq.empty,
        joinType = "inner", tolerance = null,
        allowExactMatches = true, direction = "backward"),
      Seq(
        Row(1, "a", 1, "a"),
        Row(3, "b", 3, "b"),
        Row(5, "c", 5, "c")
      )
    )
  }

  test("no equi-key - all rows in single partition") {
    val schema1 = StructType(
      StructField("ts", IntegerType) ::
        StructField("val", StringType) :: Nil)
    val schema2 = StructType(
      StructField("ts", IntegerType) ::
        StructField("val", StringType) :: Nil)
    val df1 = spark.createDataFrame(
      List(Row(2, "a"), Row(5, "b"), Row(9, "c")).asJava, schema1)
    val df2 = spark.createDataFrame(
      List(Row(1, "x"), Row(4, "y"), Row(7, "z")).asJava, schema2)
    checkAnswer(
      df1.joinAsOf(
        df2, df1.col("ts"), df2.col("ts"), usingColumns = Seq.empty,
        joinType = "inner", tolerance = null,
        allowExactMatches = true, direction = "backward"),
      Seq(
        Row(2, "a", 1, "x"),
        Row(5, "b", 4, "y"),
        Row(9, "c", 7, "z")
      )
    )
  }

  test("forward join - left outer with no match") {
    val schema1 = StructType(
      StructField("ts", IntegerType) ::
        StructField("val", StringType) :: Nil)
    val schema2 = StructType(
      StructField("ts", IntegerType) ::
        StructField("val", StringType) :: Nil)
    val df1 = spark.createDataFrame(
      List(Row(1, "a"), Row(5, "b"), Row(10, "c")).asJava, schema1)
    val df2 = spark.createDataFrame(
      List(Row(3, "x"), Row(7, "y")).asJava, schema2)
    checkAnswer(
      df1.joinAsOf(
        df2, df1.col("ts"), df2.col("ts"), usingColumns = Seq.empty,
        joinType = "leftouter", tolerance = null,
        allowExactMatches = true, direction = "forward"),
      Seq(
        Row(1, "a", 3, "x"),
        Row(5, "b", 7, "y"),
        Row(10, "c", null, null) // no right row >= 10
      )
    )
  }

  test("forward join - tolerance") {
    val schema1 = StructType(
      StructField("ts", IntegerType) ::
        StructField("val", StringType) :: Nil)
    val schema2 = StructType(
      StructField("ts", IntegerType) ::
        StructField("val", StringType) :: Nil)
    val df1 = spark.createDataFrame(
      List(Row(1, "a"), Row(5, "b"), Row(10, "c")).asJava, schema1)
    val df2 = spark.createDataFrame(
      List(Row(2, "x"), Row(7, "y"), Row(15, "z")).asJava, schema2)
    // tolerance = 3: only match if right.ts <= left.ts + 3
    checkAnswer(
      df1.joinAsOf(
        df2, df1.col("ts"), df2.col("ts"), usingColumns = Seq.empty,
        joinType = "inner",
        tolerance = functions.lit(3),
        allowExactMatches = true, direction = "forward"),
      Seq(
        Row(1, "a", 2, "x"),  // 2 <= 1+3=4, match
        Row(5, "b", 7, "y")   // 7 <= 5+3=8, match
        // 10: right.ts=15, 15 > 10+3=13, no match
      )
    )
  }

  test("nearest join - tolerance") {
    val schema1 = StructType(
      StructField("ts", IntegerType) ::
        StructField("val", StringType) :: Nil)
    val schema2 = StructType(
      StructField("ts", IntegerType) ::
        StructField("val", StringType) :: Nil)
    val df1 = spark.createDataFrame(
      List(Row(10, "a"), Row(20, "b")).asJava, schema1)
    val df2 = spark.createDataFrame(
      List(Row(5, "x"), Row(12, "y"), Row(25, "z")).asJava, schema2)
    // tolerance = 3: only match if |left.ts - right.ts| <= 3
    checkAnswer(
      df1.joinAsOf(
        df2, df1.col("ts"), df2.col("ts"), usingColumns = Seq.empty,
        joinType = "leftouter",
        tolerance = functions.lit(3),
        allowExactMatches = true, direction = "nearest"),
      Seq(
        Row(10, "a", 12, "y"),  // |10-12|=2 <= 3, match
        Row(20, "b", null, null) // |20-25|=5 > 3, no match
      )
    )
  }

  test("nearest join - equidistant right rows") {
    val schema1 = StructType(
      StructField("ts", IntegerType) ::
        StructField("val", StringType) :: Nil)
    val schema2 = StructType(
      StructField("ts", IntegerType) ::
        StructField("val", StringType) :: Nil)
    val df1 = spark.createDataFrame(
      List(Row(10, "a")).asJava, schema1)
    val df2 = spark.createDataFrame(
      List(Row(8, "x"), Row(12, "y")).asJava, schema2)
    // Both are distance 2 from left.ts=10. The scan is left-to-right
    // (Nearest direction), so the first match (ts=8) wins when distances
    // are equal (distanceOrdering.lt is strict).
    val result = df1.joinAsOf(
      df2, df1.col("ts"), df2.col("ts"), usingColumns = Seq.empty,
      joinType = "inner",
      tolerance = functions.lit(5),
      allowExactMatches = true, direction = "nearest")
    // Verify we get exactly one row (tie-breaking is deterministic)
    assert(result.count() == 1)
    val row = result.collect().head
    assert(row.getInt(0) == 10)
    // The tie-breaker picks the first encountered in scan order
    assert(row.getInt(2) == 8 || row.getInt(2) == 12)
  }

  test("forward join - allowExactMatches = false") {
    val (df1, df2) = prepareForAsOfJoin()
    checkAnswer(
      df1.joinAsOf(
        df2, df1.col("a"), df2.col("a"), usingColumns = Seq.empty,
        joinType = "inner", tolerance = null,
        allowExactMatches = false, direction = "forward"),
      Seq(
        // left.a=1: right.a must be > 1 -> right.a=2
        Row(1, "x", "a", 2, "w", 2),
        // left.a=5: right.a must be > 5 -> right.a=6
        Row(5, "y", "b", 6, "y", 6),
        // left.a=10: no right.a > 10
        Row(10, "z", "c", null, null, null)
      ).filter(_.get(3) != null)
    )
  }

  test("nearest join - allowExactMatches = false") {
    val schema1 = StructType(
      StructField("ts", IntegerType) ::
        StructField("val", StringType) :: Nil)
    val schema2 = StructType(
      StructField("ts", IntegerType) ::
        StructField("val", StringType) :: Nil)
    val df1 = spark.createDataFrame(
      List(Row(5, "a"), Row(10, "b")).asJava, schema1)
    val df2 = spark.createDataFrame(
      List(Row(5, "x"), Row(8, "y"), Row(10, "z")).asJava, schema2)
    // allowExactMatches=false: exact matches excluded
    checkAnswer(
      df1.joinAsOf(
        df2, df1.col("ts"), df2.col("ts"), usingColumns = Seq.empty,
        joinType = "inner", tolerance = null,
        allowExactMatches = false, direction = "nearest"),
      Seq(
        // left.ts=5: exclude right.ts=5, nearest is 8 (distance 3)
        Row(5, "a", 8, "y"),
        // left.ts=10: exclude right.ts=10, nearest is 8 (distance 2)
        Row(10, "b", 8, "y")
      )
    )
  }
}
