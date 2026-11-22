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

import java.time.Duration

import scala.jdk.CollectionConverters._

import org.apache.spark.sql.catalyst.plans.AsOfJoinDirection
import org.apache.spark.sql.execution.adaptive.AdaptiveSparkPlanHelper
import org.apache.spark.sql.functions._
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.test.SharedSparkSession
import org.apache.spark.sql.types._
import org.apache.spark.tags.SlowSQLTest

@SlowSQLTest
class DataFrameAsOfJoinSuite extends SharedSparkSession
  with AdaptiveSparkPlanHelper {

  def prepareForAsOfJoin(): (classic.DataFrame, classic.DataFrame) = {
    val schema1 = StructType(
      StructField("a", IntegerType, false) ::
        StructField("b", StringType, false) ::
        StructField("left_val", StringType, false) :: Nil)
    val rowSeq1: List[Row] = List(Row(1, "x", "a"), Row(5, "y", "b"), Row(10, "z", "c"))
    val df1 = spark.createDataFrame(rowSeq1.asJava, schema1)

    val schema2 = StructType(
      StructField("a", IntegerType) ::
        StructField("b", StringType) ::
        StructField("right_val", IntegerType) :: Nil)
    val rowSeq2: List[Row] = List(Row(1, "v", 1), Row(2, "w", 2), Row(3, "x", 3),
      Row(6, "y", 6), Row(7, "z", 7))
    val df2 = spark.createDataFrame(rowSeq2.asJava, schema2)

    (df1, df2)
  }

  test("as-of join - simple") {
    val (df1, df2) = prepareForAsOfJoin()
    checkAnswer(
      df1.joinAsOf(
        df2, df1.col("a"), df2.col("a"), usingColumns = Seq.empty,
        joinType = "inner", tolerance = null, allowExactMatches = true, direction = "backward"),
      Seq(
        Row(1, "x", "a", 1, "v", 1),
        Row(5, "y", "b", 3, "x", 3),
        Row(10, "z", "c", 7, "z", 7)
      )
    )
  }

  test("as-of join - usingColumns") {
    val (df1, df2) = prepareForAsOfJoin()
    checkAnswer(
      df1.joinAsOf(df2, df1.col("a"), df2.col("a"), usingColumns = Seq("b"),
        joinType = "inner", tolerance = null, allowExactMatches = true, direction = "backward"),
      Seq(
        Row(10, "z", "c", 7, "z", 7)
      )
    )
  }

  test("as-of join - usingColumns, left outer") {
    val (df1, df2) = prepareForAsOfJoin()
    checkAnswer(
      df1.joinAsOf(df2, df1.col("a"), df2.col("a"), usingColumns = Seq("b"),
        joinType = "left", tolerance = null, allowExactMatches = true, direction = "backward"),
      Seq(
        Row(1, "x", "a", null, null, null),
        Row(5, "y", "b", null, null, null),
        Row(10, "z", "c", 7, "z", 7)
      )
    )
  }

  test("as-of join - tolerance = 1") {
    val (df1, df2) = prepareForAsOfJoin()
    checkAnswer(
      df1.joinAsOf(df2, df1.col("a"), df2.col("a"), usingColumns = Seq.empty,
        joinType = "inner", tolerance = lit(1), allowExactMatches = true, direction = "backward"),
      Seq(
        Row(1, "x", "a", 1, "v", 1)
      )
    )
  }

  test("as-of join - tolerance should be a constant") {
    val (df1, df2) = prepareForAsOfJoin()
    checkError(
      exception = intercept[AnalysisException] {
        df1.joinAsOf(
          df2, df1.col("a"), df2.col("a"), usingColumns = Seq.empty,
          joinType = "inner", tolerance = df1.col("b"), allowExactMatches = true,
          direction = "backward")
      },
      condition = "AS_OF_JOIN.TOLERANCE_IS_UNFOLDABLE",
      parameters = Map.empty)
  }

  test("as-of join - tolerance should be non-negative") {
    val (df1, df2) = prepareForAsOfJoin()
    checkError(
      exception = intercept[AnalysisException] {
        df1.joinAsOf(df2, df1.col("a"), df2.col("a"), usingColumns = Seq.empty,
          joinType = "inner", tolerance = lit(-1), allowExactMatches = true,
          direction = "backward")
      },
      condition = "AS_OF_JOIN.TOLERANCE_IS_NON_NEGATIVE",
      parameters = Map.empty)
  }

  test("as-of join - unsupported direction") {
    val (df1, df2) = prepareForAsOfJoin()
    val direction = "unknown"
    checkError(
      exception = intercept[AnalysisException] {
        df1.joinAsOf(df2, df1.col("a"), df2.col("a"), usingColumns = Seq.empty,
          joinType = "inner", tolerance = lit(-1), allowExactMatches = true,
          direction = direction)
      },
      condition = "AS_OF_JOIN.UNSUPPORTED_DIRECTION",
      sqlState = "42604",
      parameters = Map(
        "direction" -> direction,
        "supported" -> AsOfJoinDirection.supported.mkString("'", "', '", "'")
      )
    )
  }

  test("as-of join - allowExactMatches = false") {
    val (df1, df2) = prepareForAsOfJoin()
    checkAnswer(
      df1.joinAsOf(df2, df1.col("a"), df2.col("a"), usingColumns = Seq.empty,
        joinType = "inner", tolerance = null, allowExactMatches = false, direction = "backward"),
      Seq(
        Row(5, "y", "b", 3, "x", 3),
        Row(10, "z", "c", 7, "z", 7)
      )
    )
  }

  test("as-of join - direction = \"forward\"") {
    val (df1, df2) = prepareForAsOfJoin()
    checkAnswer(
      df1.joinAsOf(df2, df1.col("a"), df2.col("a"), usingColumns = Seq.empty,
        joinType = "inner", tolerance = null, allowExactMatches = true, direction = "forward"),
      Seq(
        Row(1, "x", "a", 1, "v", 1),
        Row(5, "y", "b", 6, "y", 6)
      )
    )
  }

  test("as-of join - direction = \"nearest\"") {
    val (df1, df2) = prepareForAsOfJoin()
    checkAnswer(
      df1.joinAsOf(df2, df1.col("a"), df2.col("a"), usingColumns = Seq.empty,
        joinType = "inner", tolerance = null, allowExactMatches = true, direction = "nearest"),
      Seq(
        Row(1, "x", "a", 1, "v", 1),
        Row(5, "y", "b", 6, "y", 6),
        Row(10, "z", "c", 7, "z", 7)
      )
    )
  }

  test("as-of join - self") {
    val (df1, _) = prepareForAsOfJoin()
    checkAnswer(
      df1.joinAsOf(
        df1, df1.col("a"), df1.col("a"), usingColumns = Seq.empty,
        joinType = "left", tolerance = null, allowExactMatches = false, direction = "nearest"),
      Seq(
        Row(1, "x", "a", 5, "y", "b"),
        Row(5, "y", "b", 1, "x", "a"),
        Row(10, "z", "c", 5, "y", "b")
      )
    )
  }

  // Right key offsets from the left key (offset 0 is the left key) for each case. The tolerance is
  // 2, so offsets -2 and 2 sit exactly on the tolerance bound.
  private val toleranceCases: Seq[(String, Seq[Int])] = Seq(
    "on_bound_before" -> Seq(-2),
    "on_bound_after" -> Seq(2),
    "past_bound_before" -> Seq(-3),
    "past_bound_after" -> Seq(3),
    "exact_only" -> Seq(0),
    "exact_and_bounds" -> Seq(-2, 0, 1),
    "tie_on_bounds" -> Seq(-2, 2),
    "inside" -> Seq(-1, 2))

  // The right offset the join must pick, or None. Only the match depends on allowExactMatches.
  // The tolerance bound is always inclusive.
  private def expectedOffset(
      direction: String,
      allowExactMatches: Boolean,
      offsets: Seq[Int]): Option[Int] = {
    val matching = offsets.filter { r =>
      math.abs(r) <= 2 && (direction match {
        case "backward" => if (allowExactMatches) r <= 0 else r < 0
        case "forward" => if (allowExactMatches) r >= 0 else r > 0
        case _ => allowExactMatches || r != 0
      })
    }
    if (matching.isEmpty) {
      None
    } else {
      direction match {
        case "backward" => Some(matching.max)
        case "forward" => Some(matching.min)
        case _ => Some(matching.minBy(r => (math.abs(r), r)))
      }
    }
  }

  private def toleranceJoinResults(
      keyType: String,
      direction: String,
      allowExactMatches: Boolean,
      cases: Seq[(String, Seq[Int])]): DataFrame = {
    def key(offset: String): String = keyType match {
      case "INT" => s"10 + $offset"
      case "DATE" => s"date_add(DATE'2024-01-10', $offset)"
      case _ => s"TIMESTAMP'2024-01-10 00:00:00' + make_dt_interval($offset)"
    }
    val leftRows = cases.map(c => s"('${c._1}')").mkString(", ")
    val rightRows = cases.flatMap { case (id, offsets) =>
      offsets.map(o => s"('$id', $o)")
    }.mkString(", ")
    val left = spark.sql(
      s"SELECT case_id, ${key("0")} AS k FROM VALUES $leftRows AS t(case_id)")
    val right = spark.sql(
      s"SELECT case_id, ${key("ro")} AS k, ro FROM VALUES $rightRows AS t(case_id, ro)")
    val tolerance = if (keyType == "INT") lit(2) else lit(Duration.ofDays(2))
    left.joinAsOf(
      right, left.col("k"), right.col("k"), usingColumns = Seq("case_id"),
      joinType = "left", tolerance = tolerance, allowExactMatches = allowExactMatches,
      direction = direction
    ).select(left.col("case_id"), right.col("ro"))
  }

  for {
    keyType <- Seq("INT", "DATE", "TIMESTAMP")
    direction <- Seq("backward", "forward", "nearest")
  } {
    test(s"as-of join - tolerance bound is inclusive: $direction, $keyType keys") {
      for {
        sortMerge <- Seq(false, true)
        singlePass <- if (sortMerge) Seq(false, true) else Seq(false)
        ansi <- Seq(false, true)
        allowExactMatches <- Seq(false, true)
      } {
        withSQLConf(
          SQLConf.SORT_MERGE_AS_OF_JOIN_ENABLED.key -> sortMerge.toString,
          SQLConf.ANALYZER_SINGLE_PASS_RESOLVER_ENABLED.key -> singlePass.toString,
          SQLConf.ANALYZER_SINGLE_PASS_RESOLVER_ENABLED_TENTATIVELY.key -> "false",
          SQLConf.ANALYZER_DUAL_RUN_LEGACY_AND_SINGLE_PASS_RESOLVER.key -> "false",
          SQLConf.ANALYZER_SINGLE_PASS_RESOLVER_ENABLE_ASOF_JOIN_RESOLUTION.key -> "true",
          SQLConf.ANSI_ENABLED.key -> ansi.toString) {
          // The old rewrite picks an arbitrary row among nearest candidates at equal distance.
          val cases = toleranceCases.filter { c =>
            sortMerge || direction != "nearest" || c._1 != "tie_on_bounds"
          }
          val expected = cases.map { case (id, offsets) =>
            Row(id, expectedOffset(direction, allowExactMatches, offsets)
              .map(Int.box).orNull)
          }
          withClue(s"sortMerge=$sortMerge, singlePass=$singlePass, ansi=$ansi, " +
              s"allowExactMatches=$allowExactMatches: ") {
            val result = toleranceJoinResults(keyType, direction, allowExactMatches, cases)
            // checkAnswer re-analyzes the plan with an encoder, which the single-pass analyzer
            // does not support yet.
            val mismatch = QueryTest.sameRows(expected, result.collect().toSeq)
            assert(mismatch.isEmpty, mismatch.getOrElse(""))
          }
        }
      }
    }
  }

  test("SPARK-62239: as-of join - decorrelateInnerQuery disabled") {
    val (df1, df2) = prepareForAsOfJoin()
    val left = df1.withColumn("m", map(lit("k"), col("a")))
    // With DecorrelateInnerQuery disabled, the first join used to fail with INTERNAL_ERROR (an
    // outer reference was left in the MIN_BY order expression), and the second used to return
    // every right row with `right_val <= m['k']` instead of the nearest one. The second also
    // needs the map member pulled out of the subquery before decorrelation.
    Seq((left.col("a"), null), (lit(10), left.col("m")("k") >= df2.col("right_val"))).foreach {
      case (leftAsOf, joinExprs) =>
        def asOfJoin(): DataFrame = left.joinAsOf(df2, leftAsOf, df2.col("a"), joinExprs,
          joinType = "left", tolerance = null, allowExactMatches = true, direction = "backward")
        val expected = asOfJoin()
        val expectedPlan = expected.queryExecution.optimizedPlan
        val expectedAnswer = expected.collect().toSeq
        // The nearest right `a` of each left row.
        assert(expectedAnswer.map(_.getInt(4)).sorted == Seq(1, 3, 7))
        withSQLConf(SQLConf.DECORRELATE_INNER_QUERY_ENABLED.key -> "false") {
          val df = asOfJoin()
          comparePlans(df.queryExecution.optimizedPlan, expectedPlan)
          checkAnswer(df, expectedAnswer)
        }
    }
  }
}
